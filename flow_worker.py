#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Independent side-effect worker for 采集 leaves.

The worker is a separate process.  It watches the persisted effect ledger in
the flow database and performs the corresponding HTTP calls against the
dedicated mock target:

* pending create intents -> ``POST {target}/create`` with ``[address, data]``
* pending cancel duties  -> ``POST {target}/cancel`` with ``address``

Both target operations are idempotent, so every transport failure (timeout,
reset, accept-then-disconnect) is retried until a definitive answer arrives:

* ``/create`` 200 records the answer and settles it in the same SQLite
  transaction — serialised with commands, at the current logical clock.
  ``/create`` 409 means the key was banned by a cancel; the create is
  abandoned and the cancel duty owns the cleanup.
* ``/cancel`` 200 confirms the duty, shrinking ``GET /effects``'s ``pending``.

All local state changes go through ``flow_store.Store`` transactions, so a
hard kill at any point (including after the target accepted but before the
answer was recorded locally) simply resumes from the persisted ledger on the
next start.
"""

from __future__ import annotations

import http.client
import json
import signal
import sys
import threading
import urllib.error
import urllib.request
from typing import Any, Dict, List, Optional, Tuple

from flow_schema import canonical_json
from flow_store import Store

# Timeouts, resets, dropped connections, DNS failures, ...
_TRANSPORT_ERRORS = (OSError, http.client.HTTPException)


class EffectWorker:
    """Scans the ledger and drives one retrying thread per pending effect."""

    def __init__(self, db: str, target: str, timeout: float = 1.0,
                 retry: float = 0.2):
        self.store = Store(db)
        self.target = target.rstrip("/")
        self.timeout = timeout
        self.retry = retry
        self.stop_event = threading.Event()
        self._threads: Dict[Tuple[str, str], threading.Thread] = {}
        self._lock = threading.Lock()

    def close(self) -> None:
        self.stop_event.set()
        self.store.close()

    # -- manager ---------------------------------------------------------

    def scan(self) -> None:
        """Spawn a retry thread for every pending effect without one."""
        creates, cancels = self.store.pending_effects()
        with self._lock:
            self._threads = {
                ident: thread for ident, thread in self._threads.items()
                if thread.is_alive()
            }
            for op in creates:
                self._spawn_locked("create", op["address"], op["data"])
            for address in cancels:
                self._spawn_locked("cancel", address, None)

    def _spawn_locked(self, kind: str, address: List[int],
                      data: Optional[str]) -> None:
        ident = (kind, canonical_json(address))
        if ident in self._threads:
            return
        thread = threading.Thread(
            target=self._run,
            args=(kind, list(address), data),
            daemon=True,
            name="effect-%s-%s" % ident,
        )
        self._threads[ident] = thread
        thread.start()

    def run_once(self) -> None:
        """Single synchronous pass: one attempt per pending effect."""
        creates, cancels = self.store.pending_effects()
        for op in creates:
            self._safe_attempt("create", op["address"], op["data"])
        for address in cancels:
            self._safe_attempt("cancel", address, None)

    # -- per-effect retry loop --------------------------------------------

    def _run(self, kind: str, address: List[int], data: Optional[str]) -> None:
        while not self.stop_event.is_set():
            if self._safe_attempt(kind, address, data):
                return
            self.stop_event.wait(self.retry)

    def _safe_attempt(self, kind: str, address: List[int],
                      data: Optional[str]) -> bool:
        try:
            return self._attempt(kind, address, data)
        except _TRANSPORT_ERRORS:
            return False  # transport failure: retry
        except Exception as exc:  # defensive: keep the worker alive
            sys.stderr.write(
                "worker: %s %s attempt failed: %r\n" % (kind, address, exc))
            return False

    def _attempt(self, kind: str, address: List[int],
                 data: Optional[str]) -> bool:
        """One attempt; True when the effect is finished (or obsolete)."""
        if kind == "create":
            if not self.store.create_pending(address):
                return True  # answered already, or the leaf lost its race
            status, body = self._post("/create", [address, data])
            if status == 200:
                try:
                    payload = json.loads(body)
                except ValueError:
                    return False
                if not isinstance(payload, str):
                    return False
                self.store.record_create_response(address, payload)
                return True
            # 409: banned by a cancel — the cancel duty owns the cleanup.
            return status == 409
        if not self.store.cancel_pending(address):
            return True
        status, _ = self._post("/cancel", address)
        if status == 200:
            self.store.record_cancel_confirmation(address)
            return True
        return False

    def _post(self, path: str, payload: Any) -> Tuple[int, str]:
        request = urllib.request.Request(
            self.target + path,
            data=canonical_json(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                return response.status, response.read().decode("utf-8")
        except urllib.error.HTTPError as exc:
            return exc.code, exc.read().decode("utf-8", "replace")


def run_worker(db: str, target: str, interval: float = 0.1,
               timeout: float = 1.0, retry: float = 0.2,
               once: bool = False) -> None:
    """Run the worker loop until SIGTERM/SIGINT (or a single pass if once)."""
    worker = EffectWorker(db, target, timeout=timeout, retry=retry)
    if once:
        try:
            worker.run_once()
        finally:
            worker.close()
        return

    def handle(_signum: int, _frame: Any) -> None:
        worker.stop_event.set()

    signal.signal(signal.SIGTERM, handle)
    signal.signal(signal.SIGINT, handle)
    print("flow worker running (db=%s, target=%s)" % (db, target), flush=True)
    try:
        while not worker.stop_event.is_set():
            worker.scan()
            worker.stop_event.wait(interval)
    finally:
        worker.close()
