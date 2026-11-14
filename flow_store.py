#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""SQLite persistence for the flow backend.

Layout (SQLite):

    meta(key, value)     'flow' (canonical initial flow), 'schema'
    engine(id=1, revision, clock, state_json)
    receipts(cmd_id, command, receipt)   one row per accepted command id

Durability and atomicity:

* ``journal_mode=WAL`` + ``synchronous=FULL``; every mutation runs inside one
  ``BEGIN IMMEDIATE`` transaction.  A commit is all-or-nothing, so a hard kill
  leaves either the complete old state or the complete new state -- never a
  half-applied command, a receipt without its side effects, or a consumed
  signal without its receipt.
* Engine writes are guarded by ``WHERE revision = ?`` so a concurrent or
  duplicate application cannot double-apply.
* One ``RLock`` serialises commands inside the process; SQLite locking plus
  the revision guard extend that to other processes.
* 采集 side effects live inside the same state JSON: activation intents,
  their answers, cancellation duties and their confirmations all commit in
  the same transaction as the settlement that produced them, so a restart
  simply resumes from the persisted ledger.

Idempotency:

* Command ids are global.  A retry with the same content returns the stored
  receipt (post-command query object) without re-applying or re-settling.
  A retry with different content is rejected at path [0].
* A 推进 retry is equivalent when it targets the same clock value; the stored
  receipt is returned.
"""

from __future__ import annotations

import json
import sqlite3
import threading
from typing import Any, Dict, List, Tuple

from flow_engine import (
    backlog_ids,
    build_tree,
    clone,
    effects_of,
    node_at_address,
    normalize,
    pending_cancellations,
    pending_create_ops,
    result_of,
    settle,
    status_of,
)
from flow_schema import FlowError, ST_ACTIVE, canonical_json

SCHEMA_STATEMENTS = [
    """CREATE TABLE IF NOT EXISTS meta (
        key   TEXT PRIMARY KEY,
        value TEXT NOT NULL
    )""",
    """CREATE TABLE IF NOT EXISTS engine (
        id          INTEGER PRIMARY KEY CHECK (id = 1),
        revision    INTEGER NOT NULL,
        clock       INTEGER NOT NULL,
        state_json  TEXT NOT NULL
    )""",
    """CREATE TABLE IF NOT EXISTS receipts (
        cmd_id   TEXT PRIMARY KEY,
        command  TEXT NOT NULL,
        receipt  TEXT NOT NULL
    )""",
]


class _Txn:
    """Re-entrant ``BEGIN IMMEDIATE`` transaction with a process lock."""

    def __init__(self, store: "Store"):
        self.store = store
        self.depth = 0

    def __enter__(self) -> "_Txn":
        self.store._lock.acquire()
        if self.depth == 0:
            self.store.conn.execute("BEGIN IMMEDIATE")
        self.depth += 1
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        self.depth -= 1
        try:
            if self.depth == 0:
                self.store.conn.execute("COMMIT" if exc_type is None else "ROLLBACK")
        finally:
            self.store._lock.release()
        return False


class Store:
    """A single-open SQLite database holding the flow state."""

    def __init__(self, path: str, require_initialised: bool = True,
                 ensure_schema: bool = True):
        self.path = path
        self._lock = threading.RLock()
        self.conn = sqlite3.connect(
            path,
            timeout=30.0,
            isolation_level=None,
            check_same_thread=False,
        )
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=FULL")
        self.conn.execute("PRAGMA busy_timeout=30000")
        if ensure_schema:
            with _Txn(self):
                for statement in SCHEMA_STATEMENTS:
                    self.conn.execute(statement)
        if require_initialised and not self._is_initialised():
            self.conn.close()
            raise FlowError(
                "database %r is not initialised; run `flow_backend.py init`"
                % path,
                [],
                kind="state",
            )

    def _is_initialised(self) -> bool:
        row = self.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='engine'"
        ).fetchone()
        if row is None:
            return False
        row = self.conn.execute("SELECT COUNT(*) AS n FROM engine").fetchone()
        return row["n"] == 1

    # -- low level -----------------------------------------------------

    def _txn(self) -> _Txn:
        return _Txn(self)

    def _load(self) -> Tuple[int, Dict[str, Any]]:
        row = self.conn.execute(
            "SELECT revision, state_json FROM engine WHERE id = 1"
        ).fetchone()
        if row is None:
            raise FlowError("database is not initialised", [], kind="state")
        return row["revision"], normalize(json.loads(row["state_json"]))

    def _save(self, revision: int, state: Dict[str, Any]) -> None:
        self.conn.execute(
            "UPDATE engine SET revision = ?, clock = ?, state_json = ? "
            "WHERE id = 1 AND revision = ?",
            (revision + 1, state["clock"], canonical_json(state), revision),
        )

    # -- views ---------------------------------------------------------

    @staticmethod
    def view(state: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "clock": state["clock"],
            "status": status_of(state),
            "result": result_of(state),
            "backlog": backlog_ids(state),
        }

    def query(self) -> Dict[str, Any]:
        with self._lock:
            _, state = self._load()
            return self.view(state)

    # -- side-effect ledger (采集) --------------------------------------

    def effects_view(self) -> Dict[str, Any]:
        """The /effects object: addresses with unconfirmed cancellations."""
        with self._lock:
            _, state = self._load()
            return {"pending": pending_cancellations(state)}

    def pending_effects(self) -> Tuple[List[Dict[str, Any]], List[List[int]]]:
        """(create operations, cancel addresses) the worker should act on."""
        with self._lock:
            _, state = self._load()
            return pending_create_ops(state), pending_cancellations(state)

    def create_pending(self, address: List[int]) -> bool:
        """Whether a /create attempt for `address` is still meaningful."""
        key = canonical_json(address)
        with self._lock:
            _, state = self._load()
            intent = effects_of(state)["creates"].get(key)
            if intent is None or intent["response"] is not None:
                return False
            leaf = node_at_address(state, intent["address"])
            return leaf is not None and leaf["status"] == ST_ACTIVE

    def cancel_pending(self, address: List[int]) -> bool:
        """Whether the cancellation duty for `address` is unconfirmed."""
        key = canonical_json(address)
        with self._lock:
            _, state = self._load()
            duty = effects_of(state)["cancels"].get(key)
            return duty is not None and not duty["confirmed"]

    def record_create_response(self, address: List[int], data: str) -> bool:
        """Persist a /create 200 answer and settle it in one transaction.

        Serialised with commands exactly like another command: a still-active
        leaf becomes ready at the current logical clock and settlement runs
        to a fixpoint.  A late answer for a leaf that is no longer active is
        recorded but changes nothing.  Returns False when the answer was
        already recorded (idempotent retry).
        """
        key = canonical_json(address)
        with self._txn():
            revision, state = self._load()
            intent = effects_of(state)["creates"].get(key)
            if intent is None or intent["response"] is not None:
                return False
            intent["response"] = data
            settle(state)
            self._save(revision, state)
            return True

    def record_cancel_confirmation(self, address: List[int]) -> bool:
        """Persist a /cancel 200 answer; False when already confirmed."""
        key = canonical_json(address)
        with self._txn():
            revision, state = self._load()
            duty = effects_of(state)["cancels"].get(key)
            if duty is None or duty["confirmed"]:
                return False
            duty["confirmed"] = True
            self._save(revision, state)
            return True

    # -- init ----------------------------------------------------------

    def initialise(self, flow: Any) -> Dict[str, Any]:
        """Initialise exactly once with clock 0 and flow E.

        Re-initialising with an identical flow is idempotent and returns the
        current state.  A different flow is rejected without changing state.
        """
        flow_json = canonical_json(flow)
        with self._txn():
            row = self.conn.execute("SELECT COUNT(*) AS n FROM engine").fetchone()
            if row["n"] == 1:
                stored = self.conn.execute(
                    "SELECT value FROM meta WHERE key = 'flow'"
                ).fetchone()
                _, state = self._load()
                if stored is not None and _same_flow(
                    json.loads(stored["value"]), flow
                ):
                    return self.view(state)
                raise FlowError(
                    "database already initialised with a different flow",
                    [],
                    kind="conflict",
                )
            state = build_tree(flow)
            settle(state)
            self.conn.execute(
                "INSERT INTO engine (id, revision, clock, state_json) "
                "VALUES (1, 0, ?, ?)",
                (state["clock"], canonical_json(state)),
            )
            self.conn.execute(
                "INSERT OR REPLACE INTO meta (key, value) VALUES ('flow', ?)",
                (flow_json,),
            )
            self.conn.execute(
                "INSERT OR REPLACE INTO meta (key, value) VALUES ('schema', '1')"
            )
            return self.view(state)

    # -- commands ------------------------------------------------------

    def commit_command(self, command: List[Any]) -> Dict[str, Any]:
        """Atomically apply one validated command; return its receipt."""
        cmd_id = command[0]
        canonical = canonical_json(command)
        with self._txn():
            revision, state = self._load()
            stored = self.conn.execute(
                "SELECT command, receipt FROM receipts WHERE cmd_id = ?",
                (cmd_id,),
            ).fetchone()
            if stored is not None:
                previous = json.loads(stored["command"])
                if _same_command(previous, command):
                    return json.loads(stored["receipt"])
                raise FlowError(
                    "command id %r was already used with different content"
                    % (cmd_id,),
                    [0],
                )

            new_state = clone(state)
            _apply(new_state, command)
            settle(new_state, limit=command[2] if command[1] == "推进" else None)
            receipt = self.view(new_state)
            self._save(revision, new_state)
            self.conn.execute(
                "INSERT INTO receipts (cmd_id, command, receipt) VALUES (?, ?, ?)",
                (cmd_id, canonical, canonical_json(receipt)),
            )
            return receipt

    def close(self) -> None:
        self.conn.close()


# ---------------------------------------------------------------------------
# Command application (pure, on a cloned state)
# ---------------------------------------------------------------------------

def _apply(state: Dict[str, Any], command: List[Any]) -> None:
    if command[1] == "送达":
        state["backlog"].append(
            {"id": command[0], "name": command[2], "data": command[3]}
        )
        return
    target = command[2]
    if target < state["clock"]:
        raise FlowError(
            "clock cannot move backwards (clock=%d, target=%d)"
            % (state["clock"], target),
            [2],
        )
    # The clock jump itself is performed by settle(limit=target).


def _same_flow(previous: Any, current: Any) -> bool:
    return previous == current


def _same_command(previous: Any, current: List[Any]) -> bool:
    """Whether a retried command is the same as the stored one.

    For 送达 the full command (id, name, data) must match.  For 推进 only the
    target clock matters; replaying it is a no-op and the original receipt is
    returned.
    """
    if not isinstance(previous, list) or not isinstance(current, list):
        return False
    if previous[1:2] != current[1:2]:
        return False
    if previous[1] == "推进":
        return len(previous) == 3 and len(current) == 3 and previous[2] == current[2]
    return previous == current


def initial_view(flow: Any) -> Dict[str, Any]:
    state = build_tree(flow)
    settle(state)
    return Store.view(state)
