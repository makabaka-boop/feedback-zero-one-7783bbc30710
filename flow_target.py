#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Dedicated persistent mock target for 采集 side effects.

    POST /create   body [key, data] -> 200 with the JSON string data
                                       (idempotent); 409 once key is banned
    POST /cancel   body key         -> 200 null (idempotent); deletes the
                                       artifact and permanently bans the key
    GET  /state                     -> {"artifacts": {key: data},
                                        "banned": [key, ...],
                                        "stats": {"create": n, "cancel": n}}
    GET  /control                   -> current fault-injection settings
    POST /control  {"create": mode, "cancel": mode, "hang": seconds}
    GET  /health                    -> {"ok": true}

`key` is the zero-based node address array (e.g. [0, 2]); it is stored in
canonical JSON form.  Fault modes per operation: "normal" responds
immediately, "hang" accepts (applies the state change) and withholds the
response for `--hang` seconds, "disconnect" accepts and then drops the
connection without responding.  Artifacts and bans live in SQLite
(WAL + FULL sync), so a target restart keeps them.
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import socket
import sqlite3
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlsplit

from flow_schema import canonical_json

MODES = ("normal", "hang", "disconnect")


# ---------------------------------------------------------------------------
# Persistent artifact/ban ledger
# ---------------------------------------------------------------------------

class TargetStore:
    def __init__(self, path: str):
        self.path = path
        self.lock = threading.RLock()
        self.conn = sqlite3.connect(
            path, timeout=30.0, isolation_level=None, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=FULL")
        self.conn.execute("PRAGMA busy_timeout=30000")
        with self.lock:
            self.conn.execute("BEGIN IMMEDIATE")
            try:
                self.conn.execute(
                    "CREATE TABLE IF NOT EXISTS artifacts ("
                    "key TEXT PRIMARY KEY, data TEXT NOT NULL)")
                self.conn.execute(
                    "CREATE TABLE IF NOT EXISTS banned (key TEXT PRIMARY KEY)")
                self.conn.execute("COMMIT")
            except Exception:
                self.conn.execute("ROLLBACK")
                raise

    def create(self, key: str, data: str) -> Tuple[str, Optional[str]]:
        """Idempotent create; returns ("ok", effective data) or ("banned", None)."""
        with self.lock:
            self.conn.execute("BEGIN IMMEDIATE")
            try:
                banned = self.conn.execute(
                    "SELECT 1 FROM banned WHERE key = ?", (key,)).fetchone()
                if banned is not None:
                    self.conn.execute("COMMIT")
                    return "banned", None
                row = self.conn.execute(
                    "SELECT data FROM artifacts WHERE key = ?", (key,)).fetchone()
                if row is not None:
                    self.conn.execute("COMMIT")
                    return "ok", row["data"]
                self.conn.execute(
                    "INSERT INTO artifacts (key, data) VALUES (?, ?)", (key, data))
                self.conn.execute("COMMIT")
                return "ok", data
            except Exception:
                self.conn.execute("ROLLBACK")
                raise

    def cancel(self, key: str) -> None:
        """Idempotent delete + permanent ban."""
        with self.lock:
            self.conn.execute("BEGIN IMMEDIATE")
            try:
                self.conn.execute("DELETE FROM artifacts WHERE key = ?", (key,))
                self.conn.execute(
                    "INSERT OR IGNORE INTO banned (key) VALUES (?)", (key,))
                self.conn.execute("COMMIT")
            except Exception:
                self.conn.execute("ROLLBACK")
                raise

    def snapshot(self) -> Tuple[Dict[str, str], List[str]]:
        with self.lock:
            artifacts = {
                row["key"]: row["data"]
                for row in self.conn.execute(
                    "SELECT key, data FROM artifacts ORDER BY key")
            }
            banned = [
                row["key"]
                for row in self.conn.execute("SELECT key FROM banned ORDER BY key")
            ]
        return artifacts, banned

    def close(self) -> None:
        self.conn.close()


# ---------------------------------------------------------------------------
# HTTP server
# ---------------------------------------------------------------------------

def _is_key(value: Any) -> bool:
    return (
        isinstance(value, list)
        and all(
            isinstance(item, int) and not isinstance(item, bool) and item >= 0
            for item in value
        )
    )


class TargetHandler(BaseHTTPRequestHandler):
    server_version = "flow-target/1.0"
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt: str, *args: Any) -> None:  # quiet by default
        if os.environ.get("FLOW_TARGET_LOG"):
            super().log_message(fmt, *args)

    # -- helpers -------------------------------------------------------

    def _send(self, status: int, body: Any) -> None:
        if isinstance(body, str):
            data = body.encode("utf-8")
        else:
            data = json.dumps(body, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _error(self, status: int, message: str) -> None:
        self._send(status, {"error": {"kind": "reject", "message": message}})

    def _read_json(self) -> Any:
        length = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(length) if length else b""
        try:
            return json.loads(raw.decode("utf-8"))
        except (ValueError, UnicodeDecodeError):
            raise _BadRequest("request body must be valid JSON")

    def _fault(self, op: str) -> bool:
        """Apply the configured fault for `op` *after* the state change was
        accepted; True when the connection was dropped and no response must
        be sent."""
        mode = self.server.control.get(op, "normal")
        if mode == "disconnect":
            self.close_connection = True
            try:
                self.connection.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            try:
                self.connection.close()
            except OSError:
                pass
            return True
        if mode == "hang":
            time.sleep(self.server.hang_seconds)
        return False

    # -- routes ----------------------------------------------------------

    def do_GET(self) -> None:  # noqa: N802
        path = urlsplit(self.path).path.rstrip("/") or "/"
        if path == "/health":
            return self._send(200, {"ok": True})
        if path == "/state":
            artifacts, banned = self.server.store.snapshot()
            with self.server.stats_lock:
                stats = dict(self.server.stats)
            return self._send(200, {
                "artifacts": artifacts, "banned": banned, "stats": stats})
        if path == "/control":
            return self._send(200, self._control_view())
        self._error(404, "no such endpoint")

    def do_POST(self) -> None:  # noqa: N802
        path = urlsplit(self.path).path.rstrip("/") or "/"
        try:
            if path == "/create":
                return self._handle_create()
            if path == "/cancel":
                return self._handle_cancel()
            if path == "/control":
                return self._handle_control()
            self._error(404, "no such endpoint")
        except _BadRequest as exc:
            self._error(400, str(exc))

    def _handle_create(self) -> None:
        payload = self._read_json()
        if not (isinstance(payload, list) and len(payload) == 2):
            raise _BadRequest("create body must be [key, data]")
        key, data = payload
        if not _is_key(key):
            raise _BadRequest("key must be an array of non-negative integers")
        if not isinstance(data, str):
            raise _BadRequest("data must be a JSON string")
        with self.server.stats_lock:
            self.server.stats["create"] += 1
        outcome, effective = self.server.store.create(canonical_json(key), data)
        if self._fault("create"):
            return
        if outcome == "banned":
            return self._error(
                409, "key %s is permanently banned" % canonical_json(key))
        self._send(200, json.dumps(effective, ensure_ascii=False))

    def _handle_cancel(self) -> None:
        payload = self._read_json()
        if not _is_key(payload):
            raise _BadRequest("cancel body must be a key array")
        with self.server.stats_lock:
            self.server.stats["cancel"] += 1
        self.server.store.cancel(canonical_json(payload))
        if self._fault("cancel"):
            return
        self._send(200, "null")

    def _handle_control(self) -> None:
        payload = self._read_json()
        if not isinstance(payload, dict):
            raise _BadRequest("control body must be an object")
        for op in ("create", "cancel"):
            if op in payload:
                if payload[op] not in MODES:
                    raise _BadRequest(
                        "%s mode must be one of %s" % (op, ", ".join(MODES)))
                self.server.control[op] = payload[op]
        if "hang" in payload:
            hang = payload["hang"]
            if (not isinstance(hang, (int, float)) or isinstance(hang, bool)
                    or hang < 0):
                raise _BadRequest("hang must be a non-negative number")
            self.server.hang_seconds = float(hang)
        self._send(200, self._control_view())

    def _control_view(self) -> Dict[str, Any]:
        return {
            "create": self.server.control.get("create", "normal"),
            "cancel": self.server.control.get("cancel", "normal"),
            "hang": self.server.hang_seconds,
        }


class _BadRequest(Exception):
    pass


class TargetServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, address: Tuple[str, int], store: TargetStore,
                 hang_seconds: float):
        super().__init__(address, TargetHandler)
        self.store = store
        self.hang_seconds = hang_seconds
        self.control: Dict[str, str] = {"create": "normal", "cancel": "normal"}
        self.stats: Dict[str, int] = {"create": 0, "cancel": 0}
        self.stats_lock = threading.Lock()

    def handle_error(self, request: Any, client_address: Any) -> None:
        # Dropped connections are an intended fault mode; stay quiet.
        if os.environ.get("FLOW_TARGET_LOG"):
            super().handle_error(request, client_address)


def serve(db: str, host: str, port: int, hang: float) -> None:
    store = TargetStore(db)
    httpd = TargetServer((host, port), store, hang)

    def stop(_signum: int, _frame: Any) -> None:
        threading.Thread(target=httpd.shutdown, daemon=True).start()

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    print("flow target listening on http://%s:%d (db=%s)"
          % (host, port, os.path.abspath(db)), flush=True)
    try:
        httpd.serve_forever()
    finally:
        httpd.server_close()
        store.close()


def main(argv: Optional[list] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Persistent mock target for 采集 side effects.")
    sub = parser.add_subparsers(dest="mode", required=True)
    server = sub.add_parser("serve", help="run the mock target HTTP API")
    server.add_argument("--db", required=True)
    server.add_argument("--host", default="127.0.0.1")
    server.add_argument("--port", type=int, default=8090)
    server.add_argument("--hang", type=float, default=30.0,
                        help="seconds a 'hang'-mode answer is withheld")
    args = parser.parse_args(argv)
    serve(args.db, args.host, args.port, args.hang)
    return 0


if __name__ == "__main__":
    sys.exit(main())
