#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Entry point for the persistent flow backend.

CLI
---
    python3 flow_backend.py init  --db flow.db --flow '["计时",5]'
    python3 flow_backend.py query --db flow.db
    python3 flow_backend.py submit --db flow.db --command '["c1","推进",5]'
    python3 flow_backend.py serve --db flow.db --port 8080

`--flow` / `--command` accept the JSON text directly, `@path` reads it from a
file, and `-` reads it from stdin.

HTTP
----
    POST /init     body: {"flow": E}  -> initial/current query object
    POST /query                      -> query object
    GET  /query                      -> query object
    POST /command  body: [id, op, ...] -> receipt (post-command query object)
    GET  /health                     -> {"ok": true, "db": "..."}

Rejections use HTTP 400 (409 only for a conflicting re-initialisation) with:

    {"error": {"kind": "syntax" | "reject" | "structure" | "conflict" | "state",
               "message": "...", "path": "[$][0]...", "path_indices": [...]}}

`flow_schema.validate_command` is used before anything touches SQLite, so a
rejected command cannot change persisted state.
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import sqlite3
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Optional
from urllib.parse import urlsplit

from flow_schema import (
    FlowError,
    canonical_json,
    decode_json,
    format_path,
    validate_command,
    validate_flow,
)
from flow_store import Store


# ---------------------------------------------------------------------------
# Shared operations (used by CLI and HTTP)
# ---------------------------------------------------------------------------

def op_init(db: str, flow_text: Any) -> Any:
    flow = decode_json(flow_text)
    validate_flow(flow)
    store = Store(db, require_initialised=False)
    try:
        return store.initialise(flow)
    finally:
        store.close()


def op_query(db: str) -> Any:
    store = Store(db, ensure_schema=False)
    try:
        return store.query()
    finally:
        store.close()


def op_submit(db: str, command_text: Any) -> Any:
    command = decode_json(command_text)
    validate_command(command)
    store = Store(db)
    try:
        return store.commit_command(command)
    finally:
        store.close()


def ok_body(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=False)


def error_body(exc: FlowError) -> str:
    payload = exc.to_dict()
    payload["error"]["path"] = format_path(exc.path)
    payload["error"]["path_indices"] = exc.path
    return json.dumps(payload, ensure_ascii=False)


# ---------------------------------------------------------------------------
# HTTP server
# ---------------------------------------------------------------------------

class Handler(BaseHTTPRequestHandler):
    server_version = "flow-backend/1.0"
    protocol_version = "HTTP/1.1"

    # set by the factory
    db_path: str = ""

    def log_message(self, fmt: str, *args: Any) -> None:  # quiet by default
        if os.environ.get("FLOW_HTTP_LOG"):
            super().log_message(fmt, *args)

    # -- helpers -------------------------------------------------------

    def _send(self, status: int, body: Any) -> None:
        if isinstance(body, (dict, list)):
            data = ok_body(body).encode("utf-8")
        else:
            data = str(body).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _read_body(self) -> bytes:
        length = int(self.headers.get("Content-Length") or 0)
        if length > 16 * 1024 * 1024:
            raise FlowError("request body too large", [], kind="reject")
        return self.rfile.read(length) if length else b""

    def _route(self, method: str) -> None:
        path = urlsplit(self.path).path.rstrip("/") or "/"
        try:
            if method == "GET" and path in ("/", "/health"):
                if path == "/health":
                    return self._send(200, {"ok": True, "db": os.path.abspath(self.db_path)})
                return self._send(200, {"service": "flow-backend", "endpoints": [
                    "POST /init", "POST|GET /query", "POST /command"]})
            if method == "GET" and path == "/query":
                return self._send(200, op_query(self.db_path))
            if method == "POST" and path == "/init":
                body = self._read_body()
                payload = decode_json(body)
                if not isinstance(payload, dict) or "flow" not in payload:
                    raise FlowError('init body must be {"flow": E}', [], kind="structure")
                return self._send(200, op_init(self.db_path, canonical_json(payload["flow"])))
            if method == "POST" and path in ("/query", "/submit"):
                return self._send(200, op_query(self.db_path))
            if method == "POST" and path == "/command":
                return self._send(200, op_submit(self.db_path, self._read_body()))
            self._send(404, {"error": {"kind": "not_found", "message": "no such endpoint"}})
        except FlowError as exc:
            status = 409 if exc.kind == "conflict" else 400
            self._send(status, json.loads(error_body(exc)))
        except Exception as exc:  # pragma: no cover - defensive
            self._send(500, {"error": {"kind": "internal", "message": str(exc)}})

    def do_GET(self) -> None:  # noqa: N802
        self._route("GET")

    def do_POST(self) -> None:  # noqa: N802
        self._route("POST")


def serve(db: str, host: str, port: int) -> None:
    # Ensure the db exists and is initialised; fail fast if not.
    Store(db, require_initialised=True).conn.close()
    handler = type("BoundHandler", (Handler,), {"db_path": db})
    httpd = ThreadingHTTPServer((host, port), handler)
    httpd.daemon_threads = True

    def stop(_signum: int, _frame: Any) -> None:
        threading.Thread(target=httpd.shutdown, daemon=True).start()

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    print("flow backend listening on http://%s:%d (db=%s)"
          % (host, port, os.path.abspath(db)), flush=True)
    try:
        httpd.serve_forever()
    finally:
        httpd.server_close()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _read_arg(value: Optional[str]) -> Any:
    if value is None:
        raise SystemExit("missing JSON argument")
    if value == "-":
        return sys.stdin.buffer.read()
    if value.startswith("@"):
        with open(value[1:], "rb") as handle:
            return handle.read()
    return value


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Persistent callback/race flow backend (Python + SQLite)."
    )
    sub = parser.add_subparsers(dest="mode", required=True)

    init = sub.add_parser("init", help="initialise a fresh database with flow E")
    init.add_argument("--db", required=True)
    init.add_argument("--flow", required=True, help="flow JSON, @file or -")

    query = sub.add_parser("query", help="print the current query object")
    query.add_argument("--db", required=True)

    submit = sub.add_parser("submit", help="submit one command and print its receipt")
    submit.add_argument("--db", required=True)
    submit.add_argument("--command", required=True, help="command JSON, @file or -")

    server = sub.add_parser("serve", help="run the HTTP API")
    server.add_argument("--db", required=True)
    server.add_argument("--host", default="127.0.0.1")
    server.add_argument("--port", type=int, default=8080)

    return parser


def main(argv: Optional[list] = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.mode == "init":
            print(ok_body(op_init(args.db, _read_arg(args.flow))))
        elif args.mode == "query":
            print(ok_body(op_query(args.db)))
        elif args.mode == "submit":
            print(ok_body(op_submit(args.db, _read_arg(args.command))))
        elif args.mode == "serve":
            serve(args.db, args.host, args.port)
            return 0
        return 0
    except FlowError as exc:
        sys.stderr.write(error_body(exc) + "\n")
        return 2
    except (FileNotFoundError, sqlite3.Error) as exc:
        sys.stderr.write(json.dumps(
            {"error": {"kind": "state", "message": str(exc), "path": "$"}},
            ensure_ascii=False) + "\n")
        return 2


if __name__ == "__main__":
    sys.exit(main())
