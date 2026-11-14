#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Acceptance tests for the flow backend.

Run: python3 tests/acceptance.py
Everything is end-to-end through the CLI subprocess and the HTTP server;
databases live in a temporary directory that is removed afterwards.
"""

from __future__ import annotations

import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
import unittest
import urllib.error
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
ENTRY = os.path.join(ROOT, "flow_backend.py")

sys.path.insert(0, ROOT)
import flow_engine  # noqa: E402
import flow_schema  # noqa: E402
import flow_store  # noqa: E402


def cli(*args: str, stdin: str = "") -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, ENTRY, *args],
        input=stdin,
        capture_output=True,
        text=True,
        cwd=ROOT,
        timeout=30,
    )


class Base(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.mkdtemp(prefix="flowtest-")
        self.db = os.path.join(self.tmp, "state.db")

    def tearDown(self) -> None:
        shutil.rmtree(self.tmp, ignore_errors=True)

    def init(self, flow) -> dict:
        proc = cli("init", "--db", self.db, "--flow", json.dumps(flow, ensure_ascii=False))
        self.assertEqual(proc.returncode, 0, proc.stderr)
        return json.loads(proc.stdout)

    def submit(self, command, expect_ok: bool = True) -> dict:
        proc = cli("submit", "--db", self.db,
                   "--command", json.dumps(command, ensure_ascii=False))
        if expect_ok:
            self.assertEqual(proc.returncode, 0, proc.stderr)
            return json.loads(proc.stdout)
        self.assertNotEqual(proc.returncode, 0)
        return json.loads(proc.stderr)

    def query(self) -> dict:
        proc = cli("query", "--db", self.db)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        return json.loads(proc.stdout)


# ---------------------------------------------------------------------------
# Engine unit tests (fast, no persistence)
# ---------------------------------------------------------------------------

class EngineTests(unittest.TestCase):
    def run_flow(self, flow, commands):
        state = flow_engine.build_tree(flow)
        flow_engine.settle(state)
        for command in commands:
            flow_store._apply(state, command)
            limit = command[2] if command[1] == "推进" else None
            flow_engine.settle(state, limit=limit)
        return flow_store.Store.view(state)

    def test_spec_example_two_timers(self):
        flow = ["串行", ["计时", 5], ["计时", 5]]
        out = self.run_flow(flow, [["a", "推进", 10]])
        self.assertEqual(out, {"clock": 10, "status": "完成",
                               "result": [None, None], "backlog": []})
        out = self.run_flow(flow, [["a", "推进", 4]])
        self.assertEqual(out["status"], "运行")
        self.assertEqual(out["clock"], 4)

    def test_serial_activation_is_staggered(self):
        # First timer runs 0..5, second 5..10: reaching 5 must not finish it.
        flow = ["串行", ["计时", 5], ["计时", 5]]
        out = self.run_flow(flow, [["a", "推进", 5]])
        self.assertEqual(out["status"], "运行")
        self.assertEqual(out["clock"], 5)
        out = self.run_flow(flow, [["a", "推进", 5], ["b", "推进", 10]])
        self.assertEqual(out["result"], [None, None])

    def test_parallel_zero_timers(self):
        out = self.run_flow(["并行", ["计时", 0], ["计时", 0]], [])
        self.assertEqual(out["status"], "完成")
        self.assertEqual(out["result"], [None, None])

    def test_zero_timer_tie(self):
        out = self.run_flow(["并行", ["计时", 0], ["计时", 0]], [])
        self.assertEqual(out["result"], [None, None])

    def test_race_deterministic_tie(self):
        out = self.run_flow(["竞速", ["计时", 0], ["计时", 0]], [])
        self.assertEqual(out["status"], "完成")
        self.assertEqual(out["result"], [0, None])

    def test_race_waits_same_name_backlog_order(self):
        flow = ["竞速", ["等待", "a"], ["等待", "a"]]
        out = self.run_flow(flow, [
            ["s1", "送达", "a", "first"],
            ["s2", "送达", "a", "second"],
        ])
        self.assertEqual(out["result"], [0, "first"])
        self.assertEqual(out["backlog"], ["s2"])

    def test_race_consume_then_loser_cannot_reuse(self):
        flow = ["竞速", ["等待", "a"], ["等待", "a"]]
        out = self.run_flow(flow, [
            ["s1", "送达", "a", "x"],
            ["s2", "送达", "a", "y"],
            ["s3", "送达", "a", "z"],
        ])
        self.assertEqual(out["result"], [0, "x"])
        self.assertEqual(out["backlog"], ["s2", "s3"])

    def test_race_nested(self):
        # Outer race: child 0 is a serial that needs two timers (t=5,5);
        # child 1 is a timer 12.  At t=5 child0's first timer fires but
        # serial is not complete; at t=10 child0 completes and wins.
        flow = ["竞速", ["串行", ["计时", 5], ["计时", 5]], ["计时", 12]]
        out = self.run_flow(flow, [["a", "推进", 12]])
        self.assertEqual(out["status"], "完成")
        self.assertEqual(out["result"], [0, [None, None]])
        # The advance target is reached even though the flow finished earlier.
        self.assertEqual(out["clock"], 12)

    def test_race_nested_cancels_pending_subtree(self):
        # child1 is a serial whose second timer would start at 3; if child0
        # wins at 2, child1 must be cancelled and its pending subtree too.
        flow = ["竞速", ["计时", 2], ["串行", ["计时", 3], ["计时", 50]]]
        out = self.run_flow(flow, [["a", "推进", 100]])
        self.assertEqual(out["result"], [0, None])
        self.assertEqual(out["clock"], 100)

    def test_parallel_rejects_signal_backlog(self):
        # inner race consumes one "a"; the outer parallel's second wait sees
        # nothing and stays running.
        flow = ["并行", ["竞速", ["等待", "a"], ["等待", "a"]], ["等待", "a"]]
        out = self.run_flow(flow, [["s1", "送达", "a", "1"]])
        self.assertEqual(out["status"], "运行")
        self.assertEqual(out["result"], None)
        self.assertEqual(out["backlog"], [])

    def test_backlog_survives_until_consumer(self):
        flow = ["串行", ["计时", 3], ["等待", "a"]]
        out = self.run_flow(flow, [["s1", "送达", "a", "d"]])
        self.assertEqual(out["backlog"], ["s1"])
        self.assertEqual(out["status"], "运行")
        out = self.run_flow(flow, [["s1", "送达", "a", "d"], ["c", "推进", 3]])
        self.assertEqual(out["result"], [None, "d"])
        self.assertEqual(out["backlog"], [])

    def test_no_advance_does_not_reach_timer(self):
        flow = ["计时", 5]
        out = self.run_flow(flow, [["s1", "送达", "a", "d"]])
        self.assertEqual(out["clock"], 0)
        self.assertEqual(out["status"], "运行")

    def test_reaching_target_after_earlier_timers(self):
        # Settle events at 2 then 7 while advancing to 10 with waits that
        # consume backlog; verify timers start at real activation times.
        flow = ["串行",
                ["计时", 2],
                ["等待", "a"],
                ["计时", 3]]
        out = self.run_flow(flow, [
            ["s1", "送达", "a", "v"],   # queued before activation
            ["t", "推进", 10],
        ])
        # timer0: 0..2; wait consumes s1 at 2; timer1: 2..5 -> completes at 5;
        # the clock nevertheless ends at the requested target 10.
        self.assertEqual(out["result"], [None, "v", None])
        self.assertEqual(out["clock"], 10)

    def test_wait_zero_time_after_activation(self):
        flow = ["串行", ["计时", 4], ["等待", "a"]]
        out = self.run_flow(flow, [
            ["s1", "送达", "a", "x"],
            ["t", "推进", 4],
        ])
        self.assertEqual(out["result"], [None, "x"])

    def test_camelcase_names(self):
        flow = ["并行", ["等待", "sig1"], ["等待", "AbC9"]]
        out = self.run_flow(flow, [["s1", "送达", "AbC9", "z"]])
        self.assertEqual(out["status"], "运行")
        self.assertEqual(out["backlog"], [])
        out = self.run_flow(flow, [["s1", "送达", "AbC9", "z"],
                                   ["s2", "送达", "sig1", "y"]])
        self.assertEqual(out["result"], ["y", "z"])


# ---------------------------------------------------------------------------
# Validation tests
# ---------------------------------------------------------------------------

class ValidationTests(unittest.TestCase):
    def test_flow_errors(self):
        cases = [
            (["计时", True], [1]),
            (["计时", -1], [1]),
            (["计时", 1.5], [1]),
            (["计时", "5"], [1]),
            (["等待", ""], [1]),
            (["等待", "a-b"], [1]),
            (["等待", "信号"], [1]),
            (["串行"], []),
            (["并行"], []),
            (["竞速"], []),
            (["串行", ["计时", 1], ["计时", True]], [2, 1]),
            (["计时"], []),
            (["计时", 1, 2], []),
            (["等待", "a", "b"], []),
            ([], []),
            ("x", []),
            ([1, 2], [0]),
            (["未知", 1], [0]),
        ]
        for flow, path in cases:
            with self.assertRaises(flow_schema.FlowError) as ctx:
                flow_schema.validate_flow(flow)
            self.assertEqual(ctx.exception.path, path, flow)

    def test_command_errors(self):
        cases = [
            ([], []),
            ([""], []),
            (["a-b", "推进", 0], [0]),
            (["a", "跑", 0], [1]),
            (["a", "推进", True], [2]),
            (["a", "推进", -1], [2]),
            (["a", "推进"], []),
            (["a", "推进", 1, 2], []),
            (["a", "送达", "", "x"], [2]),
            (["a", "送达", "b", 5], [3]),
            (["a", "送达", "b", None], [3]),
            (["a", "送达", "b"], []),
            (["a", "送达", "b", "x", 1], []),
        ]
        for command, path in cases:
            with self.assertRaises(flow_schema.FlowError) as ctx:
                flow_schema.validate_command(command)
            self.assertEqual(ctx.exception.path, path, command)

    def test_json_syntax(self):
        for text in ("{", "[1,", "", "\x00"):
            with self.assertRaises(flow_schema.FlowError) as ctx:
                flow_schema.decode_json(text)
            self.assertEqual(ctx.exception.path, [])

    def test_data_strings_allowed(self):
        flow_schema.validate_command(["a", "送达", "b", "hi"])
        flow_schema.validate_command(["a", "送达", "b", ""])


# ---------------------------------------------------------------------------
# Persistence / CLI end-to-end
# ---------------------------------------------------------------------------

class CliTests(Base):
    def test_init_once_and_query(self):
        flow = ["竞速", ["等待", "a"], ["计时", 9]]
        first = self.init(flow)
        self.assertEqual(first, {"clock": 0, "status": "运行",
                                 "result": None, "backlog": []})
        # Re-initialising with the same flow is idempotent.
        proc = cli("init", "--db", self.db, "--flow", json.dumps(flow))
        self.assertEqual(proc.returncode, 0, proc.stderr)
        # A different flow is refused.
        err = cli("init", "--db", self.db, "--flow", '["计时",1]')
        self.assertNotEqual(err.returncode, 0)
        self.assertIn("already initialised", err.stderr)

    def test_deliver_and_race_receipt(self):
        self.init(["竞速", ["等待", "a"], ["计时", 9]])
        receipt = self.submit(["s1", "送达", "a", "hello"])
        self.assertEqual(receipt, {"clock": 0, "status": "完成",
                                   "result": [0, "hello"], "backlog": []})
        self.assertEqual(self.query(), receipt)

    def test_backlog_and_late_signals(self):
        self.init(["并行", ["等待", "a"], ["等待", "b"]])
        self.submit(["s1", "送达", "a", "1"])
        view = self.query()
        self.assertEqual(view["status"], "运行")
        # s1 was consumed immediately by the waiting node -> backlog empty.
        self.assertEqual(view["backlog"], [])
        self.submit(["s2", "送达", "b", "2"])
        view = self.query()
        self.assertEqual(view["result"], ["1", "2"])

    def test_queued_signals_are_not_consumed_by_timers(self):
        self.init(["串行", ["计时", 3], ["等待", "a"]])
        self.submit(["s1", "送达", "a", "d"])
        self.assertEqual(self.query()["backlog"], ["s1"])
        self.submit(["c", "推进", 3])
        self.assertEqual(self.query()["result"], [None, "d"])

    def test_idempotent_retry_does_not_consume_again(self):
        self.init(["竞速", ["等待", "a"], ["等待", "a"]])
        r1 = self.submit(["s1", "送达", "a", "1"])
        r2 = self.submit(["s1", "送达", "a", "1"])
        self.assertEqual(r1, r2)
        self.assertEqual(self.query()["backlog"], [])
        # A different command with the same id is rejected.
        err = self.submit(["s1", "送达", "a", "2"], expect_ok=False)
        self.assertEqual(err["error"]["path"], "$[0]")
        self.assertIn("already used", err["error"]["message"])

    def test_idempotent_advance_retry(self):
        self.init(["计时", 5])
        r1 = self.submit(["t1", "推进", 5])
        r2 = self.submit(["t1", "推进", 5])
        self.assertEqual(r1, r2)
        self.assertEqual(r1["clock"], 5)
        # Same id with a different target is rejected.
        err = self.submit(["t1", "推进", 6], expect_ok=False)
        self.assertEqual(err["error"]["path"], "$[0]")

    def test_clock_cannot_go_backwards(self):
        self.init(["计时", 5])
        self.submit(["t1", "推进", 5])
        err = self.submit(["t2", "推进", 4], expect_ok=False)
        self.assertEqual(err["error"]["path"], "$[2]")
        self.assertEqual(self.query()["clock"], 5)

    def test_rejection_does_not_change_state(self):
        self.init(["计时", 5])
        before = self.query()
        for bad in ('{"oops"', '["x","推进",-1]', '["x","推进",true]',
                    '["bad-id!","推进",0]', '["x","送达",""]', '["x","送达","a"]'):
            proc = cli("submit", "--db", self.db, "--command", bad)
            self.assertNotEqual(proc.returncode, 0, bad)
        self.assertEqual(self.query(), before)

    def test_syntax_error_path_is_root(self):
        self.init(["计时", 5])
        proc = cli("submit", "--db", self.db, "--command", '["x",')
        self.assertNotEqual(proc.returncode, 0)
        err = json.loads(proc.stderr)
        self.assertEqual(err["error"]["path"], "$")
        self.assertEqual(err["error"]["kind"], "syntax")

    def test_completed_still_accepts_commands(self):
        self.init(["计时", 0])
        self.assertEqual(self.query()["status"], "完成")
        self.submit(["s9", "送达", "zz", "ignored"])
        view = self.query()
        self.assertEqual(view["status"], "完成")
        self.assertEqual(view["backlog"], ["s9"])
        self.submit(["t9", "推进", 100])
        self.assertEqual(self.query()["clock"], 100)

    def test_init_with_zero_second_timer_completes(self):
        view = self.init(["计时", 0])
        self.assertEqual(view["status"], "完成")
        self.assertEqual(view["result"], None)
        self.assertEqual(view["clock"], 0)

    def test_restart_preserves_state(self):
        self.init(["竞速", ["等待", "a"], ["计时", 9]])
        self.submit(["s1", "送达", "b", "queued"])
        view = self.query()
        self.assertEqual(view["backlog"], ["s1"])
        # Every CLI call opens/closes the DB, so this already exercises
        # reopening; verify the receipt table survived too.
        receipt = self.submit(["s1", "送达", "b", "queued"])
        self.assertEqual(receipt, view)


# ---------------------------------------------------------------------------
# Hard-kill / durability
# ---------------------------------------------------------------------------

class CrashTests(Base):
    def test_hard_kill_keeps_sqlite_consistent(self):
        """Kill the server mid-stream; the database must open cleanly and
        expose a complete state (receipts and state agree)."""
        self.init(["串行", ["计时", 2], ["计时", 2]])
        # A streaming writer process killed hard while committing repeatedly.
        code = r'''
import json, sys, os
sys.path.insert(0, %r)
from flow_store import Store
store = Store(%r)
for i in range(1, 1000):
    store.commit_command(["k%%d" %% i, "推进", i])
    print("done", i, flush=True)
''' % (ROOT, self.db)
        proc = subprocess.Popen([sys.executable, "-c", code],
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                text=True)
        # Wait until some commands have committed, then SIGKILL.
        for _ in range(50):
            line = proc.stdout.readline()
            if line.strip():
                break
        time.sleep(0.05)
        proc.send_signal(signal.SIGKILL)
        proc.wait()
        # Reopen and verify consistency.
        store = flow_store.Store(self.db)
        try:
            view = store.query()
            with store._lock:
                revision, state = store._load()
                rows = store.conn.execute(
                    "SELECT COUNT(*) AS n FROM receipts").fetchone()["n"]
            self.assertIsInstance(view["clock"], int)
            self.assertIn(view["status"], ("运行", "完成"))
            self.assertGreaterEqual(rows, 1)
            self.assertEqual(state["clock"], view["clock"])
            # No command id is duplicated and receipts replay exactly.
            duplicate = store.conn.execute(
                "SELECT cmd_id, COUNT(*) c FROM receipts GROUP BY cmd_id HAVING c > 1"
            ).fetchall()
            self.assertEqual(duplicate, [])
            rec = store.conn.execute(
                "SELECT command, receipt FROM receipts ORDER BY rowid DESC LIMIT 1"
            ).fetchone()
            cmd = json.loads(rec["command"])
            self.assertEqual(json.loads(rec["receipt"])["clock"], cmd[2])
        finally:
            store.close()


# ---------------------------------------------------------------------------
# HTTP API
# ---------------------------------------------------------------------------

class HttpTests(Base):
    @classmethod
    def setUpClass(cls):
        pass

    def setUp(self):
        super().setUp()
        self.init(["竞速", ["等待", "a"], ["计时", 7]])
        self.port = self._free_port()
        self.proc = subprocess.Popen(
            [sys.executable, ENTRY, "serve", "--db", self.db,
             "--port", str(self.port)],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, cwd=ROOT,
        )
        for _ in range(100):
            try:
                self._http("GET", "/health")
                break
            except Exception:
                time.sleep(0.05)
        else:
            self.fail("server did not start")

    def tearDown(self):
        self.proc.terminate()
        try:
            self.proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            self.proc.kill()
        super().tearDown()

    @staticmethod
    def _free_port() -> int:
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            return sock.getsockname()[1]

    def _http(self, method, path, body=None):
        data = None
        if body is not None:
            data = json.dumps(body, ensure_ascii=False).encode()
        req = urllib.request.Request(
            "http://127.0.0.1:%d%s" % (self.port, path), data=data, method=method)
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                return resp.status, json.loads(resp.read().decode())
        except urllib.error.HTTPError as exc:
            return exc.code, json.loads(exc.read().decode())

    def test_http_flow(self):
        status, body = self._http("POST", "/query")
        self.assertEqual((status, body["status"]), (200, "运行"))
        status, receipt = self._http("POST", "/command", ["s1", "送达", "a", "h"])
        self.assertEqual(status, 200)
        self.assertEqual(receipt["result"], [0, "h"])
        status, receipt2 = self._http("POST", "/command", ["s1", "送达", "a", "h"])
        self.assertEqual(receipt2, receipt)

    def test_http_rejections(self):
        status, _ = self._http("POST", "/command", ["t0", "推进", 6])
        self.assertEqual(status, 200)
        status, body = self._http("POST", "/command", ["t1", "推进", 5])
        self.assertEqual(status, 400)
        self.assertEqual(body["error"]["path"], "$[2]")
        # malformed body
        req = urllib.request.Request(
            "http://127.0.0.1:%d/command" % self.port, data=b"{",
            method="POST")
        with self.assertRaises(urllib.error.HTTPError) as ctx:
            urllib.request.urlopen(req, timeout=10)
        self.assertEqual(ctx.exception.code, 400)
        body = json.loads(ctx.exception.read().decode())
        self.assertEqual(body["error"]["path"], "$")

    def test_http_restart_persistence(self):
        self._http("POST", "/command", ["s1", "送达", "b", "queued"])
        self.proc.terminate()
        self.proc.wait(timeout=10)
        # start a second server on the same DB
        port2 = self._free_port()
        proc2 = subprocess.Popen(
            [sys.executable, ENTRY, "serve", "--db", self.db,
             "--port", str(port2)],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, cwd=ROOT,
        )
        try:
            for _ in range(100):
                try:
                    with urllib.request.urlopen(
                            "http://127.0.0.1:%d/query" % port2, timeout=1) as r:
                        view = json.loads(r.read().decode())
                    break
                except Exception:
                    time.sleep(0.05)
            self.assertEqual(view["backlog"], ["s1"])
            self.assertEqual(view["status"], "运行")
        finally:
            proc2.terminate()
            proc2.wait(timeout=10)


if __name__ == "__main__":
    unittest.main(verbosity=2)
