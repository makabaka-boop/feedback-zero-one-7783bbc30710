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
TARGET_ENTRY = os.path.join(ROOT, "flow_target.py")

sys.path.insert(0, ROOT)
import flow_engine  # noqa: E402
import flow_schema  # noqa: E402
import flow_store  # noqa: E402


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


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

    # -- 采集 --------------------------------------------------------------

    def test_collect_intent_registered_and_completes(self):
        state = flow_engine.build_tree(["串行", ["采集", "x"], ["计时", 1]])
        flow_engine.settle(state)
        creates = state["effects"]["creates"]
        self.assertEqual(list(creates), ["[0]"])
        self.assertEqual(creates["[0]"]["data"], "x")
        self.assertIsNone(creates["[0]"]["response"])
        self.assertEqual(flow_engine.status_of(state), "运行")
        # The recorded answer makes the leaf ready; settlement is immediate.
        creates["[0]"]["response"] = "x"
        flow_engine.settle(state)
        self.assertEqual(flow_engine.status_of(state), "运行")  # timer pending
        flow_store._apply(state, ["c", "推进", 1])
        flow_engine.settle(state, limit=1)
        self.assertEqual(flow_engine.result_of(state), ["x", None])

    def test_collect_root_registers_intent(self):
        state = flow_engine.build_tree(["采集", "x"])
        flow_engine.settle(state)
        self.assertIn("[]", state["effects"]["creates"])
        self.assertEqual(flow_engine.status_of(state), "运行")

    def test_collect_loser_obligation(self):
        state = flow_engine.build_tree(["竞速", ["采集", "x"], ["计时", 0]])
        flow_engine.settle(state)
        self.assertEqual(flow_engine.result_of(state), [1, None])
        cancels = state["effects"]["cancels"]
        self.assertEqual(list(cancels), ["[0]"])
        self.assertFalse(cancels["[0]"]["confirmed"])
        self.assertEqual(flow_engine.pending_cancellations(state), [[0]])

    def test_collect_done_leaf_obligation_when_ancestor_loses(self):
        flow = ["竞速", ["串行", ["采集", "x"], ["计时", 50]], ["计时", 1]]
        state = flow_engine.build_tree(flow)
        flow_engine.settle(state)
        state["effects"]["creates"]["[0,0]"]["response"] = "x"
        flow_engine.settle(state)
        leaf = flow_engine.node_at_address(state, [0, 0])
        self.assertEqual(leaf["status"], "done")
        flow_store._apply(state, ["c", "推进", 1])
        flow_engine.settle(state, limit=1)
        self.assertEqual(flow_engine.result_of(state), [1, None])
        self.assertIn("[0,0]", state["effects"]["cancels"])
        # The completed leaf keeps its status and result.
        self.assertEqual(leaf["status"], "done")
        self.assertEqual(leaf["result"], "x")

    def test_collect_pending_answer_obligation_and_late_answer_ignored(self):
        state = flow_engine.build_tree(["竞速", ["采集", "x"], ["计时", 1]])
        flow_engine.settle(state)
        flow_store._apply(state, ["c", "推进", 1])
        flow_engine.settle(state, limit=1)
        self.assertEqual(flow_engine.result_of(state), [1, None])
        self.assertIn("[0]", state["effects"]["cancels"])
        # A late answer is recorded but must not change the settled result.
        state["effects"]["creates"]["[0]"]["response"] = "x"
        flow_engine.settle(state)
        self.assertEqual(flow_engine.result_of(state), [1, None])
        leaf = flow_engine.node_at_address(state, [0])
        self.assertEqual(leaf["status"], "cancelled")
        self.assertIsNone(leaf["result"])


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
            (["采集"], []),
            (["采集", "a", "b"], []),
            (["采集", 1], [1]),
            (["采集", None], [1]),
            (["采集", True], [1]),
            (["竞速", ["采集", 1], ["计时", 1]], [1, 1]),
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
        flow_schema.validate_flow(["采集", ""])
        flow_schema.validate_flow(["采集", "任意字符串 data"])


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

    def test_invalid_collect_leaf_rejected(self):
        proc = cli("init", "--db", self.db, "--flow", '["采集",5]')
        self.assertNotEqual(proc.returncode, 0)
        err = json.loads(proc.stderr)
        self.assertEqual(err["error"]["path"], "$[1]")
        # The rejection changed nothing: the db is still uninitialised.
        proc = cli("query", "--db", self.db)
        self.assertNotEqual(proc.returncode, 0)
        proc = cli("init", "--db", self.db, "--flow", '["竞速",["采集","x"],["采集",true]]')
        self.assertNotEqual(proc.returncode, 0)
        err = json.loads(proc.stderr)
        self.assertEqual(err["error"]["path"], "$[2][1]")


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

    def test_http_invalid_collect_leaf_is_400(self):
        status, body = self._http("POST", "/init", {"flow": ["采集", 5]})
        self.assertEqual(status, 400)
        self.assertEqual(body["error"]["path"], "$[1]")
        # State untouched: the original flow still runs.
        status, view = self._http("GET", "/query")
        self.assertEqual((status, view["status"]), (200, "运行"))

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


# ---------------------------------------------------------------------------
# 采集 full-stack: backend + independent worker + persistent mock target
# ---------------------------------------------------------------------------

class CollectTests(Base):
    worker_timeout = 3.0  # above the target's 2s hang so late answers arrive

    def setUp(self):
        super().setUp()
        self.target_db = os.path.join(self.tmp, "target.db")
        self.target_port = _free_port()
        self.target_url = "http://127.0.0.1:%d" % self.target_port
        self.target_proc = self._start_target()
        self.worker_proc = None

    def tearDown(self):
        self._stop_worker()
        self._stop_target()
        super().tearDown()

    # -- process management -------------------------------------------------

    def _start_target(self, hang=2.0):
        proc = subprocess.Popen(
            [sys.executable, TARGET_ENTRY, "serve", "--db", self.target_db,
             "--port", str(self.target_port), "--hang", str(hang)],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, cwd=ROOT)
        for _ in range(100):
            try:
                with urllib.request.urlopen(
                        self.target_url + "/health", timeout=1) as resp:
                    if resp.status == 200:
                        return proc
            except Exception:
                time.sleep(0.05)
        proc.kill()
        self.fail("target did not start")

    def _stop_target(self, hard=False):
        proc = getattr(self, "target_proc", None)
        if proc is None:
            return
        proc.kill() if hard else proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
        self.target_proc = None

    def _start_worker(self):
        self.worker_proc = subprocess.Popen(
            [sys.executable, ENTRY, "worker", "--db", self.db,
             "--target", self.target_url,
             "--interval", "0.05", "--retry", "0.1",
             "--timeout", str(self.worker_timeout)],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, cwd=ROOT)

    def _stop_worker(self, hard=False):
        proc = self.worker_proc
        if proc is None:
            return
        proc.kill() if hard else proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
        self.worker_proc = None

    # -- helpers --------------------------------------------------------------

    def wait_until(self, predicate, timeout=15.0, message="condition"):
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                if predicate():
                    return
            except Exception:
                pass
            time.sleep(0.05)
        self.fail("timed out waiting for %s" % message)

    def effects(self):
        proc = cli("effects", "--db", self.db)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        return json.loads(proc.stdout)

    def target_state(self):
        with urllib.request.urlopen(self.target_url + "/state", timeout=5) as resp:
            return json.loads(resp.read().decode())

    def set_control(self, **modes):
        req = urllib.request.Request(self.target_url + "/control",
                                     data=json.dumps(modes).encode(),
                                     method="POST")
        with urllib.request.urlopen(req, timeout=5) as resp:
            return json.loads(resp.read().decode())

    def leaf(self, address):
        store = flow_store.Store(self.db)
        try:
            _, state = store._load()
            return flow_engine.node_at_address(state, address)
        finally:
            store.close()

    def intent_response(self, address):
        store = flow_store.Store(self.db)
        try:
            _, state = store._load()
            key = flow_schema.canonical_json(address)
            return state["effects"]["creates"][key]["response"]
        finally:
            store.close()

    # -- tests -----------------------------------------------------------------

    def test_collect_root_completes(self):
        view = self.init(["采集", "hello"])
        self.assertEqual(view["status"], "运行")
        self._start_worker()
        self.wait_until(lambda: self.query()["status"] == "完成",
                        message="collect completion")
        self.assertEqual(self.query()["result"], "hello")
        self.assertEqual(self.effects(), {"pending": []})
        state = self.target_state()
        self.assertEqual(state["artifacts"], {"[]": "hello"})
        self.assertEqual(state["banned"], [])

    def test_spec_example_hanging_create_then_timer_wins(self):
        # [竞速,[采集,"x"],[计时,1]]: the create answer hangs, 推进 1 settles
        # [1, null], the loser is cancelled and the remote artifact removed.
        self.set_control(create="hang")
        self.init(["竞速", ["采集", "x"], ["计时", 1]])
        self._start_worker()
        self.wait_until(lambda: self.target_state()["stats"]["create"] >= 1,
                        message="create attempt")
        receipt = self.submit(["c1", "推进", 1])
        self.assertEqual(receipt, {"clock": 1, "status": "完成",
                                   "result": [1, None], "backlog": []})
        self.wait_until(lambda: self.effects()["pending"] == [],
                        message="cancel confirmation")
        state = self.target_state()
        self.assertEqual(state["artifacts"], {})
        self.assertIn("[0]", state["banned"])
        leaf = self.leaf([0])
        self.assertEqual(leaf["status"], "cancelled")
        self.assertIsNone(leaf["result"])
        # The hanging create eventually answers; the late answer is recorded
        # but must not change the settled result.
        self.wait_until(lambda: self.intent_response([0]) is not None,
                        message="late create answer")
        self.assertEqual(self.query(), {"clock": 1, "status": "完成",
                                        "result": [1, None], "backlog": []})

    def test_completed_collect_cancelled_by_ancestor(self):
        flow = ["竞速", ["串行", ["采集", "x"], ["计时", 50]], ["计时", 1]]
        self.init(flow)
        self._start_worker()
        self.wait_until(
            lambda: (self.leaf([0, 0]) or {}).get("status") == "done",
            message="collect done")
        receipt = self.submit(["c1", "推进", 1])
        self.assertEqual(receipt["status"], "完成")
        self.assertEqual(receipt["result"], [1, None])
        self.wait_until(lambda: self.effects()["pending"] == [],
                        message="cancel confirmation")
        state = self.target_state()
        self.assertEqual(state["artifacts"], {})
        self.assertIn("[0,0]", state["banned"])
        leaf = self.leaf([0, 0])
        self.assertEqual(leaf["status"], "done")
        self.assertEqual(leaf["result"], "x")
        self.assertEqual(self.query()["result"], [1, None])

    def test_worker_killed_after_remote_accept_before_local_confirm(self):
        # The target accepts the create and drops the response; the worker is
        # hard-killed before recording anything.  A restarted worker resumes
        # from the persisted intent and the create is idempotent remotely.
        self.set_control(create="disconnect")
        self.init(["采集", "x"])
        self._start_worker()
        self.wait_until(
            lambda: self.target_state()["artifacts"].get("[]") == "x",
            message="remote accept")
        self._stop_worker(hard=True)
        self.assertEqual(self.query()["status"], "运行")
        self.set_control(create="normal")
        self._start_worker()
        self.wait_until(lambda: self.query()["status"] == "完成",
                        message="completion after worker restart")
        self.assertEqual(self.query()["result"], "x")
        self.assertGreaterEqual(self.target_state()["stats"]["create"], 2)

    def test_cancel_confirmed_only_after_comms_recover(self):
        # The create answer hangs so the timer wins the race; the cancel is
        # then accepted remotely but its response is dropped.
        self.set_control(create="hang")
        self.init(["竞速", ["采集", "x"], ["计时", 1]])
        self._start_worker()
        self.wait_until(lambda: self.target_state()["stats"]["create"] >= 1,
                        message="create applied")
        self.set_control(cancel="disconnect")
        receipt = self.submit(["c1", "推进", 1])
        self.assertEqual(receipt["result"], [1, None])
        # The remote applies the cancel but drops the response: the duty
        # stays pending and survives a hard kill of the worker.
        self.wait_until(lambda: "[0]" in self.target_state()["banned"],
                        message="remote cancel")
        self.assertEqual(self.effects(), {"pending": [[0]]})
        self._stop_worker(hard=True)
        self.assertEqual(self.query()["status"], "完成")  # done, cleanup owed
        self.assertEqual(self.effects(), {"pending": [[0]]})
        self.set_control(cancel="normal")
        self._start_worker()
        self.wait_until(lambda: self.effects()["pending"] == [],
                        message="cancel confirmation after recovery")
        self.assertEqual(self.query()["result"], [1, None])

    def test_target_restart_keeps_artifacts_and_bans(self):
        self.set_control(create="hang")
        self.init(["竞速", ["采集", "x"], ["计时", 1]])
        self._start_worker()
        self.wait_until(
            lambda: self.target_state()["artifacts"].get("[0]") == "x",
            message="artifact visible")
        self._stop_target(hard=True)
        receipt = self.submit(["c1", "推进", 1])
        self.assertEqual(receipt["result"], [1, None])
        time.sleep(0.3)  # the target is down: no confirmation possible
        self.assertEqual(self.effects(), {"pending": [[0]]})
        self.target_proc = self._start_target()
        self.wait_until(lambda: self.effects()["pending"] == [],
                        message="cancel after target restart")
        state = self.target_state()
        self.assertEqual(state["artifacts"], {})
        self.assertIn("[0]", state["banned"])

    def test_race_between_two_collects(self):
        self.init(["竞速", ["采集", "a"], ["采集", "b"]])
        self._start_worker()
        self.wait_until(lambda: self.query()["status"] == "完成",
                        message="race done")
        result = self.query()["result"]
        winner = result[0]
        self.assertIn(winner, (0, 1))
        self.assertEqual(result[1], "ab"[winner])
        loser = 1 - winner
        self.wait_until(lambda: self.effects()["pending"] == [],
                        message="loser cancelled")
        state = self.target_state()
        self.assertEqual(state["artifacts"],
                         {flow_schema.canonical_json([winner]): "ab"[winner]})
        self.assertIn(flow_schema.canonical_json([loser]), state["banned"])

    def test_http_effects_endpoint(self):
        self.init(["竞速", ["采集", "x"], ["计时", 1]])
        # No worker: the cancellation duty is owed but cannot be confirmed.
        receipt = self.submit(["c1", "推进", 1])
        self.assertEqual(receipt["result"], [1, None])
        port = _free_port()
        server = subprocess.Popen(
            [sys.executable, ENTRY, "serve", "--db", self.db,
             "--port", str(port)],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, cwd=ROOT)
        try:
            body = None
            for _ in range(100):
                try:
                    with urllib.request.urlopen(
                            "http://127.0.0.1:%d/effects" % port,
                            timeout=1) as resp:
                        body = json.loads(resp.read().decode())
                    break
                except Exception:
                    time.sleep(0.05)
            self.assertEqual(body, {"pending": [[0]]})
            self._start_worker()
            self.wait_until(
                lambda: json.loads(urllib.request.urlopen(
                    "http://127.0.0.1:%d/effects" % port, timeout=5)
                    .read().decode())["pending"] == [],
                message="effects cleared over HTTP")
        finally:
            server.terminate()
            server.wait(timeout=10)

    def test_record_create_response_directly(self):
        # Store-level: recording an answer settles atomically, idempotently.
        self.init(["串行", ["采集", "x"], ["计时", 1]])
        store = flow_store.Store(self.db)
        try:
            self.assertTrue(store.record_create_response([0], "x"))
            self.assertFalse(store.record_create_response([0], "x"))
            self.assertEqual(store.query()["status"], "运行")
            store.commit_command(["c", "推进", 1])
            self.assertEqual(store.query()["result"], ["x", None])
            self.assertEqual(store.effects_view(), {"pending": []})
        finally:
            store.close()


if __name__ == "__main__":
    unittest.main(verbosity=2)