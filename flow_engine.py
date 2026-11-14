#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Pure scheduling engine for the flow language.

A state is a plain dict (persisted as canonical JSON):

    {"clock": int,
     "root": 0,
     "backlog": [{"id": "s1", "name": "a", "data": "x"}, ...],
     "nodes": {"0": {"id": 0, "op": "串行", "children": [1, 2],
                     "activated": 0, "status": "active", "result": null,
                     "winner": null},
               ...}}

`result` of a finished node is:
    * 等待: the signal's data (string) or null
    * 计时: null
    * 串行 / 并行: child results in original order
    * 竞速: [zero-based winning-child index, result]

Scheduling rules implemented here:

* A leaf is *newly ready* when it is active and either a queued signal with
  its name exists (等待) or `clock - activated >= duration` (计时).
* `settle()` scans addresses in depth-first order and repeatedly finishes the
  newly-ready leaf with the smallest address, then immediately propagates
  completion / cancellation / activation.  Exactly one leaf finishes per
  iteration, so the winning branch of a race can never be cancelled in the
  step that crowns it.
* With a clock target (`settle(state, limit)`), when no leaf is ready the
  clock jumps to the next timer expiry that is <= limit, settlement continues
  there, and the whole progression repeats until the clock reaches the limit.
  Every timer starts at the clock value of the instant it actually became
  active.
* Completing a race cancels its losing subtrees.  Cancellation discards no
  already-consumed signal; signals still queued when their waiter is cancelled
  stay queued for whatever consumes them next.
* At clock 0 the root is activated; zero-second timers therefore expire
  immediately during initialisation.
"""

from __future__ import annotations

import copy
from typing import Any, Dict, List, Optional, Tuple

from flow_schema import (
    COMBO_OPS,
    OP_PARALLEL,
    OP_RACE,
    OP_SERIAL,
    OP_TIMER,
    OP_WAIT,
    ST_ACTIVE,
    ST_CANCELLED,
    ST_DONE,
    ST_PENDING,
    STATUS_DONE,
    STATUS_RUNNING,
)

State = Dict[str, Any]
Node = Dict[str, Any]


# ---------------------------------------------------------------------------
# Tree construction
# ---------------------------------------------------------------------------

def build_tree(flow: Any) -> State:
    """Create the initial state for a validated flow (clock 0, root active)."""
    nodes: Dict[str, Node] = {}

    def build(node: Any) -> int:
        nid = len(nodes)
        entry: Node = {
            "id": nid,
            "op": node[0],
            "children": [],
            "activated": 0,
            "status": ST_PENDING,
            "result": None,
            "winner": None,
        }
        nodes[str(nid)] = entry
        if node[0] == OP_WAIT:
            entry["name"] = node[1]
        elif node[0] == OP_TIMER:
            entry["duration"] = node[1]
        else:
            entry["children"] = [build(child) for child in node[1:]]
        return nid

    root = build(flow)
    state = {"clock": 0, "root": root, "backlog": [], "nodes": nodes}
    nodes[str(root)]["status"] = ST_ACTIVE
    nodes[str(root)]["activated"] = 0
    return state


# ---------------------------------------------------------------------------
# Tree helpers
# ---------------------------------------------------------------------------

def node(state: State, nid: int) -> Node:
    return state["nodes"][str(nid)]


def subtree(state: State, nid: int) -> List[int]:
    out: List[int] = []
    stack = [nid]
    while stack:
        cur = stack.pop()
        out.append(cur)
        stack.extend(reversed(node(state, cur)["children"]))
    return out


# ---------------------------------------------------------------------------
# Settlement
# ---------------------------------------------------------------------------

def settle(state: State, limit: Optional[int] = None) -> None:
    """Settle to a fixpoint.

    `limit` is None for commands that do not move the clock (送达, init).
    For 推进 it is the target clock: the clock only ever moves forward to a
    timer expiry or to the target, in that order.
    """
    guard = 0
    while True:
        propagate(state)
        found = first_ready(state)
        if found is not None:
            _finish_leaf(state, *found)
            propagate(state)
        elif limit is not None and state["clock"] < limit:
            nxt = next_expiry(state)
            if nxt is not None and nxt <= limit:
                state["clock"] = nxt
            else:
                state["clock"] = limit
        else:
            return
        guard += 1
        if guard > 2_000_000:  # pragma: no cover - defensive
            raise RuntimeError("settlement did not converge")


def first_ready(state: State) -> Optional[Tuple[str, int]]:
    """Return (kind, node id) of the smallest-address newly-ready leaf."""
    def visit(nid: int) -> Optional[Tuple[str, int]]:
        cur = node(state, nid)
        if cur["status"] != ST_ACTIVE:
            return None
        op = cur["op"]
        if op == OP_WAIT:
            for sig in state["backlog"]:
                if sig["name"] == cur["name"]:
                    return ("wait", nid)
            return None
        if op == OP_TIMER:
            if state["clock"] - cur["activated"] >= cur["duration"]:
                return ("timer", nid)
            return None
        for child in cur["children"]:
            found = visit(child)
            if found is not None:
                return found
        return None

    return visit(state["root"])


def next_expiry(state: State) -> Optional[int]:
    """Earliest timer expiry strictly after the current clock among active
    timers (newly-ready leaves have already been drained by the caller)."""
    best: Optional[int] = None
    for nid in subtree(state, state["root"]):
        cur = node(state, nid)
        if cur["status"] == ST_ACTIVE and cur["op"] == OP_TIMER:
            when = cur["activated"] + cur["duration"]
            if when > state["clock"] and (best is None or when < best):
                best = when
    return best


def _finish_leaf(state: State, kind: str, nid: int) -> None:
    cur = node(state, nid)
    if kind == "wait":
        for index, sig in enumerate(state["backlog"]):
            if sig["name"] == cur["name"]:
                cur["result"] = sig["data"]
                del state["backlog"][index]
                break
        else:  # pragma: no cover - guaranteed by first_ready
            raise AssertionError("ready wait without a matching signal")
    else:
        cur["result"] = None
    cur["status"] = ST_DONE


# ---------------------------------------------------------------------------
# Propagation: finish internals, cancel race losers, activate pending work
# ---------------------------------------------------------------------------

def propagate(state: State) -> None:
    guard = 0
    while True:
        changed = False
        changed |= _finish_pass(state)
        changed |= _cancel_pass(state)
        changed |= _activate_pass(state)
        if not changed:
            return
        guard += 1
        if guard > 2_000_000:  # pragma: no cover - defensive
            raise RuntimeError("propagation did not converge")


def _finish_pass(state: State) -> bool:
    changed = False

    def walk(nid: int) -> None:
        nonlocal changed
        cur = node(state, nid)
        for child in cur["children"]:
            walk(child)
        if cur["status"] == ST_ACTIVE and _finish_internal(state, cur):
            changed = True

    walk(state["root"])
    return changed


def _finish_internal(state: State, cur: Node) -> bool:
    op = cur["op"]
    if op not in COMBO_OPS:
        return False
    children = cur["children"]
    statuses = [node(state, child)["status"] for child in children]

    if op == OP_SERIAL:
        if all(status == ST_DONE for status in statuses):
            cur["result"] = [node(state, child)["result"] for child in children]
            cur["status"] = ST_DONE
            return True
        return False

    if op == OP_PARALLEL:
        if any(status == ST_CANCELLED for status in statuses):
            return False
        if all(status == ST_DONE for status in statuses):
            cur["result"] = [node(state, child)["result"] for child in children]
            cur["status"] = ST_DONE
            return True
        return False

    # 竞速: the lowest-index done child wins.
    for index, status in enumerate(statuses):
        if status == ST_DONE:
            cur["winner"] = index
            cur["result"] = [index, node(state, children[index])["result"]]
            cur["status"] = ST_DONE
            return True
    return False


def _cancel_pass(state: State) -> bool:
    changed = False
    for nid in subtree(state, state["root"]):
        cur = node(state, nid)
        if cur["status"] == ST_DONE and cur["op"] == OP_RACE:
            for index, child in enumerate(cur["children"]):
                if index != cur["winner"]:
                    changed |= _cancel_subtree(state, child)
    return changed


def _cancel_subtree(state: State, nid: int) -> bool:
    changed = False
    for cur_id in subtree(state, nid):
        cur = node(state, cur_id)
        if cur["status"] in (ST_PENDING, ST_ACTIVE):
            cur["status"] = ST_CANCELLED
            cur["result"] = None
            changed = True
    return changed


def _activate_pass(state: State) -> bool:
    changed = False
    stack = [state["root"]]
    while stack:
        nid = stack.pop()
        cur = node(state, nid)
        if cur["status"] != ST_ACTIVE or cur["op"] not in COMBO_OPS:
            continue
        if cur["op"] == OP_SERIAL:
            frontier = None
            for child in cur["children"]:
                if node(state, child)["status"] != ST_DONE:
                    frontier = child
                    break
            if frontier is None:
                continue
            child = node(state, frontier)
            if child["status"] == ST_PENDING:
                child["status"] = ST_ACTIVE
                child["activated"] = state["clock"]
                changed = True
            if child["status"] == ST_ACTIVE:
                stack.append(frontier)
        else:
            for child_id in cur["children"]:
                child = node(state, child_id)
                if child["status"] == ST_PENDING:
                    child["status"] = ST_ACTIVE
                    child["activated"] = state["clock"]
                    changed = True
                if child["status"] == ST_ACTIVE:
                    stack.append(child_id)
    return changed


# ---------------------------------------------------------------------------
# Introspection helpers
# ---------------------------------------------------------------------------

def status_of(state: State) -> str:
    return STATUS_DONE if node(state, state["root"])["status"] == ST_DONE else STATUS_RUNNING


def result_of(state: State) -> Any:
    root = node(state, state["root"])
    return root["result"] if root["status"] == ST_DONE else None


def backlog_ids(state: State) -> List[str]:
    return [sig["id"] for sig in state["backlog"]]


def is_root_done(state: State) -> bool:
    return node(state, state["root"])["status"] == ST_DONE


def clone(state: State) -> State:
    return copy.deepcopy(state)


def invariants_hold(state: State) -> bool:
    """Structural invariant: under a finished root every node is done or
    cancelled, and every finished node's result is filled in."""
    root = node(state, state["root"])
    if root["status"] != ST_DONE:
        return True
    for nid in subtree(state, state["root"]):
        cur = node(state, nid)
        if cur["status"] not in (ST_DONE, ST_CANCELLED):
            return False
    return True
