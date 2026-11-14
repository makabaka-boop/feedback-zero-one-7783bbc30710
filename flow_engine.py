#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Pure scheduling engine for the flow language.

A state is a plain dict (persisted as canonical JSON):

    {"clock": int,
     "root": 0,
     "backlog": [{"id": "s1", "name": "a", "data": "x"}, ...],
     "nodes": {"0": {"id": 0, "op": "串行", "children": [1, 2],
                     "activated": 0, "status": "active", "result": null,
                     "winner": null, "address": []},
               ...},
     "effects": {"creates": {"[0]": {"address": [0], "data": "x",
                                     "response": null}},
                 "cancels": {"[0]": {"address": [0], "confirmed": false}}}}

`result` of a finished node is:
    * 等待: the signal's data (string) or null
    * 计时: null
    * 采集: the recorded /create answer (string)
    * 串行 / 并行: child results in original order
    * 竞速: [zero-based winning-child index, result]

Scheduling rules implemented here:

* A leaf is *newly ready* when it is active and either a queued signal with
  its name exists (等待), `clock - activated >= duration` (计时), or its
  create answer has been recorded in the effect ledger (采集).
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
* 采集 leaves are effectful.  Activating one registers a durable create intent
  in ``state["effects"]["creates"]`` (keyed by the zero-based node address);
  an independent worker process performs the HTTP call against the mock
  target and records the answer, which makes the leaf newly ready — it then
  finishes with the answer's data.  Cancelling a subtree records a durable
  duty in ``state["effects"]["cancels"]`` for every activated 采集 leaf of
  that subtree, including finished leaves and leaves whose create answer is
  still in flight.  Late create answers are recorded but never change an
  already-settled result.
"""

from __future__ import annotations

import copy
from typing import Any, Dict, List, Optional, Tuple

from flow_schema import (
    COMBO_OPS,
    OP_COLLECT,
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
    canonical_json,
)

State = Dict[str, Any]
Node = Dict[str, Any]


# ---------------------------------------------------------------------------
# Tree construction
# ---------------------------------------------------------------------------

def build_tree(flow: Any) -> State:
    """Create the initial state for a validated flow (clock 0, root active)."""
    nodes: Dict[str, Node] = {}

    def build(node: Any, address: List[int]) -> int:
        nid = len(nodes)
        entry: Node = {
            "id": nid,
            "op": node[0],
            "children": [],
            "activated": 0,
            "status": ST_PENDING,
            "result": None,
            "winner": None,
            "address": list(address),
        }
        nodes[str(nid)] = entry
        if node[0] == OP_WAIT:
            entry["name"] = node[1]
        elif node[0] == OP_TIMER:
            entry["duration"] = node[1]
        elif node[0] == OP_COLLECT:
            entry["data"] = node[1]
        else:
            entry["children"] = [
                build(child, address + [index])
                for index, child in enumerate(node[1:])
            ]
        return nid

    root = build(flow, [])
    state = {
        "clock": 0,
        "root": root,
        "backlog": [],
        "nodes": nodes,
        "effects": {"creates": {}, "cancels": {}},
    }
    root_entry = nodes[str(root)]
    root_entry["status"] = ST_ACTIVE
    root_entry["activated"] = 0
    _on_activation(state, root_entry)
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
# Side-effect ledger (采集)
# ---------------------------------------------------------------------------

def effects_of(state: State) -> Dict[str, Any]:
    """Ensure and return the persisted side-effect ledger.

    ``creates`` maps a canonical address key to
    ``{"address": [...], "data": str, "response": str | None}`` — the durable
    intent registered when a 采集 leaf is activated.  ``cancels`` maps a
    canonical address key to ``{"address": [...], "confirmed": bool}`` — the
    durable duty to cancel an activated 采集 leaf of a losing subtree.
    """
    effects = state.setdefault("effects", {})
    effects.setdefault("creates", {})
    effects.setdefault("cancels", {})
    return effects


def effect_key(address: List[int]) -> str:
    return canonical_json(address)


def normalize(state: State) -> State:
    """Bring a loaded state up to the current shape (idempotent)."""
    effects_of(state)
    if "address" not in node(state, state["root"]):
        def walk(nid: int, address: List[int]) -> None:
            cur = node(state, nid)
            cur["address"] = list(address)
            for index, child in enumerate(cur["children"]):
                walk(child, address + [index])

        walk(state["root"], [])
    return state


def node_at_address(state: State, address: List[int]) -> Optional[Node]:
    """Resolve a zero-based address path ([] is the root) to its node."""
    cur = node(state, state["root"])
    for index in address:
        children = cur["children"]
        if (not isinstance(index, int) or isinstance(index, bool)
                or index < 0 or index >= len(children)):
            return None
        cur = node(state, children[index])
    return cur


def _on_activation(state: State, entry: Node) -> None:
    """Persist the durable create intent when a 采集 leaf is activated."""
    if entry["op"] != OP_COLLECT:
        return
    creates = effects_of(state)["creates"]
    key = effect_key(entry["address"])
    if key not in creates:
        creates[key] = {
            "address": list(entry["address"]),
            "data": entry["data"],
            "response": None,
        }


def pending_create_ops(state: State) -> List[Dict[str, Any]]:
    """Create calls the worker should perform: intents without a recorded
    response whose leaf is still active (cancelled leaves are owned by their
    cancel duty instead)."""
    out = []
    for intent in effects_of(state)["creates"].values():
        if intent["response"] is not None:
            continue
        leaf = node_at_address(state, intent["address"])
        if leaf is not None and leaf["status"] == ST_ACTIVE:
            out.append({"address": list(intent["address"]), "data": intent["data"]})
    out.sort(key=lambda op: op["address"])
    return out


def pending_cancellations(state: State) -> List[List[int]]:
    """Addresses whose cancellation duty is not yet confirmed, sorted in
    integer lexicographic order (the /effects ``pending`` array)."""
    return sorted(
        list(duty["address"])
        for duty in effects_of(state)["cancels"].values()
        if not duty["confirmed"]
    )


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
        if op == OP_COLLECT:
            intent = effects_of(state)["creates"].get(effect_key(cur["address"]))
            if intent is not None and intent["response"] is not None:
                return ("collect", nid)
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
    elif kind == "collect":
        key = effect_key(cur["address"])
        cur["result"] = effects_of(state)["creates"][key]["response"]
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
    cancels = effects_of(state)["cancels"]
    for cur_id in subtree(state, nid):
        cur = node(state, cur_id)
        if cur["op"] == OP_COLLECT and cur["status"] in (ST_ACTIVE, ST_DONE):
            # Every activated 采集 leaf of a losing subtree must be cancelled
            # remotely — including completed leaves and leaves whose create
            # answer is still in flight.
            key = effect_key(cur["address"])
            if key not in cancels:
                cancels[key] = {"address": list(cur["address"]), "confirmed": False}
                changed = True
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
                _on_activation(state, child)
                changed = True
            if child["status"] == ST_ACTIVE:
                stack.append(frontier)
        else:
            for child_id in cur["children"]:
                child = node(state, child_id)
                if child["status"] == ST_PENDING:
                    child["status"] = ST_ACTIVE
                    child["activated"] = state["clock"]
                    _on_activation(state, child)
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
