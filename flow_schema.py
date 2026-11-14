#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Flow-language vocabulary, validation and canonical JSON helpers.

A *flow* is a JSON array:

    ["等待", name]                 wait for signal `name`
    ["计时", d]                    timer, d seconds from activation
    ["串行", E, ...]               serial: children activated one at a time
    ["并行", E, ...]               parallel: all children at once
    ["竞速", E, ...]               race: all children at once, first wins

A *command* is a JSON array:

    [id, "送达", name, data]       enqueue a signal
    [id, "推进", t]                advance the clock to t

Errors carry a zero-based array path to the offending element.  A JSON syntax
error is reported at "$".  `id` and `name` must be non-empty ASCII strings of
letters and digits only.  `d` and `t` are non-negative integers, booleans are
explicitly rejected.
"""

from __future__ import annotations

import json
import re
from typing import Any, List

OP_WAIT = "等待"
OP_TIMER = "计时"
OP_SERIAL = "串行"
OP_PARALLEL = "并行"
OP_RACE = "竞速"

CMD_DELIVER = "送达"
CMD_ADVANCE = "推进"

FLOW_OPS = {OP_WAIT, OP_TIMER, OP_SERIAL, OP_PARALLEL, OP_RACE}
COMBO_OPS = {OP_SERIAL, OP_PARALLEL, OP_RACE}
CMD_OPS = {CMD_DELIVER, CMD_ADVANCE}

# Internal node/root statuses (persisted verbatim; not part of the API).
ST_PENDING = "pending"
ST_ACTIVE = "active"
ST_DONE = "done"
ST_CANCELLED = "cancelled"

STATUS_RUNNING = "运行"
STATUS_DONE = "完成"

TOKEN_RE = re.compile(r"[0-9A-Za-z]+")
ID_RE = re.compile(r"[0-9A-Za-z]+")


class FlowError(Exception):
    """Rejection of malformed JSON, an illegal flow/command or bad state."""

    def __init__(self, message: str, path: List[int] | None = None, kind: str = "reject"):
        super().__init__(message)
        self.message = message
        self.path = path if path is not None else []
        self.kind = kind

    def to_dict(self) -> dict:
        return {
            "error": {
                "kind": self.kind,
                "message": self.message,
                "path": self.path,
            }
        }


def format_path(path: List[int]) -> str:
    return "$" + "".join("[%d]" % p for p in path)


def canonical_json(value: Any) -> str:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    )


def is_token(value: Any) -> bool:
    return isinstance(value, str) and TOKEN_RE.fullmatch(value) is not None


def _is_path_list(value: Any) -> bool:
    return (
        isinstance(value, list)
        and all(
            isinstance(item, int) and not isinstance(item, bool) and item >= 0
            for item in value
        )
    )


def _is_nonneg_int(value: Any) -> bool:
    return (
        isinstance(value, int)
        and not isinstance(value, bool)
        and value >= 0
    )


def validate_flow(node: Any, path: List[int] | None = None) -> None:
    """Validate a flow tree in place (no mutation); raise FlowError on error."""
    if path is None:
        path = []
    if not isinstance(node, list) or not node:
        raise FlowError("flow node must be a non-empty array", path)
    op = node[0]
    if not isinstance(op, str):
        raise FlowError("operation name must be a string", path + [0])
    if op not in FLOW_OPS:
        raise FlowError("unknown operation %r" % (op,), path + [0])

    if op == OP_WAIT:
        if len(node) != 2:
            raise FlowError("等待 takes exactly one name", path, kind="structure")
        if not is_token(node[1]):
            raise FlowError(
                "signal name must be a non-empty ASCII alphanumeric string",
                path + [1],
            )
        return

    if op == OP_TIMER:
        if len(node) != 2:
            raise FlowError("计时 takes exactly one duration", path, kind="structure")
        if not _is_nonneg_int(node[1]):
            raise FlowError(
                "duration must be a non-negative integer (booleans rejected)",
                path + [1],
            )
        return

    # 串行 / 并行 / 竞速
    if len(node) < 2:
        raise FlowError("%s needs at least one child" % op, path, kind="structure")
    for index in range(1, len(node)):
        validate_flow(node[index], path + [index])


def _validate_deliver_name(name: Any) -> None:
    if not is_token(name):
        raise FlowError(
            "signal name must be a non-empty ASCII alphanumeric string", [2]
        )


def validate_command(cmd: Any) -> None:
    """Validate a command shape and values; raise FlowError on error."""
    if not isinstance(cmd, list) or len(cmd) < 2:
        raise FlowError("command must be an array [id, op, ...]", [])
    if not isinstance(cmd[0], str) or ID_RE.fullmatch(cmd[0]) is None:
        raise FlowError(
            "command id must be a non-empty ASCII alphanumeric string", [0]
        )
    op = cmd[1]
    if not isinstance(op, str):
        raise FlowError("command operation must be a string", [1])
    if op not in CMD_OPS:
        raise FlowError("unknown command operation %r" % (op,), [1])

    if op == CMD_ADVANCE:
        if len(cmd) != 3:
            raise FlowError("推进 takes exactly one argument", [], kind="structure")
        if not _is_nonneg_int(cmd[2]):
            raise FlowError(
                "advance target t must be a non-negative integer (booleans rejected)",
                [2],
            )
        return

    # CMD_DELIVER
    if len(cmd) != 4:
        raise FlowError(
            "送达 takes exactly [id, 送达, name, data]", [], kind="structure"
        )
    _validate_deliver_name(cmd[2])
    if not isinstance(cmd[3], str):
        raise FlowError("signal data must be a JSON string", [3])


def ids_of(flow: Any, out: List[int]) -> None:
    out.append(id(flow))
    if isinstance(flow, list) and flow and flow[0] in COMBO_OPS:
        for child in flow[1:]:
            ids_of(child, out)


def decode_json(text: Any) -> Any:
    """Parse JSON, mapping every syntax/encoding problem to a $ path error."""
    if isinstance(text, (bytes, bytearray)):
        try:
            text = bytes(text).decode("utf-8")
        except UnicodeDecodeError:
            raise FlowError("request body is not valid UTF-8", [], kind="syntax")
    if not isinstance(text, str):
        raise FlowError("request body must be text", [], kind="syntax")
    try:
        return json.loads(text)
    except json.JSONDecodeError as exc:
        raise FlowError("invalid JSON: %s" % exc.msg, [], kind="syntax")
    except (ValueError, RecursionError) as exc:
        raise FlowError("invalid JSON: %s" % exc, [], kind="syntax")
