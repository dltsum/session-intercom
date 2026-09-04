# -*- coding: utf-8 -*-
"""NDJSON wire protocol between CLI clients and the broker (127.0.0.1 TCP).

Every message is one JSON object per line. Client → broker:

    {"op": "spawn",   "name": "alice"}
    {"op": "send",    "name": "alice", "text": "..."}
    {"op": "link",    "a": "alice", "b": "bob"}
    {"op": "unlink",  "a": "alice", "b": "bob"}
    {"op": "list"}
    {"op": "tail",    "name": "alice"}          # subscribe; broker streams events
    {"op": "stop",    "name": "alice"}
    {"op": "shutdown"}

Broker → client: one {"ok": bool, ...} reply per command, except `tail`
which replies {"ok": true} then streams {"event": ...} lines until close.
"""
from __future__ import annotations

import json

DEFAULT_PORT = 9779
HOST = "127.0.0.1"


def encode(obj: dict) -> bytes:
    return (json.dumps(obj, ensure_ascii=False) + "\n").encode("utf-8")


def decode(line: bytes) -> dict:
    return json.loads(line.decode("utf-8"))
