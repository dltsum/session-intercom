# -*- coding: utf-8 -*-
"""Zero-dependency web console for the intercom broker.

A minimal asyncio HTTP/1.1 server (no framework) serving:

    GET  /                  -> static/index.html
    GET  /api/state         -> sessions + links + profiles (tokens masked)
    GET  /api/events        -> SSE stream of broker events
    GET  /api/transcript?name=alice&limit=50
    POST /api/spawn|stop|send|link|unlink|profile_set|profile_del

Runs inside the broker process and calls Broker methods directly.
Binds 127.0.0.1 only.
"""
from __future__ import annotations

import asyncio
import json
from pathlib import Path

from .broker import Broker
from .process import assistant_text
from .profiles import Profile, ProfileError

STATIC_DIR = Path(__file__).parent / "static"


def _http(status: str, body: bytes, content_type: str = "application/json; charset=utf-8",
          extra_headers: dict | None = None) -> bytes:
    headers = {
        "Content-Type": content_type,
        "Content-Length": str(len(body)),
        "Connection": "close",
        "Cache-Control": "no-store",
        **(extra_headers or {}),
    }
    head = f"HTTP/1.1 {status}\r\n" + "".join(f"{k}: {v}\r\n" for k, v in headers.items()) + "\r\n"
    return head.encode() + body


def _mask(profile: Profile) -> dict:
    return {
        "name": profile.name,
        "base_url": profile.base_url,
        "has_token": bool(profile.auth_token),
        "model": profile.model,
        "effort": profile.effort,
        "cwd": profile.cwd,
        "preamble": profile.preamble,
        "resume_session_id": profile.resume_session_id,
    }


def _transcript(broker: Broker, name: str, limit: int) -> list[str]:
    path = broker.state_dir / "logs" / f"{name}.jsonl"
    texts: list[str] = []
    try:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return []
    for line in lines:
        try:
            ev = json.loads(line)
        except json.JSONDecodeError:
            continue
        t = assistant_text(ev)
        if t:
            texts.append(t)
    return texts[-limit:]


async def _read_request(reader: asyncio.StreamReader) -> tuple[str, str, bytes] | None:
    """Parse one HTTP request (request line + headers + optional body)."""
    head = await reader.readuntil(b"\r\n\r\n")
    lines = head.decode("utf-8", "replace").split("\r\n")
    method, target, _ = lines[0].split(" ", 2)
    headers = {}
    for line in lines[1:]:
        if ": " in line:
            k, v = line.split(": ", 1)
            headers[k.lower()] = v
    body = b""
    if "content-length" in headers:
        body = await reader.readexactly(int(headers["content-length"]))
    return method, target, body


async def _handle_ws_client(broker: Broker, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    try:
        req = await _read_request(reader)
        if req is None:
            return
        method, target, body = req
        path, _, query = target.partition("?")
        params = dict(p.split("=", 1) for p in query.split("&") if "=" in p)

        if method == "GET" and path == "/":
            html = (STATIC_DIR / "index.html").read_bytes()
            writer.write(_http("200 OK", html, "text/html; charset=utf-8"))
        elif method == "GET" and path == "/api/state":
            state = broker.list_sessions()
            state["profiles"] = [_mask(p) for p in broker.profiles.list()]
            writer.write(_http("200 OK", json.dumps(state, ensure_ascii=False).encode()))
        elif method == "GET" and path == "/api/transcript":
            name = params.get("name", "")
            limit = int(params.get("limit", "50"))
            writer.write(_http("200 OK", json.dumps(
                {"ok": True, "name": name, "texts": _transcript(broker, name, limit)},
                ensure_ascii=False).encode()))
        elif method == "GET" and path == "/api/events":
            await _sse(broker, writer)
            return  # SSE owns the connection until close
        elif method == "POST" and path.startswith("/api/"):
            res = await _dispatch(broker, path[5:], body)
            writer.write(_http("200 OK" if res.get("ok") else "400 Bad Request",
                               json.dumps(res, ensure_ascii=False).encode()))
        else:
            writer.write(_http("404 Not Found", b'{"ok": false, "error": "not found"}'))
        await writer.drain()
    except (OSError, asyncio.IncompleteReadError, ValueError):
        pass
    finally:
        try:
            writer.close()
        except OSError:
            pass


async def _sse(broker: Broker, writer: asyncio.StreamWriter) -> None:
    q = broker.subscribe()
    try:
        writer.write(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\n"
                     b"Cache-Control: no-store\r\nConnection: close\r\n\r\n")
        await writer.drain()
        while True:
            try:
                ev = await asyncio.wait_for(q.get(), timeout=25)
                writer.write(b"data: " + json.dumps(ev, ensure_ascii=False).encode() + b"\n\n")
            except asyncio.TimeoutError:
                writer.write(b": ping\n\n")  # keep-alive
            await writer.drain()
    except (OSError, ConnectionError, asyncio.CancelledError):
        pass
    finally:
        broker.unsubscribe(q)


async def _dispatch(broker: Broker, op: str, body: bytes) -> dict:
    try:
        req = json.loads(body or b"{}")
    except json.JSONDecodeError:
        return {"ok": False, "error": "请求不是合法 JSON"}
    if op == "spawn":
        return await broker.spawn(str(req.get("name") or ""))
    if op == "stop":
        return await broker.stop(str(req.get("name") or ""))
    if op == "send":
        return await broker.send(str(req.get("name") or ""), str(req.get("text") or ""))
    if op == "link":
        return await broker.link(str(req.get("a") or ""), str(req.get("b") or ""))
    if op == "unlink":
        return await broker.unlink(str(req.get("a") or ""), str(req.get("b") or ""))
    if op == "profile_set":
        try:
            broker.profiles.set(Profile(
                name=str(req.get("name") or ""),
                base_url=req.get("base_url") or None,
                auth_token=req.get("auth_token") or None,
                model=req.get("model") or None,
                effort=req.get("effort") or None,
                cwd=req.get("cwd") or None,
                preamble=req.get("preamble") or None,
            ))
        except ProfileError as exc:
            return {"ok": False, "error": str(exc)}
        return {"ok": True}
    if op == "profile_del":
        return {"ok": broker.profiles.delete(str(req.get("name") or ""))}
    return {"ok": False, "error": f"未知操作: {op}"}


async def serve_web(broker: Broker, port: int = 9780) -> asyncio.AbstractServer:
    return await asyncio.start_server(
        lambda r, w: _handle_ws_client(broker, r, w), "127.0.0.1", port)
