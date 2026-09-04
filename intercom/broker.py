# -*- coding: utf-8 -*-
"""Broker: TCP server owning all managed sessions and the routing logic.

Routing rules
-------------
- ``send`` injects a user message into a session's stream.
- ``link a b`` creates a duplex bridge: every completed turn's text from one
  side is forwarded to the other as ``[from <name>] ...``. A per-link hop
  counter pauses the bridge after ``max_hops`` consecutive auto-forwards with
  no outside input, so two agents cannot polite-loop forever.
- Sessions may also address each other autonomously: a fenced
  ``intercom`` block in assistant output is parsed and routed.
"""
from __future__ import annotations

import asyncio
import json
import re
import time
from dataclasses import dataclass, field
from pathlib import Path

from .process import ManagedSession, assistant_text, is_turn_end
from .profiles import ProfileStore
from . import protocol

_INTERCOM_RE = re.compile(r"```intercom\s*(\{.*?\})\s*```", re.DOTALL)


@dataclass
class Link:
    a: str
    b: str
    hops: int = 0
    max_hops: int = 20
    paused: bool = False

    def peer(self, name: str) -> str | None:
        if name == self.a:
            return self.b
        if name == self.b:
            return self.a
        return None

    def key(self) -> tuple[str, str]:
        return tuple(sorted((self.a, self.b)))


@dataclass
class Broker:
    profiles: ProfileStore
    state_dir: Path
    claude_bin: str = "claude"
    sessions: dict[str, ManagedSession] = field(default_factory=dict)
    links: dict[tuple[str, str], Link] = field(default_factory=dict)
    subscribers: set[asyncio.Queue] = field(default_factory=set)  # event queues (TCP tail / web SSE)
    _pending: dict[str, list[str]] = field(default_factory=dict)  # name -> assistant texts of current turn

    def subscribe(self) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue(maxsize=1000)
        self.subscribers.add(q)
        return q

    def unsubscribe(self, q: asyncio.Queue) -> None:
        self.subscribers.discard(q)

    # ── session lifecycle ─────────────────────────────────────────────
    async def spawn(self, name: str) -> dict:
        if name in self.sessions and self.sessions[name].alive:
            return {"ok": True, "already": True, "session_id": self.sessions[name].session_id}
        profile = self.profiles.get(name)
        if profile is None:
            return {"ok": False, "error": f"profile 不存在: {name}（先 intercom profile-set {name} ...）"}
        sess = ManagedSession(
            profile,
            self._on_event,
            claude_bin=self.claude_bin,
            transcript_path=self.state_dir / "logs" / f"{name}.jsonl",
        )
        self.sessions[name] = sess
        try:
            await sess.start()
        except OSError as exc:
            del self.sessions[name]
            return {"ok": False, "error": f"启动失败: {exc}"}
        await self._broadcast({"type": "spawned", "name": name})
        return {"ok": True, "session_id": sess.session_id}

    async def stop(self, name: str) -> dict:
        sess = self.sessions.get(name)
        if not sess:
            return {"ok": False, "error": f"未运行: {name}"}
        await sess.close()
        del self.sessions[name]
        await self._broadcast({"type": "stopped", "name": name})
        return {"ok": True}

    # ── messaging ─────────────────────────────────────────────────────
    async def send(self, name: str, text: str, *, outside_input: bool = True) -> dict:
        if name not in self.sessions or not self.sessions[name].alive:
            res = await self.spawn(name)
            if not res.get("ok"):
                return res
        if outside_input:
            for link in self.links.values():
                if link.peer(name) is not None:
                    link.hops = 0
                    link.paused = False
        await self.sessions[name].send(text)
        await self._broadcast({"type": "sent", "name": name, "text": text})
        return {"ok": True}

    async def _on_event(self, sess: ManagedSession, event: dict) -> None:
        name = sess.profile.name
        text = assistant_text(event)
        if text:
            self._pending.setdefault(name, []).append(text)
            await self._broadcast({"type": "text", "name": name, "text": text})
        if event.get("type") == "system" and event.get("subtype") == "init":
            await self._broadcast({"type": "init", "name": name, "session_id": sess.session_id})
        if is_turn_end(event):
            sess.turns += 1
            full = "".join(self._pending.pop(name, []))
            await self._broadcast({"type": "turn_end", "name": name, "turns": sess.turns})
            await self._route_turn(sess, full)

    async def _route_turn(self, sess: ManagedSession, full_text: str) -> None:
        name = sess.profile.name
        # 1) autonomous addressing via intercom blocks
        for m in _INTERCOM_RE.finditer(full_text or ""):
            try:
                payload = json.loads(m.group(1))
                target, text = str(payload["to"]), str(payload["text"])
            except (json.JSONDecodeError, KeyError, TypeError):
                continue
            await self.send(target, f"[intercom from {name}] {text}", outside_input=False)
        # 2) duplex links forward the whole turn
        for link in self.links.values():
            peer = link.peer(name)
            if peer is None or link.paused or not (full_text or "").strip():
                continue
            link.hops += 1
            if link.hops > link.max_hops:
                link.paused = True
                await self._broadcast({"type": "link_paused", "a": link.a, "b": link.b,
                                       "reason": f"连续 {link.max_hops} 跳无外部输入，防死循环暂停"})
                continue
            await self.send(peer, f"[from {name}] {full_text.strip()}", outside_input=False)

    # ── links ─────────────────────────────────────────────────────────
    async def link(self, a: str, b: str, max_hops: int = 20) -> dict:
        if a == b:
            return {"ok": False, "error": "不能 link 自身"}
        link = Link(a=a, b=b, max_hops=max_hops)
        self.links[link.key()] = link
        return {"ok": True}

    async def unlink(self, a: str, b: str) -> dict:
        return {"ok": self.links.pop(tuple(sorted((a, b))), None) is not None}

    # ── introspection / pub-sub ───────────────────────────────────────
    def list_sessions(self) -> dict:
        return {
            "ok": True,
            "sessions": [
                {
                    "name": n,
                    "alive": s.alive,
                    "session_id": s.session_id,
                    "turns": s.turns,
                    "model": s.profile.model,
                    "effort": s.profile.effort,
                    "base_url": s.profile.base_url,
                }
                for n, s in sorted(self.sessions.items())
            ],
            "links": [
                {"a": l.a, "b": l.b, "hops": l.hops, "paused": l.paused}
                for l in self.links.values()
            ],
        }

    async def _broadcast(self, event: dict) -> None:
        event = {"ts": time.time(), **event}
        for q in self.subscribers:
            try:
                q.put_nowait(event)
            except asyncio.QueueFull:
                self.subscribers.discard(q)


async def _handle(broker: Broker, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    try:
        while True:
            line = await reader.readline()
            if not line:
                break
            try:
                req = protocol.decode(line)
            except (json.JSONDecodeError, UnicodeDecodeError):
                writer.write(protocol.encode({"ok": False, "error": "bad json"}))
                await writer.drain()
                continue
            op = req.get("op")
            if op == "spawn":
                res = await broker.spawn(str(req.get("name") or ""))
            elif op == "send":
                res = await broker.send(str(req.get("name") or ""), str(req.get("text") or ""))
            elif op == "link":
                res = await broker.link(str(req.get("a") or ""), str(req.get("b") or ""),
                                        int(req.get("max_hops", 20)))
            elif op == "unlink":
                res = await broker.unlink(str(req.get("a") or ""), str(req.get("b") or ""))
            elif op == "list":
                res = broker.list_sessions()
            elif op == "stop":
                res = await broker.stop(str(req.get("name") or ""))
            elif op == "tail":
                q = broker.subscribe()
                writer.write(protocol.encode({"ok": True, "tailing": True}))
                await writer.drain()
                try:
                    while True:
                        ev = await q.get()
                        writer.write(protocol.encode({"event": ev}))
                        await writer.drain()
                except (OSError, ConnectionError, asyncio.CancelledError):
                    pass
                finally:
                    broker.unsubscribe(q)
                break
            elif op == "shutdown":
                res = {"ok": True}
                writer.write(protocol.encode(res))
                await writer.drain()
                asyncio.get_running_loop().call_later(0.1, asyncio.get_running_loop().stop)
                break
            else:
                res = {"ok": False, "error": f"未知 op: {op}"}
            writer.write(protocol.encode(res))
            await writer.drain()
    finally:
        try:
            writer.close()
        except OSError:
            pass


async def serve(broker: Broker, port: int = protocol.DEFAULT_PORT) -> asyncio.AbstractServer:
    server = await asyncio.start_server(lambda r, w: _handle(broker, r, w), protocol.HOST, port)
    broker.state_dir.mkdir(parents=True, exist_ok=True)
    (broker.state_dir / "broker.json").write_text(
        json.dumps({"port": server.sockets[0].getsockname()[1], "pid": __import__("os").getpid()}),
        encoding="utf-8",
    )
    return server
