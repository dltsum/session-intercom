# -*- coding: utf-8 -*-
"""Managed Claude Code session: a persistent bidirectional stream-json process.

``claude -p --input-format stream-json --output-format stream-json --verbose``
keeps one long-lived conversation per process: user messages go in on stdin as
NDJSON, assistant/result events come out on stdout as NDJSON. The broker owns
one of these per registered session and routes text between them.
"""
from __future__ import annotations

import asyncio
import json
import os
import shlex
import shutil
from pathlib import Path
from typing import Any, Awaitable, Callable

from .profiles import Profile


def _split_bin(claude_bin: str) -> list[str]:
    """claude_bin may carry arguments, e.g. ``python tests/fake_claude.py``."""
    if os.name == "nt":
        # posix shlex eats Windows path backslashes; keep them and strip quotes
        parts = [p.strip('"') for p in shlex.split(claude_bin, posix=False)]
    else:
        parts = shlex.split(claude_bin)
    if parts:
        parts[0] = shutil.which(parts[0]) or parts[0]
    return parts

INTERCOM_INSTRUCTION = (
    "你可以通过输出如下代码块向其他 session 发消息（broker 会实时路由）：\n"
    "```intercom\n{\"to\": \"<目标session名>\", \"text\": \"<消息>\"}\n```\n"
    "收到以 [intercom from <名字>] 开头的消息时，说明它来自另一个 session。"
)


def build_argv(profile: Profile, claude_bin: str) -> list[str]:
    argv = [
        *_split_bin(claude_bin), "-p",
        "--input-format", "stream-json",
        "--output-format", "stream-json",
        "--verbose",
    ]
    if profile.resume_session_id:
        argv += ["--resume", profile.resume_session_id]
    if profile.model:
        argv += ["--model", profile.model]
    if profile.effort:
        argv += ["--effort", profile.effort]
    argv += ["--append-system-prompt", (profile.preamble or "") + "\n" + INTERCOM_INSTRUCTION]
    return argv


def _wrap_for_platform(argv: list[str]) -> list[str]:
    if os.name == "nt" and argv and str(argv[0]).lower().endswith((".cmd", ".bat")):
        return [os.environ.get("COMSPEC", "cmd.exe"), "/c", *argv]
    return argv


def user_message(text: str) -> bytes:
    return (json.dumps({
        "type": "user",
        "message": {"role": "user", "content": [{"type": "text", "text": text}]},
    }, ensure_ascii=False) + "\n").encode("utf-8")


def assistant_text(event: dict) -> str:
    """Extract text from a stream-json assistant event."""
    if event.get("type") != "assistant":
        return ""
    msg = event.get("message") or {}
    parts = [
        str(c.get("text", ""))
        for c in (msg.get("content") or [])
        if isinstance(c, dict) and c.get("type") == "text"
    ]
    return "".join(parts)


def is_turn_end(event: dict) -> bool:
    return event.get("type") == "result"


OnEvent = Callable[["ManagedSession", dict], Awaitable[None]]


class ManagedSession:
    def __init__(
        self,
        profile: Profile,
        on_event: OnEvent,
        *,
        claude_bin: str = "claude",
        transcript_path: Path | None = None,
    ):
        self.profile = profile
        self.on_event = on_event
        self.claude_bin = claude_bin
        self.transcript_path = transcript_path
        self.proc: asyncio.subprocess.Process | None = None
        self.session_id: str | None = profile.resume_session_id
        self.turns = 0
        self._pump_task: asyncio.Task | None = None

    @property
    def alive(self) -> bool:
        return self.proc is not None and self.proc.returncode is None

    async def start(self) -> None:
        if self.alive:
            return
        argv = _wrap_for_platform(build_argv(self.profile, self.claude_bin))
        env = {**os.environ, **self.profile.env_overrides()}
        self.proc = await asyncio.create_subprocess_exec(
            *argv,
            cwd=self.profile.cwd or None,
            env=env,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )
        self._pump_task = asyncio.create_task(self._pump())

    async def _pump(self) -> None:
        assert self.proc and self.proc.stdout
        while True:
            line = await self.proc.stdout.readline()
            if not line:
                break
            self._log(line)
            try:
                event: dict[str, Any] = json.loads(line.decode("utf-8", "replace"))
            except json.JSONDecodeError:
                continue
            if event.get("type") == "system" and event.get("subtype") == "init":
                self.session_id = str(event.get("session_id") or self.session_id or "") or None
            await self.on_event(self, event)

    def _log(self, line: bytes) -> None:
        if not self.transcript_path:
            return
        try:
            self.transcript_path.parent.mkdir(parents=True, exist_ok=True)
            with self.transcript_path.open("ab") as fh:
                fh.write(line)
        except OSError:
            pass

    async def send(self, text: str) -> None:
        if not self.alive or not self.proc or not self.proc.stdin:
            raise RuntimeError(f"session {self.profile.name} 未运行")
        self.proc.stdin.write(user_message(text))
        await self.proc.stdin.drain()

    async def close(self) -> None:
        if self.proc and self.proc.returncode is None:
            try:
                if self.proc.stdin:
                    self.proc.stdin.close()
                await asyncio.wait_for(self.proc.wait(), timeout=5)
            except (OSError, asyncio.TimeoutError, ProcessLookupError):
                self.proc.kill()
        if self._pump_task:
            self._pump_task.cancel()
