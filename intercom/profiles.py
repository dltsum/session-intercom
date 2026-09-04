# -*- coding: utf-8 -*-
"""Session profiles: per-session API endpoint, key, model and effort.

Profiles live in ``~/.session-intercom/profiles.json``:

{
  "alice": {
    "base_url": "https://api.anthropic.com",
    "auth_token": "sk-ant-...",
    "model": "claude-opus-5",
    "effort": "high",
    "cwd": "C:/work/proj",
    "preamble": "你是后端负责人。",
    "resume_session_id": null
  }
}

base_url / auth_token are injected as ANTHROPIC_BASE_URL / ANTHROPIC_AUTH_TOKEN
into that session's process environment only. The file is created with
owner-only permissions; never commit it.
"""
from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass
from pathlib import Path

DEFAULT_STATE_DIR = Path.home() / ".session-intercom"
EFFORTS = ("low", "medium", "high", "xhigh", "max")


class ProfileError(ValueError):
    pass


@dataclass
class Profile:
    name: str
    base_url: str | None = None
    auth_token: str | None = None
    model: str | None = None
    effort: str | None = None
    cwd: str | None = None
    preamble: str | None = None
    resume_session_id: str | None = None

    def validate(self) -> None:
        if not self.name or not self.name.replace("-", "").replace("_", "").isalnum():
            raise ProfileError(f"非法 session 名: {self.name!r}（仅限字母数字与 - _）")
        if self.effort is not None and self.effort not in EFFORTS:
            raise ProfileError(f"effort 必须是 {EFFORTS} 之一, 得到 {self.effort!r}")

    def env_overrides(self) -> dict[str, str]:
        env: dict[str, str] = {}
        if self.base_url:
            env["ANTHROPIC_BASE_URL"] = self.base_url
        if self.auth_token:
            env["ANTHROPIC_AUTH_TOKEN"] = self.auth_token
        return env


class ProfileStore:
    def __init__(self, state_dir: Path | None = None):
        self.dir = state_dir or DEFAULT_STATE_DIR
        self.path = self.dir / "profiles.json"

    def _read(self) -> dict:
        try:
            return json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {}

    def list(self) -> list[Profile]:
        return [Profile(name=k, **v) for k, v in self._read().items()]

    def get(self, name: str) -> Profile | None:
        raw = self._read().get(name)
        return Profile(name=name, **raw) if raw is not None else None

    def set(self, profile: Profile) -> None:
        profile.validate()
        self.dir.mkdir(parents=True, exist_ok=True)
        data = self._read()
        entry = asdict(profile)
        entry.pop("name")
        data[profile.name] = entry
        self.path.write_text(json.dumps(data, ensure_ascii=False, indent=1), encoding="utf-8")
        if os.name != "nt":
            os.chmod(self.path, 0o600)

    def delete(self, name: str) -> bool:
        data = self._read()
        if name not in data:
            return False
        del data[name]
        self.path.write_text(json.dumps(data, ensure_ascii=False, indent=1), encoding="utf-8")
        return True
