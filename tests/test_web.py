# -*- coding: utf-8 -*-
"""E2E tests for the web console: real HTTP + SSE against a live broker."""
import asyncio
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from intercom.broker import Broker
from intercom.profiles import Profile, ProfileStore
from intercom.web import serve_web

if os.name == "nt":
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

FAKE_BIN = f'{sys.executable} {Path(__file__).with_name("fake_claude.py")}'


async def http(port, method, path, body=None, timeout=10):
    """Minimal async HTTP client returning (status_line, parsed_json_or_bytes)."""
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    payload = json.dumps(body).encode() if body is not None else b""
    req = (f"{method} {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n"
           f"Content-Length: {len(payload)}\r\n\r\n").encode() + payload
    writer.write(req)
    await writer.drain()
    raw = await asyncio.wait_for(reader.read(-1), timeout)
    writer.close()
    head, _, resp_body = raw.partition(b"\r\n\r\n")
    status = head.split(b"\r\n", 1)[0].decode()
    if b"application/json" in head:
        return status, json.loads(resp_body)
    return status, resp_body


async def wait_for(cond, timeout=15.0, interval=0.05):
    elapsed = 0.0
    while elapsed < timeout:
        if cond():
            return True
        await asyncio.sleep(interval)
        elapsed += interval
    return False


class WebTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self.tmp.name)
        self.store = ProfileStore(self.dir)
        self.store.set(Profile(name="alice", model="m", effort="low", cwd=str(self.dir)))
        self.broker = Broker(profiles=self.store, state_dir=self.dir, claude_bin=FAKE_BIN)

    def tearDown(self):
        self.tmp.cleanup()

    async def asyncTearDown(self):
        for name in list(self.broker.sessions):
            await self.broker.stop(name)

    async def _start(self):
        server = await serve_web(self.broker, port=0)
        self.addAsyncCleanup(self._stop_server, server)
        return server.sockets[0].getsockname()[1]

    async def _stop_server(self, server):
        server.close()
        await server.wait_closed()

    async def test_index_page(self):
        port = await self._start()
        status, body = await http(port, "GET", "/")
        self.assertIn("200", status)
        self.assertIn("Intercom", body.decode("utf-8"))

    async def test_profile_set_masks_token_in_state(self):
        port = await self._start()
        status, res = await http(port, "POST", "/api/profile_set", {
            "name": "bob", "base_url": "https://proxy.example.com",
            "auth_token": "sk-secret-999", "model": "m2", "effort": "high"})
        self.assertTrue(res["ok"])
        _, state = await http(port, "GET", "/api/state")
        bob = [p for p in state["profiles"] if p["name"] == "bob"][0]
        self.assertTrue(bob["has_token"])
        self.assertNotIn("sk-secret-999", json.dumps(state))  # 密钥绝不出现在 API 响应里

    async def test_send_via_http_and_sse_receives(self):
        port = await self._start()
        # SSE 订阅
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        writer.write(b"GET /api/events HTTP/1.1\r\nHost: x\r\n\r\n")
        await writer.drain()
        await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), 5)

        status, res = await http(port, "POST", "/api/send", {"name": "alice", "text": "你好"})
        self.assertTrue(res["ok"])

        got = []
        async def collect():
            buf = b""
            while len([g for g in got if g.get("type") == "turn_end"]) < 1:
                chunk = await asyncio.wait_for(reader.read(4096), 15)
                if not chunk:
                    break
                buf += chunk
                while b"\n\n" in buf:
                    frame, _, buf = buf.partition(b"\n\n")
                    for line in frame.split(b"\n"):
                        if line.startswith(b"data: "):
                            got.append(json.loads(line[6:]))
        await collect()
        writer.close()
        kinds = [g["type"] for g in got]
        self.assertIn("sent", kinds)
        self.assertIn("text", kinds)
        self.assertIn("turn_end", kinds)
        text_ev = [g for g in got if g["type"] == "text"]
        self.assertIn("echo: 你好", "".join(e.get("text", "") for e in text_ev))

    async def test_transcript_endpoint(self):
        port = await self._start()
        await http(port, "POST", "/api/send", {"name": "alice", "text": "记住这句话"})
        self.assertTrue(await wait_for(lambda: self.broker.sessions["alice"].turns >= 1))
        status, res = await http(port, "GET", "/api/transcript?name=alice")
        self.assertTrue(res["ok"])
        self.assertIn("echo: 记住这句话", "".join(res["texts"]))

    async def test_state_after_link(self):
        port = await self._start()
        self.store.set(Profile(name="bob", cwd=str(self.dir)))
        await http(port, "POST", "/api/link", {"a": "alice", "b": "bob"})
        _, state = await http(port, "GET", "/api/state")
        self.assertEqual(state["links"][0]["hops"], 0)
        self.assertFalse(state["links"][0]["paused"])


if __name__ == "__main__":
    unittest.main()
