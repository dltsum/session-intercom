# -*- coding: utf-8 -*-
"""End-to-end tests: real broker, real subprocess pipes, fake claude binary."""
import asyncio
import os
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from intercom import protocol
from intercom.broker import Broker, serve
from intercom.profiles import Profile, ProfileStore

if os.name == "nt":
    # subprocess support requires the proactor loop
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

FAKE_BIN = f'{sys.executable} {Path(__file__).with_name("fake_claude.py")}'


async def wait_for(cond, timeout=15.0, interval=0.05):
    elapsed = 0.0
    while elapsed < timeout:
        if cond():
            return True
        await asyncio.sleep(interval)
        elapsed += interval
    return False


class IntercomTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self.tmp.name)
        self.store = ProfileStore(self.dir)
        for name in ("alice", "bob"):
            self.store.set(Profile(name=name, model="m", effort="low", cwd=str(self.dir)))
        self.events = []
        self.broker = Broker(profiles=self.store, state_dir=self.dir, claude_bin=FAKE_BIN)
        orig = self.broker._broadcast

        async def collect(ev):
            self.events.append(dict(ev))
            await orig(ev)

        self.broker._broadcast = collect

    def tearDown(self):
        self.tmp.cleanup()

    async def asyncTearDown(self):
        for name in list(self.broker.sessions):
            await self.broker.stop(name)

    def texts(self, name):
        return "".join(e.get("text", "") for e in self.events
                       if e.get("type") == "text" and e.get("name") == name)

    def sent_to(self, name):
        return [e["text"] for e in self.events if e.get("type") == "sent" and e.get("name") == name]

    async def test_spawn_send_echo(self):
        res = await self.broker.spawn("alice")
        self.assertTrue(res["ok"])
        self.assertTrue(await wait_for(lambda: self.broker.sessions["alice"].session_id))
        await self.broker.send("alice", "你好")
        self.assertTrue(await wait_for(lambda: "echo: 你好" in self.texts("alice")))

    async def test_autonomous_intercom_routing(self):
        await self.broker.spawn("alice")
        await self.broker.send("alice", "!intercom bob hello-bob")
        # bob 应被自动拉起并收到 alice 的 intercom 消息，然后 echo 它
        self.assertTrue(await wait_for(lambda: "[intercom from alice] hello-bob" in "".join(self.sent_to("bob"))))
        self.assertTrue(await wait_for(lambda: "echo: [intercom from alice] hello-bob" in self.texts("bob")))

    async def test_link_duplex_and_hop_guard(self):
        await self.broker.link("alice", "bob", max_hops=4)
        await self.broker.send("alice", "ping")
        # 双方开始互相转发
        self.assertTrue(await wait_for(lambda: any("[from alice]" in t and "echo: ping" in t for t in self.sent_to("bob"))))
        # 回声循环最终在 hop 上限处暂停
        self.assertTrue(await wait_for(
            lambda: any(e.get("type") == "link_paused" for e in self.events), timeout=30))
        link = list(self.broker.links.values())[0]
        self.assertTrue(link.paused)
        self.assertGreaterEqual(link.hops, 5)
        # 外部输入解除暂停
        await self.broker.send("alice", "再来一轮", outside_input=True)
        self.assertFalse(link.paused)
        self.assertEqual(link.hops, 0)

    async def test_link_does_not_leak_intercom_block(self):
        """intercom 块被路由后，link 仍会转发完整回合文本（含块）——接受这一语义，
        但块本身必须触发独立路由。这里验证两种机制不互相吞掉。"""
        await self.broker.link("alice", "bob", max_hops=2)
        await self.broker.send("alice", "!intercom bob direct-msg")
        self.assertTrue(await wait_for(lambda: "[intercom from alice] direct-msg" in "".join(self.sent_to("bob"))))

    async def test_tcp_list_smoke(self):
        server = await serve(self.broker, port=0)
        port = server.sockets[0].getsockname()[1]
        async with server:
            serve_task = asyncio.create_task(server.serve_forever())
            reader, writer = await asyncio.open_connection(protocol.HOST, port)
            writer.write(protocol.encode({"op": "list"}))
            await writer.drain()
            line = await asyncio.wait_for(reader.readline(), 5)
            msg = protocol.decode(line)
            self.assertTrue(msg["ok"])
            self.assertIn("sessions", msg)
            writer.close()
            serve_task.cancel()
            try:
                await serve_task
            except asyncio.CancelledError:
                pass

    async def test_send_autospawns_missing_session(self):
        res = await self.broker.send("bob", "起床干活")
        self.assertTrue(res["ok"])
        self.assertTrue(await wait_for(lambda: "echo: 起床干活" in self.texts("bob")))

    async def test_profile_env_injection(self):
        """profile 的 base_url/token 必须只注入对应进程的环境。"""
        self.store.set(Profile(name="carol", base_url="https://proxy.example.com",
                               auth_token="sk-test-123", cwd=str(self.dir)))
        res = await self.broker.spawn("carol")
        self.assertTrue(res["ok"])
        sess = self.broker.sessions["carol"]
        self.assertEqual(sess.profile.env_overrides(),
                         {"ANTHROPIC_BASE_URL": "https://proxy.example.com",
                          "ANTHROPIC_AUTH_TOKEN": "sk-test-123"})
        await self.broker.stop("carol")


if __name__ == "__main__":
    unittest.main()
