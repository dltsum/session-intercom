# -*- coding: utf-8 -*-
"""intercom CLI.

    intercom profile-set alice [--base-url U] [--token K] [--model M] [--effort E] [--cwd D] [--preamble T]
    intercom profile-list
    intercom broker [--port N]             # foreground daemon
    intercom spawn alice
    intercom send alice "继续昨天的话题"
    intercom link alice bob [--max-hops 20]
    intercom unlink alice bob
    intercom list
    intercom tail [name]                   # stream live events
    intercom stop alice
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path

from . import protocol
from .broker import Broker, serve
from .profiles import DEFAULT_STATE_DIR, Profile, ProfileError, ProfileStore


def _broker_port(state_dir: Path) -> int:
    try:
        return int(json.loads((state_dir / "broker.json").read_text(encoding="utf-8"))["port"])
    except (OSError, json.JSONDecodeError, KeyError, ValueError):
        return protocol.DEFAULT_PORT


async def _request(state_dir: Path, payload: dict, tail: bool = False, name_filter: str | None = None) -> int:
    try:
        reader, writer = await asyncio.open_connection(protocol.HOST, _broker_port(state_dir))
    except OSError:
        print("无法连接 broker。先运行: intercom broker", file=sys.stderr)
        return 1
    writer.write(protocol.encode(payload))
    await writer.drain()
    while True:
        line = await reader.readline()
        if not line:
            break
        msg = protocol.decode(line)
        if "event" in msg:
            ev = msg["event"]
            if name_filter and ev.get("name") not in (None, name_filter):
                continue
            label = ev.get("type", "?")
            name = ev.get("name", "")
            text = (ev.get("text") or "")[:200].replace("\n", " ")
            print(f"[{name or '-'}] {label} {text}")
            continue
        if not tail:
            print(json.dumps(msg, ensure_ascii=False, indent=1))
            writer.close()
            return 0 if msg.get("ok") else 2
    return 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="intercom", description="Claude Code 跨 session 实时通信")
    ap.add_argument("--state-dir", help="覆盖 ~/.session-intercom")
    ap.add_argument("--claude-bin", default="claude")
    sub = ap.add_subparsers(dest="cmd", required=True)

    p_ps = sub.add_parser("profile-set", help="注册/更新一个 session 的 API 预设")
    p_ps.add_argument("name")
    p_ps.add_argument("--base-url")
    p_ps.add_argument("--token", help="ANTHROPIC_AUTH_TOKEN，仅写入本机 profiles.json")
    p_ps.add_argument("--model")
    p_ps.add_argument("--effort", choices=["low", "medium", "high", "xhigh", "max"])
    p_ps.add_argument("--cwd")
    p_ps.add_argument("--preamble", help="该 session 的人设/职责提示")
    p_ps.add_argument("--resume", dest="resume_session_id", help="绑定已有 claude session id")

    sub.add_parser("profile-list", help="列出全部 profile（不显示 token）")
    p_pd = sub.add_parser("profile-del", help="删除 profile")
    p_pd.add_argument("name")

    p_br = sub.add_parser("broker", help="前台运行 broker 守护进程")
    p_br.add_argument("--port", type=int, default=protocol.DEFAULT_PORT)
    p_br.add_argument("--web-port", type=int, default=9780, help="网页控制台端口，0 关闭")
    p_br.add_argument("--no-open", action="store_true", help="不自动打开浏览器")

    for cmd, help_text in [("spawn", "启动 session 进程"), ("stop", "停止 session 进程")]:
        p = sub.add_parser(cmd, help=help_text)
        p.add_argument("name")
    p_send = sub.add_parser("send", help="向 session 发送消息（未运行则自动启动）")
    p_send.add_argument("name")
    p_send.add_argument("text")
    p_link = sub.add_parser("link", help="双向桥接两个 session（轮回复转发）")
    p_link.add_argument("a")
    p_link.add_argument("b")
    p_link.add_argument("--max-hops", type=int, default=20)
    p_ulink = sub.add_parser("unlink", help="解除桥接")
    p_ulink.add_argument("a")
    p_ulink.add_argument("b")
    sub.add_parser("list", help="列出运行中的 session 与链接")
    p_tail = sub.add_parser("tail", help="实时事件流")
    p_tail.add_argument("name", nargs="?")

    args = ap.parse_args(argv)
    state_dir = Path(args.state_dir) if args.state_dir else DEFAULT_STATE_DIR
    store = ProfileStore(state_dir)

    if args.cmd == "profile-set":
        try:
            store.set(Profile(
                name=args.name, base_url=args.base_url, auth_token=args.token,
                model=args.model, effort=args.effort, cwd=args.cwd,
                preamble=args.preamble, resume_session_id=args.resume_session_id,
            ))
        except ProfileError as exc:
            print(f"profile 错误: {exc}", file=sys.stderr)
            return 2
        print(f"OK: profile {args.name} 已保存到 {store.path}")
        return 0

    if args.cmd == "profile-list":
        for p in store.list():
            print(f"{p.name}: model={p.model or '-'} effort={p.effort or '-'} "
                  f"base_url={p.base_url or '-'} token={'已设置' if p.auth_token else '-'} cwd={p.cwd or '-'}")
        return 0

    if args.cmd == "profile-del":
        print("OK" if store.delete(args.name) else f"不存在: {args.name}")
        return 0

    if args.cmd == "broker":
        broker = Broker(profiles=store, state_dir=state_dir, claude_bin=args.claude_bin)

        async def _run() -> None:
            server = await serve(broker, args.port)
            print(f"[intercom] broker 监听 {protocol.HOST}:{server.sockets[0].getsockname()[1]}（Ctrl-C 停止）")
            if args.web_port:
                from .web import serve_web
                web = await serve_web(broker, args.web_port)
                url = f"http://127.0.0.1:{web.sockets[0].getsockname()[1]}"
                print(f"[intercom] 网页控制台: {url}")
                if not args.no_open:
                    import webbrowser
                    webbrowser.open(url)
            async with server:
                await server.serve_forever()

        try:
            asyncio.run(_run())
        except KeyboardInterrupt:
            pass
        return 0

    ops = {
        "spawn": {"op": "spawn", "name": getattr(args, "name", None)},
        "stop": {"op": "stop", "name": getattr(args, "name", None)},
        "send": {"op": "send", "name": getattr(args, "name", None), "text": getattr(args, "text", "")},
        "link": {"op": "link", "a": getattr(args, "a", ""), "b": getattr(args, "b", ""),
                 "max_hops": getattr(args, "max_hops", 20)},
        "unlink": {"op": "unlink", "a": getattr(args, "a", ""), "b": getattr(args, "b", "")},
        "list": {"op": "list"},
    }
    if args.cmd == "tail":
        return asyncio.run(_request(state_dir, {"op": "tail"}, tail=True, name_filter=args.name))
    return asyncio.run(_request(state_dir, ops[args.cmd]))


if __name__ == "__main__":
    raise SystemExit(main())
