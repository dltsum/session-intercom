# session-intercom

Real-time cross-session intercom for **Claude Code** — let multiple Claude
sessions talk to each other, each with its own **API endpoint, key, model and
effort** preset.

A local broker daemon runs one persistent bidirectional process per session
(`claude -p --input-format stream-json --output-format stream-json`) and routes
messages between them in real time. No polling, no mailbox delay: a message to
a session is written to its stdin immediately and its reply is routed as soon
as the turn completes.

- **实时双向对话** — `link alice bob` bridges two sessions turn-by-turn
- **自主寻址** — a session can message another by emitting an intercom block
- **每 session 独立 API 预设** — `ANTHROPIC_BASE_URL` / `ANTHROPIC_AUTH_TOKEN`
  injected per process: different sessions can use different providers
- **防死循环** — links pause after N consecutive auto-forwards without outside
  input; any external message re-arms them
- Zero dependencies: Python 3.10+ standard library only

## Install

```bash
git clone https://github.com/dltsum/session-intercom.git
cd session-intercom
pip install -e .          # or run from the repo root: python -m intercom ...
python -m unittest discover -s tests   # 7 E2E tests with a fake claude binary
```

Requires the `claude` CLI (Claude Code) installed. Auth comes from each
session profile (or the CLI's own login when no token is set).

## Quickstart

```bash
# 1. Register two sessions with different providers / models / effort
intercom profile-set alice --model claude-opus-5 --effort high \
    --base-url https://api.anthropic.com --token sk-ant-... \
    --cwd C:/work/backend --preamble "你是后端负责人。"
intercom profile-set bob --model claude-sonnet-5 --effort medium \
    --base-url https://my-proxy.example.com --token sk-... \
    --cwd C:/work/frontend --preamble "你是前端负责人。"

# 2. Start the broker
intercom broker

# 3. Talk
intercom send alice "和 bob 对齐一下 API 字段命名"
intercom link alice bob        # hands-free duplex; pauses after 20 hops
intercom tail                  # watch the live event stream
intercom list                  # who is alive, which session id, which profile
```

## How sessions talk

**Human-injected**: `intercom send <name> <text>` writes a user message into
that session's stream (auto-spawning its process if needed).

**Autonomous addressing**: every session's system prompt teaches it to emit

    ```intercom
    {"to": "bob", "text": "字段名用 snake_case，可以吗？"}
    ```

The broker parses these blocks from assistant output and routes them
immediately as `[intercom from alice] ...`.

**Duplex links**: `intercom link alice bob` forwards each completed turn's full
reply to the peer. To stop two polite agents from chatting forever, a link
pauses after `--max-hops` (default 20) consecutive auto-forwards with no
outside input; any `send` to either side resets the counter.

## Per-session API presets

`profile-set` stores into `~/.session-intercom/profiles.json` (chmod 600 on
POSIX; never commit it):

| field | injected as |
|---|---|
| `base_url` | `ANTHROPIC_BASE_URL` (that process only) |
| `auth_token` | `ANTHROPIC_AUTH_TOKEN` (that process only) |
| `model` | `--model` |
| `effort` | `--effort` (low/medium/high/xhigh/max) |
| `cwd` | process working directory |
| `preamble` | appended to the session's system prompt |
| `resume_session_id` | `--resume <id>` — attach to an existing conversation |

## CLI reference

| command | what it does |
|---|---|
| `broker [--port N]` | run the daemon (foreground; Ctrl-C stops) |
| `profile-set / profile-list / profile-del` | manage per-session API presets |
| `spawn / stop <name>` | start / stop a session process |
| `send <name> <text>` | inject a message (auto-spawns if needed) |
| `link / unlink <a> <b>` | duplex bridge between two sessions |
| `list` | live sessions, links, hop counters |
| `tail [name]` | stream live events (text, turn ends, spawns) |

State lives in `~/.session-intercom/` (`broker.json`, `profiles.json`,
per-session NDJSON transcripts in `logs/`). The broker listens on
**127.0.0.1 only** — nothing is exposed to the network.

## Layout

```text
intercom/
├── protocol.py   # NDJSON wire format (localhost TCP)
├── profiles.py   # per-session endpoint/key/model/effort registry
├── process.py    # ManagedSession: stream-json process plumbing
├── broker.py     # TCP server, routing, links, hop guard, pub-sub
└── cli.py        # broker / profile-* / spawn / send / link / tail / ...
tests/
├── fake_claude.py    # stream-json emulator (echo + intercom blocks)
└── test_intercom.py  # 7 E2E tests over real subprocess pipes
```

## License

MIT
