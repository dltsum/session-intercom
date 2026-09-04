# -*- coding: utf-8 -*-
"""A fake `claude` CLI speaking stream-json, for tests.

Protocol emulation:
- on start: emit a system/init event with a fresh session id
- per user message on stdin: emit an assistant event then a result event
- text starting with "!intercom <to> <msg>" produces an intercom fenced block
- everything else is echoed as "echo: <text>"
"""
import json
import sys
import uuid

SID = str(uuid.uuid4())


def emit(obj):
    sys.stdout.write(json.dumps(obj, ensure_ascii=False) + "\n")
    sys.stdout.flush()


def extract_text(msg):
    try:
        content = msg["message"]["content"]
        return "".join(c.get("text", "") for c in content if isinstance(c, dict))
    except (KeyError, TypeError):
        return ""


def main():
    emit({"type": "system", "subtype": "init", "session_id": SID})
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            msg = json.loads(line)
        except json.JSONDecodeError:
            continue
        text = extract_text(msg)
        if text.startswith("!intercom "):
            _, to, payload = text.split(" ", 2)
            reply = '```intercom\n{"to": "%s", "text": "%s"}\n```' % (to, payload)
        else:
            reply = "echo: " + text
        emit({"type": "assistant", "message": {"role": "assistant",
              "content": [{"type": "text", "text": reply}]}})
        emit({"type": "result", "subtype": "success", "session_id": SID})


if __name__ == "__main__":
    main()
