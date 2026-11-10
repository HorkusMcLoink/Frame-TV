#!/usr/bin/env python3
import asyncio
import json
import os
import re
import subprocess
from collections import deque
from datetime import datetime, timezone

from samsungtvws.async_art import SamsungTVAsyncArt

TV_IP = os.getenv("TV_IP", "192.168.5.81")
TOKEN_FILE = os.getenv("TV_TOKEN_FILE", "/home/adam/.config/samsungtvws/token.txt")
POLL_SECONDS = float(os.getenv("POLL_SECONDS", "2"))
CORRELATE_WINDOW = float(os.getenv("CORRELATE_WINDOW", "15"))
OUT_FILE = os.getenv("OUT_FILE", "/var/log/artmode-flap.log")

KEYWORDS = (
    "artmode", "upload", "set_artmode", "Start Monitoring", "SIGINT/SIGTERM",
    "failed to upload", "checking directory", "Started force-art"
)

recent_events = deque(maxlen=200)

def now_iso():
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")

def log_line(msg: str):
    line = f"{now_iso()} {msg}"
    print(line, flush=True)
    with open(OUT_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")

async def watch_journal():
    cmd = [
        "journalctl", "-f", "-u", "slideshow.service", "-u", "force-art.service",
        "-o", "json", "--no-pager"
    ]
    proc = await asyncio.create_subprocess_exec(
        *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL
    )

    while True:
        raw = await proc.stdout.readline()
        if not raw:
            await asyncio.sleep(0.1)
            continue
        try:
            obj = json.loads(raw.decode("utf-8", errors="ignore"))
        except json.JSONDecodeError:
            continue

        unit = obj.get("_SYSTEMD_UNIT", "?")
        msg = obj.get("MESSAGE", "")
        if not any(k.lower() in msg.lower() for k in KEYWORDS):
            continue

        ts_us = int(obj.get("__REALTIME_TIMESTAMP", "0") or 0)
        ts = ts_us / 1_000_000 if ts_us else datetime.now().timestamp()
        recent_events.append((ts, unit, msg))
        log_line(f'event unit={unit} msg="{msg}"')

async def monitor_tv():
    tv = SamsungTVAsyncArt(host=TV_IP, token_file=TOKEN_FILE)
    last_state = None

    while True:
        try:
            state = await tv.in_artmode()
        except Exception as e:
            log_line(f"tv_poll_error error={type(e).__name__} detail={e}")
            await asyncio.sleep(POLL_SECONDS)
            continue

        if state != last_state:
            t_now = datetime.now().timestamp()
            cause = "unknown"
            for ts, unit, msg in reversed(recent_events):
                if t_now - ts <= CORRELATE_WINDOW:
                    cause = f'{unit}: "{msg}"'
                    break
            log_line(f"ARTMODE_CHANGE new_state={state} likely_cause={cause}")
            last_state = state

        await asyncio.sleep(POLL_SECONDS)

async def main():
    log_line("flap monitor started")
    await asyncio.gather(watch_journal(), monitor_tv())

if __name__ == "__main__":
    asyncio.run(main())
