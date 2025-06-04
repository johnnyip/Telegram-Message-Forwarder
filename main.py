import os
import asyncio
from pathlib import Path
from datetime import datetime
import mimetypes
import json

from dotenv import load_dotenv
from telethon import TelegramClient, events, errors

"""telegram_forwarder_revamped.py (Telethon v5.3)
=================================================
更新
----
• **Daily log rotation** ─ 每日寫入 `<DOWNLOAD_DIR>/log/YYYY-MM-DD.log`。
• 其餘功能同 v5.2 一致。
"""

# ── env & init ──────────────────────────────────────────────────────────────

load_dotenv()

API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
SESSION_NAME = os.getenv("SESSION_NAME", "my_account")


def _parse_id_list(env_name):
    return [
        int(x) if x.lstrip("-").isdigit() else x
        for x in (y.strip() for y in os.getenv(env_name, "").split(","))
        if x
    ]

TARGET_CHATS = _parse_id_list("TARGET_CHAT")
SOURCE_CHATS = set(_parse_id_list("SOURCE_CHATS"))

DOWNLOAD_MODE = os.getenv("DOWNLOAD_MODE", "auto").lower()
DELAY_SECONDS = int(os.getenv("DELAY_SECONDS", "5"))
DOWNLOAD_DIR = Path(os.getenv("DOWNLOAD_DIR", "./downloads")).expanduser()
IGNORE_USERS = {u.strip().lower() for u in os.getenv("IGNORE_USERS", "").split(",") if u.strip()}

# ── prepare log path ───────────────────────────────────────────────────────

LOG_DIR = DOWNLOAD_DIR / "log"
LOG_DIR.mkdir(parents=True, exist_ok=True)

client = TelegramClient(SESSION_NAME, API_ID, API_HASH)

# ── helper funcs ────────────────────────────────────────────────────────────

async def throttle():
    if DELAY_SECONDS:
        await asyncio.sleep(DELAY_SECONDS)


def tstamp() -> str:
    return datetime.now().isoformat(timespec="seconds")


def log(obj: dict):
    line = json.dumps(obj, ensure_ascii=False)
    print(line)  # stdout
    try:
        today_file = LOG_DIR / f"{datetime.now().strftime('%Y-%m-%d')}.log"
        with today_file.open("a", encoding="utf-8") as fh:
            fh.write(line + "\n")
    except Exception as e:
        print(f"[LOG_ERR] {e}")


def build_filename(msg):
    sender = (
        msg.sender.username if getattr(msg.sender, "username", None) else "unknown"
    )
    ext = ""
    if msg.document and msg.file and msg.file.name:
        ext = Path(msg.file.name).suffix
    elif msg.file and msg.file.mime_type:
        ext = mimetypes.guess_extension(msg.file.mime_type) or ""
    return f"{msg.id}_{sender}{ext}"


async def run_api(coro):
    try:
        return await coro
    except errors.FloodWaitError as e:
        await asyncio.sleep(e.seconds + 1)
        return await coro


def media_type(m):
    if m.media is None:
        return "text"
    return (
        "photo" if m.photo else
        "video" if m.video else
        "document" if m.document else
        "audio" if m.audio else
        "voice" if m.voice else
        "animation" if m.animation else
        "other"
    )


def header(event, msg):
    chat_title = getattr(event.chat, "title", str(event.chat_id))
    username = (
        f"@{msg.sender.username}" if getattr(msg.sender, "username", None) else "user"
    )
    return f"[{chat_title}] {username}"


# ── main handler ────────────────────────────────────────────────────────────

@client.on(events.NewMessage(incoming=True))
async def handle(event: events.NewMessage.Event):
    msg = event.message

    if SOURCE_CHATS and event.chat_id not in SOURCE_CHATS:
        return

    log({"ts": tstamp(), "type": "in", "chat": event.chat_id, "msg": msg.id, "media": media_type(msg), "preview": (msg.text or msg.message or "")[:60]})

    if msg.sender and msg.sender.username and msg.sender.username.lower() in IGNORE_USERS:
        log({"ts": tstamp(), "type": "info", "note": "ignored", "user": msg.sender.username})
        return

    hdr = header(event, msg)

    # ── TEXT ONLY ──────────────────────────────────────────────────────────
    if msg.media is None:
        combined = f"{hdr}\n{msg.text or msg.message or '[empty]'}"
        for tgt in TARGET_CHATS:
            await throttle()
            await run_api(client.send_message(tgt, combined))
            log({"ts": tstamp(), "type": "out", "op": "send_text", "dst": tgt})
        return

    # ── MEDIA: forward/copy then fallback ─────────────────────────────────
    if DOWNLOAD_MODE != "always":
        try:
            for tgt in TARGET_CHATS:
                await throttle()
                await run_api(client.send_message(tgt, hdr))
                await run_api(client.forward_messages(tgt, msg, msg.peer_id))
                log({"ts": tstamp(), "type": "out", "op": "forward", "dst": tgt, "msg": msg.id})
            return
        except errors.ChatForwardsRestrictedError:
            pass
        except errors.RPCError as e:
            log({"ts": tstamp(), "type": "err", "op": "forward", "err": e.__class__.__name__, "msg": e.message})

    if DOWNLOAD_MODE != "always" and hasattr(client, "copy_messages"):
        try:
            for tgt in TARGET_CHATS:
                await throttle()
                await run_api(client.send_message(tgt, hdr))
                await run_api(client.copy_messages(tgt, msg.peer_id, [msg.id]))
                log({"ts": tstamp(), "type": "out", "op": "copy", "dst": tgt, "msg": msg.id})
            return
        except errors.RPCError as e:
            log({"ts": tstamp(), "type": "err", "op": "copy", "err": e.__class__.__name__, "msg": e.message})

    if DOWNLOAD_MODE != "never":
        dl_dir = DOWNLOAD_DIR / str(event.chat_id)
        dl_dir.mkdir(parents=True, exist_ok=True)
        fpath = await msg.download_media(dl_dir / build_filename(msg))
        log({"ts": tstamp(), "type": "save", "file": str(fpath)})

        caption = f"{hdr}\n{msg.text or msg.message or ''}"
        for tgt in TARGET_CHATS:
            await throttle()
            await run_api(client.send_file(tgt, fpath, caption=caption.strip()))
            log({"ts": tstamp(), "type": "out", "op": "send_file", "dst": tgt})

# ── run ─────────────────────────────────────────────────────────────────────

async def main():
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    print("✔ Telegram forwarder (Telethon) running — Ctrl+C to stop…")
    async with client:
        await client.run_until_disconnected()

if __name__ == "__main__":
    asyncio.run(main())
