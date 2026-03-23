import os
import asyncio
from pathlib import Path
from datetime import datetime
import mimetypes
import json

from dotenv import load_dotenv
from telethon import TelegramClient, events, errors, types


# telegram_forwarder.py
# ------------------------------------------------------------
# 功能：
# - 監聽 SOURCE_CHATS 訊息
# - 將文字 / 媒體轉發去 TARGET_CHATS
# - 每日 log rotation
# - sender resolve 做穩定化處理：
#   1) 優先 get_sender/get_chat
#   2) user -> username/full name/id
#   3) chat/channel sender -> username/title/id
#   4) post_author / sender_id fallback
# ------------------------------------------------------------


# ── env & init ───────────────────────────────────────────────

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


def _parse_int_set(env_name):
    return {
        int(x.strip())
        for x in os.getenv(env_name, "").split(",")
        if x.strip() and x.strip().lstrip("-").isdigit()
    }


TARGET_CHATS = _parse_id_list("TARGET_CHAT")
SOURCE_CHATS = set(_parse_id_list("SOURCE_CHATS"))

DOWNLOAD_MODE = os.getenv("DOWNLOAD_MODE", "auto").lower()   # auto / always / never
DELAY_SECONDS = int(os.getenv("DELAY_SECONDS", "5"))
DOWNLOAD_DIR = Path(os.getenv("DOWNLOAD_DIR", "./downloads")).expanduser()

IGNORE_USERS = {
    u.strip().lower()
    for u in os.getenv("IGNORE_USERS", "").split(",")
    if u.strip()
}
IGNORE_IDS = _parse_int_set("IGNORE_IDS")

LOG_DIR = DOWNLOAD_DIR / "log"
LOG_DIR.mkdir(parents=True, exist_ok=True)

client = TelegramClient(SESSION_NAME, API_ID, API_HASH)


# ── helper funcs ─────────────────────────────────────────────

async def throttle():
    if DELAY_SECONDS > 0:
        await asyncio.sleep(DELAY_SECONDS)


def tstamp() -> str:
    return datetime.now().isoformat(timespec="seconds")


def log(obj: dict):
    line = json.dumps(obj, ensure_ascii=False, default=str)
    print(line)
    try:
        today_file = LOG_DIR / f"{datetime.now().strftime('%Y-%m-%d')}.log"
        with today_file.open("a", encoding="utf-8") as fh:
            fh.write(line + "\n")
    except Exception as e:
        print(f"[LOG_ERR] {e}")


async def run_api(coro):
    try:
        return await coro
    except errors.FloodWaitError as e:
        log({
            "ts": tstamp(),
            "type": "warn",
            "op": "flood_wait",
            "seconds": e.seconds,
        })
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


def sanitize_filename_part(value: str) -> str:
    if not value:
        return "unknown"
    cleaned = "".join(c for c in value if c.isalnum() or c in ("@", "_", "-", ".", " "))
    cleaned = cleaned.strip().replace(" ", "_")
    return cleaned[:80] or "unknown"


def build_filename(msg, sender_display="unknown"):
    safe_sender = sanitize_filename_part(sender_display)
    ext = ""

    if msg.document and msg.file and msg.file.name:
        ext = Path(msg.file.name).suffix
    elif msg.file and msg.file.mime_type:
        ext = mimetypes.guess_extension(msg.file.mime_type) or ""

    return f"{msg.id}_{safe_sender}{ext}"


async def resolve_sender_info(event: events.NewMessage.Event) -> dict:
    """
    盡量穩定地 resolve sender/chat。
    注意：
    - sender 未必係 User，可能係 Chat/Channel
    - sender 有機會冇 username
    - 某些情況只得 sender_id / post_author
    """
    msg = event.message

    chat = event.chat
    if chat is None:
        try:
            chat = await event.get_chat()
        except Exception as e:
            log({
                "ts": tstamp(),
                "type": "warn",
                "op": "get_chat",
                "err": e.__class__.__name__,
                "msg": str(e),
            })
            chat = None

    sender = msg.sender
    if sender is None:
        try:
            sender = await event.get_sender()
        except Exception as e:
            log({
                "ts": tstamp(),
                "type": "warn",
                "op": "get_sender",
                "err": e.__class__.__name__,
                "msg": str(e),
            })
            sender = None

    chat_title = getattr(chat, "title", None) or str(event.chat_id)

    info = {
        "chat_obj": chat,
        "chat_id": event.chat_id,
        "chat_title": chat_title,

        "sender_obj": sender,
        "sender_id": getattr(sender, "id", None) or getattr(msg, "sender_id", None),
        "sender_type": "unknown",

        "sender_username": None,
        "sender_display": "user",

        "post_author": getattr(msg, "post_author", None),
        "raw_sender_class": sender.__class__.__name__ if sender is not None else None,
    }

    # Case 1: 普通 user
    if isinstance(sender, types.User):
        info["sender_type"] = "user"
        info["sender_username"] = sender.username

        if sender.username:
            info["sender_display"] = f"@{sender.username}"
        else:
            full_name = " ".join(
                x for x in [sender.first_name, sender.last_name] if x
            ).strip()
            info["sender_display"] = full_name or f"user:{sender.id}"

        return info

    # Case 2: sender 其實係 chat / channel
    if isinstance(sender, (types.Chat, types.Channel)):
        info["sender_type"] = "chat"

        chat_username = getattr(sender, "username", None)
        title = getattr(sender, "title", None)

        info["sender_username"] = chat_username

        if chat_username:
            info["sender_display"] = f"@{chat_username}"
        else:
            info["sender_display"] = f"[{title or sender.id}]"

        return info

    # Case 3: post_author（常見於 channel post/comment 某些情況）
    if info["post_author"]:
        info["sender_type"] = "post_author"
        info["sender_display"] = str(info["post_author"])
        return info

    # Case 4: 只有 sender_id
    if getattr(msg, "sender_id", None):
        info["sender_type"] = "sender_id_only"
        info["sender_display"] = f"id:{msg.sender_id}"
        return info

    # Case 5: 連 sender_id 都冇
    info["sender_type"] = "anonymous_or_unknown"
    info["sender_display"] = "anonymous"
    return info


async def build_header(event: events.NewMessage.Event) -> str:
    info = await resolve_sender_info(event)
    return f"[{info['chat_title']}] {info['sender_display']}"


def should_ignore(info: dict) -> bool:
    sender_username = info.get("sender_username")
    sender_id = info.get("sender_id")

    if sender_username and sender_username.lower() in IGNORE_USERS:
        return True

    if sender_id is not None and sender_id in IGNORE_IDS:
        return True

    return False


# ── main handler ─────────────────────────────────────────────

@client.on(events.NewMessage(incoming=True))
async def handle(event: events.NewMessage.Event):
    msg = event.message

    if SOURCE_CHATS and event.chat_id not in SOURCE_CHATS:
        return

    info = await resolve_sender_info(event)

    log({
        "ts": tstamp(),
        "type": "in",
        "chat": event.chat_id,
        "msg": msg.id,
        "media": media_type(msg),
        "preview": (msg.text or msg.message or "")[:120],
        "sender_type": info["sender_type"],
        "sender_id": info["sender_id"],
        "sender_username": info["sender_username"],
        "sender_display": info["sender_display"],
        "post_author": info["post_author"],
        "raw_sender_class": info["raw_sender_class"],
    })

    if should_ignore(info):
        log({
            "ts": tstamp(),
            "type": "info",
            "note": "ignored",
            "sender_id": info["sender_id"],
            "sender_username": info["sender_username"],
            "sender_display": info["sender_display"],
        })
        return

    hdr = f"[{info['chat_title']}] {info['sender_display']}"

    # ── TEXT ONLY ───────────────────────────────────────────
    if msg.media is None:
        combined = f"{hdr}\n{msg.text or msg.message or '[empty]'}"
        for tgt in TARGET_CHATS:
            await throttle()
            await run_api(client.send_message(tgt, combined))
            log({
                "ts": tstamp(),
                "type": "out",
                "op": "send_text",
                "dst": tgt,
                "src_msg": msg.id,
            })
        return

    # ── MEDIA: forward first ────────────────────────────────
    if DOWNLOAD_MODE != "always":
        try:
            for tgt in TARGET_CHATS:
                await throttle()
                await run_api(client.send_message(tgt, hdr))
                await run_api(client.forward_messages(tgt, msg, msg.peer_id))
                log({
                    "ts": tstamp(),
                    "type": "out",
                    "op": "forward",
                    "dst": tgt,
                    "src_msg": msg.id,
                })
            return

        except errors.ChatForwardsRestrictedError:
            log({
                "ts": tstamp(),
                "type": "warn",
                "op": "forward",
                "note": "chat_forwards_restricted",
                "src_msg": msg.id,
            })

        except errors.RPCError as e:
            log({
                "ts": tstamp(),
                "type": "err",
                "op": "forward",
                "err": e.__class__.__name__,
                "msg": getattr(e, "message", str(e)),
                "src_msg": msg.id,
            })

    # ── MEDIA: try copy_messages if available ───────────────
    if DOWNLOAD_MODE != "always" and hasattr(client, "copy_messages"):
        try:
            for tgt in TARGET_CHATS:
                await throttle()
                await run_api(client.send_message(tgt, hdr))
                await run_api(client.copy_messages(tgt, msg.peer_id, [msg.id]))
                log({
                    "ts": tstamp(),
                    "type": "out",
                    "op": "copy",
                    "dst": tgt,
                    "src_msg": msg.id,
                })
            return

        except errors.RPCError as e:
            log({
                "ts": tstamp(),
                "type": "err",
                "op": "copy",
                "err": e.__class__.__name__,
                "msg": getattr(e, "message", str(e)),
                "src_msg": msg.id,
            })

    # ── MEDIA: fallback download + send_file ────────────────
    if DOWNLOAD_MODE != "never":
        dl_dir = DOWNLOAD_DIR / str(event.chat_id)
        dl_dir.mkdir(parents=True, exist_ok=True)

        file_name = build_filename(msg, info["sender_display"])
        save_path = dl_dir / file_name

        fpath = await msg.download_media(save_path)

        log({
            "ts": tstamp(),
            "type": "save",
            "file": str(fpath),
            "src_msg": msg.id,
        })

        caption = f"{hdr}\n{msg.text or msg.message or ''}".strip()

        for tgt in TARGET_CHATS:
            await throttle()
            await run_api(client.send_file(tgt, fpath, caption=caption))
            log({
                "ts": tstamp(),
                "type": "out",
                "op": "send_file",
                "dst": tgt,
                "src_msg": msg.id,
                "file": str(fpath),
            })


# ── run ─────────────────────────────────────────────────────

async def main():
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    print("✔ Telegram forwarder running — Ctrl+C to stop…")
    async with client:
        await client.run_until_disconnected()


if __name__ == "__main__":
    asyncio.run(main())