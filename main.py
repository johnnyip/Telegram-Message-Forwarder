import os
import asyncio
from pathlib import Path
from datetime import datetime
import mimetypes
import json
import uuid
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv
from telethon import TelegramClient, events, errors, types


load_dotenv()

API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
SESSION_NAME = os.getenv("SESSION_NAME", "my_account")

DOWNLOAD_DIR = Path(os.getenv("DOWNLOAD_DIR", "./downloads")).expanduser()
LOG_DIR = DOWNLOAD_DIR / "log"
SPOOL_DIR = DOWNLOAD_DIR / "_spool"

DELAY_SECONDS = int(os.getenv("DELAY_SECONDS", "5"))              # text delay
MEDIA_DELAY_SECONDS = int(os.getenv("MEDIA_DELAY_SECONDS", "0"))  # media delay
WORKER_CONCURRENCY = max(1, int(os.getenv("WORKER_CONCURRENCY", "1")))
QUEUE_MAXSIZE = max(0, int(os.getenv("QUEUE_MAXSIZE", "1000")))
DELETE_AFTER_SEND = os.getenv("DELETE_AFTER_SEND", "true").strip().lower() in {"1", "true", "yes", "y"}

IGNORE_USERS = {
    u.strip().lower().lstrip("@")
    for u in os.getenv("IGNORE_USERS", "").split(",")
    if u.strip()
}
IGNORE_IDS = {
    int(x.strip())
    for x in os.getenv("IGNORE_IDS", "").split(",")
    if x.strip() and x.strip().lstrip("-").isdigit()
}

LOG_DIR.mkdir(parents=True, exist_ok=True)
SPOOL_DIR.mkdir(parents=True, exist_ok=True)

client = TelegramClient(SESSION_NAME, API_ID, API_HASH)
client.parse_mode = "md"

event_queue: asyncio.Queue = asyncio.Queue(maxsize=QUEUE_MAXSIZE if QUEUE_MAXSIZE > 0 else 0)


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


def safe_str(value) -> str:
    return "null" if value is None or value == "" else str(value)


def sanitize_filename_part(value: str) -> str:
    if not value:
        return "unknown"
    cleaned = "".join(c for c in str(value) if c.isalnum() or c in ("@", "_", "-", ".", " ", "(", ")"))
    cleaned = cleaned.strip().replace(" ", "_")
    return cleaned[:100] or "unknown"


def parse_int_or_str(value: Any):
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        v = value.strip()
        if v.lstrip("-").isdigit():
            return int(v)
        return v
    return value


def media_type(m):
    if m.media is None:
        return "text"
    return (
        "photo" if m.photo else
        "video" if m.video else
        "video_note" if getattr(m, "video_note", None) else
        "voice" if m.voice else
        "audio" if m.audio else
        "animation" if m.animation else
        "document" if m.document else
        "other"
    )


def build_filename_from_message(msg, sender_display="unknown"):
    safe_sender = sanitize_filename_part(sender_display)
    ext = ""

    if msg.document and msg.file and msg.file.name:
        ext = Path(msg.file.name).suffix
    elif msg.file and msg.file.mime_type:
        ext = mimetypes.guess_extension(msg.file.mime_type) or ""

    return f"{msg.id}_{safe_sender}{ext}"


def build_filename_from_snapshot(msg_id: Any, sender_display="unknown", original_name: Optional[str] = None, mime_type: Optional[str] = None):
    safe_sender = sanitize_filename_part(sender_display)
    ext = ""
    if original_name:
        ext = Path(original_name).suffix
    elif mime_type:
        ext = mimetypes.guess_extension(mime_type) or ""
    return f"{msg_id}_{safe_sender}{ext}"


async def throttle(seconds: int):
    if seconds > 0:
        await asyncio.sleep(seconds)


async def run_api(coro, op: str, extra: Optional[dict] = None):
    extra = extra or {}
    try:
        return await coro
    except errors.FloodWaitError as e:
        log({
            "ts": tstamp(),
            "type": "warn",
            "op": op,
            "err": "FloodWaitError",
            "seconds": e.seconds,
            **extra,
        })
        await asyncio.sleep(e.seconds + 1)
        return await coro
    except errors.RPCError as e:
        log({
            "ts": tstamp(),
            "type": "err",
            "op": op,
            "err": e.__class__.__name__,
            "msg": getattr(e, "message", str(e)),
            **extra,
        })
        raise
    except Exception as e:
        log({
            "ts": tstamp(),
            "type": "err",
            "op": op,
            "err": e.__class__.__name__,
            "msg": str(e),
            **extra,
        })
        raise


def load_routes() -> List[Dict[str, Any]]:
    routes_json = os.getenv("ROUTES_JSON", "").strip()
    if not routes_json:
        raise RuntimeError("ROUTES_JSON is required")

    try:
        raw_routes = json.loads(routes_json)
        if not isinstance(raw_routes, list):
            raise ValueError("ROUTES_JSON must be a JSON array")
    except Exception as e:
        raise RuntimeError(f"Invalid ROUTES_JSON: {e}") from e

    routes: List[Dict[str, Any]] = []
    for idx, raw in enumerate(raw_routes, start=1):
        if not isinstance(raw, dict):
            raise RuntimeError(f"Route #{idx} must be an object")

        sources = [parse_int_or_str(x) for x in raw.get("sources", [])]
        targets = [parse_int_or_str(x) for x in raw.get("targets", [])]

        if not sources:
            raise RuntimeError(f"Route #{idx} has no sources")
        if not targets:
            raise RuntimeError(f"Route #{idx} has no targets")

        routes.append({
            "name": str(raw.get("name", f"route_{idx}")),
            "sources": set(sources),
            "targets": targets,
        })

    return routes


ROUTES = load_routes()


def find_matching_routes(chat_id: Any) -> List[Dict[str, Any]]:
    return [route for route in ROUTES if chat_id in route["sources"]]


async def resolve_sender_info_from_message(event_message, chat_id_hint=None) -> dict:
    msg = event_message

    chat = None
    sender = None

    if hasattr(msg, "get_chat"):
        try:
            chat = await msg.get_chat()
        except Exception:
            chat = None

    if hasattr(msg, "get_sender"):
        try:
            sender = await msg.get_sender()
        except Exception:
            sender = None

    chat_id = chat_id_hint if chat_id_hint is not None else getattr(msg, "chat_id", None)
    chat_title = getattr(chat, "title", None) or str(chat_id)
    chat_username = getattr(chat, "username", None)

    if isinstance(chat, types.Channel):
        chat_type = "channel" if getattr(chat, "broadcast", False) else "supergroup"
    elif isinstance(chat, types.Chat):
        chat_type = "group"
    elif isinstance(chat, types.User):
        chat_type = "private"
    else:
        chat_type = "unknown"

    info = {
        "chat_obj": chat,
        "chat_id": chat_id,
        "chat_title": chat_title,
        "chat_username": chat_username,
        "chat_type": chat_type,
        "raw_chat_class": chat.__class__.__name__ if chat is not None else None,

        "sender_obj": sender,
        "sender_id": getattr(sender, "id", None) or getattr(msg, "sender_id", None),
        "sender_type": "unknown",
        "sender_username": None,
        "sender_display": "user",
        "sender_first_name": None,
        "sender_last_name": None,

        "post_author": getattr(msg, "post_author", None),
        "raw_sender_class": sender.__class__.__name__ if sender is not None else None,

        "msg_id": msg.id,
        "msg_date": getattr(msg, "date", None),
        "edit_date": getattr(msg, "edit_date", None),
        "grouped_id": getattr(msg, "grouped_id", None),
        "reply_to_msg_id": getattr(getattr(msg, "reply_to", None), "reply_to_msg_id", None),
    }

    if isinstance(sender, types.User):
        info["sender_type"] = "user"
        info["sender_username"] = sender.username
        info["sender_first_name"] = sender.first_name
        info["sender_last_name"] = sender.last_name

        if sender.username:
            info["sender_display"] = f"@{sender.username}"
        else:
            full_name = " ".join(
                x for x in [sender.first_name, sender.last_name] if x
            ).strip()
            info["sender_display"] = full_name or f"user:{sender.id}"
        return info

    if isinstance(sender, (types.Chat, types.Channel)):
        info["sender_type"] = "chat"
        chat_sender_username = getattr(sender, "username", None)
        chat_sender_title = getattr(sender, "title", None)
        info["sender_username"] = chat_sender_username

        if chat_sender_username:
            info["sender_display"] = f"@{chat_sender_username}"
        else:
            info["sender_display"] = f"[{chat_sender_title or sender.id}]"
        return info

    if info["post_author"]:
        info["sender_type"] = "post_author"
        info["sender_display"] = str(info["post_author"])
        return info

    if getattr(msg, "sender_id", None):
        info["sender_type"] = "sender_id_only"
        info["sender_display"] = f"id:{msg.sender_id}"
        return info

    info["sender_type"] = "anonymous_or_unknown"
    info["sender_display"] = "anonymous"
    return info


def md_code(value: Any) -> str:
    s = str(value)
    s = s.replace("\\", "\\\\").replace("`", "\\`")
    return f"`{s}`"


def escape_md_link_text(text: Any) -> str:
    s = str(text)
    for ch in ["\\", "[", "]", "(", ")"]:
        s = s.replace(ch, f"\\{ch}")
    return s


def build_chat_message_url(info: dict) -> Optional[str]:
    chat_username = info.get("chat_username")
    chat_id = info.get("chat_id")
    msg_id = info.get("msg_id")

    if not msg_id:
        return None

    if chat_username:
        return f"https://t.me/{chat_username}/{msg_id}"

    if isinstance(chat_id, int):
        s = str(chat_id)
        if s.startswith("-100"):
            return f"https://t.me/c/{s[4:]}/{msg_id}"

    return None


def format_copyable_identity_lines(info: dict) -> str:
    lines = []

    sender_display = info.get("sender_display")
    sender_username = info.get("sender_username")
    sender_id = info.get("sender_id")

    normalized_display = None
    if sender_display:
        normalized_display = str(sender_display).strip().lower()

    normalized_username_display = None
    if sender_username:
        normalized_username_display = f"@{str(sender_username).strip().lower()}"

    if sender_display:
        lines.append(md_code(sender_display))

    if sender_username and normalized_display != normalized_username_display:
        lines.append(md_code(sender_username))

    if sender_id is not None:
        lines.append(md_code(sender_id))

    return "\n".join(lines)


def build_header_from_info(info: dict, is_edit=False) -> str:
    chat_title = safe_str(info.get("chat_title"))
    url = build_chat_message_url(info)

    if url:
        group_line = f"[{escape_md_link_text(chat_title)}]({url})"
    else:
        group_line = f"[{escape_md_link_text(chat_title)}]"

    identity_lines = format_copyable_identity_lines(info)

    if is_edit:
        if identity_lines:
            return f"{group_line}\n`[EDITED]`\n{identity_lines}"
        return f"{group_line}\n`[EDITED]`"

    if identity_lines:
        return f"{group_line}\n{identity_lines}"
    return group_line


def should_ignore(info: dict) -> bool:
    sender_username = info.get("sender_username")
    sender_id = info.get("sender_id")

    if sender_username and sender_username.lower().lstrip("@") in IGNORE_USERS:
        return True

    if sender_id is not None and sender_id in IGNORE_IDS:
        return True

    return False


async def snapshot_media_message(msg, info: dict, source_kind: str) -> Optional[dict]:
    """
    收到 media 即刻落地快照，避免之後被 edit/replace。
    """
    sender_display = info.get("sender_display", "unknown")
    file_name = build_filename_from_message(msg, sender_display)

    chat_id = info.get("chat_id")
    msg_id = info.get("msg_id")
    unique = uuid.uuid4().hex[:8]

    spool_dir = SPOOL_DIR / source_kind / str(chat_id)
    spool_dir.mkdir(parents=True, exist_ok=True)

    save_path = spool_dir / f"{msg_id}_{unique}_{file_name}"

    extra = {
        "chat_id": chat_id,
        "chat_title": info.get("chat_title"),
        "msg_id": msg_id,
        "source_kind": source_kind,
        "save_path": str(save_path),
        "media_type": media_type(msg),
    }

    fpath = await run_api(msg.download_media(save_path), op="snapshot_download_media", extra=extra)
    if not fpath:
        log({
            "ts": tstamp(),
            "type": "err",
            "op": "snapshot_download_media",
            "msg": "download returned empty path",
            **extra,
        })
        return None

    fpath = Path(fpath)

    snapshot = {
        "path": str(fpath),
        "original_name": getattr(getattr(msg, "file", None), "name", None),
        "mime_type": getattr(getattr(msg, "file", None), "mime_type", None),
        "media_type": media_type(msg),
        "caption_text": (msg.text or msg.message or "").strip(),
    }

    log({
        "ts": tstamp(),
        "type": "snapshot",
        "source_kind": source_kind,
        "chat_id": chat_id,
        "chat_title": info.get("chat_title"),
        "msg_id": msg_id,
        "sender_id": info.get("sender_id"),
        "sender_username": info.get("sender_username"),
        "sender_display": info.get("sender_display"),
        "media_type": snapshot["media_type"],
        "file": str(fpath),
    })

    return snapshot


async def enqueue_text_payload(msg, info: dict, routes: List[Dict[str, Any]], source_kind: str):
    payload = {
        "queue_type": "text",
        "source_kind": source_kind,   # new / edited
        "routes": [r["name"] for r in routes],
        "info": info,
        "text": msg.text or msg.message or "[empty]",
    }
    await event_queue.put(payload)


async def enqueue_media_payload(msg, info: dict, routes: List[Dict[str, Any]], source_kind: str):
    snapshot = await snapshot_media_message(msg, info, source_kind)
    if not snapshot:
        return

    payload = {
        "queue_type": "media",
        "source_kind": source_kind,   # new / edited
        "routes": [r["name"] for r in routes],
        "info": info,
        "snapshot": snapshot,
    }
    await event_queue.put(payload)


def route_name_map() -> Dict[str, Dict[str, Any]]:
    return {r["name"]: r for r in ROUTES}


ROUTE_MAP = route_name_map()


async def send_text_to_target(route: Dict[str, Any], tgt: Any, combined: str, info: dict, source_kind: str):
    extra = {
        "route": route["name"],
        "dst": tgt,
        "src_msg": info["msg_id"],
        "chat_id": info["chat_id"],
        "chat_title": info["chat_title"],
        "sender_id": info["sender_id"],
        "sender_username": info["sender_username"],
        "sender_display": info["sender_display"],
        "source_kind": source_kind,
    }
    try:
        await throttle(DELAY_SECONDS)
        await run_api(client.send_message(tgt, combined), op="send_text", extra=extra)
        log({
            "ts": tstamp(),
            "type": "out",
            "op": "send_text",
            "status": "ok",
            **extra,
        })
    except Exception:
        pass


async def send_file_to_target(route: Dict[str, Any], tgt: Any, fpath: Path, caption: str, info: dict, source_kind: str):
    extra = {
        "route": route["name"],
        "dst": tgt,
        "src_msg": info["msg_id"],
        "chat_id": info["chat_id"],
        "chat_title": info["chat_title"],
        "sender_id": info["sender_id"],
        "sender_username": info["sender_username"],
        "sender_display": info["sender_display"],
        "file": str(fpath),
        "source_kind": source_kind,
    }
    try:
        await throttle(MEDIA_DELAY_SECONDS)
        await run_api(client.send_file(tgt, fpath, caption=caption), op="send_file", extra=extra)
        log({
            "ts": tstamp(),
            "type": "out",
            "op": "send_file",
            "status": "ok",
            **extra,
        })
    except Exception:
        pass


async def process_payload(payload: dict):
    info = payload["info"]

    if should_ignore(info):
        log({
            "ts": tstamp(),
            "type": "info",
            "note": "ignored",
            "source_kind": payload["source_kind"],
            "chat_id": info["chat_id"],
            "chat_title": info["chat_title"],
            "chat_username": info["chat_username"],
            "sender_id": info["sender_id"],
            "sender_username": info["sender_username"],
            "sender_display": info["sender_display"],
            "sender_type": info["sender_type"],
        })
        return

    is_edit = payload["source_kind"] == "edited"
    hdr = build_header_from_info(info, is_edit=is_edit)

    routes = [ROUTE_MAP[name] for name in payload["routes"] if name in ROUTE_MAP]
    if not routes:
        return

    if payload["queue_type"] == "text":
        body = payload["text"]
        combined = f"{hdr}\n{body}"
        for route in routes:
            for tgt in route["targets"]:
                await send_text_to_target(route, tgt, combined, info, payload["source_kind"])
        return

    if payload["queue_type"] == "media":
        snapshot = payload["snapshot"]
        fpath = Path(snapshot["path"])
        caption_body = snapshot.get("caption_text", "").strip()
        caption = f"{hdr}\n{caption_body}".strip()

        for route in routes:
            for tgt in route["targets"]:
                await send_file_to_target(route, tgt, fpath, caption, info, payload["source_kind"])

        if DELETE_AFTER_SEND:
            try:
                if fpath.exists():
                    fpath.unlink(missing_ok=True)
                    log({
                        "ts": tstamp(),
                        "type": "cleanup",
                        "op": "delete_temp_file",
                        "file": str(fpath),
                        "src_msg": info["msg_id"],
                        "source_kind": payload["source_kind"],
                    })
            except Exception as e:
                log({
                    "ts": tstamp(),
                    "type": "warn",
                    "op": "delete_temp_file",
                    "file": str(fpath),
                    "src_msg": info["msg_id"],
                    "source_kind": payload["source_kind"],
                    "err": e.__class__.__name__,
                    "msg": str(e),
                })
        return


async def worker(worker_id: int):
    log({
        "ts": tstamp(),
        "type": "info",
        "note": "worker_started",
        "worker_id": worker_id,
    })

    while True:
        payload = await event_queue.get()
        try:
            await process_payload(payload)
        except Exception as e:
            log({
                "ts": tstamp(),
                "type": "err",
                "op": "worker_process_payload",
                "worker_id": worker_id,
                "err": e.__class__.__name__,
                "msg": str(e),
            })
        finally:
            event_queue.task_done()


async def handle_incoming_message(msg, source_kind: str):
    routes = find_matching_routes(msg.chat_id)
    if not routes:
        return

    info = await resolve_sender_info_from_message(msg, chat_id_hint=msg.chat_id)

    log({
        "ts": tstamp(),
        "type": "in" if source_kind == "new" else "edit",
        "matched_routes": [r["name"] for r in routes],

        "chat_id": info["chat_id"],
        "chat_title": info["chat_title"],
        "chat_username": info["chat_username"],
        "chat_type": info["chat_type"],
        "raw_chat_class": info["raw_chat_class"],

        "msg": info["msg_id"],
        "media": media_type(msg),
        "preview": (msg.text or msg.message or "")[:160],

        "sender_type": info["sender_type"],
        "sender_id": info["sender_id"],
        "sender_username": info["sender_username"],
        "sender_display": info["sender_display"],
        "sender_first_name": info["sender_first_name"],
        "sender_last_name": info["sender_last_name"],

        "post_author": info["post_author"],
        "raw_sender_class": info["raw_sender_class"],

        "msg_date": info["msg_date"],
        "edit_date": info["edit_date"],
        "grouped_id": info["grouped_id"],
        "reply_to_msg_id": info["reply_to_msg_id"],
    })

    if msg.media is None:
        await enqueue_text_payload(msg, info, routes, source_kind)
    else:
        await enqueue_media_payload(msg, info, routes, source_kind)


@client.on(events.NewMessage(incoming=True))
async def on_new_message(event: events.NewMessage.Event):
    try:
        await handle_incoming_message(event.message, source_kind="new")
    except Exception as e:
        log({
            "ts": tstamp(),
            "type": "err",
            "op": "on_new_message",
            "chat_id": getattr(event, "chat_id", None),
            "msg_id": getattr(event.message, "id", None),
            "err": e.__class__.__name__,
            "msg": str(e),
        })


@client.on(events.MessageEdited(incoming=True))
async def on_message_edited(event: events.MessageEdited.Event):
    try:
        await handle_incoming_message(event.message, source_kind="edited")
    except Exception as e:
        log({
            "ts": tstamp(),
            "type": "err",
            "op": "on_message_edited",
            "chat_id": getattr(event, "chat_id", None),
            "msg_id": getattr(event.message, "id", None),
            "err": e.__class__.__name__,
            "msg": str(e),
        })


def print_route_summary():
    summary = []
    for route in ROUTES:
        summary.append({
            "name": route["name"],
            "sources": list(route["sources"]),
            "targets": route["targets"],
        })
    print(json.dumps({
        "startup": "ok",
        "worker_concurrency": WORKER_CONCURRENCY,
        "text_delay_seconds": DELAY_SECONDS,
        "media_delay_seconds": MEDIA_DELAY_SECONDS,
        "delete_after_send": DELETE_AFTER_SEND,
        "ignore_users": sorted(list(IGNORE_USERS)),
        "ignore_ids": sorted(list(IGNORE_IDS)),
        "routes": summary,
    }, ensure_ascii=False, indent=2))


async def main():
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    SPOOL_DIR.mkdir(parents=True, exist_ok=True)

    print_route_summary()
    print("✔ Telegram forwarder running — Ctrl+C to stop…")

    workers = [asyncio.create_task(worker(i + 1)) for i in range(WORKER_CONCURRENCY)]

    async with client:
        try:
            await client.run_until_disconnected()
        finally:
            for w in workers:
                w.cancel()
            await asyncio.gather(*workers, return_exceptions=True)


if __name__ == "__main__":
    asyncio.run(main())