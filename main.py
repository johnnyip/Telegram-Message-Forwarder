import os
import asyncio
from pathlib import Path
from datetime import datetime
import mimetypes
import json
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv
from telethon import TelegramClient, events, errors, types


load_dotenv()

API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
SESSION_NAME = os.getenv("SESSION_NAME", "my_account")

DOWNLOAD_DIR = Path(os.getenv("DOWNLOAD_DIR", "./downloads")).expanduser()
LOG_DIR = DOWNLOAD_DIR / "log"

DOWNLOAD_MODE = os.getenv("DOWNLOAD_MODE", "auto").lower()   # auto / always / never
DELAY_SECONDS = int(os.getenv("DELAY_SECONDS", "5"))
WORKER_CONCURRENCY = max(1, int(os.getenv("WORKER_CONCURRENCY", "1")))
QUEUE_MAXSIZE = max(0, int(os.getenv("QUEUE_MAXSIZE", "1000")))
DELETE_AFTER_SEND = os.getenv("DELETE_AFTER_SEND", "false").strip().lower() in {"1", "true", "yes", "y"}

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
    cleaned = "".join(c for c in value if c.isalnum() or c in ("@", "_", "-", ".", " ", "(", ")"))
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
        "document" if m.document else
        "audio" if m.audio else
        "voice" if m.voice else
        "animation" if m.animation else
        "other"
    )


def build_filename(msg, sender_display="unknown"):
    # 注意：filename 用原始 sender_display，唔用 markdown/code format
    safe_sender = sanitize_filename_part(sender_display)
    ext = ""

    if msg.document and msg.file and msg.file.name:
        ext = Path(msg.file.name).suffix
    elif msg.file and msg.file.mime_type:
        ext = mimetypes.guess_extension(msg.file.mime_type) or ""

    return f"{msg.id}_{safe_sender}{ext}"


async def throttle():
    if DELAY_SECONDS > 0:
        await asyncio.sleep(DELAY_SECONDS)


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
    matched = []
    for route in ROUTES:
        if chat_id in route["sources"]:
            matched.append(route)
    return matched


async def resolve_sender_info(event: events.NewMessage.Event) -> dict:
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
                "chat_id": event.chat_id,
                "msg_id": msg.id,
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
                "chat_id": event.chat_id,
                "msg_id": msg.id,
            })
            sender = None

    chat_title = getattr(chat, "title", None) or str(event.chat_id)
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
        "chat_id": event.chat_id,
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

    # 如果 sender_display 已經等於 @username，就唔重複再出 username
    if sender_username and normalized_display != normalized_username_display:
        lines.append(md_code(sender_username))

    if sender_id is not None:
        lines.append(md_code(sender_id))

    return "\n".join(lines)


def build_header_from_info(info: dict) -> str:
    group_line = f"[{safe_str(info.get('chat_title'))}]"
    identity_lines = format_copyable_identity_lines(info)

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


async def send_text_to_target(route: Dict[str, Any], tgt: Any, combined: str, info: dict):
    extra = {
        "route": route["name"],
        "dst": tgt,
        "src_msg": info["msg_id"],
        "chat_id": info["chat_id"],
        "chat_title": info["chat_title"],
        "sender_id": info["sender_id"],
        "sender_username": info["sender_username"],
        "sender_display": info["sender_display"],
    }
    try:
        await throttle()
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


async def send_header_and_forward(route: Dict[str, Any], tgt: Any, hdr: str, msg, info: dict) -> bool:
    extra = {
        "route": route["name"],
        "dst": tgt,
        "src_msg": info["msg_id"],
        "chat_id": info["chat_id"],
        "chat_title": info["chat_title"],
        "sender_id": info["sender_id"],
        "sender_username": info["sender_username"],
        "sender_display": info["sender_display"],
    }
    try:
        await throttle()
        await run_api(client.send_message(tgt, hdr), op="send_header_before_forward", extra=extra)
        await run_api(client.forward_messages(tgt, msg, msg.peer_id), op="forward", extra=extra)
        log({
            "ts": tstamp(),
            "type": "out",
            "op": "forward",
            "status": "ok",
            **extra,
        })
        return True
    except errors.ChatForwardsRestrictedError:
        log({
            "ts": tstamp(),
            "type": "warn",
            "op": "forward",
            "note": "chat_forwards_restricted",
            **extra,
        })
        return False
    except Exception:
        return False


async def send_header_and_copy(route: Dict[str, Any], tgt: Any, hdr: str, msg, info: dict) -> bool:
    extra = {
        "route": route["name"],
        "dst": tgt,
        "src_msg": info["msg_id"],
        "chat_id": info["chat_id"],
        "chat_title": info["chat_title"],
        "sender_id": info["sender_id"],
        "sender_username": info["sender_username"],
        "sender_display": info["sender_display"],
    }
    try:
        await throttle()
        await run_api(client.send_message(tgt, hdr), op="send_header_before_copy", extra=extra)
        await run_api(client.copy_messages(tgt, msg.peer_id, [msg.id]), op="copy", extra=extra)
        log({
            "ts": tstamp(),
            "type": "out",
            "op": "copy",
            "status": "ok",
            **extra,
        })
        return True
    except Exception:
        return False


async def send_file_to_target(route: Dict[str, Any], tgt: Any, fpath: Path, caption: str, info: dict):
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
    }
    try:
        await throttle()
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


async def process_route(route: Dict[str, Any], event: events.NewMessage.Event, info: dict):
    msg = event.message

    if should_ignore(info):
        log({
            "ts": tstamp(),
            "type": "info",
            "note": "ignored",
            "route": route["name"],
            "chat_id": info["chat_id"],
            "chat_title": info["chat_title"],
            "chat_username": info["chat_username"],
            "sender_id": info["sender_id"],
            "sender_username": info["sender_username"],
            "sender_display": info["sender_display"],
            "sender_type": info["sender_type"],
        })
        return

    hdr = build_header_from_info(info)

    if msg.media is None:
        body = msg.text or msg.message or "[empty]"
        combined = f"{hdr}\n{body}"
        for tgt in route["targets"]:
            await send_text_to_target(route, tgt, combined, info)
        return

    if DOWNLOAD_MODE != "always":
        forward_success_count = 0
        for tgt in route["targets"]:
            ok = await send_header_and_forward(route, tgt, hdr, msg, info)
            if ok:
                forward_success_count += 1

        if forward_success_count == len(route["targets"]) and route["targets"]:
            return

    if DOWNLOAD_MODE != "always":
        copy_success_count = 0
        for tgt in route["targets"]:
            ok = await send_header_and_copy(route, tgt, hdr, msg, info)
            if ok:
                copy_success_count += 1

        if copy_success_count == len(route["targets"]) and route["targets"]:
            return

    if DOWNLOAD_MODE != "never":
        dl_dir = DOWNLOAD_DIR / route["name"] / str(event.chat_id)
        dl_dir.mkdir(parents=True, exist_ok=True)

        # filename 用原始 display，唔受 markdown header 影響
        file_name = build_filename(msg, info.get("sender_display", "unknown"))
        save_path = dl_dir / file_name

        extra = {
            "route": route["name"],
            "chat_id": info["chat_id"],
            "chat_title": info["chat_title"],
            "src_msg": info["msg_id"],
            "save_path": str(save_path),
        }

        try:
            fpath = await run_api(msg.download_media(save_path), op="download_media", extra=extra)
            if not fpath:
                log({
                    "ts": tstamp(),
                    "type": "err",
                    "op": "download_media",
                    "msg": "download returned empty path",
                    **extra,
                })
                return

            fpath = Path(fpath)
            log({
                "ts": tstamp(),
                "type": "save",
                "route": route["name"],
                "src_msg": info["msg_id"],
                "chat_id": info["chat_id"],
                "chat_title": info["chat_title"],
                "sender_id": info["sender_id"],
                "sender_username": info["sender_username"],
                "sender_display": info["sender_display"],
                "file": str(fpath),
            })

            caption_body = (msg.text or msg.message or "").strip()
            caption = f"{hdr}\n{caption_body}".strip()

            for tgt in route["targets"]:
                await send_file_to_target(route, tgt, fpath, caption, info)

        finally:
            if DELETE_AFTER_SEND:
                try:
                    if save_path.exists():
                        save_path.unlink(missing_ok=True)
                        log({
                            "ts": tstamp(),
                            "type": "cleanup",
                            "op": "delete_temp_file",
                            "route": route["name"],
                            "file": str(save_path),
                            "src_msg": info["msg_id"],
                        })
                except Exception as e:
                    log({
                        "ts": tstamp(),
                        "type": "warn",
                        "op": "delete_temp_file",
                        "route": route["name"],
                        "file": str(save_path),
                        "src_msg": info["msg_id"],
                        "err": e.__class__.__name__,
                        "msg": str(e),
                    })


async def process_event(event: events.NewMessage.Event):
    msg = event.message
    routes = find_matching_routes(event.chat_id)

    if not routes:
        return

    info = await resolve_sender_info(event)

    log({
        "ts": tstamp(),
        "type": "in",
        "matched_routes": [r["name"] for r in routes],

        "chat_id": info["chat_id"],
        "chat_title": info["chat_title"],
        "chat_username": info["chat_username"],
        "chat_type": info["chat_type"],
        "raw_chat_class": info["raw_chat_class"],

        "msg": msg.id,
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

    for route in routes:
        try:
            await process_route(route, event, info)
        except Exception as e:
            log({
                "ts": tstamp(),
                "type": "err",
                "op": "process_route",
                "route": route["name"],
                "chat_id": info["chat_id"],
                "chat_title": info["chat_title"],
                "src_msg": info["msg_id"],
                "err": e.__class__.__name__,
                "msg": str(e),
            })


async def worker(worker_id: int):
    log({
        "ts": tstamp(),
        "type": "info",
        "note": "worker_started",
        "worker_id": worker_id,
    })

    while True:
        event = await event_queue.get()
        try:
            await process_event(event)
        except Exception as e:
            log({
                "ts": tstamp(),
                "type": "err",
                "op": "worker_process_event",
                "worker_id": worker_id,
                "err": e.__class__.__name__,
                "msg": str(e),
            })
        finally:
            event_queue.task_done()


@client.on(events.NewMessage(incoming=True))
async def handle(event: events.NewMessage.Event):
    routes = find_matching_routes(event.chat_id)
    if not routes:
        return

    try:
        await event_queue.put(event)
    except Exception as e:
        log({
            "ts": tstamp(),
            "type": "err",
            "op": "queue_put",
            "chat_id": event.chat_id,
            "msg_id": event.message.id,
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
        "download_mode": DOWNLOAD_MODE,
        "delay_seconds": DELAY_SECONDS,
        "delete_after_send": DELETE_AFTER_SEND,
        "ignore_users": sorted(list(IGNORE_USERS)),
        "ignore_ids": sorted(list(IGNORE_IDS)),
        "routes": summary,
    }, ensure_ascii=False, indent=2))


async def main():
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)

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