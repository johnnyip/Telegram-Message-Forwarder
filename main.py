import os
import asyncio
from pathlib import Path
from datetime import datetime
import mimetypes
import json
import uuid
import time
from typing import Any, Dict, List, Optional, Tuple

from dotenv import load_dotenv
from telethon import TelegramClient, events, errors, types
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer


# ============================================================
# Telegram Forwarder + Kafka + Album handling + Auto forward detect
# ============================================================

load_dotenv()

API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
SESSION_NAME = os.getenv("SESSION_NAME", "my_account")

DOWNLOAD_DIR = Path(os.getenv("DOWNLOAD_DIR", "./downloads")).expanduser()
LOG_DIR = DOWNLOAD_DIR / "log"
SPOOL_DIR = DOWNLOAD_DIR / "_spool"

# Delay
DELAY_SECONDS = int(os.getenv("DELAY_SECONDS", "5"))                  # text send delay
MEDIA_DELAY_SECONDS = int(os.getenv("MEDIA_DELAY_SECONDS", "0"))      # media send delay

# Concurrency
DOWNLOAD_CONCURRENCY = max(1, int(os.getenv("DOWNLOAD_CONCURRENCY", "3")))

# Cleanup
DELETE_AFTER_SEND = os.getenv("DELETE_AFTER_SEND", "true").strip().lower() in {"1", "true", "yes", "y"}

# Album gather window
ALBUM_GATHER_SECONDS = float(os.getenv("ALBUM_GATHER_SECONDS", "1.0"))

# Kafka
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TEXT_TOPIC = os.getenv("KAFKA_TEXT_TOPIC", "tg-forward-text")
KAFKA_MEDIA_TOPIC = os.getenv("KAFKA_MEDIA_TOPIC", "tg-forward-media")
KAFKA_CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "tg-forwarder")

# Large media forward threshold
LARGE_MEDIA_FORWARD_THRESHOLD_MB = int(os.getenv("LARGE_MEDIA_FORWARD_THRESHOLD_MB", "30"))
LARGE_MEDIA_FORWARD_THRESHOLD_BYTES = LARGE_MEDIA_FORWARD_THRESHOLD_MB * 1024 * 1024

# Forward policy:
# auto      -> auto detect, unknown chat will try forward for large media if not blocked
# allowlist -> only FORWARDABLE_SOURCE_CHATS may try large-media forward
# never     -> never try direct forward for large media
FORWARD_POLICY = os.getenv("FORWARD_POLICY", "auto").strip().lower()
LISTEN_EDITED_MESSAGES = os.getenv("LISTEN_EDITED_MESSAGES", "true").strip().lower() in {"1", "true", "yes", "y"}

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

# Optional allowlist / denylist
FORWARDABLE_SOURCE_CHATS = {
    int(x.strip())
    for x in os.getenv("FORWARDABLE_SOURCE_CHATS", "").split(",")
    if x.strip() and x.strip().lstrip("-").isdigit()
}
NONFORWARDABLE_SOURCE_CHATS = {
    int(x.strip())
    for x in os.getenv("NONFORWARDABLE_SOURCE_CHATS", "").split(",")
    if x.strip() and x.strip().lstrip("-").isdigit()
}

LOG_DIR.mkdir(parents=True, exist_ok=True)
SPOOL_DIR.mkdir(parents=True, exist_ok=True)

client = TelegramClient(SESSION_NAME, API_ID, API_HASH)
client.parse_mode = "md"

download_semaphore = asyncio.Semaphore(DOWNLOAD_CONCURRENCY)
BACKGROUND_TASKS = set()

producer: Optional[AIOKafkaProducer] = None
text_consumer: Optional[AIOKafkaConsumer] = None
media_consumer: Optional[AIOKafkaConsumer] = None

# Auto-detected forward capability cache
# chat_id -> True / False
FORWARD_CAP_CACHE: Dict[int, bool] = {}

# Pending albums:
# (source_kind, chat_id, grouped_id) -> {"messages": {msg_id: msg}, "task": asyncio.Task}
PENDING_ALBUMS: Dict[Tuple[str, int, int], dict] = {}


# ============================================================
# Basic helpers
# ============================================================

def tstamp() -> str:
    return datetime.now().isoformat(timespec="seconds")


def now_ts() -> float:
    return time.time()


def log(obj: dict):
    line = json.dumps(obj, ensure_ascii=False, default=str)
    print(line)
    try:
        today_file = LOG_DIR / f"{datetime.now().strftime('%Y-%m-%d')}.log"
        with today_file.open("a", encoding="utf-8") as fh:
            fh.write(line + "\n")
    except Exception as e:
        print(f"[LOG_ERR] {e}")


def spawn_bg(coro):
    task = asyncio.create_task(coro)
    BACKGROUND_TASKS.add(task)
    task.add_done_callback(BACKGROUND_TASKS.discard)
    return task


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


def json_bytes(obj: dict) -> bytes:
    return json.dumps(obj, ensure_ascii=False).encode("utf-8")


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


def get_media_size(msg) -> Optional[int]:
    file_obj = getattr(msg, "file", None)
    size = getattr(file_obj, "size", None)
    if isinstance(size, int):
        return size
    return None


def build_filename_from_message(msg, sender_display="unknown"):
    safe_sender = sanitize_filename_part(sender_display)
    ext = ""

    if msg.document and msg.file and msg.file.name:
        ext = Path(msg.file.name).suffix
    elif msg.file and msg.file.mime_type:
        ext = mimetypes.guess_extension(msg.file.mime_type) or ""

    return f"{msg.id}_{safe_sender}{ext}"


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


def cleanup_files(paths: List[str], source_kind: str, src_msg: Any):
    for path in paths:
        try:
            p = Path(path)
            if p.exists():
                p.unlink(missing_ok=True)
                log({
                    "ts": tstamp(),
                    "type": "cleanup",
                    "op": "delete_temp_file",
                    "file": str(p),
                    "src_msg": src_msg,
                    "source_kind": source_kind,
                })
        except Exception as e:
            log({
                "ts": tstamp(),
                "type": "warn",
                "op": "delete_temp_file",
                "file": path,
                "src_msg": src_msg,
                "source_kind": source_kind,
                "err": e.__class__.__name__,
                "msg": str(e),
            })


# ============================================================
# Routes
# ============================================================

def load_routes() -> List[Dict[str, Any]]:
    routes_json = os.getenv("ROUTES_JSON", "").strip()
    if not routes_json:
        raise RuntimeError("ROUTES_JSON is required")

    raw_routes = json.loads(routes_json)
    if not isinstance(raw_routes, list):
        raise RuntimeError("ROUTES_JSON must be a JSON array")

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
ROUTE_MAP = {r["name"]: r for r in ROUTES}


def find_matching_routes(chat_id: Any) -> List[Dict[str, Any]]:
    return [route for route in ROUTES if chat_id in route["sources"]]


def route_names(routes: List[Dict[str, Any]]) -> List[str]:
    return [r["name"] for r in routes]


# ============================================================
# Sender/chat info resolution
# ============================================================

async def resolve_sender_info_from_message(msg, chat_id_hint=None) -> dict:
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
    chat_noforwards = bool(getattr(chat, "noforwards", False)) if chat is not None else False
    msg_noforwards = bool(getattr(msg, "noforwards", False))

    if isinstance(chat, types.Channel):
        chat_type = "channel" if getattr(chat, "broadcast", False) else "supergroup"
    elif isinstance(chat, types.Chat):
        chat_type = "group"
    elif isinstance(chat, types.User):
        chat_type = "private"
    else:
        chat_type = "unknown"

    info = {
        "chat_id": chat_id,
        "chat_title": chat_title,
        "chat_username": chat_username,
        "chat_type": chat_type,
        "raw_chat_class": chat.__class__.__name__ if chat is not None else None,
        "chat_noforwards": chat_noforwards,
        "msg_noforwards": msg_noforwards,

        "sender_id": getattr(sender, "id", None) or getattr(msg, "sender_id", None),
        "sender_type": "unknown",
        "sender_username": None,
        "sender_display": "user",
        "sender_first_name": None,
        "sender_last_name": None,

        "post_author": getattr(msg, "post_author", None),
        "raw_sender_class": sender.__class__.__name__ if sender is not None else None,

        "msg_id": msg.id,
        "msg_date": str(getattr(msg, "date", None)),
        "edit_date": str(getattr(msg, "edit_date", None)),
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
            full_name = " ".join(x for x in [sender.first_name, sender.last_name] if x).strip()
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


# ============================================================
# Formatting
# ============================================================

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


# ============================================================
# Forward auto-detect logic
# ============================================================

def message_or_chat_blocks_forward(info: dict) -> bool:
    return bool(info.get("msg_noforwards")) or bool(info.get("chat_noforwards"))


def can_attempt_large_forward(chat_id: int, info: dict) -> bool:
    # Hard deny
    if chat_id in NONFORWARDABLE_SOURCE_CHATS:
        return False

    # Raw flags already tell us no
    if message_or_chat_blocks_forward(info):
        return False

    # Cache hit
    cached = FORWARD_CAP_CACHE.get(chat_id)
    if cached is False:
        return False
    if cached is True:
        return True

    # Explicit policy
    if FORWARD_POLICY == "never":
        return False

    if FORWARD_POLICY == "allowlist":
        return chat_id in FORWARDABLE_SOURCE_CHATS

    # auto
    if FORWARD_POLICY == "auto":
        # allowlist, if present, can still be used as strong allow
        if FORWARDABLE_SOURCE_CHATS and chat_id in FORWARDABLE_SOURCE_CHATS:
            return True
        # unknown chat -> try once
        return True

    # fallback
    return False


def should_direct_forward_large_media(info: dict, msg) -> bool:
    size = get_media_size(msg)
    if size is None or size <= LARGE_MEDIA_FORWARD_THRESHOLD_BYTES:
        return False
    return can_attempt_large_forward(info["chat_id"], info)


def should_direct_forward_large_album(chat_id: int, info: dict, msgs: List[Any]) -> bool:
    if not can_attempt_large_forward(chat_id, info):
        return False

    # Album: if any item > threshold, treat whole album as large forward candidate
    for m in msgs:
        size = get_media_size(m)
        if size is not None and size > LARGE_MEDIA_FORWARD_THRESHOLD_BYTES:
            return True
    return False


# ============================================================
# Snapshot / spool
# ============================================================

async def snapshot_media_message(msg, info: dict, source_kind: str) -> Optional[dict]:
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
        "media_size": get_media_size(msg),
    }

    async with download_semaphore:
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
        "media_type": media_type(msg),
        "media_size": get_media_size(msg),
        "caption_text": (msg.text or msg.message or "").strip(),
        "msg_id": msg.id,
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
        "media_size": snapshot["media_size"],
        "file": str(fpath),
    })
    return snapshot


async def snapshot_album_messages(msgs: List[Any], info: dict, source_kind: str) -> List[dict]:
    sorted_msgs = sorted(msgs, key=lambda m: m.id)
    snapshots = await asyncio.gather(
        *(snapshot_media_message(m, info, source_kind) for m in sorted_msgs),
        return_exceptions=True,
    )

    result = []
    for item in snapshots:
        if isinstance(item, Exception):
            log({
                "ts": tstamp(),
                "type": "err",
                "op": "snapshot_album_messages",
                "err": item.__class__.__name__,
                "msg": str(item),
                "chat_id": info.get("chat_id"),
                "grouped_id": info.get("grouped_id"),
            })
            continue
        if item:
            result.append(item)
    return result


# ============================================================
# Kafka producer side
# ============================================================

async def kafka_send(topic: str, payload: dict):
    assert producer is not None
    await producer.send_and_wait(topic, json_bytes(payload))


def due_ts_for_text() -> float:
    return now_ts() + DELAY_SECONDS


def due_ts_for_media() -> float:
    return now_ts() + MEDIA_DELAY_SECONDS


async def publish_text_job(msg, info: dict, routes: List[Dict[str, Any]], source_kind: str):
    job = {
        "job_type": "text",
        "source_kind": source_kind,
        "route_names": route_names(routes),
        "info": info,
        "text": msg.text or msg.message or "[empty]",
        "due_at": due_ts_for_text(),
        "created_at": tstamp(),
    }
    await kafka_send(KAFKA_TEXT_TOPIC, job)

    log({
        "ts": tstamp(),
        "type": "kafka_produce",
        "topic": KAFKA_TEXT_TOPIC,
        "job_type": "text",
        "chat_id": info["chat_id"],
        "msg_id": info["msg_id"],
        "source_kind": source_kind,
    })


async def publish_media_job(msg, info: dict, routes: List[Dict[str, Any]], source_kind: str):
    if should_direct_forward_large_media(info, msg):
        job = {
            "job_type": "media_forward",
            "source_kind": source_kind,
            "route_names": route_names(routes),
            "info": info,
            "chat_id": info["chat_id"],
            "msg_id": info["msg_id"],
            "caption_text": (msg.text or msg.message or "").strip(),
            "media_type": media_type(msg),
            "media_size": get_media_size(msg),
            "due_at": due_ts_for_media(),
            "created_at": tstamp(),
        }
        await kafka_send(KAFKA_MEDIA_TOPIC, job)

        log({
            "ts": tstamp(),
            "type": "kafka_produce",
            "topic": KAFKA_MEDIA_TOPIC,
            "job_type": "media_forward",
            "chat_id": info["chat_id"],
            "msg_id": info["msg_id"],
            "source_kind": source_kind,
            "media_size": get_media_size(msg),
            "forward_policy": FORWARD_POLICY,
        })
        return

    snapshot = await snapshot_media_message(msg, info, source_kind)
    if not snapshot:
        return

    job = {
        "job_type": "media_file",
        "source_kind": source_kind,
        "route_names": route_names(routes),
        "info": info,
        "snapshot": snapshot,
        "due_at": due_ts_for_media(),
        "created_at": tstamp(),
    }
    await kafka_send(KAFKA_MEDIA_TOPIC, job)

    log({
        "ts": tstamp(),
        "type": "kafka_produce",
        "topic": KAFKA_MEDIA_TOPIC,
        "job_type": "media_file",
        "chat_id": info["chat_id"],
        "msg_id": info["msg_id"],
        "source_kind": source_kind,
        "file": snapshot["path"],
    })


async def publish_album_job(msgs: List[Any], info: dict, routes: List[Dict[str, Any]], source_kind: str):
    sorted_msgs = sorted(msgs, key=lambda m: m.id)
    msg_ids = [m.id for m in sorted_msgs]

    if should_direct_forward_large_album(info["chat_id"], info, sorted_msgs):
        job = {
            "job_type": "media_album_forward",
            "source_kind": source_kind,
            "route_names": route_names(routes),
            "info": info,
            "chat_id": info["chat_id"],
            "msg_ids": msg_ids,
            "caption_text": (sorted_msgs[0].text or sorted_msgs[0].message or "").strip(),
            "media_count": len(sorted_msgs),
            "due_at": due_ts_for_media(),
            "created_at": tstamp(),
        }
        await kafka_send(KAFKA_MEDIA_TOPIC, job)

        log({
            "ts": tstamp(),
            "type": "kafka_produce",
            "topic": KAFKA_MEDIA_TOPIC,
            "job_type": "media_album_forward",
            "chat_id": info["chat_id"],
            "grouped_id": info.get("grouped_id"),
            "msg_ids": msg_ids,
            "source_kind": source_kind,
            "forward_policy": FORWARD_POLICY,
        })
        return

    snapshots = await snapshot_album_messages(sorted_msgs, info, source_kind)
    if not snapshots:
        return

    job = {
        "job_type": "media_album_file",
        "source_kind": source_kind,
        "route_names": route_names(routes),
        "info": info,
        "snapshots": snapshots,
        "due_at": due_ts_for_media(),
        "created_at": tstamp(),
    }
    await kafka_send(KAFKA_MEDIA_TOPIC, job)

    log({
        "ts": tstamp(),
        "type": "kafka_produce",
        "topic": KAFKA_MEDIA_TOPIC,
        "job_type": "media_album_file",
        "chat_id": info["chat_id"],
        "grouped_id": info.get("grouped_id"),
        "msg_ids": [s["msg_id"] for s in snapshots],
        "source_kind": source_kind,
    })


# ============================================================
# Send helpers
# ============================================================

async def send_text_to_target(route: Dict[str, Any], tgt: Any, combined: str, info: dict, source_kind: str) -> bool:
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
        await run_api(client.send_message(tgt, combined), op="send_text", extra=extra)
        log({
            "ts": tstamp(),
            "type": "out",
            "op": "send_text",
            "status": "ok",
            **extra,
        })
        return True
    except Exception:
        return False


async def send_file_to_target(route: Dict[str, Any], tgt: Any, fpath: Path, caption: str, info: dict, source_kind: str) -> bool:
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
        await run_api(client.send_file(tgt, fpath, caption=caption), op="send_file", extra=extra)
        log({
            "ts": tstamp(),
            "type": "out",
            "op": "send_file",
            "status": "ok",
            **extra,
        })
        return True
    except Exception:
        return False


async def send_album_to_target(route: Dict[str, Any], tgt: Any, files: List[str], caption: str, info: dict, source_kind: str) -> bool:
    extra = {
        "route": route["name"],
        "dst": tgt,
        "src_msg": info["msg_id"],
        "chat_id": info["chat_id"],
        "chat_title": info["chat_title"],
        "sender_id": info["sender_id"],
        "sender_username": info["sender_username"],
        "sender_display": info["sender_display"],
        "files": files,
        "source_kind": source_kind,
    }
    try:
        await run_api(client.send_file(tgt, files, caption=caption), op="send_album", extra=extra)
        log({
            "ts": tstamp(),
            "type": "out",
            "op": "send_album",
            "status": "ok",
            **extra,
        })
        return True
    except Exception:
        return False


async def fetch_message_by_id(chat_id: int, msg_id: int):
    try:
        return await client.get_messages(chat_id, ids=msg_id)
    except Exception as e:
        log({
            "ts": tstamp(),
            "type": "err",
            "op": "get_messages",
            "chat_id": chat_id,
            "msg_id": msg_id,
            "err": e.__class__.__name__,
            "msg": str(e),
        })
        return None


async def fetch_messages_by_ids(chat_id: int, msg_ids: List[int]):
    try:
        msgs = await client.get_messages(chat_id, ids=msg_ids)
        if msgs is None:
            return []
        if isinstance(msgs, list):
            return [m for m in msgs if m]
        return [msgs] if msgs else []
    except Exception as e:
        log({
            "ts": tstamp(),
            "type": "err",
            "op": "get_messages_album",
            "chat_id": chat_id,
            "msg_ids": msg_ids,
            "err": e.__class__.__name__,
            "msg": str(e),
        })
        return []


# ============================================================
# Kafka consumer side processing
# Return cleanup paths to delete only AFTER commit.
# ============================================================

async def process_text_job(job: dict) -> List[str]:
    info = job["info"]
    if should_ignore(info):
        log({"ts": tstamp(), "type": "info", "note": "ignored_text_job", "chat_id": info["chat_id"], "msg_id": info["msg_id"]})
        return []

    is_edit = job["source_kind"] == "edited"
    hdr = build_header_from_info(info, is_edit=is_edit)
    body = job["text"]
    combined = f"{hdr}\n{body}"

    routes = [ROUTE_MAP[name] for name in job["route_names"] if name in ROUTE_MAP]
    success_count = 0
    total_count = 0

    for route in routes:
        for tgt in route["targets"]:
            total_count += 1
            ok = await send_text_to_target(route, tgt, combined, info, job["source_kind"])
            if ok:
                success_count += 1

    if total_count > 0 and success_count == 0:
        raise RuntimeError(f"text job failed for all targets chat_id={info['chat_id']} msg_id={info['msg_id']}")

    return []


async def process_media_file_job(job: dict) -> List[str]:
    info = job["info"]
    if should_ignore(info):
        log({"ts": tstamp(), "type": "info", "note": "ignored_media_file_job", "chat_id": info["chat_id"], "msg_id": info["msg_id"]})
        return []

    snapshot = job["snapshot"]
    fpath = Path(snapshot["path"])
    if not fpath.exists():
        log({
            "ts": tstamp(),
            "type": "err",
            "op": "media_file_missing",
            "chat_id": info["chat_id"],
            "msg_id": info["msg_id"],
            "file": str(fpath),
        })
        return []

    is_edit = job["source_kind"] == "edited"
    hdr = build_header_from_info(info, is_edit=is_edit)
    caption_body = snapshot.get("caption_text", "").strip()
    caption = f"{hdr}\n{caption_body}".strip()

    routes = [ROUTE_MAP[name] for name in job["route_names"] if name in ROUTE_MAP]
    success_count = 0
    total_count = 0

    for route in routes:
        for tgt in route["targets"]:
            total_count += 1
            ok = await send_file_to_target(route, tgt, fpath, caption, info, job["source_kind"])
            if ok:
                success_count += 1

    if total_count > 0 and success_count == 0:
        raise RuntimeError(f"media_file job failed for all targets chat_id={info['chat_id']} msg_id={info['msg_id']}")

    return [str(fpath)] if DELETE_AFTER_SEND else []


async def process_media_album_file_job(job: dict) -> List[str]:
    info = job["info"]
    if should_ignore(info):
        log({"ts": tstamp(), "type": "info", "note": "ignored_media_album_file_job", "chat_id": info["chat_id"], "msg_id": info["msg_id"]})
        return []

    snapshots = job["snapshots"]
    files = [s["path"] for s in snapshots if Path(s["path"]).exists()]
    if not files:
        log({
            "ts": tstamp(),
            "type": "err",
            "op": "media_album_files_missing",
            "chat_id": info["chat_id"],
            "grouped_id": info.get("grouped_id"),
        })
        return []

    is_edit = job["source_kind"] == "edited"
    hdr = build_header_from_info(info, is_edit=is_edit)
    caption_body = snapshots[0].get("caption_text", "").strip() if snapshots else ""
    caption = f"{hdr}\n{caption_body}".strip()

    routes = [ROUTE_MAP[name] for name in job["route_names"] if name in ROUTE_MAP]
    success_count = 0
    total_count = 0

    for route in routes:
        for tgt in route["targets"]:
            total_count += 1
            ok = await send_album_to_target(route, tgt, files, caption, info, job["source_kind"])
            if ok:
                success_count += 1

    if total_count > 0 and success_count == 0:
        raise RuntimeError(f"media_album_file job failed for all targets chat_id={info['chat_id']} grouped_id={info.get('grouped_id')}")

    return files if DELETE_AFTER_SEND else []


async def process_media_forward_job(job: dict) -> List[str]:
    info = job["info"]
    if should_ignore(info):
        log({"ts": tstamp(), "type": "info", "note": "ignored_media_forward_job", "chat_id": info["chat_id"], "msg_id": info["msg_id"]})
        return []

    msg = await fetch_message_by_id(job["chat_id"], job["msg_id"])
    if not msg:
        log({
            "ts": tstamp(),
            "type": "err",
            "op": "media_forward_fetch_failed",
            "chat_id": job["chat_id"],
            "msg_id": job["msg_id"],
        })
        return []

    is_edit = job["source_kind"] == "edited"
    hdr = build_header_from_info(info, is_edit=is_edit)
    routes = [ROUTE_MAP[name] for name in job["route_names"] if name in ROUTE_MAP]

    success_count = 0
    total_count = 0

    try:
        for route in routes:
            for tgt in route["targets"]:
                total_count += 1
                extra = {
                    "route": route["name"],
                    "dst": tgt,
                    "src_msg": info["msg_id"],
                    "chat_id": info["chat_id"],
                    "chat_title": info["chat_title"],
                    "sender_id": info["sender_id"],
                    "sender_username": info["sender_username"],
                    "sender_display": info["sender_display"],
                    "source_kind": job["source_kind"],
                    "media_size": job.get("media_size"),
                    "media_type": job.get("media_type"),
                }
                await run_api(client.send_message(tgt, hdr), op="send_header_before_forward_large", extra=extra)
                await run_api(client.forward_messages(tgt, msg, msg.peer_id), op="forward_large_media", extra=extra)

                log({
                    "ts": tstamp(),
                    "type": "out",
                    "op": "forward_large_media",
                    "status": "ok",
                    **extra,
                })
                success_count += 1

        if success_count > 0:
            FORWARD_CAP_CACHE[info["chat_id"]] = True

        if total_count > 0 and success_count == 0:
            raise RuntimeError(f"media_forward job failed for all targets chat_id={info['chat_id']} msg_id={info['msg_id']}")

        return []

    except errors.ChatForwardsRestrictedError:
        FORWARD_CAP_CACHE[info["chat_id"]] = False
        log({
            "ts": tstamp(),
            "type": "warn",
            "op": "forward_large_media",
            "note": "chat_forwards_restricted_fallback_to_download",
            "chat_id": info["chat_id"],
            "msg_id": info["msg_id"],
        })

        snapshot = await snapshot_media_message(msg, info, job["source_kind"])
        if not snapshot:
            return []

        fallback_job = {
            "job_type": "media_file",
            "source_kind": job["source_kind"],
            "route_names": job["route_names"],
            "info": info,
            "snapshot": snapshot,
        }
        return await process_media_file_job(fallback_job)


async def process_media_album_forward_job(job: dict) -> List[str]:
    info = job["info"]
    if should_ignore(info):
        log({"ts": tstamp(), "type": "info", "note": "ignored_media_album_forward_job", "chat_id": info["chat_id"], "msg_id": info["msg_id"]})
        return []

    msgs = await fetch_messages_by_ids(job["chat_id"], job["msg_ids"])
    msgs = sorted([m for m in msgs if m], key=lambda m: m.id)
    if not msgs:
        log({
            "ts": tstamp(),
            "type": "err",
            "op": "media_album_forward_fetch_failed",
            "chat_id": job["chat_id"],
            "msg_ids": job["msg_ids"],
        })
        return []

    is_edit = job["source_kind"] == "edited"
    hdr = build_header_from_info(info, is_edit=is_edit)
    routes = [ROUTE_MAP[name] for name in job["route_names"] if name in ROUTE_MAP]

    success_count = 0
    total_count = 0

    try:
        for route in routes:
            for tgt in route["targets"]:
                total_count += 1
                extra = {
                    "route": route["name"],
                    "dst": tgt,
                    "chat_id": info["chat_id"],
                    "chat_title": info["chat_title"],
                    "sender_id": info["sender_id"],
                    "sender_username": info["sender_username"],
                    "sender_display": info["sender_display"],
                    "source_kind": job["source_kind"],
                    "msg_ids": job["msg_ids"],
                    "grouped_id": info.get("grouped_id"),
                }
                await run_api(client.send_message(tgt, hdr), op="send_header_before_forward_album", extra=extra)
                await run_api(client.forward_messages(tgt, msgs, msgs[0].peer_id), op="forward_large_album", extra=extra)

                log({
                    "ts": tstamp(),
                    "type": "out",
                    "op": "forward_large_album",
                    "status": "ok",
                    **extra,
                })
                success_count += 1

        if success_count > 0:
            FORWARD_CAP_CACHE[info["chat_id"]] = True

        if total_count > 0 and success_count == 0:
            raise RuntimeError(f"media_album_forward job failed for all targets chat_id={info['chat_id']} grouped_id={info.get('grouped_id')}")

        return []

    except errors.ChatForwardsRestrictedError:
        FORWARD_CAP_CACHE[info["chat_id"]] = False
        log({
            "ts": tstamp(),
            "type": "warn",
            "op": "forward_large_album",
            "note": "chat_forwards_restricted_fallback_to_download",
            "chat_id": info["chat_id"],
            "grouped_id": info.get("grouped_id"),
            "msg_ids": job["msg_ids"],
        })

        snapshots = await snapshot_album_messages(msgs, info, job["source_kind"])
        if not snapshots:
            return []

        fallback_job = {
            "job_type": "media_album_file",
            "source_kind": job["source_kind"],
            "route_names": job["route_names"],
            "info": info,
            "snapshots": snapshots,
        }
        return await process_media_album_file_job(fallback_job)


# ============================================================
# Kafka consumer loops
# ============================================================

async def sleep_until_due(job: dict):
    due_at = float(job.get("due_at", now_ts()))
    delay = due_at - now_ts()
    if delay > 0:
        await asyncio.sleep(delay)


async def text_consumer_loop():
    assert text_consumer is not None
    await text_consumer.start()
    try:
        async for record in text_consumer:
            cleanup_paths: List[str] = []
            job = None
            try:
                job = json.loads(record.value.decode("utf-8"))
                await sleep_until_due(job)
                cleanup_paths = await process_text_job(job)
                await text_consumer.commit()

                if cleanup_paths:
                    cleanup_files(cleanup_paths, job.get("source_kind", "unknown"), job.get("info", {}).get("msg_id"))
            except Exception as e:
                log({
                    "ts": tstamp(),
                    "type": "err",
                    "op": "text_consumer_loop",
                    "err": e.__class__.__name__,
                    "msg": str(e),
                    "job_type": job.get("job_type") if isinstance(job, dict) else None,
                })
    finally:
        await text_consumer.stop()


async def media_consumer_loop():
    assert media_consumer is not None
    await media_consumer.start()
    try:
        async for record in media_consumer:
            cleanup_paths: List[str] = []
            job = None
            try:
                job = json.loads(record.value.decode("utf-8"))
                await sleep_until_due(job)

                job_type = job.get("job_type")
                if job_type == "media_file":
                    cleanup_paths = await process_media_file_job(job)
                elif job_type == "media_forward":
                    cleanup_paths = await process_media_forward_job(job)
                elif job_type == "media_album_file":
                    cleanup_paths = await process_media_album_file_job(job)
                elif job_type == "media_album_forward":
                    cleanup_paths = await process_media_album_forward_job(job)
                else:
                    raise RuntimeError(f"unknown media job_type={job_type}")

                await media_consumer.commit()

                if cleanup_paths:
                    cleanup_files(cleanup_paths, job.get("source_kind", "unknown"), job.get("info", {}).get("msg_id"))
            except Exception as e:
                log({
                    "ts": tstamp(),
                    "type": "err",
                    "op": "media_consumer_loop",
                    "err": e.__class__.__name__,
                    "msg": str(e),
                    "job_type": job.get("job_type") if isinstance(job, dict) else None,
                })
    finally:
        await media_consumer.stop()


# ============================================================
# Album buffering
# ============================================================

async def flush_album(source_kind: str, chat_id: int, grouped_id: int):
    key = (source_kind, chat_id, grouped_id)
    album = PENDING_ALBUMS.pop(key, None)
    if not album:
        return

    msgs = sorted(album["messages"].values(), key=lambda m: m.id)
    if not msgs:
        return

    routes = find_matching_routes(chat_id)
    if not routes:
        return

    info = await resolve_sender_info_from_message(msgs[0], chat_id_hint=chat_id)

    if should_ignore(info):
        log({
            "ts": tstamp(),
            "type": "info",
            "note": "ignored_album_early",
            "source_kind": source_kind,
            "chat_id": info["chat_id"],
            "chat_title": info["chat_title"],
            "grouped_id": grouped_id,
            "msg_ids": [m.id for m in msgs],
            "sender_type": info["sender_type"],
            "sender_id": info["sender_id"],
            "sender_username": info["sender_username"],
            "sender_display": info["sender_display"],
        })
        return
        
    log({
        "ts": tstamp(),
        "type": "album_flush",
        "source_kind": source_kind,
        "chat_id": info["chat_id"],
        "chat_title": info["chat_title"],
        "grouped_id": grouped_id,
        "msg_ids": [m.id for m in msgs],
        "media_count": len(msgs),
        "matched_routes": [r["name"] for r in routes],
        "sender_id": info["sender_id"],
        "sender_username": info["sender_username"],
        "sender_display": info["sender_display"],
        "chat_noforwards": info["chat_noforwards"],
        "msg_noforwards": info["msg_noforwards"],
    })

    await publish_album_job(msgs, info, routes, source_kind)


def add_to_album_buffer(msg, source_kind: str):
    chat_id = msg.chat_id
    grouped_id = getattr(msg, "grouped_id", None)
    if grouped_id is None:
        return False

    key = (source_kind, chat_id, grouped_id)
    if key not in PENDING_ALBUMS:
        task = spawn_bg(_album_timer(source_kind, chat_id, grouped_id))
        PENDING_ALBUMS[key] = {
            "messages": {},
            "task": task,
        }

    PENDING_ALBUMS[key]["messages"][msg.id] = msg
    return True


async def _album_timer(source_kind: str, chat_id: int, grouped_id: int):
    await asyncio.sleep(ALBUM_GATHER_SECONDS)
    await flush_album(source_kind, chat_id, grouped_id)


# ============================================================
# Incoming Telethon event handling
# ============================================================

async def handle_incoming_message(msg, source_kind: str):
    routes = find_matching_routes(msg.chat_id)
    if not routes:
        return

    info = await resolve_sender_info_from_message(msg, chat_id_hint=msg.chat_id)

    if should_ignore(info):
        log({
            "ts": tstamp(),
            "type": "info",
            "note": "ignored_early",
            "source_kind": source_kind,
            "chat_id": info["chat_id"],
            "chat_title": info["chat_title"],
            "msg": info["msg_id"],
            "media": media_type(msg),
            "sender_type": info["sender_type"],
            "sender_id": info["sender_id"],
            "sender_username": info["sender_username"],
            "sender_display": info["sender_display"],
        })
        return

    log({
        "ts": tstamp(),
        "type": "in" if source_kind == "new" else "edit",
        "matched_routes": [r["name"] for r in routes],
        "chat_id": info["chat_id"],
        "chat_title": info["chat_title"],
        "chat_username": info["chat_username"],
        "chat_type": info["chat_type"],
        "raw_chat_class": info["raw_chat_class"],
        "chat_noforwards": info["chat_noforwards"],
        "msg_noforwards": info["msg_noforwards"],
        "msg": info["msg_id"],
        "media": media_type(msg),
        "media_size": get_media_size(msg),
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
        await publish_text_job(msg, info, routes, source_kind)
        return

    if getattr(msg, "grouped_id", None) is not None:
        added = add_to_album_buffer(msg, source_kind)
        if added:
            log({
                "ts": tstamp(),
                "type": "album_buffered",
                "source_kind": source_kind,
                "chat_id": info["chat_id"],
                "chat_title": info["chat_title"],
                "grouped_id": info["grouped_id"],
                "msg_id": info["msg_id"],
            })
            return

    spawn_bg(publish_media_job(msg, info, routes, source_kind))


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
    if not LISTEN_EDITED_MESSAGES:
        return

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

# ============================================================
# Kafka startup/shutdown
# ============================================================

async def startup_kafka():
    global producer, text_consumer, media_consumer

    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    )
    await producer.start()

    text_consumer = AIOKafkaConsumer(
        KAFKA_TEXT_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=f"{KAFKA_CONSUMER_GROUP}-text",
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )

    media_consumer = AIOKafkaConsumer(
        KAFKA_MEDIA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=f"{KAFKA_CONSUMER_GROUP}-media",
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )


async def shutdown_kafka():
    global producer
    if producer is not None:
        await producer.stop()
        producer = None


# ============================================================
# Startup info
# ============================================================

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
        "kafka_bootstrap_servers": KAFKA_BOOTSTRAP_SERVERS,
        "kafka_text_topic": KAFKA_TEXT_TOPIC,
        "kafka_media_topic": KAFKA_MEDIA_TOPIC,
        "kafka_consumer_group": KAFKA_CONSUMER_GROUP,
        "text_delay_seconds": DELAY_SECONDS,
        "media_delay_seconds": MEDIA_DELAY_SECONDS,
        "download_concurrency": DOWNLOAD_CONCURRENCY,
        "album_gather_seconds": ALBUM_GATHER_SECONDS,
        "large_media_forward_threshold_mb": LARGE_MEDIA_FORWARD_THRESHOLD_MB,
        "forward_policy": FORWARD_POLICY,
        "listen_edited_messages": LISTEN_EDITED_MESSAGES,
        "forwardable_source_chats": sorted(list(FORWARDABLE_SOURCE_CHATS)),
        "nonforwardable_source_chats": sorted(list(NONFORWARDABLE_SOURCE_CHATS)),
        "delete_after_send": DELETE_AFTER_SEND,
        "ignore_users": sorted(list(IGNORE_USERS)),
        "ignore_ids": sorted(list(IGNORE_IDS)),
        "routes": summary,
    }, ensure_ascii=False, indent=2))

# ============================================================
# Main
# ============================================================

async def main():
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    SPOOL_DIR.mkdir(parents=True, exist_ok=True)

    print_route_summary()
    print("✔ Telegram forwarder + Kafka + Album + Auto-forward-detect running — Ctrl+C to stop…")

    await startup_kafka()

    consumer_tasks = [
        asyncio.create_task(text_consumer_loop()),
        asyncio.create_task(media_consumer_loop()),
    ]

    try:
        async with client:
            await client.run_until_disconnected()
    finally:
        for t in consumer_tasks:
            t.cancel()
        await asyncio.gather(*consumer_tasks, return_exceptions=True)

        for key, item in list(PENDING_ALBUMS.items()):
            task = item.get("task")
            if task:
                task.cancel()
        await asyncio.gather(
            *[item["task"] for item in PENDING_ALBUMS.values() if item.get("task")],
            return_exceptions=True,
        )
        PENDING_ALBUMS.clear()

        for t in list(BACKGROUND_TASKS):
            t.cancel()
        await asyncio.gather(*BACKGROUND_TASKS, return_exceptions=True)

        await shutdown_kafka()


if __name__ == "__main__":
    asyncio.run(main())