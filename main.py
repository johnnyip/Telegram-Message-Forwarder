import os
import asyncio
from pathlib import Path
from datetime import datetime
import json
import uuid
from typing import Any, Dict, List, Optional, Tuple

from dotenv import load_dotenv
from telethon import TelegramClient, events, errors, types
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from telegram import Bot
from telegram.error import TelegramError

from tg_forwarder.bot_sender import bot_send_album, bot_send_file, bot_send_text
from tg_forwarder.file_policy import is_stale_file
from tg_forwarder.healthcheck import default_health_path, write_health_file
from tg_forwarder.debug_flags import ENABLE_DEBUG_LOGS, configure_telethon_logger, maybe_debug_log
from tg_forwarder.formatting import build_header_from_info, should_ignore
from tg_forwarder.forward_cache import forward_cache_get, forward_cache_set
from tg_forwarder.kafka_jobs import publish_album_job as publish_album_job_mod, publish_media_job as publish_media_job_mod, publish_text_job as publish_text_job_mod
from tg_forwarder.lag_stats import lag_record, lag_summary
from tg_forwarder.logging_setup import setup_logging
from tg_forwarder.media import get_media_size, media_type
from tg_forwarder.metrics import StepTimer
from tg_forwarder.routes import find_matching_routes, load_routes
from tg_forwarder.senders import fetch_message_by_id as fetch_message_by_id_mod, fetch_messages_by_ids as fetch_messages_by_ids_mod, send_album_to_target as send_album_to_target_mod, send_file_to_target as send_file_to_target_mod, send_text_to_many as send_text_to_many_mod, send_text_to_target as send_text_to_target_mod
from tg_forwarder.snapshot import snapshot_album_messages as snapshot_album_messages_mod, snapshot_media_message as snapshot_media_message_mod
from tg_forwarder.telegram_info import resolve_sender_info_from_message
from tg_forwarder.utils import cleanup_files, json_bytes, log as base_log, now_ts, tstamp


# ============================================================
# Telegram Forwarder + Kafka + Album handling + Auto forward detect
# ============================================================

load_dotenv()

API_ID_RAW = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")
SESSION_NAME = os.getenv("SESSION_NAME", "my_account")
API_ID = int(API_ID_RAW) if API_ID_RAW else None

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
APP_MODE = os.getenv("APP_MODE", "listen").strip().lower()
if APP_MODE not in {"listen", "send"}:
    raise RuntimeError("APP_MODE must be 'listen' or 'send'")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()

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

LOGGER = setup_logging()
configure_telethon_logger()

client: Optional[TelegramClient] = None
if APP_MODE == "listen":
    if API_ID is None or not API_HASH:
        raise RuntimeError("API_ID and API_HASH are required in APP_MODE=listen")
    client = TelegramClient(SESSION_NAME, API_ID, API_HASH, base_logger="telethon")
    client.parse_mode = "md"

download_semaphore = asyncio.Semaphore(DOWNLOAD_CONCURRENCY)
BACKGROUND_TASKS = set()

producer: Optional[AIOKafkaProducer] = None
text_consumer: Optional[AIOKafkaConsumer] = None
media_consumer: Optional[AIOKafkaConsumer] = None
bot: Optional[Bot] = None

# Pending albums:
# (source_kind, chat_id, grouped_id) -> {"messages": {msg_id: msg}, "task": asyncio.Task}
PENDING_ALBUMS: Dict[Tuple[str, int, int], dict] = {}


# ============================================================
# Basic helpers
# ============================================================

def log(obj: dict):
    base_log(obj, LOG_DIR)


def spawn_bg(coro):
    task = asyncio.create_task(coro)
    BACKGROUND_TASKS.add(task)
    task.add_done_callback(BACKGROUND_TASKS.discard)
    return task


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


ROUTES = load_routes(os.getenv("ROUTES_JSON", ""))
ROUTE_MAP = {r["name"]: r for r in ROUTES}


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
    cached = forward_cache_get(chat_id)
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
    if APP_MODE == "listen":
        return False
    size = get_media_size(msg)
    if size is None or size <= LARGE_MEDIA_FORWARD_THRESHOLD_BYTES:
        return False
    return can_attempt_large_forward(info["chat_id"], info)


def should_direct_forward_large_album(chat_id: int, info: dict, msgs: List[Any]) -> bool:
    if APP_MODE == "listen":
        return False
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
    return await snapshot_media_message_mod(msg, info, source_kind, SPOOL_DIR, download_semaphore, run_api, log)


async def snapshot_album_messages(msgs: List[Any], info: dict, source_kind: str) -> List[dict]:
    return await snapshot_album_messages_mod(msgs, info, source_kind, SPOOL_DIR, download_semaphore, run_api, log)


# ============================================================
# Kafka producer side
# ============================================================

async def publish_text_job(msg, info: dict, routes: List[Dict[str, Any]], source_kind: str):
    assert producer is not None
    await publish_text_job_mod(msg, info, routes, source_kind, delay_seconds=DELAY_SECONDS, topic=KAFKA_TEXT_TOPIC, producer=producer, json_bytes=json_bytes, log=log)


async def publish_media_job(msg, info: dict, routes: List[Dict[str, Any]], source_kind: str):
    assert producer is not None
    await publish_media_job_mod(
        msg, info, routes, source_kind,
        media_delay_seconds=MEDIA_DELAY_SECONDS,
        topic=KAFKA_MEDIA_TOPIC,
        producer=producer,
        json_bytes=json_bytes,
        log=log,
        should_direct_forward_large_media=should_direct_forward_large_media,
        snapshot_kwargs={
            "spool_dir": SPOOL_DIR,
            "download_semaphore": download_semaphore,
            "run_api": run_api,
            "media_type_fn": media_type,
            "get_media_size_fn": get_media_size,
            "forward_policy": FORWARD_POLICY,
        },
    )


async def publish_album_job(msgs: List[Any], info: dict, routes: List[Dict[str, Any]], source_kind: str):
    assert producer is not None
    await publish_album_job_mod(
        msgs, info, routes, source_kind,
        media_delay_seconds=MEDIA_DELAY_SECONDS,
        topic=KAFKA_MEDIA_TOPIC,
        producer=producer,
        json_bytes=json_bytes,
        log=log,
        should_direct_forward_large_album=should_direct_forward_large_album,
        snapshot_kwargs={
            "spool_dir": SPOOL_DIR,
            "download_semaphore": download_semaphore,
            "run_api": run_api,
            "media_type_fn": media_type,
            "get_media_size_fn": get_media_size,
            "forward_policy": FORWARD_POLICY,
        },
    )


# ============================================================
# Send helpers
# ============================================================

TEXT_TARGET_PARALLEL = os.getenv("TEXT_TARGET_PARALLEL", "true").strip().lower() in {"1", "true", "yes", "y"}
MEDIA_TARGET_PARALLEL = os.getenv("MEDIA_TARGET_PARALLEL", "false").strip().lower() in {"1", "true", "yes", "y"}


def job_media_type_from_info(info: dict) -> Optional[str]:
    if isinstance(info, dict):
        if info.get("_media_type"):
            return info.get("_media_type")
        snaps = info.get("_album_snapshots")
        if isinstance(snaps, list) and snaps:
            return snaps[0].get("media_type")
    return None


async def send_text_to_target(route: Dict[str, Any], tgt: Any, combined: str, info: dict, source_kind: str) -> bool:
    if APP_MODE == "send" and bot is not None:
        extra = {
            "route": route["name"], "dst": tgt, "src_msg": info["msg_id"], "chat_id": info["chat_id"],
            "chat_title": info["chat_title"], "sender_id": info["sender_id"], "sender_username": info["sender_username"],
            "sender_display": info["sender_display"], "source_kind": source_kind,
        }
        try:
            await bot_send_text(bot, tgt, combined)
            log({"ts": tstamp(), "type": "out", "op": "send_text", "status": "ok", **extra})
            return True
        except Exception as e:
            log({"ts": tstamp(), "type": "err", "op": "send_text", "err": e.__class__.__name__, "msg": str(e), **extra, "target": tgt, "text_preview": combined[:200]})
            return False
    return await send_text_to_target_mod(route, tgt, combined, info, source_kind, client=client, run_api=run_api, log=log)


async def send_file_to_target(route: Dict[str, Any], tgt: Any, fpath: Path, caption: str, info: dict, source_kind: str) -> bool:
    if APP_MODE == "send" and bot is not None:
        extra = {
            "route": route["name"], "dst": tgt, "src_msg": info["msg_id"], "chat_id": info["chat_id"],
            "chat_title": info["chat_title"], "sender_id": info["sender_id"], "sender_username": info["sender_username"],
            "sender_display": info["sender_display"], "file": str(fpath), "source_kind": source_kind,
        }
        try:
            await bot_send_file(bot, tgt, str(fpath), caption, media_type=job_media_type_from_info(info))
            log({"ts": tstamp(), "type": "out", "op": "send_file", "status": "ok", **extra})
            return True
        except Exception as e:
            log({"ts": tstamp(), "type": "err", "op": "send_file", "err": e.__class__.__name__, "msg": str(e), **extra, "target": tgt, "caption_preview": caption[:200]})
            return False
    return await send_file_to_target_mod(route, tgt, fpath, caption, info, source_kind, client=client, run_api=run_api, log=log)


async def send_album_to_target(route: Dict[str, Any], tgt: Any, files: List[str], caption: str, info: dict, source_kind: str) -> bool:
    if APP_MODE == "send" and bot is not None:
        extra = {
            "route": route["name"], "dst": tgt, "src_msg": info["msg_id"], "chat_id": info["chat_id"],
            "chat_title": info["chat_title"], "sender_id": info["sender_id"], "sender_username": info["sender_username"],
            "sender_display": info["sender_display"], "files": files, "source_kind": source_kind,
        }
        try:
            media_types = [s.get("media_type") for s in info.get("_album_snapshots", [])] if isinstance(info.get("_album_snapshots"), list) else None
            await bot_send_album(bot, tgt, files, caption, media_types=media_types)
            log({"ts": tstamp(), "type": "out", "op": "send_album", "status": "ok", **extra})
            return True
        except Exception as e:
            log({"ts": tstamp(), "type": "warn", "op": "send_album_fallback_to_single", "err": e.__class__.__name__, "msg": str(e), **extra})
            ok_count = 0
            for idx, single in enumerate(files):
                single_type = media_types[idx] if media_types and idx < len(media_types) else None
                single_caption = caption if idx == 0 else ""
                try:
                    await bot_send_file(bot, tgt, single, single_caption, media_type=single_type)
                    ok_count += 1
                except Exception as inner:
                    log({"ts": tstamp(), "type": "err", "op": "send_album_single_fallback", "err": inner.__class__.__name__, "msg": str(inner), "file": single, "target": tgt, **extra})
            return ok_count > 0
    return await send_album_to_target_mod(route, tgt, files, caption, info, source_kind, client=client, run_api=run_api, log=log)


async def fetch_message_by_id(chat_id: int, msg_id: int):
    if client is None:
        raise RuntimeError("Telethon client unavailable in current mode")
    return await fetch_message_by_id_mod(chat_id, msg_id, client=client, log=log)


async def fetch_messages_by_ids(chat_id: int, msg_ids: List[int]):
    if client is None:
        raise RuntimeError("Telethon client unavailable in current mode")
    return await fetch_messages_by_ids_mod(chat_id, msg_ids, client=client, log=log)


# ============================================================
# Kafka consumer side processing
# Return cleanup paths to delete only AFTER commit.
# ============================================================

async def process_text_job(job: dict) -> List[str]:
    info = job["info"]
    if should_ignore(info, IGNORE_USERS, IGNORE_IDS):
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
        total_count += len(route["targets"])
        results = await send_text_to_many_mod(route, route["targets"], combined, info, job["source_kind"], client=client, run_api=run_api, log=log, parallel=TEXT_TARGET_PARALLEL)
        success_count += sum(1 for ok in results if ok)

    if total_count > 0 and success_count == 0:
        raise RuntimeError(f"text job failed for all targets chat_id={info['chat_id']} msg_id={info['msg_id']}")

    return []


async def process_media_file_job(job: dict) -> List[str]:
    info = job["info"]
    if should_ignore(info, IGNORE_USERS, IGNORE_IDS):
        log({"ts": tstamp(), "type": "info", "note": "ignored_media_file_job", "chat_id": info["chat_id"], "msg_id": info["msg_id"]})
        return []

    snapshot = job["snapshot"]
    info["_media_type"] = snapshot.get("media_type")
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
    if is_stale_file(str(fpath)):
        log({
            "ts": tstamp(),
            "type": "warn",
            "op": "media_file_stale",
            "chat_id": info["chat_id"],
            "msg_id": info["msg_id"],
            "file": str(fpath),
        })

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
    if should_ignore(info, IGNORE_USERS, IGNORE_IDS):
        log({"ts": tstamp(), "type": "info", "note": "ignored_media_album_file_job", "chat_id": info["chat_id"], "msg_id": info["msg_id"]})
        return []

    snapshots = job["snapshots"]
    files = [s["path"] for s in snapshots if Path(s["path"]).exists()]
    info["_album_snapshots"] = snapshots
    if not files:
        log({
            "ts": tstamp(),
            "type": "err",
            "op": "media_album_files_missing",
            "chat_id": info["chat_id"],
            "grouped_id": info.get("grouped_id"),
        })
        return []
    stale_files = [f for f in files if is_stale_file(f)]
    if stale_files:
        log({
            "ts": tstamp(),
            "type": "warn",
            "op": "media_album_files_stale",
            "chat_id": info["chat_id"],
            "grouped_id": info.get("grouped_id"),
            "count": len(stale_files),
        })

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
    if should_ignore(info, IGNORE_USERS, IGNORE_IDS):
        log({"ts": tstamp(), "type": "info", "note": "ignored_media_forward_job", "chat_id": info["chat_id"], "msg_id": info["msg_id"]})
        return []

    if APP_MODE == "send" and bot is not None:
        log({"ts": tstamp(), "type": "warn", "op": "media_forward_job_unsupported_in_bot_mode", "chat_id": info["chat_id"], "msg_id": info["msg_id"]})
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
            forward_cache_set(info["chat_id"], True)

        if total_count > 0 and success_count == 0:
            raise RuntimeError(f"media_forward job failed for all targets chat_id={info['chat_id']} msg_id={info['msg_id']}")

        return []

    except errors.ChatForwardsRestrictedError:
        forward_cache_set(info["chat_id"], False)
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
    if should_ignore(info, IGNORE_USERS, IGNORE_IDS):
        log({"ts": tstamp(), "type": "info", "note": "ignored_media_album_forward_job", "chat_id": info["chat_id"], "msg_id": info["msg_id"]})
        return []

    if APP_MODE == "send" and bot is not None:
        log({"ts": tstamp(), "type": "warn", "op": "media_album_forward_job_unsupported_in_bot_mode", "chat_id": info["chat_id"], "grouped_id": info.get("grouped_id")})
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
            forward_cache_set(info["chat_id"], True)

        if total_count > 0 and success_count == 0:
            raise RuntimeError(f"media_album_forward job failed for all targets chat_id={info['chat_id']} grouped_id={info.get('grouped_id')}")

        return []

    except errors.ChatForwardsRestrictedError:
        forward_cache_set(info["chat_id"], False)
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


def touch_health(status: str = "ok"):
    write_health_file(default_health_path(), status)


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
                    cleanup_files(cleanup_paths, job.get("source_kind", "unknown"), job.get("info", {}).get("msg_id"), log)
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
                    cleanup_files(cleanup_paths, job.get("source_kind", "unknown"), job.get("info", {}).get("msg_id"), log)
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

    if should_ignore(info, IGNORE_USERS, IGNORE_IDS):
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
    overall_timer = StepTimer()

    msg_date_obj = getattr(msg, "date", None)
    edit_date_obj = getattr(msg, "edit_date", None)
    event_now = now_ts()
    event_lag_ms = int((event_now - msg_date_obj.timestamp()) * 1000) if msg_date_obj else None
    edit_lag_ms = int((event_now - edit_date_obj.timestamp()) * 1000) if edit_date_obj else None

    lag_record(getattr(msg, "chat_id", None), event_lag_ms)
    maybe_debug_log(log, {
        "ts": tstamp(),
        "type": "event_received",
        "source_kind": source_kind,
        "chat_id": getattr(msg, "chat_id", None),
        "msg_id": getattr(msg, "id", None),
        "msg_date": str(msg_date_obj),
        "edit_date": str(edit_date_obj),
        "event_lag_ms": event_lag_ms,
        "edit_lag_ms": edit_lag_ms,
        "background_tasks_count": len(BACKGROUND_TASKS),
        "pending_albums_count": len(PENDING_ALBUMS),
        "download_slots_in_use": DOWNLOAD_CONCURRENCY - getattr(download_semaphore, "_value", DOWNLOAD_CONCURRENCY),
        "has_media": msg.media is not None,
        "grouped_id": getattr(msg, "grouped_id", None),
    })

    summary = lag_summary(getattr(msg, "chat_id", None))
    if summary and summary.get("samples", 0) in {1, 5, 10, 20, 50, 100, 200}:
        log({
            "ts": tstamp(),
            "type": "lag_summary",
            "chat_id": getattr(msg, "chat_id", None),
            **summary,
        })

    routes_timer = StepTimer()
    routes = find_matching_routes(msg.chat_id, ROUTES)
    routes_ms = routes_timer.ms()
    if not routes:
        maybe_debug_log(log, {
            "ts": tstamp(),
            "type": "perf",
            "step": "find_matching_routes",
            "chat_id": getattr(msg, "chat_id", None),
            "msg_id": getattr(msg, "id", None),
            "ms": routes_ms,
            "matched": 0,
        })
        return

    resolve_timer = StepTimer()
    info = await resolve_sender_info_from_message(msg, chat_id_hint=msg.chat_id)
    resolve_ms = resolve_timer.ms()

    if should_ignore(info, IGNORE_USERS, IGNORE_IDS):
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

    maybe_debug_log(log, {
        "ts": tstamp(),
        "type": "perf",
        "step": "handle_incoming_message_preprocess",
        "chat_id": info["chat_id"],
        "msg_id": info["msg_id"],
        "find_matching_routes_ms": routes_ms,
        "resolve_sender_info_ms": resolve_ms,
        "total_preprocess_ms": overall_timer.ms(),
    })

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

def register_telethon_handlers():
    assert client is not None
    client.add_event_handler(on_new_message, events.NewMessage(incoming=True))
    client.add_event_handler(on_message_edited, events.MessageEdited(incoming=True))


async def startup_kafka_producer():
    global producer
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    )
    await producer.start()


async def startup_kafka_consumers():
    global text_consumer, media_consumer

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
    global producer, text_consumer, media_consumer
    if producer is not None:
        await producer.stop()
        producer = None
    text_consumer = None
    media_consumer = None


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
        "app_mode": APP_MODE,
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
    touch_health(f"starting:{APP_MODE}")
    print(f"✔ Telegram forwarder mode={APP_MODE} running — Ctrl+C to stop…")

    consumer_tasks = []

    try:
        if APP_MODE == "listen":
            LOGGER.info("startup listen mode sender=telethon-user producer=true consumers=false")
            assert client is not None
            register_telethon_handlers()
            async with client:
                try:
                    try:
                        me = await client.get_me()
                        LOGGER.info("telethon auth ok id=%s username=%s", getattr(me, "id", None), getattr(me, "username", None))
                    except Exception as e:
                        LOGGER.warning("telethon auth probe failed err=%s msg=%s", e.__class__.__name__, str(e))

                    await startup_kafka_producer()
                    touch_health("listen:running")
                    await client.run_until_disconnected()
                finally:
                    LOGGER.warning("client disconnected")

        elif APP_MODE == "send":
            LOGGER.info("startup send mode sender=telegram-bot producer=false consumers=true")
            if not TELEGRAM_BOT_TOKEN:
                raise RuntimeError("TELEGRAM_BOT_TOKEN is required in APP_MODE=send")
            global bot
            bot = Bot(token=TELEGRAM_BOT_TOKEN)
            try:
                me = await bot.get_me()
                LOGGER.info("bot auth ok id=%s username=%s", getattr(me, "id", None), getattr(me, "username", None))
            except TelegramError as e:
                LOGGER.warning("bot auth probe failed err=%s msg=%s", e.__class__.__name__, str(e))
            await startup_kafka_consumers()
            touch_health("send:running")
            consumer_tasks = [
                asyncio.create_task(text_consumer_loop()),
                asyncio.create_task(media_consumer_loop()),
            ]
            await asyncio.gather(*consumer_tasks)
    finally:
        for t in consumer_tasks:
            if not t.done():
                t.cancel()
        if consumer_tasks:
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