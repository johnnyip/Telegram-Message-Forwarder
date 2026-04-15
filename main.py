import os
import asyncio
from pathlib import Path
import json
from typing import Any, Callable, Dict, List, Optional, Tuple
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from io import BytesIO

from dotenv import load_dotenv
from telethon import TelegramClient, events, errors
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from telegram import Bot, InputFile
from telegram.error import TelegramError
from telegram.request import HTTPXRequest

from tg_forwarder.runtime.bot_runtime import send_album_via_bot, send_file_via_bot, send_text_via_bot
from tg_forwarder.domain.forwarding_policy import ForwardingPolicyConfig, should_direct_forward_large_album as should_direct_forward_large_album_mod, should_direct_forward_large_media as should_direct_forward_large_media_mod
from tg_forwarder.domain.file_policy import is_stale_file
from tg_forwarder.runtime.healthcheck import default_health_path, write_health_file
from tg_forwarder.runtime.debug_flags import configure_telethon_logger, maybe_debug_log
from tg_forwarder.storage.dedup_cache import dedup_mark, dedup_seen
from tg_forwarder.storage.edit_mapping import enabled as edit_mapping_enabled, get_redis
from tg_forwarder.delivery.edit_updates import edit_forwarded_album_caption, edit_forwarded_media_caption, edit_forwarded_text
from tg_forwarder.domain.formatting import build_header_from_info, should_ignore
from tg_forwarder.runtime.bot_runtime import resolve_topic_thread_id
from tg_forwarder.storage.forward_cache import forward_cache_get, forward_cache_set
from tg_forwarder.processing.job_processing import JobProcessingContext, dispatch_media_job, process_text_job as process_text_job_mod
from tg_forwarder.processing.kafka_jobs import publish_album_job as publish_album_job_mod, publish_media_job as publish_media_job_mod, publish_text_job as publish_text_job_mod
from tg_forwarder.runtime.lag_stats import lag_record, lag_summary
from tg_forwarder.runtime.logging_setup import setup_logging
from tg_forwarder.domain.media import get_media_size, media_type
from tg_forwarder.core.metrics import StepTimer
from tg_forwarder.domain.routes import find_matching_routes, load_routes
from tg_forwarder.delivery.senders import fetch_message_by_id as fetch_message_by_id_mod, fetch_messages_by_ids as fetch_messages_by_ids_mod, send_album_to_target as send_album_to_target_mod, send_file_to_target as send_file_to_target_mod, send_text_to_target as send_text_to_target_mod
from tg_forwarder.processing.snapshot import snapshot_album_messages as snapshot_album_messages_mod, snapshot_media_message as snapshot_media_message_mod
from tg_forwarder.domain.telegram_info import resolve_sender_info_from_message
from tg_forwarder.storage.send_journal import journal_claim, journal_get, journal_mark, journal_record_failure, journal_is_poison, SEND_JOURNAL_MAX_FAILURES
from tg_forwarder.core.utils import cleanup_files, cleanup_logs, cleanup_retained_files, json_bytes, log as base_log, now_ts, tstamp
from tg_forwarder.runtime.verbose_flags import BOT_STARTUP_SMOKE_TEST
# migrate imports are lazy — loaded only when APP_MODE=migrate


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
RETAIN_DIR = DOWNLOAD_DIR / "retained"

# Delay
DELAY_SECONDS = int(os.getenv("DELAY_SECONDS", "5"))                  # text send delay
MEDIA_DELAY_SECONDS = int(os.getenv("MEDIA_DELAY_SECONDS", "0"))      # media send delay

# Concurrency
DOWNLOAD_CONCURRENCY = max(1, int(os.getenv("DOWNLOAD_CONCURRENCY", "8")))

# Cleanup
DELETE_AFTER_SEND = os.getenv("DELETE_AFTER_SEND", "true").strip().lower() in {"1", "true", "yes", "y"}

# Album gather window
ALBUM_GATHER_SECONDS = float(os.getenv("ALBUM_GATHER_SECONDS", "3"))

# Kafka
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TEXT_TOPIC = os.getenv("KAFKA_TEXT_TOPIC", "tg-forward-text")
KAFKA_MEDIA_TOPIC = os.getenv("KAFKA_MEDIA_TOPIC", "tg-forward-media")
KAFKA_CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "tg-forwarder")
APP_MODE = os.getenv("APP_MODE", "listen").strip().lower()
if APP_MODE not in {"listen", "send", "migrate"}:
    raise RuntimeError("APP_MODE must be 'listen', 'send', or 'migrate'")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
BOT_HTTP_POOL_SIZE = max(1, int(os.getenv("BOT_HTTP_POOL_SIZE", "80")))
BOT_POOL_TIMEOUT_SECONDS = float(os.getenv("BOT_POOL_TIMEOUT_SECONDS", "90"))
BOT_READ_TIMEOUT_SECONDS = float(os.getenv("BOT_READ_TIMEOUT_SECONDS", "180"))
BOT_WRITE_TIMEOUT_SECONDS = float(os.getenv("BOT_WRITE_TIMEOUT_SECONDS", "180"))
BOT_CONNECT_TIMEOUT_SECONDS = float(os.getenv("BOT_CONNECT_TIMEOUT_SECONDS", "45"))

# Large media forward threshold
LARGE_MEDIA_FORWARD_THRESHOLD_MB = int(os.getenv("LARGE_MEDIA_FORWARD_THRESHOLD_MB", "30"))
LARGE_MEDIA_FORWARD_THRESHOLD_BYTES = LARGE_MEDIA_FORWARD_THRESHOLD_MB * 1024 * 1024

# Forward policy:
# auto      -> auto detect, unknown chat will try forward for large media if not blocked
# allowlist -> only FORWARDABLE_SOURCE_CHATS may try large-media forward
# never     -> never try direct forward for large media
FORWARD_POLICY = os.getenv("FORWARD_POLICY", "auto").strip().lower()
LISTEN_EDITED_MESSAGES = os.getenv("LISTEN_EDITED_MESSAGES", "true").strip().lower() in {"1", "true", "yes", "y"}
ENABLE_DIRECT_FORWARD_JOBS = os.getenv("ENABLE_DIRECT_FORWARD_JOBS", "false").strip().lower() in {"1", "true", "yes", "y"}

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
RETAIN_DIR.mkdir(parents=True, exist_ok=True)

LOGGER = setup_logging()
configure_telethon_logger()

client: Optional[TelegramClient] = None
if APP_MODE in {"listen", "migrate"}:
    if API_ID is None or not API_HASH:
        raise RuntimeError(f"API_ID and API_HASH are required in APP_MODE={APP_MODE}")
    client = TelegramClient(SESSION_NAME, API_ID, API_HASH, base_logger="telethon")
    client.parse_mode = "md"

if TELEGRAM_BOT_TOKEN:
    bot_request = HTTPXRequest(
        connection_pool_size=BOT_HTTP_POOL_SIZE,
        pool_timeout=BOT_POOL_TIMEOUT_SECONDS,
        read_timeout=BOT_READ_TIMEOUT_SECONDS,
        write_timeout=BOT_WRITE_TIMEOUT_SECONDS,
        connect_timeout=BOT_CONNECT_TIMEOUT_SECONDS,
    )
    bot = Bot(token=TELEGRAM_BOT_TOKEN, request=bot_request)

download_semaphore = asyncio.Semaphore(DOWNLOAD_CONCURRENCY)
BACKGROUND_TASKS = set()

producer: Optional[AIOKafkaProducer] = None
text_consumer: Optional[AIOKafkaConsumer] = None
media_consumer: Optional[AIOKafkaConsumer] = None
BOT_SEND_CONCURRENCY = max(1, int(os.getenv("BOT_SEND_CONCURRENCY", "3")))
bot_send_semaphore = asyncio.Semaphore(BOT_SEND_CONCURRENCY)
edit_bot_semaphore = asyncio.Semaphore(1)
DATE_SEPARATOR_TIMEZONE = os.getenv("DATE_SEPARATOR_TIMEZONE", os.getenv("TZ", "Asia/Hong_Kong")).strip() or "Asia/Hong_Kong"
DATE_SEPARATOR_ENABLED = os.getenv("DATE_SEPARATOR_ENABLED", "true").strip().lower() in {"1", "true", "yes", "y"}

# Pending albums:
# (source_kind, chat_id, grouped_id) -> {"messages": {msg_id: msg}, "task": asyncio.Task, "last_update": float}
PENDING_ALBUMS: Dict[Tuple[str, int, int], dict] = {}
FORWARDING_POLICY_CONFIG = ForwardingPolicyConfig(
    threshold_bytes=LARGE_MEDIA_FORWARD_THRESHOLD_BYTES,
    policy=FORWARD_POLICY,
    allowlist=FORWARDABLE_SOURCE_CHATS,
    denylist=NONFORWARDABLE_SOURCE_CHATS,
    enable_direct_forward_jobs=ENABLE_DIRECT_FORWARD_JOBS,
)


# ============================================================
# Basic helpers
# ============================================================

def log(obj: dict):
    base_log(obj, LOG_DIR)


def spawn_bg(coro):
    task = asyncio.create_task(coro)
    BACKGROUND_TASKS.add(task)

    def _on_done(t: asyncio.Task):
        BACKGROUND_TASKS.discard(t)
        try:
            exc = t.exception()
        except asyncio.CancelledError:
            log({
                "ts": tstamp(),
                "type": "info",
                "op": "background_task_cancelled",
            })
            return
        except Exception as e:
            log({
                "ts": tstamp(),
                "type": "err",
                "op": "background_task_exception_probe_failed",
                "err": e.__class__.__name__,
                "msg": str(e),
            })
            return

        if exc is not None:
            log({
                "ts": tstamp(),
                "type": "err",
                "op": "background_task_failed",
                "err": exc.__class__.__name__,
                "msg": str(exc),
                "task": repr(t),
                "coro": repr(coro),
            })

    task.add_done_callback(_on_done)
    return task


async def throttle(seconds: int):
    if seconds > 0:
        await asyncio.sleep(seconds)


async def run_api(coro_fn: Callable[[], Any], op: str, extra: Optional[dict] = None):
    """
    Call *coro_fn()* (a zero-argument callable that returns a coroutine) and
    handle Telethon errors.  A fresh coroutine is created on each attempt so
    that the FloodWait retry path actually works – re-awaiting an already-
    exhausted coroutine raises RuntimeError in Python 3.10+.
    """
    extra = extra or {}
    try:
        return await coro_fn()
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
        return await coro_fn()
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


def should_direct_forward_large_media(info: dict, msg) -> bool:
    return should_direct_forward_large_media_mod(
        info,
        msg,
        config=FORWARDING_POLICY_CONFIG,
        get_media_size=get_media_size,
        forward_cache_get=forward_cache_get,
    )


def should_direct_forward_large_album(chat_id: int, info: dict, msgs: List[Any]) -> bool:
    return should_direct_forward_large_album_mod(
        chat_id,
        info,
        msgs,
        config=FORWARDING_POLICY_CONFIG,
        get_media_size=get_media_size,
        forward_cache_get=forward_cache_get,
    )


def dedup_key_for_message(info: dict, source_kind: str) -> str:
    return f"media:{info.get('chat_id')}:{info.get('msg_id')}:{source_kind}"


def dedup_key_for_album(chat_id: int, grouped_id: int, source_kind: str) -> str:
    return f"album:{chat_id}:{grouped_id}:{source_kind}"


def job_fingerprint(job: dict) -> str:
    job_type = job.get("job_type", "unknown")
    source_kind = job.get("source_kind", "unknown")
    info = job.get("info", {}) if isinstance(job.get("info"), dict) else {}
    chat_id = info.get("chat_id") or job.get("chat_id")
    grouped_id = info.get("grouped_id")
    msg_id = info.get("msg_id") or job.get("msg_id")
    if isinstance(job.get("msg_ids"), list) and job.get("msg_ids"):
        msg_ids = ",".join(str(x) for x in job.get("msg_ids"))
        return f"{job_type}:{chat_id}:msgs:{msg_ids}:{source_kind}"
    if grouped_id is not None:
        snapshot_ids = []
        if isinstance(job.get("snapshots"), list):
            snapshot_ids = [str(s.get("msg_id")) for s in job.get("snapshots") if isinstance(s, dict) and s.get("msg_id") is not None]
        if snapshot_ids:
            return f"{job_type}:{chat_id}:group:{grouped_id}:msgs:{','.join(snapshot_ids)}:{source_kind}"
        return f"{job_type}:{chat_id}:group:{grouped_id}:msg:{msg_id}:{source_kind}"
    return f"{job_type}:{chat_id}:msg:{msg_id}:{source_kind}"


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


async def direct_forward_large_media(msg, info: dict, routes: List[Dict[str, Any]], source_kind: str):
    if client is None:
        raise RuntimeError("Telethon client unavailable for direct forward")

    is_edit = source_kind == "edited"
    hdr = build_header_from_info(info, is_edit=is_edit)
    success_count = 0
    total_count = 0
    try:
        for route in routes:
            for target in route["targets"]:
                total_count += 1
                extra = {
                    "route": route["name"],
                    "dst": target,
                    "src_msg": info["msg_id"],
                    "chat_id": info["chat_id"],
                    "chat_title": info["chat_title"],
                    "sender_id": info["sender_id"],
                    "sender_username": info["sender_username"],
                    "sender_display": info["sender_display"],
                    "source_kind": source_kind,
                    "media_size": get_media_size(msg),
                    "media_type": media_type(msg),
                }
                await run_api(lambda t=target, h=hdr: client.send_message(t, h), op="send_header_before_forward_large", extra=extra)
                await run_api(lambda t=target, m=msg: client.forward_messages(t, m, m.peer_id), op="forward_large_media", extra=extra)
                log({"ts": tstamp(), "type": "out", "op": "forward_large_media", "status": "ok", **extra})
                success_count += 1
        if success_count > 0:
            forward_cache_set(info["chat_id"], True)
        if total_count > 0 and success_count == 0:
            raise RuntimeError(f"direct forward media failed for all targets chat_id={info['chat_id']} msg_id={info['msg_id']}")
    except errors.ChatForwardsRestrictedError:
        forward_cache_set(info["chat_id"], False)
        log({"ts": tstamp(), "type": "warn", "op": "forward_large_media", "note": "chat_forwards_restricted_fallback_to_kafka_snapshot", "chat_id": info["chat_id"], "msg_id": info["msg_id"]})
        await publish_media_job_mod(
            msg, info, routes, source_kind,
            media_delay_seconds=MEDIA_DELAY_SECONDS,
            topic=KAFKA_MEDIA_TOPIC,
            producer=producer,
            json_bytes=json_bytes,
            log=log,
            should_direct_forward_large_media=lambda *_args, **_kwargs: False,
            snapshot_kwargs={
                "spool_dir": SPOOL_DIR,
                "download_semaphore": download_semaphore,
                "run_api": run_api,
                "media_type_fn": media_type,
                "get_media_size_fn": get_media_size,
                "forward_policy": FORWARD_POLICY,
            },
        )


async def direct_forward_large_album(msgs: List[Any], info: dict, routes: List[Dict[str, Any]], source_kind: str):
    if client is None:
        raise RuntimeError("Telethon client unavailable for direct album forward")

    sorted_msgs = sorted(msgs, key=lambda m: m.id)
    is_edit = source_kind == "edited"
    hdr = build_header_from_info(info, is_edit=is_edit)
    success_count = 0
    total_count = 0
    try:
        for route in routes:
            for target in route["targets"]:
                total_count += 1
                extra = {
                    "route": route["name"],
                    "dst": target,
                    "chat_id": info["chat_id"],
                    "chat_title": info["chat_title"],
                    "sender_id": info["sender_id"],
                    "sender_username": info["sender_username"],
                    "sender_display": info["sender_display"],
                    "source_kind": source_kind,
                    "msg_ids": [m.id for m in sorted_msgs],
                    "grouped_id": info.get("grouped_id"),
                }
                await run_api(lambda t=target, h=hdr: client.send_message(t, h), op="send_header_before_forward_album", extra=extra)
                await run_api(lambda t=target, ms=sorted_msgs: client.forward_messages(t, ms, ms[0].peer_id), op="forward_large_album", extra=extra)
                log({"ts": tstamp(), "type": "out", "op": "forward_large_album", "status": "ok", **extra})
                success_count += 1
        if success_count > 0:
            forward_cache_set(info["chat_id"], True)
        if total_count > 0 and success_count == 0:
            raise RuntimeError(f"direct forward album failed for all targets chat_id={info['chat_id']} grouped_id={info.get('grouped_id')}")
    except errors.ChatForwardsRestrictedError:
        forward_cache_set(info["chat_id"], False)
        log({"ts": tstamp(), "type": "warn", "op": "forward_large_album", "note": "chat_forwards_restricted_fallback_to_kafka_snapshot", "chat_id": info["chat_id"], "grouped_id": info.get("grouped_id"), "msg_ids": [m.id for m in sorted_msgs]})
        await publish_album_job_mod(
            sorted_msgs, info, routes, source_kind,
            media_delay_seconds=MEDIA_DELAY_SECONDS,
            topic=KAFKA_MEDIA_TOPIC,
            producer=producer,
            json_bytes=json_bytes,
            log=log,
            should_direct_forward_large_album=lambda *_args, **_kwargs: False,
            snapshot_kwargs={
                "spool_dir": SPOOL_DIR,
                "download_semaphore": download_semaphore,
                "run_api": run_api,
                "media_type_fn": media_type,
                "get_media_size_fn": get_media_size,
                "forward_policy": FORWARD_POLICY,
            },
        )


TEXT_TARGET_PARALLEL = os.getenv("TEXT_TARGET_PARALLEL", "true").strip().lower() in {"1", "true", "yes", "y"}
MEDIA_TARGET_PARALLEL = os.getenv("MEDIA_TARGET_PARALLEL", "true").strip().lower() in {"1", "true", "yes", "y"}
BOT_UPLOAD_MAX_MB = int(os.getenv("BOT_UPLOAD_MAX_MB", "100"))
BOT_UPLOAD_MAX_BYTES = BOT_UPLOAD_MAX_MB * 1024 * 1024
BOT_ALBUM_MAX_TOTAL_MB = int(os.getenv("BOT_ALBUM_MAX_TOTAL_MB", "100"))
BOT_ALBUM_MAX_TOTAL_BYTES = BOT_ALBUM_MAX_TOTAL_MB * 1024 * 1024


async def send_text_to_target(route: Dict[str, Any], tgt: Any, combined: str, info: dict, source_kind: str) -> bool:
    if APP_MODE == "send" and bot is not None:
        return await send_text_via_bot(bot, tgt, combined, info, route, source_kind, bot_send_semaphore=bot_send_semaphore, log=log)
    return await send_text_to_target_mod(route, tgt, combined, info, source_kind, client=client, run_api=run_api, log=log)


async def send_file_to_target(route: Dict[str, Any], tgt: Any, fpath: Path, caption: str, info: dict, source_kind: str) -> bool:
    if APP_MODE == "send" and bot is not None:
        return await send_file_via_bot(bot, tgt, fpath, caption, info, route, source_kind, bot_send_semaphore=bot_send_semaphore, log=log, upload_max_bytes=BOT_UPLOAD_MAX_BYTES)
    return await send_file_to_target_mod(route, tgt, fpath, caption, info, source_kind, client=client, run_api=run_api, log=log)


async def send_album_to_target(route: Dict[str, Any], tgt: Any, files: List[str], caption: str, info: dict, source_kind: str) -> bool:
    if APP_MODE == "send" and bot is not None:
        return await send_album_via_bot(
            bot,
            tgt,
            files,
            caption,
            info,
            route,
            source_kind,
            bot_send_semaphore=bot_send_semaphore,
            album_max_total_bytes=BOT_ALBUM_MAX_TOTAL_BYTES,
            upload_max_bytes=BOT_UPLOAD_MAX_BYTES,
            log=log,
        )
    return await send_album_to_target_mod(route, tgt, files, caption, info, source_kind, client=client, run_api=run_api, log=log)


async def fetch_message_by_id(chat_id: int, msg_id: int):
    if client is None:
        raise RuntimeError("Telethon client unavailable in current mode")
    return await fetch_message_by_id_mod(chat_id, msg_id, client=client, log=log)


async def fetch_messages_by_ids(chat_id: int, msg_ids: List[int]):
    if client is None:
        raise RuntimeError("Telethon client unavailable in current mode")
    return await fetch_messages_by_ids_mod(chat_id, msg_ids, client=client, log=log)


def build_job_processing_ctx() -> JobProcessingContext:
    return JobProcessingContext(
        app_mode=APP_MODE,
        bot=bot,
        client=client,
        route_map=ROUTE_MAP,
        ignore_users=IGNORE_USERS,
        ignore_ids=IGNORE_IDS,
        delete_after_send=DELETE_AFTER_SEND,
        text_target_parallel=TEXT_TARGET_PARALLEL,
        media_target_parallel=MEDIA_TARGET_PARALLEL,
        log=log,
        should_ignore=should_ignore,
        build_header_from_info=build_header_from_info,
        is_stale_file=is_stale_file,
        send_text_to_target=send_text_to_target,
        send_file_to_target=send_file_to_target,
        send_album_to_target=send_album_to_target,
        fetch_message_by_id=fetch_message_by_id,
        fetch_messages_by_ids=fetch_messages_by_ids,
        run_api=run_api,
        snapshot_media_message=snapshot_media_message,
        snapshot_album_messages=snapshot_album_messages,
        forward_cache_set=forward_cache_set,
    )

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
            fingerprint = None
            try:
                job = json.loads(record.value.decode("utf-8"))
                await sleep_until_due(job)
                fingerprint = job_fingerprint(job)
                existing = journal_get(fingerprint)
                if existing and existing.get("status") in {"processing", "done"}:
                    log({
                        "ts": tstamp(),
                        "type": "info",
                        "op": "skip_text_job_from_journal",
                        "fingerprint": fingerprint,
                        "job_type": job.get("job_type"),
                        "chat_id": job.get("info", {}).get("chat_id"),
                        "msg_id": job.get("info", {}).get("msg_id"),
                        "journal_status": existing.get("status"),
                    })
                    await text_consumer.commit()
                    continue

                claimed = journal_claim(fingerprint, {
                    "job_type": job.get("job_type"),
                    "chat_id": job.get("info", {}).get("chat_id"),
                    "msg_id": job.get("info", {}).get("msg_id"),
                    "source_kind": job.get("source_kind"),
                })
                if not claimed:
                    log({
                        "ts": tstamp(),
                        "type": "info",
                        "op": "skip_text_job_claim_failed",
                        "fingerprint": fingerprint,
                        "job_type": job.get("job_type"),
                        "chat_id": job.get("info", {}).get("chat_id"),
                        "msg_id": job.get("info", {}).get("msg_id"),
                    })
                    await text_consumer.commit()
                    continue

                cleanup_paths = await process_text_job_mod(job, build_job_processing_ctx())
                await text_consumer.commit()
                journal_mark(fingerprint, {
                    "status": "done",
                    "job_type": job.get("job_type"),
                    "chat_id": job.get("info", {}).get("chat_id"),
                    "msg_id": job.get("info", {}).get("msg_id"),
                    "source_kind": job.get("source_kind"),
                    "outcome": "text_sent",
                })

                if cleanup_paths:
                    cleanup_files(cleanup_paths, job.get("source_kind", "unknown"), job.get("info", {}).get("msg_id"), log)
            except Exception as e:
                # P12: Track consecutive failures per fingerprint.  After
                # SEND_JOURNAL_MAX_FAILURES attempts the message is treated as a
                # poison pill: we commit the offset to unblock the partition and
                # log at critical level so operators can investigate.
                if fingerprint:
                    failures = journal_record_failure(fingerprint, {
                        "job_type": job.get("job_type") if isinstance(job, dict) else None,
                        "err": e.__class__.__name__,
                    })
                    if failures >= SEND_JOURNAL_MAX_FAILURES:
                        log({
                            "ts": tstamp(),
                            "type": "critical",
                            "op": "text_consumer_poison_skip",
                            "fingerprint": fingerprint,
                            "failures": failures,
                            "job_type": job.get("job_type") if isinstance(job, dict) else None,
                            "chat_id": job.get("info", {}).get("chat_id") if isinstance(job, dict) else None,
                            "msg_id": job.get("info", {}).get("msg_id") if isinstance(job, dict) else None,
                            "note": "max failures exceeded; committing offset to unblock partition",
                        })
                        await text_consumer.commit()
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
            fingerprint = None
            try:
                job = json.loads(record.value.decode("utf-8"))
                maybe_debug_log(log, {
                    "ts": tstamp(),
                    "type": "debug",
                    "op": "media_consumer_job_received",
                    "job_type": job.get("job_type"),
                    "source_kind": job.get("source_kind"),
                    "chat_id": job.get("info", {}).get("chat_id") if isinstance(job.get("info"), dict) else None,
                    "msg_id": job.get("info", {}).get("msg_id") if isinstance(job.get("info"), dict) else None,
                    "grouped_id": job.get("info", {}).get("grouped_id") if isinstance(job.get("info"), dict) else None,
                    "due_at": job.get("due_at"),
                    "created_at": job.get("created_at"),
                })
                # P1: Sleep BEFORE claiming the journal entry.  If the process
                # crashes during the delay, the offset is uncommitted and Kafka
                # redelivers the message on restart.  Claiming first and then
                # sleeping causes the journal to have a "processing" entry that
                # causes a skip on restart, silently dropping the message.
                await sleep_until_due(job)
                fingerprint = job_fingerprint(job)
                existing = journal_get(fingerprint)
                if existing and existing.get("status") in {"processing", "done"}:
                    log({
                        "ts": tstamp(),
                        "type": "info",
                        "op": "skip_media_job_from_journal",
                        "fingerprint": fingerprint,
                        "job_type": job.get("job_type"),
                        "chat_id": job.get("info", {}).get("chat_id") if isinstance(job.get("info"), dict) else None,
                        "msg_id": job.get("info", {}).get("msg_id") if isinstance(job.get("info"), dict) else None,
                        "grouped_id": job.get("info", {}).get("grouped_id") if isinstance(job.get("info"), dict) else None,
                        "journal_status": existing.get("status"),
                    })
                    await media_consumer.commit()
                    continue

                claimed = journal_claim(fingerprint, {
                    "job_type": job.get("job_type"),
                    "chat_id": job.get("info", {}).get("chat_id") if isinstance(job.get("info"), dict) else None,
                    "msg_id": job.get("info", {}).get("msg_id") if isinstance(job.get("info"), dict) else None,
                    "grouped_id": job.get("info", {}).get("grouped_id") if isinstance(job.get("info"), dict) else None,
                    "source_kind": job.get("source_kind"),
                })
                if not claimed:
                    log({
                        "ts": tstamp(),
                        "type": "info",
                        "op": "skip_media_job_claim_failed",
                        "fingerprint": fingerprint,
                        "job_type": job.get("job_type"),
                        "chat_id": job.get("info", {}).get("chat_id") if isinstance(job.get("info"), dict) else None,
                        "msg_id": job.get("info", {}).get("msg_id") if isinstance(job.get("info"), dict) else None,
                        "grouped_id": job.get("info", {}).get("grouped_id") if isinstance(job.get("info"), dict) else None,
                    })
                    await media_consumer.commit()
                    continue

                cleanup_paths = await dispatch_media_job(job, build_job_processing_ctx())

                await media_consumer.commit()
                journal_mark(fingerprint, {
                    "status": "done",
                    "job_type": job.get("job_type"),
                    "chat_id": job.get("info", {}).get("chat_id") if isinstance(job.get("info"), dict) else None,
                    "msg_id": job.get("info", {}).get("msg_id") if isinstance(job.get("info"), dict) else None,
                    "grouped_id": job.get("info", {}).get("grouped_id") if isinstance(job.get("info"), dict) else None,
                    "source_kind": job.get("source_kind"),
                    "outcome": "cleanup_pending" if cleanup_paths else "retained_or_forwarded",
                })
                maybe_debug_log(log, {
                    "ts": tstamp(),
                    "type": "debug",
                    "op": "media_consumer_job_committed",
                    "job_type": job.get("job_type") if isinstance(job, dict) else None,
                    "chat_id": job.get("info", {}).get("chat_id") if isinstance(job, dict) and isinstance(job.get("info"), dict) else None,
                    "msg_id": job.get("info", {}).get("msg_id") if isinstance(job, dict) and isinstance(job.get("info"), dict) else None,
                    "grouped_id": job.get("info", {}).get("grouped_id") if isinstance(job, dict) and isinstance(job.get("info"), dict) else None,
                    "cleanup_count": len(cleanup_paths),
                })

                if cleanup_paths:
                    cleanup_files(cleanup_paths, job.get("source_kind", "unknown"), job.get("info", {}).get("msg_id"), log)
            except Exception as e:
                if fingerprint:
                    failures = journal_record_failure(fingerprint, {
                        "job_type": job.get("job_type") if isinstance(job, dict) else None,
                        "err": e.__class__.__name__,
                    })
                    if failures >= SEND_JOURNAL_MAX_FAILURES:
                        log({
                            "ts": tstamp(),
                            "type": "critical",
                            "op": "media_consumer_poison_skip",
                            "fingerprint": fingerprint,
                            "failures": failures,
                            "job_type": job.get("job_type") if isinstance(job, dict) else None,
                            "chat_id": job.get("info", {}).get("chat_id") if isinstance(job, dict) else None,
                            "msg_id": job.get("info", {}).get("msg_id") if isinstance(job, dict) else None,
                            "grouped_id": job.get("info", {}).get("grouped_id") if isinstance(job, dict) else None,
                            "note": "max failures exceeded; committing offset to unblock partition",
                        })
                        await media_consumer.commit()
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

    routes = find_matching_routes(chat_id, ROUTES)
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

    if APP_MODE == "listen" and should_direct_forward_large_album(info["chat_id"], info, msgs):
        await direct_forward_large_album(msgs, info, routes, source_kind)
    else:
        await publish_album_job(msgs, info, routes, source_kind)
    dedup_mark(dedup_key_for_album(info["chat_id"], grouped_id, source_kind))


def add_to_album_buffer(msg, source_kind: str):
    chat_id = msg.chat_id
    grouped_id = getattr(msg, "grouped_id", None)
    if grouped_id is None:
        return False

    key = (source_kind, chat_id, grouped_id)
    now = now_ts()
    if key not in PENDING_ALBUMS:
        PENDING_ALBUMS[key] = {
            "messages": {},
            "task": None,
            "last_update": now,
        }

    album = PENDING_ALBUMS[key]
    album["messages"][msg.id] = msg
    album["last_update"] = now

    task = album.get("task")
    if task is None or task.done():
        album["task"] = spawn_bg(_album_timer(source_kind, chat_id, grouped_id))

    return True


async def _album_timer(source_kind: str, chat_id: int, grouped_id: int):
    key = (source_kind, chat_id, grouped_id)
    while True:
        await asyncio.sleep(ALBUM_GATHER_SECONDS)
        album = PENDING_ALBUMS.get(key)
        if not album:
            return
        idle_for = now_ts() - float(album.get("last_update", 0))
        if idle_for >= ALBUM_GATHER_SECONDS:
            await flush_album(source_kind, chat_id, grouped_id)
            return


# ============================================================
# Incoming Telethon event handling
# ============================================================

async def handle_incoming_message(msg, source_kind: str):
    overall_timer = StepTimer()

    msg_date_obj = getattr(msg, "date", None)
    edit_date_obj = getattr(msg, "edit_date", None)
    event_now = now_ts()

    if msg.media is not None:
        grouped_id = getattr(msg, "grouped_id", None)
        if grouped_id is not None:
            album_key = dedup_key_for_album(getattr(msg, "chat_id", None), grouped_id, source_kind)
            if dedup_seen(album_key):
                log({
                    "ts": tstamp(),
                    "type": "info",
                    "op": "skip_duplicate_album_event",
                    "chat_id": getattr(msg, "chat_id", None),
                    "msg_id": getattr(msg, "id", None),
                    "grouped_id": grouped_id,
                    "source_kind": source_kind,
                })
                return
        else:
            media_key = f"media:{getattr(msg, 'chat_id', None)}:{getattr(msg, 'id', None)}:{source_kind}"
            if dedup_seen(media_key):
                log({
                    "ts": tstamp(),
                    "type": "info",
                    "op": "skip_duplicate_media_event",
                    "chat_id": getattr(msg, "chat_id", None),
                    "msg_id": getattr(msg, "id", None),
                    "source_kind": source_kind,
                })
                return
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
        # P7: Dedup text events so Telethon reconnect re-deliveries do not
        # publish a second Kafka job for the same message.
        _text_dedup_key = f"text:{info.get('chat_id')}:{info.get('msg_id')}:{source_kind}"
        if dedup_seen(_text_dedup_key):
            log({
                "ts": tstamp(),
                "type": "info",
                "op": "skip_duplicate_text_event",
                "chat_id": info.get("chat_id"),
                "msg_id": info.get("msg_id"),
                "source_kind": source_kind,
            })
            return
        await publish_text_job(msg, info, routes, source_kind)
        dedup_mark(_text_dedup_key)
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

    # Dedup mark is set only AFTER Kafka publish succeeds.
    # Marking before publish means a Kafka failure (broker down, network blip)
    # causes the downloaded file to be orphaned in spool with no job referencing
    # it, and Telethon re-deliveries of the same message are silently dropped.
    _dedup_key = dedup_key_for_message(info, source_kind)

    async def _publish_and_mark():
        if APP_MODE == "listen" and should_direct_forward_large_media(info, msg):
            await direct_forward_large_media(msg, info, routes, source_kind)
        else:
            await publish_media_job(msg, info, routes, source_kind)
        dedup_mark(_dedup_key)

    spawn_bg(_publish_and_mark())


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
        msg = event.message
        info = await resolve_sender_info_from_message(msg, chat_id_hint=getattr(event, "chat_id", None))

        if APP_MODE != "listen" or not edit_mapping_enabled() or bot is None:
            if getattr(msg, "media", None) is not None:
                log({
                    "ts": tstamp(),
                    "type": "info",
                    "op": "skip_edited_media_message",
                    "chat_id": getattr(event, "chat_id", None),
                    "msg_id": getattr(msg, "id", None),
                    "grouped_id": getattr(msg, "grouped_id", None),
                    "note": "edited media/album not re-forwarded to avoid duplicates",
                })
                return
            await handle_incoming_message(msg, source_kind="edited")
            return

        redis = await get_redis()
        if redis is None:
            log({
                "ts": tstamp(),
                "type": "warn",
                "op": "edit_mapping_redis_unavailable",
                "chat_id": info.get("chat_id"),
                "msg_id": info.get("msg_id"),
            })
            return

        if getattr(msg, "media", None) is None:
            edited = await edit_forwarded_text(bot, info, msg.text or msg.message or "[empty]", log, semaphore=edit_bot_semaphore)
            if not edited:
                log({"ts": tstamp(), "type": "warn", "op": "edited_text_no_mapping_or_failed", "chat_id": info.get("chat_id"), "msg_id": info.get("msg_id")})
            return

        if getattr(msg, "grouped_id", None) is not None:
            edited = await edit_forwarded_album_caption(bot, info, (msg.text or msg.message or "").strip(), log, semaphore=edit_bot_semaphore)
            if not edited:
                log({"ts": tstamp(), "type": "warn", "op": "edited_album_no_mapping_or_failed", "chat_id": info.get("chat_id"), "grouped_id": info.get("grouped_id")})
            return

        edited = await edit_forwarded_media_caption(bot, info, (msg.text or msg.message or "").strip(), log, semaphore=edit_bot_semaphore)
        if not edited:
            log({"ts": tstamp(), "type": "warn", "op": "edited_media_no_mapping_or_failed", "chat_id": info.get("chat_id"), "msg_id": info.get("msg_id")})
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

def _render_date_separator_png(now_local: datetime) -> bytes:
    try:
        from PIL import Image, ImageDraw, ImageFont
    except Exception as exc:
        raise RuntimeError(f"Pillow required for date separator image: {exc}")

    width, height = 1600, 320
    bg = (245, 247, 250)
    fg = (32, 33, 36)
    sub = (95, 99, 104)
    line = (210, 214, 220)

    image = Image.new("RGB", (width, height), bg)
    draw = ImageDraw.Draw(image)

    try:
        font_big = ImageFont.truetype("/System/Library/Fonts/Supplemental/Arial Bold.ttf", 92)
        font_small = ImageFont.truetype("/System/Library/Fonts/Supplemental/Arial.ttf", 42)
    except Exception:
        font_big = ImageFont.load_default()
        font_small = ImageFont.load_default()

    date_text = now_local.strftime("%Y-%m-%d")
    weekday_text = now_local.strftime("%A")

    draw.line((80, 160, 520, 160), fill=line, width=4)
    draw.line((1080, 160, 1520, 160), fill=line, width=4)

    bbox = draw.textbbox((0, 0), date_text, font=font_big)
    tw = bbox[2] - bbox[0]
    th = bbox[3] - bbox[1]
    x = (width - tw) / 2
    y = 78
    draw.text((x, y), date_text, fill=fg, font=font_big)

    bbox2 = draw.textbbox((0, 0), weekday_text, font=font_small)
    tw2 = bbox2[2] - bbox2[0]
    x2 = (width - tw2) / 2
    draw.text((x2, y + th + 18), weekday_text, fill=sub, font=font_small)

    out = BytesIO()
    image.save(out, format="PNG")
    return out.getvalue()


async def send_daily_date_separator(bot: Bot):
    tz = ZoneInfo(DATE_SEPARATOR_TIMEZONE)
    now_local = datetime.now(tz)
    image_bytes = _render_date_separator_png(now_local)
    filename = f"date-separator-{now_local.strftime('%Y-%m-%d')}.png"

    seen_targets = set()
    for route in ROUTES:
        for target in route["targets"]:
            if target in seen_targets:
                continue
            seen_targets.add(target)
            thread_id = None
            try:
                chat = await bot.get_chat(int(target))
                if getattr(chat, "is_forum", False):
                    thread_id = None
            except Exception as exc:
                log({"ts": tstamp(), "type": "warn", "op": "date_separator_get_chat_failed", "target": target, "err": exc.__class__.__name__, "msg": str(exc)})
            try:
                async with bot_send_semaphore:
                    await bot.send_photo(chat_id=int(target), photo=InputFile(BytesIO(image_bytes), filename=filename), caption="", message_thread_id=thread_id)
                log({"ts": tstamp(), "type": "out", "op": "date_separator_send", "status": "ok", "target": int(target), "message_thread_id": thread_id, "date": now_local.strftime("%Y-%m-%d")})
            except Exception as exc:
                log({"ts": tstamp(), "type": "err", "op": "date_separator_send", "target": int(target), "message_thread_id": thread_id, "err": exc.__class__.__name__, "msg": str(exc), "date": now_local.strftime("%Y-%m-%d")})


async def daily_date_separator_loop(bot: Bot):
    tz = ZoneInfo(DATE_SEPARATOR_TIMEZONE)
    last_sent_date = None
    while True:
        now_local = datetime.now(tz)
        next_midnight = (now_local + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        sleep_seconds = max(1.0, (next_midnight - now_local).total_seconds())
        await asyncio.sleep(sleep_seconds)
        send_now = datetime.now(tz)
        date_key = send_now.strftime("%Y-%m-%d")
        if last_sent_date == date_key:
            continue
        await send_daily_date_separator(bot)
        last_sent_date = date_key


def print_route_summary():
    summary = []
    for route in ROUTES:
        summary.append({
            "name": route["name"],
            "sources": list(route["sources"]),
            "targets": route["targets"],
        })

    payload = {
        "startup": "ok",
        "app_mode": APP_MODE,
        "kafka_bootstrap_servers": KAFKA_BOOTSTRAP_SERVERS,
        "kafka_text_topic": KAFKA_TEXT_TOPIC,
        "kafka_media_topic": KAFKA_MEDIA_TOPIC,
        "kafka_consumer_group": KAFKA_CONSUMER_GROUP,
        "text_delay_seconds": DELAY_SECONDS,
        "media_delay_seconds": MEDIA_DELAY_SECONDS,
        "delete_after_send": DELETE_AFTER_SEND,
        "ignore_users": sorted(list(IGNORE_USERS)),
        "ignore_ids": sorted(list(IGNORE_IDS)),
        "routes": summary,
    }

    if APP_MODE == "listen":
        payload.update({
            "download_concurrency": DOWNLOAD_CONCURRENCY,
            "album_gather_seconds": ALBUM_GATHER_SECONDS,
            "large_media_forward_threshold_mb": LARGE_MEDIA_FORWARD_THRESHOLD_MB,
            "forward_policy": FORWARD_POLICY,
            "enable_direct_forward_jobs": ENABLE_DIRECT_FORWARD_JOBS,
            "listen_edited_messages": LISTEN_EDITED_MESSAGES,
            "forwardable_source_chats": sorted(list(FORWARDABLE_SOURCE_CHATS)),
            "nonforwardable_source_chats": sorted(list(NONFORWARDABLE_SOURCE_CHATS)),
        })
    elif APP_MODE == "send":
        payload.update({
            "sender": "telegram-bot",
            "text_target_parallel": TEXT_TARGET_PARALLEL,
            "media_target_parallel": MEDIA_TARGET_PARALLEL,
            "bot_send_concurrency": BOT_SEND_CONCURRENCY,
            "has_bot_token": bool(TELEGRAM_BOT_TOKEN),
        })
    elif APP_MODE == "migrate":
        from tg_forwarder.migrate.runner import (
            MIGRATE_TARGET_CHAT_ID_RAW, MIGRATE_GENERAL_THREAD_ID,
            MIGRATE_DELAY_SECONDS, MIGRATE_MIGRATION_TOPIC_NAME, MIGRATE_DRY_RUN,
        )
        from tg_forwarder.migrate.checkpoint import MIGRATE_CHECKPOINT_PATH
        payload.update({
            "target_chat_id": MIGRATE_TARGET_CHAT_ID_RAW,
            "general_thread_id": MIGRATE_GENERAL_THREAD_ID,
            "delay_seconds": MIGRATE_DELAY_SECONDS,
            "dry_run": MIGRATE_DRY_RUN,
            "migration_topic_name": MIGRATE_MIGRATION_TOPIC_NAME,
            "checkpoint_path": str(MIGRATE_CHECKPOINT_PATH),
        })

    print(json.dumps(payload, ensure_ascii=False, indent=2))

# ============================================================
# Main
# ============================================================

def startup_banner() -> str:
    if APP_MODE == "listen":
        return f"🚀 tg-forwarder listen up | session={SESSION_NAME} | receiver=telethon | kafka=producer"
    if APP_MODE == "migrate":
        _dry = os.getenv("MIGRATE_DRY_RUN", "false").strip().lower() in {"1", "true", "yes", "y"}
        dry = " [DRY RUN]" if _dry else ""
        return f"🚀 tg-forwarder migrate up | session={SESSION_NAME} | telethon=reader+forwarder{dry}"
    return "🚀 tg-forwarder send up | sender=telegram-bot | kafka=consumers"


async def main():
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    SPOOL_DIR.mkdir(parents=True, exist_ok=True)
    RETAIN_DIR.mkdir(parents=True, exist_ok=True)
    cleanup_logs(LOG_DIR, log)
    cleanup_retained_files(RETAIN_DIR, log)

    print_route_summary()
    touch_health(f"starting:{APP_MODE}")
    banner = startup_banner()
    print(banner)
    LOGGER.info(banner)

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

        elif APP_MODE == "migrate":
            LOGGER.info("startup migrate mode reader=telethon-user forwarder=telethon-user kafka=none")
            assert client is not None
            async with client:
                try:
                    me = await client.get_me()
                    LOGGER.info("telethon auth ok id=%s username=%s", getattr(me, "id", None), getattr(me, "username", None))
                except Exception as e:
                    LOGGER.warning("telethon auth probe failed err=%s msg=%s", e.__class__.__name__, str(e))
                touch_health("migrate:running")
                from tg_forwarder.migrate.runner import run_migration
                await run_migration(client, log)
                touch_health("migrate:done")

        elif APP_MODE == "send":
            LOGGER.info("startup send mode sender=telegram-bot producer=false consumers=true")
            if not TELEGRAM_BOT_TOKEN:
                raise RuntimeError("TELEGRAM_BOT_TOKEN is required in APP_MODE=send")
            assert bot is not None
            try:
                me = await bot.get_me()
                LOGGER.info("bot auth ok id=%s username=%s", getattr(me, "id", None), getattr(me, "username", None))
                if BOT_STARTUP_SMOKE_TEST:
                    for smoke_target in (-1003836445993, -1002548092183):
                        try:
                            r = await bot.send_message(chat_id=smoke_target, text="startup smoke test from tg-forwarder-send")
                            LOGGER.info("bot startup smoke ok target=%s message_id=%s", smoke_target, getattr(r, "message_id", None))
                        except Exception as smoke_err:
                            LOGGER.warning("bot startup smoke failed target=%s err=%s msg=%s", smoke_target, smoke_err.__class__.__name__, str(smoke_err))
            except TelegramError as e:
                LOGGER.warning("bot auth probe failed err=%s msg=%s", e.__class__.__name__, str(e))
            await startup_kafka_consumers()
            touch_health("send:running")
            consumer_tasks = [
                asyncio.create_task(text_consumer_loop()),
                asyncio.create_task(media_consumer_loop()),
            ]
            if DATE_SEPARATOR_ENABLED:
                consumer_tasks.append(asyncio.create_task(daily_date_separator_loop(bot)))
            await asyncio.gather(*consumer_tasks)
    finally:
        for t in consumer_tasks:
            if not t.done():
                t.cancel()
        if consumer_tasks:
            await asyncio.gather(*consumer_tasks, return_exceptions=True)

        # Flush all in-progress album buffers before cancelling their timers.
        # Without this, any album whose gather window hasn't closed yet is
        # silently discarded on every restart / deploy.  flush_album() pops
        # the key from PENDING_ALBUMS itself, so the dict will be empty (or
        # nearly so) by the time we reach the cancel loop below.
        if PENDING_ALBUMS and APP_MODE == "listen":
            pending_keys = list(PENDING_ALBUMS.keys())
            log({
                "ts": tstamp(),
                "type": "info",
                "op": "shutdown_flush_pending_albums",
                "count": len(pending_keys),
                "keys": [f"{sk}:{cid}:{gid}" for sk, cid, gid in pending_keys],
            })
            await asyncio.gather(
                *(flush_album(sk, cid, gid) for sk, cid, gid in pending_keys),
                return_exceptions=True,
            )

        # Cancel any timer tasks that are still running (e.g. a flush that
        # arrived just after the gather above already popped its key).
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
