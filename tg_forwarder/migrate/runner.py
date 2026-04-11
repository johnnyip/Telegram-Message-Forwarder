"""
APP_MODE=migrate — bulk re-forward of old general-topic messages to per-sender
forum topics within the same group.

Flow
----
1.  Load checkpoint (last processed msg_id, counters, migration topic id).
2.  Determine cap_id = latest message in the general thread at startup.
    Messages above cap_id were delivered by the live listen+send pipeline and
    are intentionally excluded.
3.  Iterate messages in the general thread oldest-first (reverse=True, min_id=checkpoint).
4.  For each message:
    a.  Skip text-only messages.
    b.  Skip service messages (pin, join, …).
    c.  Parse the sender from forward metadata or message caption.
    d.  Resolve or create the target forum topic (Telethon CreateForumTopic).
    e.  Forward the message into that topic (Telethon ForwardMessages with top_msg_id).
    f.  Save checkpoint.
    g.  Sleep MIGRATE_DELAY_SECONDS.
5.  Print a summary when done.

Topic resolution
----------------
* sender_id known   → Redis get_topic_mapping(target, sender_id).
  If miss → resolve username→id if needed, create topic via Telethon, store in Redis.
* username only     → try client.get_entity(username) to obtain user_id, then above.
* unknown sender    → shared "Migration" topic (name configurable).
  Created once; its thread_id is stored in the checkpoint.

Dry-run mode (MIGRATE_DRY_RUN=true)
------------------------------------
Parses and logs every decision without creating topics or forwarding anything.
"""

import asyncio
import os
import random
import time
from typing import Optional

from telethon import errors
from telethon.tl.functions.channels import CreateForumTopicRequest
from telethon.tl.functions.messages import ForwardMessagesRequest

from ..core.utils import tstamp
from ..domain.media import media_type
from ..storage.edit_mapping import get_topic_mapping, store_topic_mapping
from .checkpoint import load_checkpoint, save_checkpoint, MIGRATE_CHECKPOINT_PATH
from .parser import ParsedSender, parse_sender


# ── Config ────────────────────────────────────────────────────────────────

MIGRATE_TARGET_CHAT_ID_RAW = os.getenv("MIGRATE_TARGET_CHAT_ID", "")
MIGRATE_GENERAL_THREAD_ID = int(os.getenv("MIGRATE_GENERAL_THREAD_ID", "1"))
MIGRATE_DELAY_SECONDS = float(os.getenv("MIGRATE_DELAY_SECONDS", "0.5"))
MIGRATE_DRY_RUN = os.getenv("MIGRATE_DRY_RUN", "false").strip().lower() in {"1", "true", "yes", "y"}
MIGRATE_MIGRATION_TOPIC_NAME = os.getenv("MIGRATE_MIGRATION_TOPIC_NAME", "Migration")
MIGRATE_BATCH_LOG_EVERY = int(os.getenv("MIGRATE_BATCH_LOG_EVERY", "100"))

# Media types that trigger topic creation / forwarding.  Text-only messages
# and everything else (voice, audio, document, …) are skipped.
_VISUAL_TYPES = {"photo", "video", "animation"}

# In-process cache: username (str, lower) → user_id (int)
_USERNAME_ID_CACHE: dict[str, int] = {}

# In-process lock map to avoid duplicate topic creation within this process
_TOPIC_LOCKS: dict[int, asyncio.Lock] = {}


# ── Helpers ───────────────────────────────────────────────────────────────

def _desired_topic_title(sender: ParsedSender, resolved_id: Optional[int] = None) -> str:
    """Build a topic title consistent with the live pipeline's _desired_topic_title."""
    sid = resolved_id or sender.sender_id
    if sender.username:
        if sid:
            return f"@{sender.username} ({sid})"
        return f"@{sender.username}"
    if sender.display:
        if sid:
            return f"{sender.display} ({sid})"
        return sender.display
    if sid:
        return f"user ({sid})"
    return MIGRATE_MIGRATION_TOPIC_NAME


async def _resolve_username_to_id(client, username: str, log) -> Optional[int]:
    """Try to resolve a Telegram @username to a numeric user_id."""
    key = username.lower()
    if key in _USERNAME_ID_CACHE:
        return _USERNAME_ID_CACHE[key]
    try:
        entity = await client.get_entity(username)
        uid = getattr(entity, "id", None)
        if isinstance(uid, int):
            _USERNAME_ID_CACHE[key] = uid
            log({"ts": tstamp(), "type": "info", "op": "migrate_resolve_username",
                 "username": username, "resolved_id": uid})
            return uid
    except Exception as exc:
        log({"ts": tstamp(), "type": "warn", "op": "migrate_resolve_username",
             "username": username, "err": exc.__class__.__name__, "msg": str(exc)})
    return None


async def _telethon_create_topic(client, chat_id: int, title: str, log) -> Optional[int]:
    """Create a forum topic via Telethon and return its message_thread_id."""
    try:
        input_entity = await client.get_input_entity(chat_id)
        result = await client(CreateForumTopicRequest(
            channel=input_entity,
            title=title,
            random_id=random.randint(1, 2 ** 31),
        ))
        # The result is an Updates object.  The service message whose ID becomes
        # the thread_id is the first message update that carries an 'action'.
        thread_id: Optional[int] = None
        for upd in getattr(result, "updates", []):
            msg = getattr(upd, "message", None)
            if msg is None:
                continue
            if getattr(msg, "action", None) is not None:
                thread_id = msg.id
                break
        if thread_id:
            log({"ts": tstamp(), "type": "out", "op": "migrate_create_topic",
                 "chat_id": chat_id, "title": title, "thread_id": thread_id})
        else:
            log({"ts": tstamp(), "type": "warn", "op": "migrate_create_topic",
                 "chat_id": chat_id, "title": title, "note": "could not parse thread_id from result"})
        return thread_id
    except Exception as exc:
        log({"ts": tstamp(), "type": "err", "op": "migrate_create_topic",
             "chat_id": chat_id, "title": title,
             "err": exc.__class__.__name__, "msg": str(exc)})
        return None


async def _get_or_create_topic(
    client,
    chat_id: int,
    sender_id: int,
    topic_title: str,
    log,
) -> Optional[int]:
    """Look up an existing topic in Redis; create via Telethon if absent."""
    # 1. Redis lookup (shared with the live bot pipeline)
    try:
        existing = await get_topic_mapping(chat_id, sender_id)
        thread_id = existing.get("message_thread_id") if isinstance(existing, dict) else None
        if thread_id:
            return int(thread_id)
    except Exception as exc:
        log({"ts": tstamp(), "type": "warn", "op": "migrate_topic_lookup",
             "chat_id": chat_id, "sender_id": sender_id,
             "err": exc.__class__.__name__, "msg": str(exc)})

    # 2. Per-sender in-process lock (guards against concurrent coroutines)
    lock = _TOPIC_LOCKS.setdefault(sender_id, asyncio.Lock())
    async with lock:
        # Re-check after acquiring lock
        try:
            existing = await get_topic_mapping(chat_id, sender_id)
            thread_id = existing.get("message_thread_id") if isinstance(existing, dict) else None
            if thread_id:
                return int(thread_id)
        except Exception:
            pass

        thread_id = await _telethon_create_topic(client, chat_id, topic_title, log)
        if thread_id:
            try:
                await store_topic_mapping(chat_id, sender_id, {
                    "message_thread_id": thread_id,
                    "title": topic_title,
                })
            except Exception as exc:
                log({"ts": tstamp(), "type": "warn", "op": "migrate_store_topic",
                     "chat_id": chat_id, "sender_id": sender_id, "thread_id": thread_id,
                     "err": exc.__class__.__name__, "msg": str(exc)})
        return thread_id


async def _forward_to_topic(client, chat_id: int, msg_id: int, thread_id: int, log) -> bool:
    """Forward a message within the same group to a specific forum topic thread."""
    try:
        input_entity = await client.get_input_entity(chat_id)
        await client(ForwardMessagesRequest(
            from_peer=input_entity,
            id=[msg_id],
            to_peer=input_entity,
            top_msg_id=thread_id,
            random_id=[random.randint(0, 2 ** 63)],
            silent=True,
        ))
        return True
    except errors.FloodWaitError as exc:
        log({"ts": tstamp(), "type": "warn", "op": "migrate_forward_flood_wait",
             "chat_id": chat_id, "msg_id": msg_id, "thread_id": thread_id,
             "wait_seconds": exc.seconds})
        await asyncio.sleep(exc.seconds + 1)
        try:
            input_entity = await client.get_input_entity(chat_id)
            await client(ForwardMessagesRequest(
                from_peer=input_entity,
                id=[msg_id],
                to_peer=input_entity,
                top_msg_id=thread_id,
                random_id=[random.randint(0, 2 ** 63)],
                silent=True,
            ))
            return True
        except Exception as exc2:
            log({"ts": tstamp(), "type": "err", "op": "migrate_forward",
                 "chat_id": chat_id, "msg_id": msg_id, "thread_id": thread_id,
                 "err": exc2.__class__.__name__, "msg": str(exc2)})
            return False
    except Exception as exc:
        log({"ts": tstamp(), "type": "err", "op": "migrate_forward",
             "chat_id": chat_id, "msg_id": msg_id, "thread_id": thread_id,
             "err": exc.__class__.__name__, "msg": str(exc)})
        return False


# ── Main runner ───────────────────────────────────────────────────────────

async def run_migration(client, log):
    """Entry point called from main() when APP_MODE=migrate."""

    if not MIGRATE_TARGET_CHAT_ID_RAW:
        raise RuntimeError("MIGRATE_TARGET_CHAT_ID env var is required in APP_MODE=migrate")

    # Support both -100xxxxxxxxxx and plain chat_id formats
    raw = MIGRATE_TARGET_CHAT_ID_RAW.strip()
    chat_id = int(raw) if raw.lstrip("-").isdigit() else int(raw)
    thread_id = MIGRATE_GENERAL_THREAD_ID

    # ── Load checkpoint ──────────────────────────────────────────────────
    chk = load_checkpoint()
    last_processed = chk.get("last_processed_msg_id", 0)
    processed_count = chk.get("processed_count", 0)
    migrated_count = chk.get("migrated_count", 0)
    skipped_count = chk.get("skipped_count", 0)
    migration_topic_id: Optional[int] = chk.get("migration_topic_id")

    # ── Determine cap_id (snapshot of current latest message at startup) ─
    cap_id: Optional[int] = None
    try:
        msgs = await client.get_messages(chat_id, limit=1, reply_to=thread_id)
        if msgs:
            cap_id = msgs[0].id
    except Exception as exc:
        log({"ts": tstamp(), "type": "warn", "op": "migrate_cap_id_fetch",
             "err": exc.__class__.__name__, "msg": str(exc),
             "note": "will iterate without upper bound"})

    log({
        "ts": tstamp(),
        "type": "info",
        "op": "migrate_start",
        "chat_id": chat_id,
        "thread_id": thread_id,
        "resume_from_msg_id": last_processed,
        "cap_id": cap_id,
        "dry_run": MIGRATE_DRY_RUN,
        "delay_seconds": MIGRATE_DELAY_SECONDS,
        "checkpoint": str(MIGRATE_CHECKPOINT_PATH),
    })

    if MIGRATE_DRY_RUN:
        log({"ts": tstamp(), "type": "info", "op": "migrate_dry_run_active",
             "note": "DRY RUN — no topics will be created, no messages forwarded"})

    # ── Iterate messages oldest-first ────────────────────────────────────
    iter_kwargs: dict = dict(
        entity=chat_id,
        reverse=True,         # oldest → newest
        min_id=last_processed,
        reply_to=thread_id,
    )
    if cap_id:
        iter_kwargs["max_id"] = cap_id

    async for msg in client.iter_messages(**iter_kwargs):
        processed_count += 1

        # ── Skip service / non-media messages ────────────────────────────
        mtype = media_type(msg)
        if mtype not in _VISUAL_TYPES:
            skipped_count += 1
            last_processed = msg.id
            if processed_count % MIGRATE_BATCH_LOG_EVERY == 0:
                log({
                    "ts": tstamp(), "type": "info", "op": "migrate_progress",
                    "processed": processed_count, "migrated": migrated_count,
                    "skipped": skipped_count, "last_msg_id": last_processed,
                })
            continue

        # ── Parse sender ──────────────────────────────────────────────────
        sender: ParsedSender = parse_sender(msg)

        log({
            "ts": tstamp(),
            "type": "debug" if not MIGRATE_DRY_RUN else "info",
            "op": "migrate_msg",
            "msg_id": msg.id,
            "media_type": mtype,
            "sender_id": sender.sender_id,
            "username": sender.username,
            "display": sender.display,
            "fmt": sender.fmt,
            "is_unknown": sender.is_unknown,
        })

        # ── Resolve topic ─────────────────────────────────────────────────
        target_thread_id: Optional[int] = None

        if sender.is_unknown:
            # Shared "Migration" topic for all unidentifiable senders
            if migration_topic_id is None and not MIGRATE_DRY_RUN:
                migration_topic_id = await _telethon_create_topic(
                    client, chat_id, MIGRATE_MIGRATION_TOPIC_NAME, log
                )
                # Persist so future runs reuse the same topic
                save_checkpoint(
                    chat_id=chat_id, thread_id=thread_id,
                    last_processed_msg_id=last_processed,
                    processed_count=processed_count,
                    migrated_count=migrated_count,
                    skipped_count=skipped_count,
                    migration_topic_id=migration_topic_id,
                )
            target_thread_id = migration_topic_id
        else:
            # Resolve sender_id — if only username available, look it up
            resolved_id = sender.sender_id
            if resolved_id is None and sender.username:
                resolved_id = await _resolve_username_to_id(client, sender.username, log)

            if resolved_id is not None:
                topic_title = _desired_topic_title(sender, resolved_id)
                if not MIGRATE_DRY_RUN:
                    target_thread_id = await _get_or_create_topic(
                        client, chat_id, resolved_id, topic_title, log
                    )
                else:
                    target_thread_id = -1  # placeholder for dry-run logging
            else:
                # username exists but couldn't be resolved → Migration topic
                if migration_topic_id is None and not MIGRATE_DRY_RUN:
                    migration_topic_id = await _telethon_create_topic(
                        client, chat_id, MIGRATE_MIGRATION_TOPIC_NAME, log
                    )
                    save_checkpoint(
                        chat_id=chat_id, thread_id=thread_id,
                        last_processed_msg_id=last_processed,
                        processed_count=processed_count,
                        migrated_count=migrated_count,
                        skipped_count=skipped_count,
                        migration_topic_id=migration_topic_id,
                    )
                target_thread_id = migration_topic_id

        # ── Forward ───────────────────────────────────────────────────────
        if target_thread_id and not MIGRATE_DRY_RUN:
            forwarded = await _forward_to_topic(client, chat_id, msg.id, target_thread_id, log)
            if forwarded:
                migrated_count += 1
                log({
                    "ts": tstamp(), "type": "out", "op": "migrate_forwarded",
                    "msg_id": msg.id, "thread_id": target_thread_id,
                    "sender_id": sender.sender_id, "username": sender.username,
                })
            else:
                log({
                    "ts": tstamp(), "type": "warn", "op": "migrate_forward_failed",
                    "msg_id": msg.id, "thread_id": target_thread_id,
                })
        elif MIGRATE_DRY_RUN:
            migrated_count += 1  # count as "would migrate" in dry-run

        # ── Checkpoint ────────────────────────────────────────────────────
        last_processed = msg.id
        save_checkpoint(
            chat_id=chat_id, thread_id=thread_id,
            last_processed_msg_id=last_processed,
            processed_count=processed_count,
            migrated_count=migrated_count,
            skipped_count=skipped_count,
            migration_topic_id=migration_topic_id,
        )

        # Progress log every N messages
        if processed_count % MIGRATE_BATCH_LOG_EVERY == 0:
            log({
                "ts": tstamp(), "type": "info", "op": "migrate_progress",
                "processed": processed_count, "migrated": migrated_count,
                "skipped": skipped_count, "last_msg_id": last_processed,
                "cap_id": cap_id,
            })

        # ── Rate limit ────────────────────────────────────────────────────
        if MIGRATE_DELAY_SECONDS > 0 and not MIGRATE_DRY_RUN:
            await asyncio.sleep(MIGRATE_DELAY_SECONDS)

    # ── Done ─────────────────────────────────────────────────────────────
    log({
        "ts": tstamp(),
        "type": "info",
        "op": "migrate_complete",
        "chat_id": chat_id,
        "thread_id": thread_id,
        "processed": processed_count,
        "migrated": migrated_count,
        "skipped": skipped_count,
        "last_msg_id": last_processed,
        "dry_run": MIGRATE_DRY_RUN,
    })


__all__ = [
    "run_migration",
    "MIGRATE_DRY_RUN",
    "MIGRATE_DELAY_SECONDS",
    "MIGRATE_MIGRATION_TOPIC_NAME",
]
