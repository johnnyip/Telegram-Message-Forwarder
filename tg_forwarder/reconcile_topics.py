import json
import os
import random
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional

from telethon.tl.functions.messages import DeleteTopicHistoryRequest, EditForumTopicRequest, ForwardMessagesRequest

from .core.utils import tstamp
from .forum_topics import iter_forum_topics, parse_sender_id_from_topic_title, pick_latest_topic, topic_sort_key
from .storage.edit_mapping import store_topic_mapping

RECONCILE_TARGET_CHAT_ID_RAW = os.getenv("RECONCILE_TARGET_CHAT_ID", "")
RECONCILE_DRY_RUN = os.getenv("RECONCILE_DRY_RUN", "false").strip().lower() in {"1", "true", "yes", "y"}
RECONCILE_DELETE_OLD = os.getenv("RECONCILE_DELETE_OLD", "true").strip().lower() in {"1", "true", "yes", "y"}
RECONCILE_CLOSE_OLD = os.getenv("RECONCILE_CLOSE_OLD", "true").strip().lower() in {"1", "true", "yes", "y"}
RECONCILE_BATCH_SIZE = max(1, int(os.getenv("RECONCILE_BATCH_SIZE", "50")))
RECONCILE_STATE_PATH = Path(os.getenv("RECONCILE_STATE_PATH", "/app/downloads/reconcile_topics_state.json"))
RECONCILE_AUDIT_MESSAGE = os.getenv("RECONCILE_AUDIT_MESSAGE", "true").strip().lower() in {"1", "true", "yes", "y"}
RECONCILE_ONLY_SENDER_ID_RAW = os.getenv("RECONCILE_ONLY_SENDER_ID", "").strip()
RECONCILE_ONLY_SENDER_ID = int(RECONCILE_ONLY_SENDER_ID_RAW) if RECONCILE_ONLY_SENDER_ID_RAW.isdigit() else None


def _load_state() -> Dict[str, Any]:
    try:
        if RECONCILE_STATE_PATH.exists():
            return json.loads(RECONCILE_STATE_PATH.read_text())
    except Exception:
        pass
    return {"topics": {}}


def _save_state(state: Dict[str, Any]) -> None:
    RECONCILE_STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    RECONCILE_STATE_PATH.write_text(json.dumps(state, ensure_ascii=False, indent=2, sort_keys=True))


def _topic_state(state: Dict[str, Any], old_topic_id: int) -> Dict[str, Any]:
    topics = state.setdefault("topics", {})
    entry = topics.setdefault(str(old_topic_id), {"moved": {}, "status": "pending"})
    entry.setdefault("moved", {})
    return entry


async def _iter_user_messages(client, chat_id: int, topic_id: int):
    async for msg in client.iter_messages(chat_id, reply_to=topic_id, reverse=True):
        if not msg:
            continue
        if getattr(msg, "action", None):
            continue
        yield msg


async def _forward_batch(client, input_peer, msg_ids: List[int], target_topic_id: int) -> List[int]:
    if not msg_ids:
        return []
    result = await client(ForwardMessagesRequest(
        from_peer=input_peer,
        id=[int(mid) for mid in msg_ids],
        to_peer=input_peer,
        top_msg_id=int(target_topic_id),
        random_id=[random.randint(0, 2 ** 63) for _ in msg_ids],
        silent=True,
    ))
    updates = list(getattr(result, "updates", []) or [])
    new_ids: List[int] = []
    for update in updates:
        mid = getattr(update, "id", None)
        if isinstance(mid, int):
            new_ids.append(mid)
    return new_ids


def _build_forward_batches(pending: List[Any], batch_size: int) -> List[List[int]]:
    batches: List[List[int]] = []
    current: List[int] = []
    current_group = None
    for msg in pending:
        grouped_id = getattr(msg, "grouped_id", None)
        should_split = False
        if current and len(current) >= batch_size:
            # Keep album/media-group messages together when we are still inside
            # the same grouped_id, but split ordinary (grouped_id=None) traffic
            # as soon as the batch reaches the limit.
            if grouped_id is None or current_group is None or grouped_id != current_group:
                should_split = True
        if should_split:
            batches.append(current)
            current = []
            current_group = None
        current.append(int(msg.id))
        current_group = grouped_id
    if current:
        batches.append(current)
    return batches


async def _move_topic_messages(client, chat_id: int, old_topic: Any, latest_topic: Any, log, state: Dict[str, Any]) -> Dict[str, Any]:
    old_topic_id = int(getattr(old_topic, "id", 0) or 0)
    latest_topic_id = int(getattr(latest_topic, "id", 0) or 0)
    entry = _topic_state(state, old_topic_id)
    entry.update({
        "source_topic_id": old_topic_id,
        "source_title": getattr(old_topic, "title", None),
        "target_topic_id": latest_topic_id,
        "target_title": getattr(latest_topic, "title", None),
        "status": "moving",
    })
    _save_state(state)

    source_msgs = []
    async for msg in _iter_user_messages(client, chat_id, old_topic_id):
        source_msgs.append(msg)

    source_ids = [int(msg.id) for msg in source_msgs]
    moved = entry.setdefault("moved", {})
    pending = [msg for msg in source_msgs if str(int(msg.id)) not in moved]
    log({
        "ts": tstamp(),
        "type": "info",
        "op": "topic_reconcile_move_start",
        "source_topic_id": old_topic_id,
        "target_topic_id": latest_topic_id,
        "source_count": len(source_ids),
        "pending_count": len(pending),
    })

    if RECONCILE_DRY_RUN:
        entry["status"] = "dry_run"
        entry["source_count"] = len(source_ids)
        entry["pending_count"] = len(pending)
        _save_state(state)
        return {"source_count": len(source_ids), "pending_count": len(pending), "missing": [int(msg.id) for msg in pending], "dry_run": True}

    input_peer = await client.get_input_entity(chat_id)

    for batch_ids in _build_forward_batches(pending, RECONCILE_BATCH_SIZE):
        try:
            new_ids = await _forward_batch(client, input_peer, batch_ids, latest_topic_id)
            if len(new_ids) != len(batch_ids):
                raise RuntimeError(f"forwarded_count_mismatch expected={len(batch_ids)} got={len(new_ids)}")
            for src_id in batch_ids:
                moved[str(src_id)] = {"moved_at": tstamp()}
            _save_state(state)
        except Exception as exc:
            entry["status"] = "move_failed"
            entry["last_error"] = {"err": exc.__class__.__name__, "msg": str(exc), "batch_ids": batch_ids}
            _save_state(state)
            log({
                "ts": tstamp(),
                "type": "err",
                "op": "topic_reconcile_move_batch",
                "source_topic_id": old_topic_id,
                "target_topic_id": latest_topic_id,
                "batch_ids": batch_ids,
                "err": exc.__class__.__name__,
                "msg": str(exc),
            })
            raise

    missing = [src_id for src_id in source_ids if str(src_id) not in moved]
    entry["source_count"] = len(source_ids)
    entry["missing"] = missing
    entry["status"] = "moved" if not missing else "incomplete"
    _save_state(state)
    return {"source_count": len(source_ids), "pending_count": len(pending), "missing": missing, "dry_run": False}


async def _post_audit_message(client, chat_id: int, latest_topic_id: int, sender_id: int, old_topic: Any, latest_topic: Any, moved_count: int, log) -> None:
    text = (
        f"[topic-merge]\n"
        f"sender_id={sender_id}\n"
        f"source_topic_id={int(getattr(old_topic, 'id', 0) or 0)}\n"
        f"target_topic_id={int(getattr(latest_topic, 'id', 0) or 0)}\n"
        f"moved_count={moved_count}"
    )
    try:
        await client.send_message(chat_id, text, reply_to=latest_topic_id)
    except Exception as exc:
        log({
            "ts": tstamp(),
            "type": "warn",
            "op": "topic_reconcile_audit_message",
            "target_topic_id": latest_topic_id,
            "err": exc.__class__.__name__,
            "msg": str(exc),
        })


async def _close_and_delete_old_topic(client, chat_id: int, old_topic: Any, latest_topic: Any, sender_id: int, moved_count: int, log, state: Dict[str, Any]) -> None:
    old_topic_id = int(getattr(old_topic, "id", 0) or 0)
    latest_topic_id = int(getattr(latest_topic, "id", 0) or 0)
    entry = _topic_state(state, old_topic_id)

    if RECONCILE_AUDIT_MESSAGE:
        await _post_audit_message(client, chat_id, latest_topic_id, sender_id, old_topic, latest_topic, moved_count, log)

    if RECONCILE_DELETE_OLD:
        await client(DeleteTopicHistoryRequest(peer=await client.get_input_entity(chat_id), top_msg_id=old_topic_id))
    if RECONCILE_CLOSE_OLD:
        old_title = getattr(old_topic, "title", "") or ""
        merged_title = f"[merged->{latest_topic_id}] {old_title}"[:120]
        await client(EditForumTopicRequest(peer=await client.get_input_entity(chat_id), topic_id=old_topic_id, title=merged_title, closed=True))

    entry["status"] = "cleaned"
    entry["cleaned_at"] = tstamp()
    _save_state(state)
    log({
        "ts": tstamp(),
        "type": "out",
        "op": "topic_reconcile_cleanup",
        "source_topic_id": old_topic_id,
        "target_topic_id": latest_topic_id,
        "moved_count": moved_count,
        "deleted": RECONCILE_DELETE_OLD,
        "closed": RECONCILE_CLOSE_OLD,
    })


async def run_topic_reconcile(client, log):
    if not RECONCILE_TARGET_CHAT_ID_RAW:
        raise RuntimeError("RECONCILE_TARGET_CHAT_ID env var is required in APP_MODE=reconcile")

    chat_id = int(RECONCILE_TARGET_CHAT_ID_RAW.strip())
    state = _load_state()

    all_topics = []
    async for topic in iter_forum_topics(client, chat_id):
        all_topics.append(topic)

    grouped: Dict[int, List[Any]] = defaultdict(list)
    for topic in all_topics:
        sender_id = parse_sender_id_from_topic_title(getattr(topic, "title", None))
        if sender_id is None:
            continue
        if RECONCILE_ONLY_SENDER_ID is not None and sender_id != RECONCILE_ONLY_SENDER_ID:
            continue
        grouped[sender_id].append(topic)

    duplicate_groups = {sender_id: topics for sender_id, topics in grouped.items() if len(topics) > 1}
    log({
        "ts": tstamp(),
        "type": "info",
        "op": "topic_reconcile_scan",
        "chat_id": chat_id,
        "topic_count": len(all_topics),
        "duplicate_sender_count": len(duplicate_groups),
    })

    for sender_id, topics in sorted(duplicate_groups.items(), key=lambda item: item[0]):
        sorted_topics = sorted(topics, key=topic_sort_key)
        latest_topic = pick_latest_topic(sorted_topics)
        if latest_topic is None:
            continue

        await store_topic_mapping(chat_id, sender_id, {
            "message_thread_id": int(getattr(latest_topic, "id", 0) or 0),
            "title": getattr(latest_topic, "title", None),
        })

        for old_topic in sorted_topics:
            if int(getattr(old_topic, "id", 0) or 0) == int(getattr(latest_topic, "id", 0) or 0):
                continue
            result = await _move_topic_messages(client, chat_id, old_topic, latest_topic, log, state)
            if result.get("dry_run"):
                continue
            if result.get("missing"):
                log({
                    "ts": tstamp(),
                    "type": "warn",
                    "op": "topic_reconcile_incomplete",
                    "sender_id": sender_id,
                    "source_topic_id": int(getattr(old_topic, "id", 0) or 0),
                    "target_topic_id": int(getattr(latest_topic, "id", 0) or 0),
                    "missing": result.get("missing"),
                })
                continue
            await _close_and_delete_old_topic(client, chat_id, old_topic, latest_topic, sender_id, int(result.get("source_count", 0)), log, state)

    log({"ts": tstamp(), "type": "out", "op": "topic_reconcile_done", "chat_id": chat_id})
