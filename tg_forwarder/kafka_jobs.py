from typing import Any, Dict, List

from .metrics import StepTimer
from .routes import route_names
from .snapshot import snapshot_album_messages, snapshot_media_message
from .utils import now_ts, tstamp


def due_ts_for_text(delay_seconds: int) -> float:
    return now_ts() + delay_seconds


def due_ts_for_media(delay_seconds: int) -> float:
    return now_ts() + delay_seconds


async def kafka_send(topic: str, payload: dict, producer, json_bytes, log):
    timer = StepTimer()
    await producer.send_and_wait(topic, json_bytes(payload))
    log({
        "ts": tstamp(),
        "type": "perf",
        "step": "kafka_send",
        "topic": topic,
        "job_type": payload.get("job_type"),
        "chat_id": payload.get("info", {}).get("chat_id") if isinstance(payload.get("info"), dict) else None,
        "msg_id": payload.get("info", {}).get("msg_id") if isinstance(payload.get("info"), dict) else None,
        "ms": timer.ms(),
    })


async def publish_text_job(msg, info: dict, routes: List[Dict[str, Any]], source_kind: str, *, delay_seconds: int, topic: str, producer, json_bytes, log):
    job = {
        "job_type": "text",
        "source_kind": source_kind,
        "route_names": route_names(routes),
        "info": info,
        "text": msg.text or msg.message or "[empty]",
        "due_at": due_ts_for_text(delay_seconds),
        "created_at": tstamp(),
    }
    await kafka_send(topic, job, producer, json_bytes, log)

    log({
        "ts": tstamp(),
        "type": "kafka_produce",
        "topic": topic,
        "job_type": "text",
        "chat_id": info["chat_id"],
        "msg_id": info["msg_id"],
        "source_kind": source_kind,
    })


async def publish_media_job(msg, info: dict, routes: List[Dict[str, Any]], source_kind: str, *, media_delay_seconds: int, topic: str, producer, json_bytes, log, should_direct_forward_large_media, snapshot_kwargs: dict):
    if should_direct_forward_large_media(info, msg):
        job = {
            "job_type": "media_forward",
            "source_kind": source_kind,
            "route_names": route_names(routes),
            "info": info,
            "chat_id": info["chat_id"],
            "msg_id": info["msg_id"],
            "caption_text": (msg.text or msg.message or "").strip(),
            "media_type": snapshot_kwargs["media_type_fn"](msg),
            "media_size": snapshot_kwargs["get_media_size_fn"](msg),
            "due_at": due_ts_for_media(media_delay_seconds),
            "created_at": tstamp(),
        }
        await kafka_send(topic, job, producer, json_bytes, log)

        log({
            "ts": tstamp(),
            "type": "kafka_produce",
            "topic": topic,
            "job_type": "media_forward",
            "chat_id": info["chat_id"],
            "msg_id": info["msg_id"],
            "source_kind": source_kind,
            "media_size": snapshot_kwargs["get_media_size_fn"](msg),
            "forward_policy": snapshot_kwargs["forward_policy"],
        })
        return

    snapshot = await snapshot_media_message(msg, info, source_kind, snapshot_kwargs["spool_dir"], snapshot_kwargs["download_semaphore"], snapshot_kwargs["run_api"], log)
    if not snapshot:
        return

    job = {
        "job_type": "media_file",
        "source_kind": source_kind,
        "route_names": route_names(routes),
        "info": info,
        "snapshot": snapshot,
        "due_at": due_ts_for_media(media_delay_seconds),
        "created_at": tstamp(),
    }
    await kafka_send(topic, job, producer, json_bytes, log)

    log({
        "ts": tstamp(),
        "type": "kafka_produce",
        "topic": topic,
        "job_type": "media_file",
        "chat_id": info["chat_id"],
        "msg_id": info["msg_id"],
        "source_kind": source_kind,
        "file": snapshot["path"],
    })


async def publish_album_job(msgs: List[Any], info: dict, routes: List[Dict[str, Any]], source_kind: str, *, media_delay_seconds: int, topic: str, producer, json_bytes, log, should_direct_forward_large_album, snapshot_kwargs: dict):
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
            "due_at": due_ts_for_media(media_delay_seconds),
            "created_at": tstamp(),
        }
        await kafka_send(topic, job, producer, json_bytes, log)

        log({
            "ts": tstamp(),
            "type": "kafka_produce",
            "topic": topic,
            "job_type": "media_album_forward",
            "chat_id": info["chat_id"],
            "grouped_id": info.get("grouped_id"),
            "msg_ids": msg_ids,
            "source_kind": source_kind,
            "forward_policy": snapshot_kwargs["forward_policy"],
        })
        return

    snapshots = await snapshot_album_messages(sorted_msgs, info, source_kind, snapshot_kwargs["spool_dir"], snapshot_kwargs["download_semaphore"], snapshot_kwargs["run_api"], log)
    if not snapshots:
        return

    job = {
        "job_type": "media_album_file",
        "source_kind": source_kind,
        "route_names": route_names(routes),
        "info": info,
        "snapshots": snapshots,
        "due_at": due_ts_for_media(media_delay_seconds),
        "created_at": tstamp(),
    }
    await kafka_send(topic, job, producer, json_bytes, log)

    log({
        "ts": tstamp(),
        "type": "kafka_produce",
        "topic": topic,
        "job_type": "media_album_file",
        "chat_id": info["chat_id"],
        "grouped_id": info.get("grouped_id"),
        "msg_ids": [s["msg_id"] for s in snapshots],
        "source_kind": source_kind,
    })
