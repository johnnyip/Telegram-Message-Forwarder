import asyncio
import uuid
from pathlib import Path
from typing import Any, List, Optional

from ..domain.media import build_filename_from_message, get_media_size, media_type
from ..core.metrics import StepTimer
from ..core.utils import tstamp


async def snapshot_media_message(msg, info: dict, source_kind: str, spool_dir: Path, download_semaphore, run_api, log) -> Optional[dict]:
    timer = StepTimer()
    sender_display = info.get("sender_display", "unknown")
    file_name = build_filename_from_message(msg, sender_display)

    chat_id = info.get("chat_id")
    msg_id = info.get("msg_id")
    unique = uuid.uuid4().hex[:8]

    target_dir = spool_dir / source_kind / str(chat_id)
    target_dir.mkdir(parents=True, exist_ok=True)

    save_path = target_dir / f"{msg_id}_{unique}_{file_name}"

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
        fpath = await run_api(lambda: msg.download_media(save_path), op="snapshot_download_media", extra=extra)

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
        "type": "perf",
        "step": "snapshot_media_message",
        "source_kind": source_kind,
        "chat_id": chat_id,
        "msg_id": msg_id,
        "ms": timer.ms(),
    })

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


async def snapshot_album_messages(msgs: List[Any], info: dict, source_kind: str, spool_dir: Path, download_semaphore, run_api, log) -> List[dict]:
    sorted_msgs = sorted(msgs, key=lambda m: m.id)
    snapshots = await asyncio.gather(
        *(snapshot_media_message(m, info, source_kind, spool_dir, download_semaphore, run_api, log) for m in sorted_msgs),
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


__all__ = [
    "snapshot_album_messages",
    "snapshot_media_message",
]
