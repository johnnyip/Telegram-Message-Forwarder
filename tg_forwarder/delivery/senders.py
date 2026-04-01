import asyncio
from pathlib import Path
from typing import Any, Dict, List

from ..core.metrics import StepTimer
from ..core.utils import tstamp


async def send_text_to_target(route: Dict[str, Any], tgt: Any, combined: str, info: dict, source_kind: str, *, client, run_api, log) -> bool:
    timer = StepTimer()
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
        log({"ts": tstamp(), "type": "perf", "step": "send_text", "ms": timer.ms(), **extra})
        log({"ts": tstamp(), "type": "out", "op": "send_text", "status": "ok", **extra})
        return True
    except Exception:
        return False


async def send_file_to_target(route: Dict[str, Any], tgt: Any, fpath: Path, caption: str, info: dict, source_kind: str, *, client, run_api, log) -> bool:
    timer = StepTimer()
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
        log({"ts": tstamp(), "type": "perf", "step": "send_file", "ms": timer.ms(), **extra})
        log({"ts": tstamp(), "type": "out", "op": "send_file", "status": "ok", **extra})
        return True
    except Exception:
        return False


async def send_album_to_target(route: Dict[str, Any], tgt: Any, files: List[str], caption: str, info: dict, source_kind: str, *, client, run_api, log) -> bool:
    timer = StepTimer()
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
        log({"ts": tstamp(), "type": "perf", "step": "send_album", "ms": timer.ms(), **extra})
        log({"ts": tstamp(), "type": "out", "op": "send_album", "status": "ok", **extra})
        return True
    except Exception:
        return False


async def send_text_to_many(route: Dict[str, Any], targets: List[Any], combined: str, info: dict, source_kind: str, *, client, run_api, log, parallel: bool = True) -> List[bool]:
    coros = [send_text_to_target(route, tgt, combined, info, source_kind, client=client, run_api=run_api, log=log) for tgt in targets]
    if parallel:
        return await asyncio.gather(*coros)
    results = []
    for c in coros:
        results.append(await c)
    return results


async def fetch_message_by_id(chat_id: int, msg_id: int, *, client, log):
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


async def fetch_messages_by_ids(chat_id: int, msg_ids: List[int], *, client, log):
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


__all__ = [
    "fetch_message_by_id",
    "fetch_messages_by_ids",
    "send_album_to_target",
    "send_file_to_target",
    "send_text_to_many",
    "send_text_to_target",
]
