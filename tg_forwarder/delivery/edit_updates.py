import asyncio
import os
from typing import Any, Awaitable, Callable

from ..storage.edit_mapping import get_album_mapping, get_message_mapping
from ..domain.formatting import build_header_from_info
from ..domain.timefmt import append_original_time, append_edited_suffix
from ..core.utils import tstamp

# P5: The mapping is only written AFTER the initial send completes, which
# happens DELAY_SECONDS after the Kafka job was produced.  The retry window
# must cover that delay plus processing overhead.  Defaults to 10 retries ×
# 2 s fixed delay = 20 s total, which comfortably covers DELAY_SECONDS=5.
_EDIT_MAPPING_RETRIES = int(os.getenv("EDIT_MAPPING_RETRIES", "10"))
_EDIT_MAPPING_RETRY_DELAY_SECONDS = float(os.getenv("EDIT_MAPPING_RETRY_DELAY_SECONDS", "2.0"))
_EDIT_API_RETRIES = 2
_EDIT_API_RETRY_DELAY_SECONDS = 1.5


def _is_transient_edit_error(exc: Exception) -> bool:
    name = exc.__class__.__name__
    msg = str(exc).lower()
    if name in {"TimedOut", "NetworkError", "ConnectError"}:
        return True
    if getattr(exc, "retry_after", None):
        return True
    return any(token in msg for token in ("pool timeout", "timed out", "connection reset", "temporarily unavailable"))


async def _get_mapping_with_retry(fetcher: Callable[[], Awaitable[dict]], missing_log: dict, log) -> dict:
    for attempt in range(1, _EDIT_MAPPING_RETRIES + 1):
        mapping = await fetcher()
        targets = mapping.get("targets", {}) if isinstance(mapping, dict) else {}
        if targets:
            return mapping
        if attempt < _EDIT_MAPPING_RETRIES:
            log({**missing_log, "attempt": attempt, "note": "mapping_missing_retrying"})
            await asyncio.sleep(_EDIT_MAPPING_RETRY_DELAY_SECONDS)
    log(missing_log)
    return {}


async def _run_edit_call(call: Callable[[], Awaitable[Any]], err_log: dict, log) -> bool:
    for attempt in range(1, _EDIT_API_RETRIES + 1):
        try:
            await call()
            return True
        except Exception as exc:
            transient = _is_transient_edit_error(exc)
            log({**err_log, "err": exc.__class__.__name__, "msg": str(exc), "attempt": attempt, "transient": transient})
            if transient and attempt < _EDIT_API_RETRIES:
                retry_after = getattr(exc, "retry_after", None)
                delay = float(retry_after) + 1 if retry_after else (_EDIT_API_RETRY_DELAY_SECONDS * attempt)
                await asyncio.sleep(delay)
                continue
            return False
    return False


async def edit_forwarded_text(bot, info: dict, text: str, log, semaphore=None) -> bool:
    mapping = await _get_mapping_with_retry(
        lambda: get_message_mapping(info["chat_id"], info["msg_id"]),
        {"ts": tstamp(), "type": "warn", "op": "edit_forwarded_text_missing_mapping", "chat_id": info.get("chat_id"), "msg_id": info.get("msg_id")},
        log,
    )
    targets = mapping.get("targets", {}) if isinstance(mapping, dict) else {}
    if not targets:
        return False

    body = f"{build_header_from_info(info, is_edit=False)}\n{text or '[empty]'}".strip()
    body = append_original_time(body, info.get("msg_date"))
    body = append_edited_suffix(body)

    ok = False
    for target, payload in targets.items():
        message_id = payload.get("message_id")
        if not message_id:
            continue

        async def _call():
            try:
                await bot.edit_message_text(chat_id=int(target), message_id=message_id, text=body, parse_mode="Markdown")
            except Exception:
                await bot.edit_message_text(chat_id=int(target), message_id=message_id, text=body)

        async def _guarded_call():
            if semaphore is None:
                await _call()
            else:
                async with semaphore:
                    await _call()

        success = await _run_edit_call(
            _guarded_call,
            {"ts": tstamp(), "type": "err", "op": "edit_forwarded_text", "target": int(target), "message_id": message_id, "chat_id": info.get("chat_id"), "msg_id": info.get("msg_id")},
            log,
        )
        if success:
            log({"ts": tstamp(), "type": "out", "op": "edit_forwarded_text", "status": "ok", "target": int(target), "message_id": message_id, "chat_id": info.get("chat_id"), "msg_id": info.get("msg_id")})
            ok = True
    return ok


async def edit_forwarded_media_caption(bot, info: dict, caption_text: str, log, semaphore=None) -> bool:
    mapping = await _get_mapping_with_retry(
        lambda: get_message_mapping(info["chat_id"], info["msg_id"]),
        {"ts": tstamp(), "type": "warn", "op": "edit_forwarded_media_missing_mapping", "chat_id": info.get("chat_id"), "msg_id": info.get("msg_id")},
        log,
    )
    targets = mapping.get("targets", {}) if isinstance(mapping, dict) else {}
    if not targets:
        return False

    caption = f"{build_header_from_info(info, is_edit=False)}\n{caption_text or ''}".strip()
    caption = append_original_time(caption, info.get("msg_date"))
    caption = append_edited_suffix(caption)

    ok = False
    for target, payload in targets.items():
        message_id = payload.get("message_id")
        if not message_id:
            continue

        async def _call():
            try:
                await bot.edit_message_caption(chat_id=int(target), message_id=message_id, caption=caption, parse_mode="Markdown")
            except Exception:
                await bot.edit_message_caption(chat_id=int(target), message_id=message_id, caption=caption)

        async def _guarded_call():
            if semaphore is None:
                await _call()
            else:
                async with semaphore:
                    await _call()

        success = await _run_edit_call(
            _guarded_call,
            {"ts": tstamp(), "type": "err", "op": "edit_forwarded_media_caption", "target": int(target), "message_id": message_id, "chat_id": info.get("chat_id"), "msg_id": info.get("msg_id")},
            log,
        )
        if success:
            log({"ts": tstamp(), "type": "out", "op": "edit_forwarded_media_caption", "status": "ok", "target": int(target), "message_id": message_id, "chat_id": info.get("chat_id"), "msg_id": info.get("msg_id")})
            ok = True
    return ok


async def edit_forwarded_album_caption(bot, info: dict, caption_text: str, log, semaphore=None) -> bool:
    grouped_id = info.get("grouped_id")
    if grouped_id is None:
        return False
    mapping = await _get_mapping_with_retry(
        lambda: get_album_mapping(info["chat_id"], grouped_id),
        {"ts": tstamp(), "type": "warn", "op": "edit_forwarded_album_missing_mapping", "chat_id": info.get("chat_id"), "grouped_id": grouped_id},
        log,
    )
    targets = mapping.get("targets", {}) if isinstance(mapping, dict) else {}
    if not targets:
        return False

    caption = f"{build_header_from_info(info, is_edit=False)}\n{caption_text or ''}".strip()
    caption = append_original_time(caption, info.get("msg_date"))
    caption = append_edited_suffix(caption)

    ok = False
    for target, payload in targets.items():
        message_id = payload.get("message_id")
        if not message_id:
            continue

        async def _call():
            try:
                await bot.edit_message_caption(chat_id=int(target), message_id=message_id, caption=caption, parse_mode="Markdown")
            except Exception:
                await bot.edit_message_caption(chat_id=int(target), message_id=message_id, caption=caption)

        async def _guarded_call():
            if semaphore is None:
                await _call()
            else:
                async with semaphore:
                    await _call()

        success = await _run_edit_call(
            _guarded_call,
            {"ts": tstamp(), "type": "err", "op": "edit_forwarded_album_caption", "target": int(target), "message_id": message_id, "chat_id": info.get("chat_id"), "grouped_id": grouped_id},
            log,
        )
        if success:
            log({"ts": tstamp(), "type": "out", "op": "edit_forwarded_album_caption", "status": "ok", "target": int(target), "message_id": message_id, "chat_id": info.get("chat_id"), "grouped_id": grouped_id})
            ok = True
    return ok


__all__ = [
    "edit_forwarded_album_caption",
    "edit_forwarded_media_caption",
    "edit_forwarded_text",
]
