import asyncio
import os
from typing import Any, Awaitable, Callable

from ..storage.edit_mapping import get_album_mapping, get_message_mapping
from ..domain.formatting import build_header_from_info, build_topic_message_url
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
            if "message is not modified" in str(exc).lower():
                return True
            transient = _is_transient_edit_error(exc)
            log({**err_log, "err": exc.__class__.__name__, "msg": str(exc), "attempt": attempt, "transient": transient})
            if transient and attempt < _EDIT_API_RETRIES:
                retry_after = getattr(exc, "retry_after", None)
                delay = float(retry_after) + 1 if retry_after else (_EDIT_API_RETRY_DELAY_SECONDS * attempt)
                await asyncio.sleep(delay)
                continue
            return False
    return False



import hashlib
from html import escape
from telethon.extensions.html import unparse
from datetime import datetime
from zoneinfo import ZoneInfo
from ..storage.edit_mapping import store_message_mapping, store_album_mapping
from ..domain.timefmt import DISPLAY_TZ

# Serialize the whole fetch/append/store transaction, not merely the API call.
# The deployed topology has one listener; sender mappings remain atomic per target.
_history_lock = asyncio.Lock()


def _units(value):
    return len(value.encode('utf-16-le')) // 2


def _chunks(value, limit=4096):
    chunk = []
    size = 0
    for char in value:
        units = _units(char)
        if size + units > limit:
            yield ''.join(chunk)
            chunk, size = [], 0
        chunk.append(char)
        size += units
    if chunk:
        yield ''.join(chunk)


def _edit_label(info):
    value = info.get('edit_date')
    try:
        dt = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=ZoneInfo('UTC'))
        return dt.astimezone(ZoneInfo(DISPLAY_TZ)).strftime('%Y-%m-%d %H:%M:%S %Z')
    except (ValueError, TypeError):
        return 'time unavailable'


def _copy_ids(payload):
    # For albums only the first item bears the source caption.
    ids = [payload.get('general_message_id'), payload.get('topic_message_id')]
    if not any(ids):
        ids = [payload.get('message_id')]
    return list(dict.fromkeys(int(value) for value in ids if value))


async def _append_history(bot, info, source_text, log, *, album=False, caption=False, original_fetcher=None):
    source_text = source_text or ''
    grouped_id = info.get('grouped_id')
    if album and grouped_id is None:
        return False
    fetch = (lambda: get_album_mapping(info['chat_id'], grouped_id)) if album else (lambda: get_message_mapping(info['chat_id'], info['msg_id']))
    store = store_album_mapping if album else store_message_mapping
    source_id = grouped_id if album else info['msg_id']
    async with _history_lock:
        mapping = await _get_mapping_with_retry(fetch, {'ts': tstamp(), 'type': 'warn', 'op': 'append_edit_missing_mapping', 'chat_id': info.get('chat_id'), 'msg_id': info.get('msg_id')}, log)
        targets = mapping.get('targets', {})
        if not targets:
            return False
        any_ok = False
        for target, payload in targets.items():
            progress = payload.setdefault('edit_progress', {})
            for message_id in _copy_ids(payload):
                key = str(message_id)
                state = progress.get(key, {})
                # Avoid treating a media-only edit or redelivered event as a new version.
                previous = state.get('source_text', payload.get('source_text'))
                if previous is not None and previous == source_text:
                    any_ok = True
                    continue
                event_date = str(info.get('edit_date') or '')
                old_date = state.get('edit_date', '')
                try:
                    if old_date and event_date and datetime.fromisoformat(event_date) < datetime.fromisoformat(old_date):
                        any_ok = True
                        continue
                except (ValueError, TypeError):
                    pass
                entry = '[Edit ' + _edit_label(info) + ']\n' + (source_text or '[empty]')
                original = None
                if original_fetcher is not None:
                    try:
                        original = await original_fetcher(int(target), message_id)
                    except Exception as exc:
                        log({'ts': tstamp(), 'type': 'warn', 'op': 'append_edit_fetch_failed', 'target': int(target), 'message_id': message_id, 'err': type(exc).__name__})
                current = getattr(original, 'message', None) if original is not None else None
                # None means the original cannot safely be read. Never reconstruct
                # it from the *new* source: that would destroy the old version.
                suffix = '\n\n' + entry
                combined = current + suffix if current is not None else None
                limit = 1024 if caption and payload.get('delivery_kind') != 'file_skip_notice' else 4096
                done = False
                if current is not None and current.endswith(suffix):
                    done = True  # API succeeded before a mapping-store/network failure.
                elif combined is not None and _units(combined) <= limit:
                    formatted = unparse(current, getattr(original, 'entities', None) or []) + escape(suffix)
                    async def call():
                        if caption and payload.get('delivery_kind') != 'file_skip_notice':
                            await bot.edit_message_caption(chat_id=int(target), message_id=message_id, caption=formatted, parse_mode="HTML")
                        else:
                            await bot.edit_message_text(chat_id=int(target), message_id=message_id, text=formatted, parse_mode="HTML")
                    # An identical edit is idempotent; transport failures can retry.
                    done = await _run_edit_call(call, {'ts': tstamp(), 'type': 'err', 'op': 'append_edit', 'target': int(target), 'message_id': message_id}, log)
                else:
                    # A pending send has an unknown outcome; do not auto-replay it.
                    digest = hashlib.sha256(entry.encode()).hexdigest()
                    attempts = state.setdefault('reply_attempts', {})
                    pending = attempts.get(digest)
                    if pending:
                        log({'ts': tstamp(), 'type': 'warn', 'op': 'append_edit_reply_uncertain', 'target': int(target), 'message_id': message_id})
                        continue
                    pieces = list(_chunks(entry))
                    receipts = []
                    pending = {'digest': digest, 'sent_ids': receipts}
                    attempts[digest] = pending
                    progress[key] = state
                    await store(info['chat_id'], source_id, target, payload)
                    try:
                        for piece in pieces:
                            kwargs = dict(chat_id=int(target), text=piece, parse_mode=None, reply_to_message_id=message_id)
                            if message_id == payload.get('topic_message_id') and payload.get('message_thread_id'):
                                kwargs['message_thread_id'] = payload['message_thread_id']
                            sent = await bot.send_message(**kwargs)
                            receipts.append(sent.message_id)
                            await store(info['chat_id'], source_id, target, payload)
                        done = True
                        log({'ts': tstamp(), 'type': 'out', 'op': 'append_edit_reply', 'target': int(target), 'message_id': message_id, 'parts': len(receipts), 'reason': 'length_limit' if current is not None else 'original_unavailable'})
                    except Exception as exc:
                        log({'ts': tstamp(), 'type': 'err', 'op': 'append_edit_reply_uncertain', 'target': int(target), 'message_id': message_id, 'err': type(exc).__name__})
                if done:
                    progress[key] = {**state, 'source_text': source_text, 'edit_date': event_date}
                    await store(info['chat_id'], source_id, target, payload)
                    log({'ts': tstamp(), 'type': 'out', 'op': 'append_edit', 'status': 'ok', 'target': int(target), 'message_id': message_id, 'chat_id': info.get('chat_id'), 'msg_id': info.get('msg_id')})
                    any_ok = True
        return any_ok


async def edit_forwarded_text(bot, info, text, log, semaphore=None, original_fetcher=None):
    return await _append_history(bot, info, text, log, original_fetcher=original_fetcher)


async def edit_forwarded_media_caption(bot, info, caption_text, log, semaphore=None, original_fetcher=None):
    return await _append_history(bot, info, caption_text, log, caption=True, original_fetcher=original_fetcher)


async def edit_forwarded_album_caption(bot, info, caption_text, log, semaphore=None, original_fetcher=None):
    return await _append_history(bot, info, caption_text, log, album=True, caption=True, original_fetcher=original_fetcher)
