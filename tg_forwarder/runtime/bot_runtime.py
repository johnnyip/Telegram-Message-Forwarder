import asyncio
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Optional

from ..delivery.bot_sender import bot_send_album, bot_send_file, bot_send_text
from ..domain.formatting import build_header_from_info
from ..domain.timefmt import append_original_time, append_edited_suffix
from ..core.utils import tstamp
from .verbose_flags import maybe_verbose_log


@dataclass(frozen=True)
class SendOutcome:
    ok: bool
    preserve_local_copy: bool = False
    sent_message_id: Optional[int] = None
    sent_message_ids: Optional[list[int]] = None
    delivery_kind: Optional[str] = None

    def __bool__(self) -> bool:
        return self.ok


def job_media_type_from_info(info: dict) -> Optional[str]:
    if not isinstance(info, dict):
        return None
    if info.get("_media_type"):
        return info.get("_media_type")
    snapshots = info.get("_album_snapshots")
    if isinstance(snapshots, list) and snapshots:
        return snapshots[0].get("media_type")
    return None


async def _call_with_retry(send: Callable[[], Any]) -> Any:
    try:
        return await send()
    except Exception as exc:
        retry_after = getattr(exc, "retry_after", None)
        if retry_after:
            await asyncio.sleep(float(retry_after) + 1)
            return await send()
        if exc.__class__.__name__ == "TimedOut":
            await asyncio.sleep(2)
            return await send()
        raise


async def send_text_via_bot(bot, target: Any, combined: str, info: dict, route: dict, source_kind: str, *, bot_send_semaphore, log) -> SendOutcome:
    combined = append_original_time(combined, info.get("msg_date"))
    if source_kind == "edited":
        combined = append_edited_suffix(combined)
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
    }
    maybe_verbose_log(log, {"ts": tstamp(), "type": "info", "op": "send_text_attempt", **extra, "target": target, "text_preview": combined[:200]})
    try:
        async with bot_send_semaphore:
            sent = await _call_with_retry(lambda: bot_send_text(bot, target, combined))
        log({"ts": tstamp(), "type": "out", "op": "send_text", "status": "ok", **extra, "sent_message_id": getattr(sent, "message_id", None)})
        return SendOutcome(True, sent_message_id=getattr(sent, "message_id", None), delivery_kind="text")
    except Exception as exc:
        log({"ts": tstamp(), "type": "err", "op": "send_text", "err": exc.__class__.__name__, "msg": str(exc), **extra, "target": target, "text_preview": combined[:200], "text_len": len(combined)})
        return SendOutcome(False)


async def send_file_via_bot(bot, target: Any, fpath: Path, caption: str, info: dict, route: dict, source_kind: str, *, bot_send_semaphore, log, upload_max_bytes: Optional[int] = None) -> SendOutcome:
    caption = append_original_time(caption, info.get("msg_date"))
    if source_kind == "edited":
        caption = append_edited_suffix(caption)
    extra = {
        "route": route["name"],
        "dst": target,
        "src_msg": info["msg_id"],
        "chat_id": info["chat_id"],
        "chat_title": info["chat_title"],
        "sender_id": info["sender_id"],
        "sender_username": info["sender_username"],
        "sender_display": info["sender_display"],
        "file": str(fpath),
        "source_kind": source_kind,
    }
    media_kind = job_media_type_from_info(info)
    file_size = fpath.stat().st_size if fpath.exists() else None
    maybe_verbose_log(log, {"ts": tstamp(), "type": "info", "op": "send_file_attempt", **extra, "target": target, "media_kind": media_kind, "caption_preview": caption[:200], "file_size": file_size, "upload_max_bytes": upload_max_bytes})

    if upload_max_bytes and file_size and file_size > upload_max_bytes:
        hdr = build_header_from_info(info, is_edit=(source_kind == "edited"))
        notice = (
            f"{hdr}\n"
            f"Media too large for bot upload (>{upload_max_bytes // (1024 * 1024)}MB).\n"
            f"Saved locally:\n"
            f"`{str(fpath)}`"
        )
        notice = append_original_time(notice, info.get("msg_date"))
        log({"ts": tstamp(), "type": "warn", "op": "send_file_skip_too_large", **extra, "target": target, "media_kind": media_kind, "file_size": file_size, "upload_max_bytes": upload_max_bytes})
        try:
            async with bot_send_semaphore:
                sent = await _call_with_retry(lambda: bot_send_text(bot, target, notice))
            log({"ts": tstamp(), "type": "out", "op": "send_file_skip_notice", "status": "ok", **extra, "target": target, "file_size": file_size, "upload_max_bytes": upload_max_bytes, "preserve_local_copy": True, "sent_message_id": getattr(sent, "message_id", None)})
            return SendOutcome(True, preserve_local_copy=True, sent_message_id=getattr(sent, "message_id", None), delivery_kind="file_skip_notice")
        except Exception as exc:
            log({"ts": tstamp(), "type": "err", "op": "send_file_skip_notice", "err": exc.__class__.__name__, "msg": str(exc), **extra, "target": target, "file_size": file_size, "upload_max_bytes": upload_max_bytes})
            return SendOutcome(False)

    try:
        async with bot_send_semaphore:
            sent = await _call_with_retry(
                lambda: bot_send_file(
                    bot,
                    target,
                    str(fpath),
                    caption,
                    media_type=media_kind,
                    log_fn=lambda payload: log({"ts": tstamp(), "type": "debug", **extra, **payload}),
                )
            )
        log({"ts": tstamp(), "type": "out", "op": "send_file", "status": "ok", **extra, "media_kind": media_kind, "sent_message_id": getattr(sent, "message_id", None)})
        return SendOutcome(True, sent_message_id=getattr(sent, "message_id", None), delivery_kind="file")
    except Exception as exc:
        log({"ts": tstamp(), "type": "err", "op": "send_file", "err": exc.__class__.__name__, "msg": str(exc), **extra, "target": target, "caption_preview": caption[:200], "media_kind": media_kind})
        return SendOutcome(False)


async def send_album_via_bot(bot, target: Any, files: list[str], caption: str, info: dict, route: dict, source_kind: str, *, bot_send_semaphore, album_max_total_bytes: int, upload_max_bytes: Optional[int], log) -> SendOutcome:
    caption = append_original_time(caption, info.get("msg_date"))
    if source_kind == "edited":
        caption = append_edited_suffix(caption)
    extra = {
        "route": route["name"],
        "dst": target,
        "src_msg": info["msg_id"],
        "chat_id": info["chat_id"],
        "chat_title": info["chat_title"],
        "sender_id": info["sender_id"],
        "sender_username": info["sender_username"],
        "sender_display": info["sender_display"],
        "files": files,
        "source_kind": source_kind,
    }
    media_types = [snap.get("media_type") for snap in info.get("_album_snapshots", [])] if isinstance(info.get("_album_snapshots"), list) else None
    total_size = sum(Path(path).stat().st_size for path in files if Path(path).exists())
    has_large_video = any(media_type == "video" for media_type in (media_types or [])) and total_size > album_max_total_bytes
    maybe_verbose_log(log, {"ts": tstamp(), "type": "info", "op": "send_album_attempt", **extra, "target": target, "media_types": media_types, "caption_preview": caption[:200], "total_size": total_size, "album_max_total_bytes": album_max_total_bytes})
    preserve_local_copy = False
    try:
        if has_large_video:
            raise RuntimeError(f"album_too_large_for_media_group total_size={total_size} threshold={album_max_total_bytes}")
        async with bot_send_semaphore:
            sent = await _call_with_retry(
                lambda: bot_send_album(
                    bot,
                    target,
                    files,
                    caption,
                    media_types=media_types,
                    log_fn=lambda payload: log({"ts": tstamp(), "type": "debug", **extra, **payload}),
                )
            )
        sent_ids = [getattr(x, "message_id", None) for x in (sent or [])]
        log({"ts": tstamp(), "type": "out", "op": "send_album", "status": "ok", **extra, "total_size": total_size, "sent_message_ids": sent_ids})
        return SendOutcome(True, sent_message_id=(sent_ids[0] if sent_ids else None), sent_message_ids=sent_ids, delivery_kind="album")
    except Exception as exc:
        err_name = exc.__class__.__name__
        err_msg = str(exc)
        allow_single_fallback = (
            err_name == "RuntimeError" and "album_too_large_for_media_group" in err_msg
        ) or (
            err_name == "BadRequest" and any(token in err_msg.lower() for token in ("group", "media", "caption", "entity", "parse"))
        )
        log({"ts": tstamp(), "type": "warn", "op": "send_album_group_failed", "err": err_name, "msg": err_msg, "allow_single_fallback": allow_single_fallback, **extra, "caption_preview": caption[:200], "media_types": media_types})
        if not allow_single_fallback:
            return SendOutcome(False)

        log({"ts": tstamp(), "type": "warn", "op": "send_album_fallback_to_single", "err": err_name, "msg": err_msg, **extra, "caption_preview": caption[:200], "media_types": media_types})
        ok_count = 0
        fallback_message_ids: list[int] = []
        for idx, single in enumerate(files):
            single_type = media_types[idx] if media_types and idx < len(media_types) else None
            single_caption = caption if idx == 0 else ""
            # Determine whether the file is too large *before* calling _call_with_retry.
            # Passing `None` (not a coroutine) to _call_with_retry causes `await None`
            # which raises TypeError and swallows the notice-text path entirely.
            single_path_obj = Path(single)
            file_too_large = bool(
                upload_max_bytes
                and single_path_obj.exists()
                and single_path_obj.stat().st_size > upload_max_bytes
            )
            try:
                sent = None
                if file_too_large:
                    hdr = build_header_from_info(info, is_edit=(source_kind == "edited"))
                    notice = (
                        f"{hdr}\n"
                        f"Media too large for bot upload (>{upload_max_bytes // (1024 * 1024)}MB).\n"
                        f"Saved locally:\n"
                        f"`{single}`"
                    )
                    notice = append_original_time(notice, info.get("msg_date"))
                    async with bot_send_semaphore:
                        sent = await _call_with_retry(lambda n=notice: bot_send_text(bot, target, n))
                    log({"ts": tstamp(), "type": "out", "op": "send_album_single_skip_notice", "status": "ok", "file": single, "target": target, "media_type": single_type, "preserve_local_copy": True, "sent_message_id": getattr(sent, "message_id", None), **extra})
                    preserve_local_copy = True
                else:
                    async with bot_send_semaphore:
                        sent = await _call_with_retry(
                            lambda sp=single, sc=single_caption, st=single_type: bot_send_file(
                                bot,
                                target,
                                sp,
                                sc,
                                media_type=st,
                                log_fn=lambda payload: log({"ts": tstamp(), "type": "debug", **extra, **payload, "fallback_single": True, "file": sp, "media_type": st}),
                            )
                        )
                    log({"ts": tstamp(), "type": "out", "op": "send_album_single_fallback", "status": "ok", "file": single, "target": target, "media_type": single_type, "sent_message_id": getattr(sent, "message_id", None), **extra})
                if getattr(sent, "message_id", None):
                    fallback_message_ids.append(getattr(sent, "message_id", None))
                ok_count += 1
            except Exception as inner:
                log({"ts": tstamp(), "type": "err", "op": "send_album_single_fallback", "err": inner.__class__.__name__, "msg": str(inner), "file": single, "target": target, "media_type": single_type, "caption_preview": single_caption[:200], **extra})
        return SendOutcome(ok_count > 0, preserve_local_copy=preserve_local_copy, sent_message_id=(fallback_message_ids[0] if fallback_message_ids else None), sent_message_ids=fallback_message_ids or None, delivery_kind="album_single_fallback")


__all__ = [
    "SendOutcome",
    "job_media_type_from_info",
    "send_album_via_bot",
    "send_file_via_bot",
    "send_text_via_bot",
]
