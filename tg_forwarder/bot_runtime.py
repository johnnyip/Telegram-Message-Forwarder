import asyncio
from pathlib import Path
from typing import Any, Callable, Optional

from .bot_sender import bot_send_album, bot_send_file, bot_send_text
from .formatting import build_header_from_info
from .timefmt import append_original_time
from .utils import tstamp
from .verbose_flags import maybe_verbose_log


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


async def send_text_via_bot(bot, target: Any, combined: str, info: dict, route: dict, source_kind: str, *, bot_send_semaphore, log) -> bool:
    combined = append_original_time(combined, info.get("msg_date"))
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
            await _call_with_retry(lambda: bot_send_text(bot, target, combined))
        log({"ts": tstamp(), "type": "out", "op": "send_text", "status": "ok", **extra})
        return True
    except Exception as exc:
        log({"ts": tstamp(), "type": "err", "op": "send_text", "err": exc.__class__.__name__, "msg": str(exc), **extra, "target": target, "text_preview": combined[:200], "text_len": len(combined)})
        return False


async def send_file_via_bot(bot, target: Any, fpath: Path, caption: str, info: dict, route: dict, source_kind: str, *, bot_send_semaphore, log, upload_max_bytes: Optional[int] = None) -> bool:
    caption = append_original_time(caption, info.get("msg_date"))
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
                await _call_with_retry(lambda: bot_send_text(bot, target, notice))
            log({"ts": tstamp(), "type": "out", "op": "send_file_skip_notice", "status": "ok", **extra, "target": target, "file_size": file_size, "upload_max_bytes": upload_max_bytes})
            return True
        except Exception as exc:
            log({"ts": tstamp(), "type": "err", "op": "send_file_skip_notice", "err": exc.__class__.__name__, "msg": str(exc), **extra, "target": target, "file_size": file_size, "upload_max_bytes": upload_max_bytes})
            return False

    try:
        async with bot_send_semaphore:
            await _call_with_retry(
                lambda: bot_send_file(
                    bot,
                    target,
                    str(fpath),
                    caption,
                    media_type=media_kind,
                    log_fn=lambda payload: log({"ts": tstamp(), "type": "debug", **extra, **payload}),
                )
            )
        log({"ts": tstamp(), "type": "out", "op": "send_file", "status": "ok", **extra, "media_kind": media_kind})
        return True
    except Exception as exc:
        log({"ts": tstamp(), "type": "err", "op": "send_file", "err": exc.__class__.__name__, "msg": str(exc), **extra, "target": target, "caption_preview": caption[:200], "media_kind": media_kind})
        return False


async def send_album_via_bot(bot, target: Any, files: list[str], caption: str, info: dict, route: dict, source_kind: str, *, bot_send_semaphore, album_max_total_bytes: int, upload_max_bytes: Optional[int], log) -> bool:
    caption = append_original_time(caption, info.get("msg_date"))
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
    try:
        if has_large_video:
            raise RuntimeError(f"album_too_large_for_media_group total_size={total_size} threshold={album_max_total_bytes}")
        async with bot_send_semaphore:
            await _call_with_retry(
                lambda: bot_send_album(
                    bot,
                    target,
                    files,
                    caption,
                    media_types=media_types,
                    log_fn=lambda payload: log({"ts": tstamp(), "type": "debug", **extra, **payload}),
                )
            )
        log({"ts": tstamp(), "type": "out", "op": "send_album", "status": "ok", **extra, "total_size": total_size})
        return True
    except Exception as exc:
        log({"ts": tstamp(), "type": "warn", "op": "send_album_fallback_to_single", "err": exc.__class__.__name__, "msg": str(exc), **extra, "caption_preview": caption[:200], "media_types": media_types})
        ok_count = 0
        for idx, single in enumerate(files):
            single_type = media_types[idx] if media_types and idx < len(media_types) else None
            single_caption = caption if idx == 0 else ""
            try:
                async with bot_send_semaphore:
                    await _call_with_retry(
                        lambda single_path=single, single_caption_text=single_caption, single_media_type=single_type: bot_send_file(
                            bot,
                            target,
                            single_path,
                            single_caption_text,
                            media_type=single_media_type,
                            log_fn=lambda payload: log({"ts": tstamp(), "type": "debug", **extra, **payload, "fallback_single": True, "file": single_path, "media_type": single_media_type}),
                        ) if not (upload_max_bytes and Path(single).exists() and Path(single).stat().st_size > upload_max_bytes) else None
                    )
                if upload_max_bytes and Path(single).exists() and Path(single).stat().st_size > upload_max_bytes:
                    hdr = build_header_from_info(info, is_edit=(source_kind == "edited"))
                    notice = (
                        f"{hdr}\n"
                        f"Media too large for bot upload (>{upload_max_bytes // (1024 * 1024)}MB).\n"
                        f"Saved locally:\n"
                        f"`{single}`"
                    )
                    notice = append_original_time(notice, info.get("msg_date"))
                    await _call_with_retry(lambda: bot_send_text(bot, target, notice))
                    log({"ts": tstamp(), "type": "out", "op": "send_album_single_skip_notice", "status": "ok", "file": single, "target": target, "media_type": single_type, **extra})
                else:
                    log({"ts": tstamp(), "type": "out", "op": "send_album_single_fallback", "status": "ok", "file": single, "target": target, "media_type": single_type, **extra})
                ok_count += 1
            except Exception as inner:
                log({"ts": tstamp(), "type": "err", "op": "send_album_single_fallback", "err": inner.__class__.__name__, "msg": str(inner), "file": single, "target": target, "media_type": single_type, "caption_preview": single_caption[:200], **extra})
        return ok_count > 0
