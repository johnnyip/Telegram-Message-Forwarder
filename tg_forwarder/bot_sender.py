import mimetypes
import os
from pathlib import Path
from typing import List, Optional

from telegram import (
    Bot,
    InputFile,
    InputMediaAnimation,
    InputMediaAudio,
    InputMediaDocument,
    InputMediaPhoto,
    InputMediaVideo,
)

BOT_SEND_AS_DOCUMENT = os.getenv("BOT_SEND_AS_DOCUMENT", "false").strip().lower() in {"1", "true", "yes", "y"}


def guess_media_kind(path: str, declared_type: Optional[str] = None) -> str:
    if declared_type in {"photo", "video", "audio", "document", "voice", "animation", "video_note"}:
        if BOT_SEND_AS_DOCUMENT and declared_type not in {"audio", "voice"}:
            return "document"
        return declared_type
    if declared_type == "other":
        return "document"
    mime, _ = mimetypes.guess_type(path)
    if mime:
        if BOT_SEND_AS_DOCUMENT and not mime.startswith("audio/"):
            return "document"
        if mime.startswith("image/"):
            return "photo"
        if mime.startswith("video/"):
            return "video"
        if mime.startswith("audio/"):
            return "audio"
    return "document"


async def bot_send_text(bot: Bot, target, text: str):
    try:
        return await bot.send_message(chat_id=target, text=text, parse_mode="Markdown")
    except Exception:
        return await bot.send_message(chat_id=target, text=text)


async def bot_send_file(bot: Bot, target, file_path: str, caption: str = "", media_type: Optional[str] = None, log_fn=None):
    media_kind = guess_media_kind(file_path, media_type)
    p = Path(file_path)
    if log_fn:
        log_fn({
            "op": "bot_send_file_start",
            "target": target,
            "file": str(p),
            "declared_media_type": media_type,
            "guessed_media_kind": media_kind,
            "exists": p.exists(),
            "size": p.stat().st_size if p.exists() else None,
            "caption_len": len(caption or ""),
        })
    last_error = None
    with p.open("rb") as fh:
        async def _send(kind: str, parse_mode=None):
            if log_fn:
                log_fn({
                    "op": "bot_send_file_attempt",
                    "target": target,
                    "file": str(p),
                    "kind": kind,
                    "parse_mode": parse_mode,
                    "caption_len": len(caption or ""),
                })
            if kind == "photo":
                result = await bot.send_photo(chat_id=target, photo=fh, caption=caption, parse_mode=parse_mode)
            elif kind == "video":
                result = await bot.send_video(chat_id=target, video=fh, caption=caption, parse_mode=parse_mode)
            elif kind == "animation":
                result = await bot.send_animation(chat_id=target, animation=fh, caption=caption, parse_mode=parse_mode)
            elif kind == "audio":
                result = await bot.send_audio(chat_id=target, audio=fh, caption=caption, parse_mode=parse_mode)
            elif kind == "voice":
                result = await bot.send_voice(chat_id=target, voice=fh, caption=caption, parse_mode=parse_mode)
            elif kind == "video_note":
                result = await bot.send_video_note(chat_id=target, video_note=fh)
            else:
                result = await bot.send_document(chat_id=target, document=fh, caption=caption, parse_mode=parse_mode)
            if log_fn:
                log_fn({
                    "op": "bot_send_file_attempt_ok",
                    "target": target,
                    "file": str(p),
                    "kind": kind,
                    "parse_mode": parse_mode,
                    "message_id": getattr(result, "message_id", None),
                })
            return result

        for kind, parse_mode in ((media_kind, "Markdown"), (media_kind, None), ("document", None)):
            try:
                fh.seek(0)
                return await _send(kind, parse_mode)
            except Exception as e:
                last_error = e
                if log_fn:
                    log_fn({
                        "op": "bot_send_file_attempt_err",
                        "target": target,
                        "file": str(p),
                        "kind": kind,
                        "parse_mode": parse_mode,
                        "err": e.__class__.__name__,
                        "msg": str(e),
                    })
                continue
    if last_error:
        raise last_error
    raise RuntimeError("bot_send_file failed without explicit exception")


async def bot_send_album(bot: Bot, target, file_paths: List[str], caption: str = "", media_types: Optional[List[str]] = None, log_fn=None):
    media = []
    handles = []
    try:
        if log_fn:
            log_fn({
                "op": "bot_send_album_start",
                "target": target,
                "file_count": len(file_paths),
                "files": file_paths,
                "media_types": media_types,
                "caption_len": len(caption or ""),
            })
        for idx, path in enumerate(file_paths):
            p = Path(path)
            fh = p.open("rb")
            handles.append(fh)
            mt = media_types[idx] if media_types and idx < len(media_types) else None
            kind = guess_media_kind(path, mt)
            item_caption = caption if idx == 0 else None
            parse_mode = "Markdown" if item_caption else None
            if log_fn:
                log_fn({
                    "op": "bot_send_album_item",
                    "target": target,
                    "idx": idx,
                    "file": str(p),
                    "declared_media_type": mt,
                    "guessed_media_kind": kind,
                    "exists": p.exists(),
                    "size": p.stat().st_size if p.exists() else None,
                    "parse_mode": parse_mode,
                })
            if kind == "photo":
                media.append(InputMediaPhoto(media=fh, caption=item_caption, parse_mode=parse_mode))
            elif kind == "video":
                media.append(InputMediaVideo(media=fh, caption=item_caption, parse_mode=parse_mode))
            elif kind == "animation":
                media.append(InputMediaAnimation(media=fh, caption=item_caption, parse_mode=parse_mode))
            elif kind in {"audio", "voice"}:
                media.append(InputMediaAudio(media=fh, caption=item_caption, parse_mode=parse_mode))
            else:
                media.append(InputMediaDocument(media=fh, caption=item_caption, parse_mode=parse_mode))

        if log_fn:
            log_fn({"op": "bot_send_album_attempt", "target": target, "file_count": len(file_paths), "parse_mode": "Markdown" if caption else None})
        try:
            result = await bot.send_media_group(chat_id=target, media=media)
            if log_fn:
                log_fn({"op": "bot_send_album_attempt_ok", "target": target, "message_ids": [getattr(x, "message_id", None) for x in result]})
            return result
        except Exception as e:
            if log_fn:
                log_fn({"op": "bot_send_album_attempt_err", "target": target, "err": e.__class__.__name__, "msg": str(e)})
            err_name = e.__class__.__name__
            err_msg = str(e)
            allow_plain_retry = bool(caption) and (
                err_name == "BadRequest" and any(token in err_msg.lower() for token in ("parse", "entity", "caption"))
            )
            if not allow_plain_retry:
                raise

        media_plain = []
        for idx, path in enumerate(file_paths):
            p2 = Path(path)
            fh2 = p2.open("rb")
            handles.append(fh2)
            mt2 = media_types[idx] if media_types and idx < len(media_types) else None
            kind2 = guess_media_kind(path, mt2)
            item_caption2 = caption if idx == 0 else None
            if kind2 == "photo":
                media_plain.append(InputMediaPhoto(media=fh2, caption=item_caption2))
            elif kind2 == "video":
                media_plain.append(InputMediaVideo(media=fh2, caption=item_caption2))
            elif kind2 == "animation":
                media_plain.append(InputMediaAnimation(media=fh2, caption=item_caption2))
            elif kind2 in {"audio", "voice"}:
                media_plain.append(InputMediaAudio(media=fh2, caption=item_caption2))
            else:
                media_plain.append(InputMediaDocument(media=fh2, caption=item_caption2))
        if log_fn:
            log_fn({"op": "bot_send_album_attempt_plain_retry", "target": target, "file_count": len(file_paths)})
        result = await bot.send_media_group(chat_id=target, media=media_plain)
        if log_fn:
            log_fn({"op": "bot_send_album_attempt_plain_retry_ok", "target": target, "message_ids": [getattr(x, "message_id", None) for x in result]})
        return result
    finally:
        for fh in handles:
            try:
                fh.close()
            except Exception:
                pass
