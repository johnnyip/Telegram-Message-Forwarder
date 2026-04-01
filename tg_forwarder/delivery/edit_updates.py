from typing import Any

from ..storage.edit_mapping import get_album_mapping, get_message_mapping
from ..domain.formatting import build_header_from_info
from ..domain.timefmt import append_original_time, append_edited_suffix
from ..core.utils import tstamp


async def edit_forwarded_text(bot, info: dict, text: str, log) -> bool:
    mapping = await get_message_mapping(info["chat_id"], info["msg_id"])
    targets = mapping.get("targets", {}) if isinstance(mapping, dict) else {}
    if not targets:
        log({"ts": tstamp(), "type": "warn", "op": "edit_forwarded_text_missing_mapping", "chat_id": info.get("chat_id"), "msg_id": info.get("msg_id")})
        return False

    body = f"{build_header_from_info(info, is_edit=False)}\n{text or '[empty]'}".strip()
    body = append_original_time(body, info.get("msg_date"))
    body = append_edited_suffix(body)

    ok = False
    for target, payload in targets.items():
        message_id = payload.get("message_id")
        if not message_id:
            continue
        try:
            try:
                await bot.edit_message_text(chat_id=int(target), message_id=message_id, text=body, parse_mode="Markdown")
            except Exception:
                await bot.edit_message_text(chat_id=int(target), message_id=message_id, text=body)
            log({"ts": tstamp(), "type": "out", "op": "edit_forwarded_text", "status": "ok", "target": int(target), "message_id": message_id, "chat_id": info.get("chat_id"), "msg_id": info.get("msg_id")})
            ok = True
        except Exception as exc:
            log({"ts": tstamp(), "type": "err", "op": "edit_forwarded_text", "err": exc.__class__.__name__, "msg": str(exc), "target": int(target), "message_id": message_id, "chat_id": info.get("chat_id"), "msg_id": info.get("msg_id")})
    return ok


async def edit_forwarded_media_caption(bot, info: dict, caption_text: str, log) -> bool:
    mapping = await get_message_mapping(info["chat_id"], info["msg_id"])
    targets = mapping.get("targets", {}) if isinstance(mapping, dict) else {}
    if not targets:
        log({"ts": tstamp(), "type": "warn", "op": "edit_forwarded_media_missing_mapping", "chat_id": info.get("chat_id"), "msg_id": info.get("msg_id")})
        return False

    caption = f"{build_header_from_info(info, is_edit=False)}\n{caption_text or ''}".strip()
    caption = append_original_time(caption, info.get("msg_date"))
    caption = append_edited_suffix(caption)

    ok = False
    for target, payload in targets.items():
        message_id = payload.get("message_id")
        if not message_id:
            continue
        try:
            try:
                await bot.edit_message_caption(chat_id=int(target), message_id=message_id, caption=caption, parse_mode="Markdown")
            except Exception:
                await bot.edit_message_caption(chat_id=int(target), message_id=message_id, caption=caption)
            log({"ts": tstamp(), "type": "out", "op": "edit_forwarded_media_caption", "status": "ok", "target": int(target), "message_id": message_id, "chat_id": info.get("chat_id"), "msg_id": info.get("msg_id")})
            ok = True
        except Exception as exc:
            log({"ts": tstamp(), "type": "err", "op": "edit_forwarded_media_caption", "err": exc.__class__.__name__, "msg": str(exc), "target": int(target), "message_id": message_id, "chat_id": info.get("chat_id"), "msg_id": info.get("msg_id")})
    return ok


async def edit_forwarded_album_caption(bot, info: dict, caption_text: str, log) -> bool:
    grouped_id = info.get("grouped_id")
    if grouped_id is None:
        return False
    mapping = await get_album_mapping(info["chat_id"], grouped_id)
    targets = mapping.get("targets", {}) if isinstance(mapping, dict) else {}
    if not targets:
        log({"ts": tstamp(), "type": "warn", "op": "edit_forwarded_album_missing_mapping", "chat_id": info.get("chat_id"), "grouped_id": grouped_id})
        return False

    caption = f"{build_header_from_info(info, is_edit=False)}\n{caption_text or ''}".strip()
    caption = append_original_time(caption, info.get("msg_date"))
    caption = append_edited_suffix(caption)

    ok = False
    for target, payload in targets.items():
        message_id = payload.get("message_id")
        if not message_id:
            continue
        try:
            try:
                await bot.edit_message_caption(chat_id=int(target), message_id=message_id, caption=caption, parse_mode="Markdown")
            except Exception:
                await bot.edit_message_caption(chat_id=int(target), message_id=message_id, caption=caption)
            log({"ts": tstamp(), "type": "out", "op": "edit_forwarded_album_caption", "status": "ok", "target": int(target), "message_id": message_id, "chat_id": info.get("chat_id"), "grouped_id": grouped_id})
            ok = True
        except Exception as exc:
            log({"ts": tstamp(), "type": "err", "op": "edit_forwarded_album_caption", "err": exc.__class__.__name__, "msg": str(exc), "target": int(target), "message_id": message_id, "chat_id": info.get("chat_id"), "grouped_id": grouped_id})
    return ok


__all__ = [
    "edit_forwarded_album_caption",
    "edit_forwarded_media_caption",
    "edit_forwarded_text",
]
