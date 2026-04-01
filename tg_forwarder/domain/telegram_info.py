from telethon import types

from ..storage.cache import CHAT_INFO_CACHE, SENDER_INFO_CACHE, cache_get, cache_set


async def resolve_sender_info_from_message(msg, chat_id_hint=None) -> dict:
    chat = None
    sender = None

    chat_id = chat_id_hint if chat_id_hint is not None else getattr(msg, "chat_id", None)
    sender_id = getattr(msg, "sender_id", None)

    cached_chat = cache_get(CHAT_INFO_CACHE, chat_id) if chat_id is not None else None
    cached_sender = cache_get(SENDER_INFO_CACHE, sender_id) if sender_id is not None else None

    if hasattr(msg, "get_chat") and not cached_chat:
        try:
            chat = await msg.get_chat()
        except Exception:
            chat = None

    if hasattr(msg, "get_sender") and not cached_sender:
        try:
            sender = await msg.get_sender()
        except Exception:
            sender = None

    chat_title = (cached_chat or {}).get("chat_title") or getattr(chat, "title", None) or str(chat_id)
    chat_username = (cached_chat or {}).get("chat_username") or getattr(chat, "username", None)
    chat_noforwards = (cached_chat or {}).get("chat_noforwards") if cached_chat else (bool(getattr(chat, "noforwards", False)) if chat is not None else False)
    msg_noforwards = bool(getattr(msg, "noforwards", False))

    if isinstance(chat, types.Channel):
        chat_type = "channel" if getattr(chat, "broadcast", False) else "supergroup"
    elif isinstance(chat, types.Chat):
        chat_type = "group"
    elif isinstance(chat, types.User):
        chat_type = "private"
    else:
        chat_type = "unknown"

    info = {
        "chat_id": chat_id,
        "chat_title": chat_title,
        "chat_username": chat_username,
        "chat_type": chat_type,
        "raw_chat_class": chat.__class__.__name__ if chat is not None else None,
        "chat_noforwards": chat_noforwards,
        "msg_noforwards": msg_noforwards,
        "sender_id": getattr(sender, "id", None) or sender_id,
        "sender_type": (cached_sender or {}).get("sender_type", "unknown"),
        "sender_username": (cached_sender or {}).get("sender_username"),
        "sender_display": (cached_sender or {}).get("sender_display", "user"),
        "sender_first_name": (cached_sender or {}).get("sender_first_name"),
        "sender_last_name": (cached_sender or {}).get("sender_last_name"),
        "post_author": getattr(msg, "post_author", None),
        "raw_sender_class": sender.__class__.__name__ if sender is not None else None,
        "msg_id": msg.id,
        "msg_date": str(getattr(msg, "date", None)),
        "edit_date": str(getattr(msg, "edit_date", None)),
        "grouped_id": getattr(msg, "grouped_id", None),
        "reply_to_msg_id": getattr(getattr(msg, "reply_to", None), "reply_to_msg_id", None),
    }

    if chat_id is not None:
        cache_set(CHAT_INFO_CACHE, chat_id, {
            "chat_title": chat_title,
            "chat_username": chat_username,
            "chat_noforwards": chat_noforwards,
        })

    if isinstance(sender, types.User):
        info["sender_type"] = "user"
        info["sender_username"] = sender.username
        info["sender_first_name"] = sender.first_name
        info["sender_last_name"] = sender.last_name

        if sender.username:
            info["sender_display"] = f"@{sender.username}"
        else:
            full_name = " ".join(x for x in [sender.first_name, sender.last_name] if x).strip()
            info["sender_display"] = full_name or f"user:{sender.id}"
        cache_set(SENDER_INFO_CACHE, info["sender_id"], {
            "sender_type": info["sender_type"],
            "sender_username": info["sender_username"],
            "sender_display": info["sender_display"],
            "sender_first_name": info["sender_first_name"],
            "sender_last_name": info["sender_last_name"],
        })
        return info

    if isinstance(sender, (types.Chat, types.Channel)):
        info["sender_type"] = "chat"
        chat_sender_username = getattr(sender, "username", None)
        chat_sender_title = getattr(sender, "title", None)
        info["sender_username"] = chat_sender_username

        if chat_sender_username:
            info["sender_display"] = f"@{chat_sender_username}"
        else:
            info["sender_display"] = f"[{chat_sender_title or sender.id}]"
        cache_set(SENDER_INFO_CACHE, info["sender_id"], {
            "sender_type": info["sender_type"],
            "sender_username": info["sender_username"],
            "sender_display": info["sender_display"],
            "sender_first_name": info["sender_first_name"],
            "sender_last_name": info["sender_last_name"],
        })
        return info

    if info["post_author"]:
        info["sender_type"] = "post_author"
        info["sender_display"] = str(info["post_author"])
        return info

    if getattr(msg, "sender_id", None):
        info["sender_type"] = "sender_id_only"
        if not info.get("sender_display") or info.get("sender_display") == "user":
            if info.get("sender_username"):
                info["sender_display"] = f"@{info['sender_username']}"
            elif info.get("sender_first_name") or info.get("sender_last_name"):
                full_name = " ".join(x for x in [info.get("sender_first_name"), info.get("sender_last_name")] if x).strip()
                info["sender_display"] = full_name or f"id:{msg.sender_id}"
            else:
                info["sender_display"] = f"id:{msg.sender_id}"
        return info

    info["sender_type"] = "anonymous_or_unknown"
    info["sender_display"] = "anonymous"
    return info


__all__ = ["resolve_sender_info_from_message"]
