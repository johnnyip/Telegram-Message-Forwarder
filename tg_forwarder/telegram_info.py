from telethon import types


async def resolve_sender_info_from_message(msg, chat_id_hint=None) -> dict:
    chat = None
    sender = None

    if hasattr(msg, "get_chat"):
        try:
            chat = await msg.get_chat()
        except Exception:
            chat = None

    if hasattr(msg, "get_sender"):
        try:
            sender = await msg.get_sender()
        except Exception:
            sender = None

    chat_id = chat_id_hint if chat_id_hint is not None else getattr(msg, "chat_id", None)
    chat_title = getattr(chat, "title", None) or str(chat_id)
    chat_username = getattr(chat, "username", None)
    chat_noforwards = bool(getattr(chat, "noforwards", False)) if chat is not None else False
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
        "sender_id": getattr(sender, "id", None) or getattr(msg, "sender_id", None),
        "sender_type": "unknown",
        "sender_username": None,
        "sender_display": "user",
        "sender_first_name": None,
        "sender_last_name": None,
        "post_author": getattr(msg, "post_author", None),
        "raw_sender_class": sender.__class__.__name__ if sender is not None else None,
        "msg_id": msg.id,
        "msg_date": str(getattr(msg, "date", None)),
        "edit_date": str(getattr(msg, "edit_date", None)),
        "grouped_id": getattr(msg, "grouped_id", None),
        "reply_to_msg_id": getattr(getattr(msg, "reply_to", None), "reply_to_msg_id", None),
    }

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
        return info

    if info["post_author"]:
        info["sender_type"] = "post_author"
        info["sender_display"] = str(info["post_author"])
        return info

    if getattr(msg, "sender_id", None):
        info["sender_type"] = "sender_id_only"
        info["sender_display"] = f"id:{msg.sender_id}"
        return info

    info["sender_type"] = "anonymous_or_unknown"
    info["sender_display"] = "anonymous"
    return info
