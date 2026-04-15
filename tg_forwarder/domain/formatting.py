from typing import Any, Optional

from ..core.utils import safe_str


def build_topic_message_url(target_chat_id: Any, message_thread_id: Optional[int], message_id: Optional[int] = None) -> Optional[str]:
    if not message_thread_id:
        return None
    try:
        s = str(int(target_chat_id))
    except Exception:
        return None
    if s.startswith("-100"):
        if message_id:
            return f"https://t.me/c/{s[4:]}/{int(message_thread_id)}/{int(message_id)}"
        return f"https://t.me/c/{s[4:]}/{int(message_thread_id)}"
    return None


def md_code(value: Any) -> str:
    s = str(value)
    s = s.replace("\\", "\\\\").replace("`", "\\`")
    return f"`{s}`"


def escape_md_link_text(text: Any) -> str:
    s = str(text)
    for ch in ["\\", "[", "]", "(", ")"]:
        s = s.replace(ch, f"\\{ch}")
    return s


def build_chat_message_url(info: dict) -> Optional[str]:
    chat_username = info.get("chat_username")
    chat_id = info.get("chat_id")
    msg_id = info.get("msg_id")

    if not msg_id:
        return None

    if chat_username:
        return f"https://t.me/{chat_username}/{msg_id}"

    if isinstance(chat_id, int):
        s = str(chat_id)
        if s.startswith("-100"):
            return f"https://t.me/c/{s[4:]}/{msg_id}"

    return None


def format_copyable_identity_lines(info: dict) -> str:
    lines = []

    sender_display = info.get("sender_display")
    sender_username = info.get("sender_username")
    sender_id = info.get("sender_id")

    normalized_display = None
    if sender_display:
        normalized_display = str(sender_display).strip().lower()

    normalized_username_display = None
    if sender_username:
        normalized_username_display = f"@{str(sender_username).strip().lower()}"

    if sender_display:
        lines.append(md_code(sender_display))

    if sender_username and normalized_display != normalized_username_display:
        lines.append(md_code(sender_username))

    if sender_id is not None:
        lines.append(md_code(sender_id))

    return "\n".join(lines)


def build_header_from_info(info: dict, is_edit=False) -> str:
    chat_title = safe_str(info.get("chat_title"))
    url = build_chat_message_url(info)

    if url:
        group_line = f"[{escape_md_link_text(chat_title)}]({url})"
    else:
        group_line = f"[{escape_md_link_text(chat_title)}]"

    identity_lines = format_copyable_identity_lines(info)

    if is_edit:
        if identity_lines:
            return f"{group_line}\n`[Edited]`\n{identity_lines}"
        return f"{group_line}\n`[Edited]`"

    if identity_lines:
        return f"{group_line}\n{identity_lines}"
    return group_line


def should_ignore(info: dict, ignore_users: set, ignore_ids: set) -> bool:
    sender_username = info.get("sender_username")
    sender_id = info.get("sender_id")

    if sender_username and sender_username.lower().lstrip("@") in ignore_users:
        return True

    if sender_id is not None and sender_id in ignore_ids:
        return True

    return False


__all__ = [
    "build_chat_message_url",
    "build_header_from_info",
    "build_topic_message_url",
    "escape_md_link_text",
    "format_copyable_identity_lines",
    "md_code",
    "should_ignore",
]
