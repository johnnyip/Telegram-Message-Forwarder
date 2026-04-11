import mimetypes
from pathlib import Path
from typing import Optional

from ..core.utils import sanitize_filename_part


def media_type(m):
    if getattr(m, "media", None) is None:
        return "text"
    return (
        "photo" if getattr(m, "photo", None) else
        "video" if getattr(m, "video", None) else
        "video_note" if getattr(m, "video_note", None) else
        "voice" if getattr(m, "voice", None) else
        "audio" if getattr(m, "audio", None) else
        "animation" if getattr(m, "animation", None) else
        "document" if getattr(m, "document", None) else
        "other"
    )


def get_media_size(msg) -> Optional[int]:
    file_obj = getattr(msg, "file", None)
    size = getattr(file_obj, "size", None)
    if isinstance(size, int):
        return size
    return None


def build_filename_from_message(msg, sender_display="unknown"):
    safe_sender = sanitize_filename_part(sender_display)
    ext = ""

    document = getattr(msg, "document", None)
    file_obj = getattr(msg, "file", None)
    if document and file_obj and getattr(file_obj, "name", None):
        ext = Path(file_obj.name).suffix
    elif file_obj and getattr(file_obj, "mime_type", None):
        ext = mimetypes.guess_extension(file_obj.mime_type) or ""

    return f"{msg.id}_{safe_sender}{ext}"


__all__ = [
    "build_filename_from_message",
    "get_media_size",
    "media_type",
]
