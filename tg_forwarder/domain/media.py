import mimetypes
from pathlib import Path
from typing import Optional

from ..core.utils import sanitize_filename_part


def media_type(m):
    if m.media is None:
        return "text"
    return (
        "photo" if m.photo else
        "video" if m.video else
        "video_note" if getattr(m, "video_note", None) else
        "voice" if m.voice else
        "audio" if m.audio else
        "animation" if m.animation else
        "document" if m.document else
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

    if msg.document and msg.file and msg.file.name:
        ext = Path(msg.file.name).suffix
    elif msg.file and msg.file.mime_type:
        ext = mimetypes.guess_extension(msg.file.mime_type) or ""

    return f"{msg.id}_{safe_sender}{ext}"


__all__ = [
    "build_filename_from_message",
    "get_media_size",
    "media_type",
]
