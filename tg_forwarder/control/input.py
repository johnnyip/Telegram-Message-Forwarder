from dataclasses import dataclass
import re
from urllib.parse import urlsplit, parse_qs
from .config import positive


@dataclass(frozen=True)
class SourceRef:
    peer: object
    post: int
    comment: int = None

    def as_dict(self):
        return {"peer": self.peer, "post": self.post, "comment": self.comment}


def parse_url(raw):
    if not isinstance(raw, str) or len(raw) > 512:
        raise ValueError("請提供原帖 t.me 連結")
    u = urlsplit(raw.strip())
    if u.scheme != "https" or u.netloc.lower() != "t.me" or u.fragment:
        raise ValueError("只接受 https://t.me 原帖連結")
    bits = u.path.strip("/").split("/")
    if len(bits) == 3 and bits[0] == "c":
        channel = positive(bits[1], 999999999999)
        peer = -(1000000000000 + channel)
        post = positive(bits[2], 2**31-1)
    elif len(bits) == 2 and re.fullmatch(r"[A-Za-z][A-Za-z0-9_]{3,31}", bits[0]):
        peer, post = bits[0].lower(), positive(bits[1], 2**31-1)
    else:
        raise ValueError("不支援此 Telegram 連結")
    q = parse_qs("&".join("single=" if p == "single" else p for p in u.query.split("&")), keep_blank_values=True, strict_parsing=True)
    if set(q) - {"comment", "single"} or any(len(v) != 1 for v in q.values()):
        raise ValueError("不支援連結參數")
    comment = positive(q["comment"][0], 2**31-1) if "comment" in q else None
    if "single" in q and q["single"][0] not in ("", "1"):
        raise ValueError("invalid single parameter")
    return SourceRef(peer, post, comment)


def from_message(message):
    text = (getattr(message, "text", None) or getattr(message, "caption", None) or "").strip()
    if text.startswith("https://"):
        return parse_url(text)
    origin = getattr(message, "forward_origin", None)
    chat = getattr(origin, "chat", None)
    mid = getattr(origin, "message_id", None)
    if chat is not None and mid is not None:
        return SourceRef(chat.id, positive(mid, 2**31-1))
    # Older Bot API objects/fakes may expose the legacy fields.
    chat = getattr(message, "forward_from_chat", None)
    mid = getattr(message, "forward_from_message_id", None)
    if chat is not None and mid is not None:
        return SourceRef(chat.id, positive(mid, 2**31-1))
    raise ValueError("來源已隱藏；請貼原帖連結，唔會只下載轉發副本")
