"""
Parse the sender identity out of a forwarded message sitting in the general topic.

Two formats are handled:

Format 1 — current bot header (build_header_from_info output):
    [Chat Title](url)          ← may or may not have URL
    `Display Name`             ← md_code(sender_display)
    `username`                 ← md_code(sender_username), optional
    `123456789`                ← md_code(sender_id), only line that is pure digits
    … message body …
    Original send HH:MM

    The sender_id line is a reliable anchor: a backtick-wrapped integer.

Format 2 — old bot header:
    [Chat Title] @username     ← @username or literal "user" after the bracket group
    … message body …

    No backtick-wrapped integers.  May have @username or just "user".

Additionally, if the Telethon message is a direct forward (msg.forward is not None),
the forward's from_id / sender_id takes priority over text parsing.
"""

import re
from dataclasses import dataclass
from typing import Optional


# ---- regex anchors -------------------------------------------------------

# A line that is exactly a backtick-wrapped integer → sender_id in Format 1
_RE_SENDER_ID_LINE = re.compile(r"^`(\d{5,15})`$")

# A line that is a backtick-wrapped potential username or display name
# (we only care if it looks like a Telegram username: 5–32 word chars, no spaces)
_RE_BACKTICK_USERNAME = re.compile(r"^`@?([\w\d_]{5,32})`$")

# Format 2: [anything] @username  OR  [anything] user  at end of first non-empty line
# Allows emojis / Unicode in the bracket portion.
_RE_OLD_HEADER = re.compile(r"^\[.+\]\s+@?([\w\d_]+)\s*$")


@dataclass
class ParsedSender:
    sender_id: Optional[int]    # numeric Telegram user ID (most reliable key)
    username:  Optional[str]    # Telegram @handle without the @
    display:   Optional[str]    # human-readable display name if extractable
    fmt:       int              # 1 = current format, 2 = old format, 0 = unknown
    is_unknown: bool            # True when neither ID nor username could be found


def _extract_from_forward(msg) -> Optional[ParsedSender]:
    """If the Telethon message is a direct forward, pull sender from forward metadata."""
    fwd = getattr(msg, "forward", None)
    if fwd is None:
        return None

    sender_id: Optional[int] = None
    username: Optional[str] = None
    display: Optional[str] = None

    from_id = getattr(fwd, "from_id", None)
    if from_id is not None:
        # PeerUser(user_id=…) or similar
        uid = getattr(from_id, "user_id", None)
        if isinstance(uid, int):
            sender_id = uid

    sender = getattr(fwd, "sender", None)
    if sender is not None:
        username = getattr(sender, "username", None)
        first = getattr(sender, "first_name", None) or ""
        last = getattr(sender, "last_name", None) or ""
        full = f"{first} {last}".strip()
        if full:
            display = full

    from_name = getattr(fwd, "from_name", None)
    if from_name and not display:
        display = from_name

    if sender_id is None and username is None:
        return None

    return ParsedSender(
        sender_id=sender_id,
        username=username,
        display=display,
        fmt=1,
        is_unknown=False,
    )


def _extract_from_text(text: str) -> ParsedSender:
    """Parse the message text/caption to find sender identity."""
    if not text:
        return ParsedSender(None, None, None, fmt=0, is_unknown=True)

    lines = [ln.strip() for ln in text.split("\n") if ln.strip()]
    if not lines:
        return ParsedSender(None, None, None, fmt=0, is_unknown=True)

    # ── Format 1 detection: scan first 6 lines for backtick-wrapped integer ──
    sender_id: Optional[int] = None
    username: Optional[str] = None
    display: Optional[str] = None

    for line in lines[:6]:
        m = _RE_SENDER_ID_LINE.match(line)
        if m:
            sender_id = int(m.group(1))
            break

    if sender_id is not None:
        # It's Format 1 — pick up username / display from adjacent lines
        for line in lines[1:6]:
            if _RE_SENDER_ID_LINE.match(line):
                continue  # skip the id line itself
            m = _RE_BACKTICK_USERNAME.match(line)
            if m:
                val = m.group(1).lstrip("@")
                # Heuristic: if the val looks like an @-handle (no spaces, all word chars)
                # treat it as username; otherwise it might be a display name with spaces
                # but since we already stripped spaces, everything here is word-chars only
                if not username:
                    username = val
                elif not display:
                    display = val

        return ParsedSender(
            sender_id=sender_id,
            username=username,
            display=display,
            fmt=1,
            is_unknown=False,
        )

    # ── Format 2 detection: [Title] @username or [Title] user on first line ──
    m2 = _RE_OLD_HEADER.match(lines[0])
    if m2:
        val = m2.group(1)
        if val.lower() == "user":
            # Completely anonymous — goes to Migration topic
            return ParsedSender(None, None, None, fmt=2, is_unknown=True)
        return ParsedSender(
            sender_id=None,
            username=val.lstrip("@"),
            display=None,
            fmt=2,
            is_unknown=False,
        )

    # Unknown format
    return ParsedSender(None, None, None, fmt=0, is_unknown=True)


def parse_sender(msg) -> ParsedSender:
    """
    Extract sender identity from a Telethon message in the general topic.

    Priority:
      1. Forward metadata (msg.forward) — most reliable for Telethon-direct-forwarded msgs
      2. Caption / message text — for bot-sent media with header captions
    """
    fwd_result = _extract_from_forward(msg)
    if fwd_result is not None:
        return fwd_result

    text = (getattr(msg, "message", None) or getattr(msg, "text", None) or "")
    return _extract_from_text(text)


__all__ = [
    "ParsedSender",
    "parse_sender",
]
