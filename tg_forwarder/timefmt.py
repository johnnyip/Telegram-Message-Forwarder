from datetime import datetime
from typing import Optional


def hhmm_from_msg_date(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
        return dt.strftime('%H:%M')
    except Exception:
        return None


def append_original_time(text: str, msg_date: Optional[str]) -> str:
    hhmm = hhmm_from_msg_date(msg_date)
    if not hhmm:
        return text
    suffix = f"\n\nOriginal send {hhmm}"
    if text.endswith(suffix):
        return text
    return f"{text}{suffix}"
