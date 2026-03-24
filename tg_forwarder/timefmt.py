import os
from datetime import datetime
from typing import Optional
from zoneinfo import ZoneInfo

DISPLAY_TZ = os.getenv("DISPLAY_TIMEZONE") or os.getenv("TZ") or "Asia/Hong_Kong"


def hhmm_from_msg_date(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=ZoneInfo('UTC'))
        local_dt = dt.astimezone(ZoneInfo(DISPLAY_TZ))
        return local_dt.strftime('%H:%M')
    except Exception:
        return None


def append_original_time(text: str, msg_date: Optional[str]) -> str:
    hhmm = hhmm_from_msg_date(msg_date)
    if not hhmm:
        return text
    suffix = f"\nOriginal send {hhmm}"
    if text.endswith(suffix):
        return text
    return f"{text}{suffix}"
