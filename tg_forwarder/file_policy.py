import os
import time
from pathlib import Path

STALE_FILE_HOURS = int(os.getenv("STALE_FILE_HOURS", "72"))


def is_stale_file(path: str) -> bool:
    p = Path(path)
    if not p.exists():
        return False
    age_seconds = time.time() - p.stat().st_mtime
    return age_seconds > STALE_FILE_HOURS * 3600
