from dataclasses import dataclass
import json
import os
from pathlib import Path
import re


def positive(value, maximum=2**63-1):
    value = str(value)
    if not re.fullmatch(r"[1-9][0-9]{0,18}", value) or int(value) > maximum:
        raise ValueError("expected bounded positive numeric ID")
    return int(value)


def allowed_ids(raw):
    if not raw.strip():
        return frozenset()
    return frozenset(positive(v.strip()) for v in raw.split(","))


def actor(message, allowed):
    """Never trust effective_user, forwarded authors, sender_chat or usernames."""
    user = getattr(message, "from_user", None)
    uid = getattr(user, "id", None)
    if (getattr(message, "sender_chat", None) is not None or user is None
            or getattr(user, "is_bot", True) or type(uid) is not int or uid not in allowed):
        raise PermissionError("未獲授權")
    return uid


@dataclass(frozen=True)
class Config:
    allowed: frozenset
    targets: tuple = ()
    root: Path = Path("/data/media-archive")
    db: Path = Path("/data/control.sqlite3")
    max_items: int = 1000
    max_bytes: int = 20 * 1024**3
    reserve_bytes: int = 1024**3
    confirm_items: int = 100
    concurrency: int = 1
    retries: int = 3
    scan_limit: int = 10000
    watch_interval: int = 300
    watch_settle_seconds: int = 30
    archive_timezone: str = "UTC"
    expected_archive_mount: object = None
    expected_archive_fstypes: tuple = ("nfs", "nfs4")

    @classmethod
    def from_env(cls):
        targets = json.loads(os.getenv("CONTROL_TARGETS_JSON", "[]"))
        if not isinstance(targets, list):
            raise ValueError("CONTROL_TARGETS_JSON must be a list")
        parsed = []
        for t in targets:
            if not isinstance(t, dict) or set(t) - {"chat_id", "thread_id"}:
                raise ValueError("invalid target")
            cid = t.get("chat_id")
            tid = t.get("thread_id")
            if type(cid) is not int or cid == 0 or abs(cid) > 2**63-1:
                raise ValueError("target chat_id must be numeric")
            if tid is not None:
                if type(tid) is not int:
                    raise ValueError("thread_id must be numeric")
                positive(tid, 2**31-1)
            parsed.append((cid, tid))
        kw = {}
        for name in ("max_items", "max_bytes", "reserve_bytes", "confirm_items", "concurrency", "retries", "scan_limit", "watch_interval", "watch_settle_seconds"):
            raw = os.getenv("CONTROL_" + name.upper())
            if raw is not None:
                kw[name] = positive(raw)
        if kw.get("concurrency", 1) > 2:
            raise ValueError("CONTROL_CONCURRENCY must be 1 or 2")
        expected = os.getenv("CONTROL_EXPECTED_ARCHIVE_MOUNT")
        fstypes = tuple(v.strip() for v in os.getenv("CONTROL_EXPECTED_ARCHIVE_FSTYPES", "nfs,nfs4").split(","))
        if not fstypes or any(not re.fullmatch(r"[a-z0-9_.-]+",v) for v in fstypes):
            raise ValueError("invalid expected filesystem types")
        if expected and not Path(expected).is_absolute():
            raise ValueError("expected archive mount must be absolute")
        from zoneinfo import ZoneInfo
        kw["archive_timezone"] = os.getenv("CONTROL_ARCHIVE_TIMEZONE","UTC")
        ZoneInfo(kw["archive_timezone"])
        kw.update(expected_archive_mount=Path(expected) if expected else None, expected_archive_fstypes=fstypes)
        return cls(allowed_ids(os.getenv("CONTROL_ALLOWED_USER_IDS", "")), tuple(parsed),
                   Path(os.getenv("MEDIA_ARCHIVE_DIR", os.getenv("MEDIA_ARCHIVE_ROOT", "/data/media-archive"))),
                   Path(os.getenv("CONTROL_DB", str(Path(os.getenv("CONTROL_STATE_DIR", "/data/control")) / "control.sqlite3"))), **kw)
