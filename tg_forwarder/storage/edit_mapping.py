import json
import os
import uuid
from typing import Any, Dict, Optional

from redis.asyncio import Redis

REDIS_HOSTNAME = os.getenv("REDIS_HOSTNAME", "")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
EDIT_MAPPING_TTL_SECONDS = int(os.getenv("EDIT_MAPPING_TTL_SECONDS", "86400"))
# Topic mappings are long-lived (forum topics rarely change); default 90 days.
TOPIC_MAPPING_TTL_SECONDS = int(os.getenv("TOPIC_MAPPING_TTL_SECONDS", str(90 * 24 * 3600)))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))
REDIS_PREFIX = os.getenv("REDIS_PREFIX", "tgfwd")

_redis: Optional[Redis] = None

# Lua script for atomic read-modify-write of the target mapping dict.
# Runs atomically inside Redis, so concurrent writers for different targets
# cannot overwrite each other's entries (P3 fix).
_ATOMIC_UPDATE_MAPPING = """
local existing = redis.call('GET', KEYS[1])
local data
if existing then
    local ok, parsed = pcall(cjson.decode, existing)
    if ok and type(parsed) == 'table' then data = parsed else data = {targets = {}} end
else
    data = {targets = {}}
end
if type(data.targets) ~= 'table' then data.targets = {} end
data.targets[ARGV[1]] = cjson.decode(ARGV[2])
redis.call('SET', KEYS[1], cjson.encode(data), 'EX', tonumber(ARGV[3]))
return 1
"""

# Lua script for safe distributed lock release: only deletes if we still own it.
_RELEASE_LOCK_SCRIPT = (
    "if redis.call('get', KEYS[1]) == ARGV[1] then "
    "return redis.call('del', KEYS[1]) "
    "else return 0 end"
)


def enabled() -> bool:
    return bool(REDIS_HOSTNAME)


async def get_redis() -> Optional[Redis]:
    global _redis
    if not enabled():
        return None
    if _redis is None:
        _redis = Redis(host=REDIS_HOSTNAME, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)
    return _redis


def _msg_key(src_chat_id: int, src_msg_id: int) -> str:
    return f"{REDIS_PREFIX}:map:msg:{src_chat_id}:{src_msg_id}"


def _album_key(src_chat_id: int, grouped_id: int) -> str:
    return f"{REDIS_PREFIX}:map:album:{src_chat_id}:{grouped_id}"


def _topic_key(target_chat_id: int, sender_id: int) -> str:
    return f"{REDIS_PREFIX}:map:topic:{target_chat_id}:{sender_id}"


def _topic_lock_key(target_chat_id: int, sender_id: int) -> str:
    return f"{REDIS_PREFIX}:topic_lock:{target_chat_id}:{sender_id}"


async def store_message_mapping(src_chat_id: int, src_msg_id: int, target: Any, payload: Dict[str, Any]) -> None:
    redis = await get_redis()
    if redis is None:
        return
    key = _msg_key(src_chat_id, src_msg_id)
    # Lua script runs atomically: concurrent writers for different targets cannot
    # overwrite each other (P3 fix for store_message_mapping race).
    await redis.eval(
        _ATOMIC_UPDATE_MAPPING,
        1,
        key,
        str(target),
        json.dumps(payload, ensure_ascii=False),
        str(EDIT_MAPPING_TTL_SECONDS),
    )


async def store_album_mapping(src_chat_id: int, grouped_id: int, target: Any, payload: Dict[str, Any]) -> None:
    redis = await get_redis()
    if redis is None:
        return
    key = _album_key(src_chat_id, grouped_id)
    await redis.eval(
        _ATOMIC_UPDATE_MAPPING,
        1,
        key,
        str(target),
        json.dumps(payload, ensure_ascii=False),
        str(EDIT_MAPPING_TTL_SECONDS),
    )


async def get_message_mapping(src_chat_id: int, src_msg_id: int) -> Dict[str, Any]:
    redis = await get_redis()
    if redis is None:
        return {}
    raw = await redis.get(_msg_key(src_chat_id, src_msg_id))
    return json.loads(raw) if raw else {}


async def get_album_mapping(src_chat_id: int, grouped_id: int) -> Dict[str, Any]:
    redis = await get_redis()
    if redis is None:
        return {}
    raw = await redis.get(_album_key(src_chat_id, grouped_id))
    return json.loads(raw) if raw else {}


async def get_topic_mapping(target_chat_id: int, sender_id: int) -> Dict[str, Any]:
    redis = await get_redis()
    if redis is None:
        return {}
    raw = await redis.get(_topic_key(target_chat_id, sender_id))
    return json.loads(raw) if raw else {}


async def store_topic_mapping(target_chat_id: int, sender_id: int, payload: Dict[str, Any]) -> None:
    redis = await get_redis()
    if redis is None:
        return
    # P4: topic mappings now have a TTL so stale entries eventually expire.
    await redis.set(
        _topic_key(target_chat_id, sender_id),
        json.dumps(payload, ensure_ascii=False),
        ex=TOPIC_MAPPING_TTL_SECONDS,
    )


async def acquire_topic_lock(target_chat_id: int, sender_id: int, *, ttl: int = 30) -> Optional[str]:
    """Try to acquire a distributed Redis lock for topic creation (P8).

    Returns a unique lock token on success, None if the lock is already held
    by another worker or Redis is unavailable.  The caller must release the
    lock via release_topic_lock() when done.
    """
    redis = await get_redis()
    if redis is None:
        return None
    token = uuid.uuid4().hex
    key = _topic_lock_key(target_chat_id, sender_id)
    acquired = await redis.set(key, token, nx=True, ex=ttl)
    return token if acquired else None


async def release_topic_lock(target_chat_id: int, sender_id: int, token: str) -> None:
    """Release the distributed topic lock only if this process still owns it (P8)."""
    redis = await get_redis()
    if redis is None:
        return
    key = _topic_lock_key(target_chat_id, sender_id)
    await redis.eval(_RELEASE_LOCK_SCRIPT, 1, key, token)


__all__ = [
    "EDIT_MAPPING_TTL_SECONDS",
    "TOPIC_MAPPING_TTL_SECONDS",
    "REDIS_DB",
    "REDIS_HOSTNAME",
    "REDIS_PORT",
    "REDIS_PREFIX",
    "acquire_topic_lock",
    "enabled",
    "get_album_mapping",
    "get_message_mapping",
    "get_redis",
    "get_topic_mapping",
    "release_topic_lock",
    "store_album_mapping",
    "store_message_mapping",
    "store_topic_mapping",
]
