import json
import os
from typing import Any, Dict, Optional

from redis.asyncio import Redis

REDIS_HOSTNAME = os.getenv("REDIS_HOSTNAME", "")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
EDIT_MAPPING_TTL_SECONDS = int(os.getenv("EDIT_MAPPING_TTL_SECONDS", "86400"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))
REDIS_PREFIX = os.getenv("REDIS_PREFIX", "tgfwd")

_redis: Optional[Redis] = None


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


async def store_message_mapping(src_chat_id: int, src_msg_id: int, target: Any, payload: Dict[str, Any]) -> None:
    redis = await get_redis()
    if redis is None:
        return
    key = _msg_key(src_chat_id, src_msg_id)
    existing_raw = await redis.get(key)
    existing = json.loads(existing_raw) if existing_raw else {"targets": {}}
    existing.setdefault("targets", {})[str(target)] = payload
    await redis.set(key, json.dumps(existing, ensure_ascii=False), ex=EDIT_MAPPING_TTL_SECONDS)


async def store_album_mapping(src_chat_id: int, grouped_id: int, target: Any, payload: Dict[str, Any]) -> None:
    redis = await get_redis()
    if redis is None:
        return
    key = _album_key(src_chat_id, grouped_id)
    existing_raw = await redis.get(key)
    existing = json.loads(existing_raw) if existing_raw else {"targets": {}}
    existing.setdefault("targets", {})[str(target)] = payload
    await redis.set(key, json.dumps(existing, ensure_ascii=False), ex=EDIT_MAPPING_TTL_SECONDS)


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
    await redis.set(_topic_key(target_chat_id, sender_id), json.dumps(payload, ensure_ascii=False))


__all__ = [
    "EDIT_MAPPING_TTL_SECONDS",
    "REDIS_DB",
    "REDIS_HOSTNAME",
    "REDIS_PORT",
    "REDIS_PREFIX",
    "enabled",
    "get_album_mapping",
    "get_message_mapping",
    "get_redis",
    "get_topic_mapping",
    "store_album_mapping",
    "store_message_mapping",
    "store_topic_mapping",
]
