import asyncio
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, AsyncIterator, Iterable, List, Optional

from telethon.tl.functions.messages import GetForumTopicsByIDRequest, GetForumTopicsRequest


_SENDER_ID_RE = re.compile(r"\((\d+)\)\s*$")


@dataclass
class TopicMatch:
    sender_id: int
    topic_id: int
    title: str
    top_message: int
    date: Optional[datetime]
    topic: Any


def parse_sender_id_from_topic_title(title: Optional[str]) -> Optional[int]:
    if not title:
        return None
    match = _SENDER_ID_RE.search(title.strip())
    if not match:
        return None
    try:
        return int(match.group(1))
    except Exception:
        return None


def topic_sort_key(topic: Any) -> tuple:
    dt = getattr(topic, "date", None)
    if isinstance(dt, datetime):
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        ts = dt.timestamp()
    else:
        ts = 0.0
    return (ts, int(getattr(topic, "top_message", 0) or 0), int(getattr(topic, "id", 0) or 0))


def pick_latest_topic(topics: Iterable[Any]) -> Optional[Any]:
    topics_list = [t for t in topics if getattr(t, "id", None)]
    if not topics_list:
        return None
    return max(topics_list, key=topic_sort_key)


async def iter_forum_topics(client, chat_id: int, *, query: Optional[str] = None, limit: int = 100) -> AsyncIterator[Any]:
    peer = await client.get_input_entity(chat_id)
    offset_date = None
    offset_id = 0
    offset_topic = 0

    while True:
        result = await client(GetForumTopicsRequest(
            peer=peer,
            offset_date=offset_date,
            offset_id=offset_id,
            offset_topic=offset_topic,
            limit=limit,
            q=query,
        ))
        topics = list(getattr(result, "topics", []) or [])
        if not topics:
            return
        for topic in topics:
            if getattr(topic, "id", None):
                yield topic
        if len(topics) < limit:
            return
        last = topics[-1]
        offset_date = getattr(last, "date", None)
        offset_id = int(getattr(last, "top_message", 0) or 0)
        offset_topic = int(getattr(last, "id", 0) or 0)


async def get_forum_topics_by_ids(client, chat_id: int, topic_ids: List[int]) -> List[Any]:
    if not topic_ids:
        return []
    peer = await client.get_input_entity(chat_id)
    result = await client(GetForumTopicsByIDRequest(peer=peer, topics=[int(t) for t in topic_ids]))
    return [topic for topic in list(getattr(result, "topics", []) or []) if getattr(topic, "id", None)]


async def find_topics_for_sender(client, chat_id: int, sender_id: int, *, scan_limit: int = 100) -> List[Any]:
    matches: List[Any] = []
    seen = set()
    async for topic in iter_forum_topics(client, chat_id, limit=scan_limit):
        topic_id = int(getattr(topic, "id", 0) or 0)
        if not topic_id or topic_id in seen:
            continue
        parsed = parse_sender_id_from_topic_title(getattr(topic, "title", None))
        if parsed == int(sender_id):
            matches.append(topic)
            seen.add(topic_id)
    return matches


async def find_latest_topic_for_sender(client, chat_id: int, sender_id: int, *, scan_limit: int = 100) -> Optional[Any]:
    topics = await find_topics_for_sender(client, chat_id, sender_id, scan_limit=scan_limit)
    return pick_latest_topic(topics)


async def topic_exists(client, chat_id: int, topic_id: int) -> bool:
    try:
        topics = await get_forum_topics_by_ids(client, chat_id, [topic_id])
        return any(int(getattr(topic, "id", 0) or 0) == int(topic_id) for topic in topics)
    except Exception:
        return False
