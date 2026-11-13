"""Bounded post/thread snapshots, never an entire discussion-group history scan."""
from datetime import datetime, timezone
from telethon import functions, utils, types
from telethon.errors import FloodWaitError


class Unavailable(RuntimeError):
    pass


def clean(text):
    return "".join(c for c in (text or "") if c >= " " or c in "\n	")[:4096]


def item(msg, section):
    kind = "photo" if getattr(msg, "photo", None) else "video" if getattr(msg, "video", None) else None
    date = getattr(msg, "date", None) or datetime.now(timezone.utc)
    file = getattr(msg, "file", None)
    return {"chat": msg.chat_id, "id": msg.id, "album": str(getattr(msg, "grouped_id", None) or ""),
            "media_id": str(getattr(getattr(msg, "photo", None) or getattr(msg, "document", None), "id", "")),
            "kind": kind, "size": getattr(file, "size", None), "caption": clean(getattr(msg, "message", "")),
            "date": date.date().isoformat(), "section": section,
            "archive_meta": {"datetime":getattr(msg,"date",None).isoformat() if getattr(msg,"date",None) else None, "title":str(msg.chat_id),
                "photo_id":getattr(getattr(msg,"photo",None),"id",None),
                "extension":getattr(file,"ext",None) or (".jpg" if kind=="photo" else ".mp4"),
                "filename":next((a.file_name for a in getattr(getattr(msg,"document",None),"attributes",[]) if hasattr(a,"file_name")),None)},
            "protected": bool(getattr(msg, "noforwards", False))}


def identity(i):
    return (i["chat"], i["id"])


class Resolver:
    def __init__(self, client, limit=10000, gate=None):
        self.client, self.limit, self.gate = client, limit, gate

    async def call(self, fn, *args, **kwargs):
        if self.gate:
            await self.gate.wait()
        try:
            return await fn(*args, **kwargs)
        except FloodWaitError as exc:
            if self.gate:
                self.gate.delay(exc.seconds)
            raise

    async def album(self, peer, seed):
        gid = getattr(seed, "grouped_id", None)
        if not gid:
            return [seed]
        found = {seed.id: seed}
        # Walk both directions to actual grouped-id boundary, not +/-10 IDs.
        # Telegram albums are ordered contiguous messages (IDs can have holes).
        for reverse in (False, True):
            kwargs = {"min_id": seed.id, "reverse": True} if reverse else {"offset_id": seed.id}
            if self.gate:
                await self.gate.wait()
            iterator = self.client.iter_messages(peer, limit=201, **kwargs)
            count = 0
            async for m in iterator:
                count += 1
                if getattr(m, "grouped_id", None) != gid:
                    break
                found[m.id] = m
            else:
                if count >= 201:
                    raise Unavailable("相簿邊界未能確認（partial）")
        return [found[k] for k in sorted(found)]

    async def replies(self, peer, thread, cursor=0):
        """Thread-only pagination. Probe beyond exact cap; incremental count may be historical."""
        messages, offset, ceiling = {}, 0, 0
        expected = None
        while True:
            limit = min(100, self.limit + 1 - len(messages))
            page = await self.call(self.client, functions.messages.GetRepliesRequest(
                peer=peer, msg_id=thread, offset_id=offset, offset_date=None,
                add_offset=0, limit=max(1, limit), max_id=ceiling, min_id=cursor, hash=0))
            expected = max(expected or 0, getattr(page, "count", 0))
            batch = [m for m in page.messages if getattr(m, "id", None)]
            if not batch:
                return messages, bool(not cursor and expected and len(messages) < expected)
            eligible = [m for m in batch if m.id > cursor]
            if ceiling == 0:
                ceiling = max(m.id for m in batch) + 1
            for m in eligible:
                messages[m.id] = m
            if not cursor and len(messages) >= expected and len(messages) <= self.limit:
                return messages, False
            if len(messages) > self.limit:
                return dict(sorted(messages.items(), reverse=True)[:self.limit]), True
            # A cursor boundary is conclusive even if an API/fake ignores min_id.
            if cursor and any(m.id <= cursor for m in batch):
                return messages, False
            next_offset = min(m.id for m in batch)
            if offset and next_offset >= offset:
                raise Unavailable("討論串分頁未前進（partial）")
            offset = next_offset

    async def new_comments(self, subscription, cursor):
        """Use persisted canonical thread, not a deleted selected comment or full history."""
        import copy
        snap = copy.deepcopy(subscription["snapshot"])
        thread = subscription["thread"]
        peer = types.InputPeerChannel(-thread["chat"]-1000000000000, thread["access_hash"]) if thread.get("access_hash") is not None else thread["chat"]
        entity = await self.call(self.client.get_entity, peer)
        messages, partial = await self.replies(entity, thread["id"], cursor)
        roots = set(thread.get("root_ids", [thread["id"]]))
        source_ids = {i["id"] for i in snap["main"]}
        replies = [m for mid,m in sorted(messages.items()) if mid not in roots and not (
            getattr(getattr(m,"fwd_from",None),"channel_post",None) in source_ids and
            getattr(getattr(getattr(m,"fwd_from",None),"from_id",None),"channel_id",None) == -snap["peer"]-1000000000000)]
        snap["discussion"] = [item(m, "discussion") for m in replies]
        for value in snap["discussion"]:
            value["protected"] |= bool(getattr(entity, "noforwards", False))
            value["access_hash"] = getattr(entity, "access_hash", None)
            value["archive_meta"]["title"] = getattr(entity,"title",None) or str(value["chat"])
        snap["partial"], snap["warning"] = partial, "新留言超過增量掃描上限" if partial else ""
        snap["cursor"] = max(messages, default=cursor)
        snap["captured"] = datetime.now(timezone.utc).isoformat()
        return snap

    async def resolve(self, ref):
        entity = await self.call(self.client.get_entity, ref.peer)
        peer = utils.get_peer_id(entity)
        seed = await self.call(self.client.get_messages, entity, ids=ref.post)
        if not seed or not getattr(seed, "id", None) or getattr(seed, "action", None):
            raise Unavailable("原帖已刪除或無法存取")
        main_messages = await self.album(entity, seed)
        main = [item(m, "main") for m in main_messages]
        protected = bool(getattr(entity, "noforwards", False))
        for m in main:
            m["protected"] |= protected
            m["access_hash"] = getattr(entity, "access_hash", None)
            m["archive_meta"]["title"] = getattr(entity,"title",None) or str(m["chat"])
        snap = {"ref": ref.as_dict(), "peer": peer, "post": min(m.id for m in main_messages),
                "main": main, "discussion": [], "selected": [m for m in main if m["id"] == seed.id],
                "selected_album": main, "partial": False, "warning": "", "thread": None,
                "captured": datetime.now(timezone.utc).isoformat(), "cursor": 0}
        try:
            discussion = await self.call(self.client, functions.messages.GetDiscussionMessageRequest(entity, seed.id))
            # Find the linked-group forwarded root, NOT a channel copy.
            roots = [m for m in discussion.messages if m.chat_id != peer and getattr(m, "fwd_from", None)]
            if not roots:
                raise Unavailable("未能確認討論串對應原帖")
            root = roots[0]
            root_peer = await self.call(self.client.get_entity, root.peer_id)
            root_album = await self.album(root_peer, root)
            root_ids = {m.id for m in root_album}
            thread = min(root_ids)  # GetDiscussionMessage may return the LAST album member.
            snap["thread"] = {"chat": root.chat_id, "id": thread, "access_hash": getattr(root_peer, "access_hash", None), "root_ids": sorted(root_ids)}
            messages, partial = await self.replies(root_peer, thread)
            if partial:
                snap["partial"], snap["warning"] = True, "討論串分頁不完整／超過掃描上限"
            # GetReplies already scopes nested replies to this root; no group scan.
            replies = [m for mid, m in sorted(messages.items()) if mid not in root_ids]
            # Defensively dedupe auto-forwarded copies referencing source album.
            main_ids = {m.id for m in main_messages}
            replies = [m for m in replies if not (
                getattr(getattr(m, "fwd_from", None), "channel_post", None) in main_ids
                and getattr(getattr(getattr(m, "fwd_from", None), "from_id", None), "channel_id", None)
                == getattr(entity, "id", None))]
            snap["cursor"] = max(messages, default=thread)
            group_protected = bool(getattr(root_peer, "noforwards", False))
            snap["discussion"] = [item(m, "discussion") for m in replies]
            for m in snap["discussion"]:
                m["protected"] |= group_protected
                m["access_hash"] = getattr(root_peer, "access_hash", None)
                m["archive_meta"]["title"] = getattr(root_peer,"title",None) or str(m["chat"])
            if ref.comment:
                selected = messages.get(ref.comment)
                if selected is None or selected.id in root_ids:
                    raise Unavailable("指定留言不存在／無法存取")
                # Album expansion stays inside this snapshotted thread.
                selected_item = next(m for m in snap["discussion"] if m["id"] == selected.id)
                snap["selected"] = [selected_item]
                gid = getattr(selected, "grouped_id", None)
                snap["selected_album"] = [m for m in snap["discussion"] if gid and m["album"] == str(gid)] or [selected_item]
        except FloodWaitError:
            raise
        except Exception as exc:
            if ref.comment:
                raise Unavailable("留言來源無法完整解析：" + type(exc).__name__) from exc
            # A genuinely disabled discussion is not an unavailable count of zero.
            snap["partial"], snap["warning"] = True, "討論串不可用／未完整：" + type(exc).__name__
        return snap


def scopes(snap, content="all"):
    result, seen = {}, set()
    for name, values in (("single", snap["selected"]), ("album", snap["selected_album"]),
                         ("discussion", snap["discussion"]), ("all", snap["main"] + snap["discussion"])):
        values = [v for v in values if v["kind"] and (content == "all" or v["kind"] == content)]
        key = tuple(sorted(identity(v) for v in values))
        if key and key not in seen:
            result[name], _ = values, seen.add(key)
    return result
