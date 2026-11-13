"""Bounded, one-post discussion snapshots; no new Telegram session or monitor."""
import asyncio
from collections import OrderedDict


class DiscussionSnapshots:
    def __init__(self, *, client, spawn, resolve_info, publish_media, publish_album,
                 should_ignore, should_skip, log, resolver_factory=None, capacity=8, limit=10000):
        self.client, self.spawn = client, spawn
        self.resolve_info, self.publish_media, self.publish_album = resolve_info, publish_media, publish_album
        self.should_ignore, self.should_skip, self.log = should_ignore, should_skip, log
        self.resolver_factory, self.capacity, self.limit = resolver_factory, capacity, limit
        self.active, self.seen = set(), OrderedDict()
        self.semaphore = asyncio.Semaphore(1)

    def schedule(self, messages, routes, source_kind):
        selected = [r for r in routes if r.get("include_discussion_media", False)]
        if not selected or source_kind != "new" or not messages:
            return False
        seed = min(messages, key=lambda m: m.id)
        key = (seed.chat_id, getattr(seed, "grouped_id", None) or seed.id)
        if key in self.seen or key in self.active:
            return False
        if len(self.active) >= self.capacity:
            self.log({"type": "err", "op": "discussion_snapshot_capacity", "source": key})
            return False
        self.active.add(key)
        self.spawn(self._run(seed, selected, key))
        return True

    async def _run(self, seed, routes, key):
        try:
            async with self.semaphore:
                await asyncio.wait_for(self._snapshot(seed, routes), timeout=300)
            self.seen[key] = True
            if len(self.seen) > 10000:
                self.seen.popitem(last=False)
        except Exception as exc:
            self.log({"type": "err", "op": "discussion_snapshot_failed", "source": key,
                      "routes": [r["name"] for r in routes], "error": type(exc).__name__, "message": str(exc)})
        finally:
            self.active.discard(key)

    async def _snapshot(self, seed, routes):
        # Imports are deliberately lazy: disabled routes do not load/control the workbench.
        from ..control.input import SourceRef
        from ..control.source import Resolver
        from ..shared_media import configured_bridge
        factory = self.resolver_factory or Resolver
        resolver = factory(self.client, limit=self.limit, gate=await configured_bridge())
        snap = await resolver.resolve(SourceRef(seed.chat_id, seed.id))
        if snap.get("partial"):
            raise RuntimeError("discussion snapshot partial: " + snap.get("warning", ""))
        groups = OrderedDict()
        # Resolver already excludes the source/root album. Only photos/videos here.
        for item in sorted(snap["discussion"], key=lambda i: i["id"]):
            if item.get("kind") not in ("photo", "video"):
                continue
            group = (item["chat"], item.get("album") or "single:" + str(item["id"]))
            groups.setdefault(group, []).append(item)
        for values in groups.values():
            messages = await resolver.call(self.client.get_messages, values[0]["chat"], ids=[i["id"] for i in values])
            messages = sorted([m for m in messages if m], key=lambda m: m.id)
            if len(messages) != len(values):
                raise RuntimeError("discussion media disappeared during snapshot")
            usable = []
            for msg in messages:
                info = await self.resolve_info(msg, chat_id_hint=msg.chat_id)
                if self.should_ignore(info) or self.should_skip(msg):
                    continue
                usable.append((msg, info))
            if not usable:
                continue
            info = dict(usable[0][1])
            info["discussion_source"] = {"chat_id": seed.chat_id, "post_id": snap["post"], "thread": snap["thread"]}
            if values[0].get("album"):
                await self.publish_album([m for m, _ in usable], info, routes, "discussion")
            else:
                await self.publish_media(usable[0][0], info, routes, "discussion")
        self.log({"type": "info", "op": "discussion_snapshot_complete", "chat_id": seed.chat_id,
                  "post_id": snap["post"], "thread": snap["thread"], "media_count": sum(len(v) for v in groups.values())})
