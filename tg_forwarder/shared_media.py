"""Opt-in shared source archive, conservative delivery ledger and Redis rate gate.
No network/client ownership: uses the legacy Redis provider and namespace.
"""
import asyncio
import hashlib
import json
import os
import shutil
import stat
import tempfile
import time
import uuid
import weakref
from pathlib import Path
from types import SimpleNamespace


_IO_GATES = weakref.WeakKeyDictionary()


async def _join(task):
    # Repeated cancellation must not detach an owned filesystem thread.
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            continue
    return task.result()


async def _io(fn, *args):
    loop = asyncio.get_running_loop()
    gate = _IO_GATES.setdefault(loop, asyncio.Semaphore(2))
    async with gate:
        task = asyncio.create_task(asyncio.to_thread(fn, *args))
        try:
            return await asyncio.shield(task)
        except asyncio.CancelledError:
            try:
                await _join(task)
            finally:
                raise


async def _owned(coro, cleanup=None):
    task = asyncio.create_task(coro)
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError:
        try:
            await _join(task)
        finally:
            if cleanup:
                # Caller cannot race its cleanup with a still-running copy.
                await _join(asyncio.create_task(_io(cleanup)))
        raise


class SharedUnavailable(RuntimeError):
    pass


class AmbiguousDelivery(SharedUnavailable):
    pass


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def media_identity(chat_id, message_id, media_revision):
    if chat_id is None or message_id is None or not str(media_revision):
        raise ValueError("complete media identity required")
    return [str(chat_id), int(message_id), str(media_revision)]


def message_identity(msg, chat_id=None):
    # Telegram's immutable document/photo ID changes on media replacement, not caption edits.
    media = getattr(msg, "photo", None) or getattr(msg, "document", None)
    revision = getattr(media, "id", None)
    if revision is None:
        raise ValueError("media has no stable document/photo revision")
    return media_identity(chat_id if chat_id is not None else msg.chat_id, msg.id, revision)


class SharedMedia:
    def __init__(self, redis, root, *, prefix="tgfwd", account_rate_key, max_bytes=2147483648,
                 min_free_bytes=1073741824, index_ttl=2592000, rate_interval=.1):
        if redis is None or not account_rate_key:
            raise SharedUnavailable("shared Redis and MEDIA_ACCOUNT_RATE_KEY required")
        self.redis, self.prefix = redis, prefix + ":media_shared:"
        if not Path(root).is_absolute():
            raise ValueError("MEDIA_SHARED_ROOT must be absolute")
        self.root = Path(root)
        self.max_bytes, self.min_free_bytes = int(max_bytes), int(min_free_bytes)
        self.index_ttl, self.rate_interval = int(index_ttl), float(rate_interval)
        if self.max_bytes <= 0 or self.min_free_bytes < 0 or self.index_ttl <= 0 or self.rate_interval <= 0:
            raise ValueError("shared size/retention/rate bounds invalid")
        self.rate_key = self.prefix + "rate:" + digest(account_rate_key)
        self._pending_delays = set()

    def _root(self):
        # Root is an operator-owned mount, never accept a symlink anywhere in its path.
        for p in [self.root, *self.root.parents]:
            if p.is_symlink():
                raise SharedUnavailable("shared root contains symlink")
        self.root.mkdir(parents=True, exist_ok=True)
        return self.root

    def _object(self, relative):
        if not isinstance(relative, str) or len(relative) != 64 or any(c not in "0123456789abcdef" for c in relative):
            raise SharedUnavailable("invalid archive relative path")
        return self._root() / relative

    def _validate(self, path, expected=None):
        s = path.lstat()
        if not stat.S_ISREG(s.st_mode) or s.st_size <= 0 or s.st_size > self.max_bytes:
            raise SharedUnavailable("unsafe/oversized archive file")
        if expected is not None and s.st_size != expected:
            raise SharedUnavailable("archive size mismatch")
        return s.st_size

    async def cached(self, identity):
        raw = await self.redis.get(self.prefix + "source:" + digest(identity))
        if not raw:
            return None
        entry = json.loads(raw)
        def lookup():
            path = self._object(entry["path"])
            try:
                self._validate(path, entry["size"])
            except FileNotFoundError:
                return None
            return path
        return await _io(lookup)

    async def publish(self, identity, source):
        async def commit():
            target, size = await _io(self._publish_file, identity, source)
            await self.redis.set(self.prefix + "source:" + digest(identity),
                                 json.dumps({"path": target.name, "size": size}), ex=self.index_ttl)
            return target
        # Publication owns thread AND index write through cancellation.
        return await _owned(commit())

    def _publish_file(self, identity, source):
        source = Path(source)
        size = self._validate(source)
        root = self._root()
        if shutil.disk_usage(root).free < size + self.min_free_bytes:
            raise SharedUnavailable("shared archive free-space guard")
        # Copy into an exclusive file, fsync, then atomic no-clobber link. Never link
        # the mutable downloader spool into the persistent archive.
        fd, temp = tempfile.mkstemp(prefix=".incoming-", dir=root)
        try:
            with os.fdopen(fd, "wb") as dst:
                flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
                with os.fdopen(os.open(source, flags), "rb") as src:
                    copied = 0
                    while True:
                        chunk = src.read(1024 * 1024)
                        if not chunk:
                            break
                        copied += len(chunk)
                        if copied > size or copied > self.max_bytes:
                            raise SharedUnavailable("source grew during archive publication")
                        dst.write(chunk)
                dst.flush()
                os.fsync(dst.fileno())
            self._validate(Path(temp), size)
            relative = digest(identity)
            target = self._object(relative)
            try:
                os.link(temp, target)
                os.chmod(target, 0o444)
            except FileExistsError:
                self._validate(target, size)
            directory = os.open(root, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
            return target, size
        finally:
            # Only our mkstemp absolute regular file; never sweep archives.
            p = Path(temp)
            if p.parent == root and p.name.startswith(".incoming-") and p.is_file() and not p.is_symlink():
                p.unlink()

    async def materialize(self, identity, destination):
        source = await self.cached(identity)
        if source is None:
            return None
        destination = Path(destination)
        created = False
        def copy():
            nonlocal created
            with os.fdopen(os.open(source, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)), "rb") as src:
                with destination.open("xb") as dst:
                    created = True
                    shutil.copyfileobj(src, dst, 1024 * 1024)
            return destination
        def cleanup():
            if created and destination.is_file() and not destination.is_symlink():
                destination.unlink()
        async def materialize():
            try:
                return await _io(copy)
            except BaseException:
                await _io(cleanup)
                raise
        return await _owned(materialize(), cleanup)

    def operation(self, identities, destination, topic, delivery_identity, *, action="upload", caption=""):
        if not delivery_identity:
            raise ValueError("explicit nonsecret delivery identity required")
        return digest({"source": sorted(identities), "destination": str(destination), "topic": topic,
                       "delivery": delivery_identity, "action": action, "caption": caption})

    async def receipt(self, operation):
        raw = await self.redis.get(self.prefix + "done:" + operation)
        return json.loads(raw) if raw else None

    async def deliver(self, operation, send):
        done = await self.receipt(operation)
        if done is not None:
            return SimpleNamespace(**done) if done else True
        # No TTL: a crashed process or uncertain API result must NEVER auto-resend.
        # Completed receipts also have no TTL; operator reconciliation is explicit.
        token = uuid.uuid4().hex
        if not await self.redis.set(self.prefix + "claim:" + operation, token, nx=True):
            done = await self.receipt(operation)
            if done is not None:
                return SimpleNamespace(**done) if done else True
            raise AmbiguousDelivery("shared delivery claimed/in-flight/ambiguous; reconcile before retry")
        await self.wait()
        try:
            result = await send()
        except BaseException as exc:
            if definite_rejection(exc):
                # Compare token: never release another process's claim.
                await self.redis.eval("if redis.call('GET',KEYS[1]) == ARGV[1] then return redis.call('DEL',KEYS[1]) else return 0 end", 1,
                                      self.prefix + "claim:" + operation, token)
            raise
        if not result:
            raise AmbiguousDelivery("send returned failure; shared claim retained for reconciliation")
        # A size-limit notice or best-effort album fallback is not proof all media
        # arrived. Keep the conservative claim, but preserve legacy retain/mapping behavior.
        if getattr(result, "preserve_local_copy", False) or getattr(result, "delivery_kind", "") in {"file_skip_notice", "album_single_fallback"}:
            return result
        if getattr(result, "message_thread_id", None) and not (getattr(result, "topic_message_id", None) or getattr(result, "topic_message_ids", None)):
            return result  # legacy degraded to general; desired topic not proven delivered
        fields = ("sent_message_id", "sent_message_ids", "general_message_id", "topic_message_id",
                  "general_message_ids", "topic_message_ids", "delivery_kind", "message_thread_id", "preserve_local_copy")
        payload = {k: getattr(result, k) for k in fields if hasattr(result, k)}
        await self.redis.set(self.prefix + "done:" + operation, json.dumps(payload))
        return result

    async def record_delay(self, seconds):
        # Existing deployment already allows EVAL (edit mapping/journal). Max is atomic.
        await self.redis.eval("local old=tonumber(redis.call('GET',KEYS[1]) or '0'); local n=tonumber(ARGV[1]); if n>old then redis.call('SET',KEYS[1],ARGV[1],'PX',ARGV[2]); end; return 1", 1,
                              self.rate_key + ":delay", str(time.time() + float(seconds) + 1), str(int((float(seconds)+2)*1000)))

    def delay(self, seconds):
        # Resolver's synchronous delay callback. wait() joins the pending write;
        # failures remain visible to the next caller instead of bypassing the gate.
        task = asyncio.create_task(self.record_delay(seconds))
        self._pending_delays.add(task)

    async def wait(self):
        if self._pending_delays:
            pending = list(self._pending_delays)
            await asyncio.gather(*pending)
            self._pending_delays.difference_update(pending)
        while True:
            until = float(await self.redis.get(self.rate_key + ":delay") or 0)
            if until > time.time():
                await asyncio.sleep(min(until - time.time(), 1))
                continue
            if await self.redis.set(self.rate_key + ":slot", "1", nx=True, px=max(1, int(self.rate_interval * 1000))):
                return
            await asyncio.sleep(max(.01, self.rate_interval))


_redis_clients = {}


async def configured_bridge():
    value = os.getenv("MEDIA_SHARED_ENABLED", "false").lower()
    if value not in ("true", "false"):
        raise ValueError("MEDIA_SHARED_ENABLED must be true or false")
    if value == "false":
        return None
    from redis.asyncio import Redis
    # Do not inherit edit_mapping import-time env snapshots: control loads its
    # protected env file after imports. Read credentials ONLY at this call.
    kwargs = {"host": os.getenv("REDIS_HOSTNAME", ""), "port": int(os.getenv("REDIS_PORT", "6379")),
              "db": int(os.getenv("REDIS_DB", "0")), "decode_responses": True}
    if not kwargs["host"]:
        raise SharedUnavailable("REDIS_HOSTNAME required")
    for env, name in (("REDIS_USERNAME", "username"), ("REDIS_PASSWORD", "password")):
        if os.getenv(env):
            kwargs[name] = os.environ[env]
    connection = digest(kwargs)
    if connection not in _redis_clients:
        _redis_clients[connection] = Redis(**kwargs)
    root = os.environ.get("MEDIA_SHARED_ROOT")
    if not root:
        raise SharedUnavailable("MEDIA_SHARED_ROOT required")
    return SharedMedia(_redis_clients[connection], root, prefix=os.getenv("REDIS_PREFIX", "tgfwd"),
                       account_rate_key=os.environ.get("MEDIA_ACCOUNT_RATE_KEY"),
                       max_bytes=int(os.getenv("MEDIA_SHARED_MAX_BYTES", "2147483648")),
                       min_free_bytes=int(os.getenv("MEDIA_SHARED_MIN_FREE_BYTES", "1073741824")),
                       index_ttl=int(os.getenv("MEDIA_SHARED_INDEX_TTL", "2592000")),
                       rate_interval=float(os.getenv("MEDIA_ACCOUNT_RATE_INTERVAL", ".1")))


async def legacy_send(send, snapshots, target, caption, info, source_kind):
    bridge = await configured_bridge()
    if bridge is None:
        return await send()
    identities = [s.get("shared_identity") for s in snapshots]
    if not identities or any(i is None for i in identities):
        raise SharedUnavailable("shared-enabled sender requires revision-bearing snapshots; re-snapshot old jobs")
    # Legacy bot sender can deliver BOTH general and sender-topic copies. This is
    # an explicit delivery semantic, not equivalent to control's single target.
    delivery = os.environ.get("MEDIA_DELIVERY_IDENTITY")
    topic = {"legacy_sender": info.get("sender_id"), "policy": os.getenv("FORUM_TOPIC_DELIVERY_MODE", "mirror"), "edit_revision": info.get("edit_date") if source_kind == "edited" else None}
    operation = bridge.operation(identities, target, topic, delivery,
                                 action="upload", caption=caption)
    return await bridge.deliver(operation, send)


def definite_rejection(exc):
    from telegram.error import RetryAfter, BadRequest, Forbidden
    from telethon.errors import FloodWaitError, BadRequestError, ForbiddenError
    return isinstance(exc, (RetryAfter, BadRequest, Forbidden, FloodWaitError, BadRequestError, ForbiddenError))


async def native_forward(messages, info, target, header, source_kind, *, client, run_api, extra, album=False):
    """Native MTProto forwarding, NOT cache download/re-upload. Header has own receipt."""
    bridge = await configured_bridge()
    msgs = sorted(messages, key=lambda m: m.id)
    header_op = "send_header_before_forward_album" if album else "send_header_before_forward_large"
    forward_op = "forward_large_album" if album else "forward_large_media"
    async def send_header():
        result = await run_api(lambda: client.send_message(target, header), op=header_op, extra=extra)
        if not result or not getattr(result, "id", None):
            raise AmbiguousDelivery("native header returned no message ID")
        return SimpleNamespace(sent_message_id=result.id)
    async def send_media():
        result = await run_api(lambda: client.forward_messages(target, msgs if album else msgs[0], msgs[0].peer_id), op=forward_op, extra=extra)
        values = result if isinstance(result, list) else [result]
        if len(values) != len(msgs) or any(not m or not getattr(m, "id", None) for m in values):
            raise AmbiguousDelivery("native forward returned incomplete response")
        return SimpleNamespace(sent_message_id=getattr(values[0], "id", None),
                               sent_message_ids=[getattr(m, "id", None) for m in values])
    if bridge is None:
        await run_api(lambda: client.send_message(target, header), op=header_op, extra=extra)
        return await run_api(lambda: client.forward_messages(target, msgs if album else msgs[0], msgs[0].peer_id), op=forward_op, extra=extra)
    identities = [message_identity(m, info["chat_id"]) for m in msgs]
    principal = os.getenv("MEDIA_NATIVE_DELIVERY_IDENTITY")
    if not principal:
        raise SharedUnavailable("MEDIA_NATIVE_DELIVERY_IDENTITY required for native MTProto forward")
    semantic = {"edit_revision": info.get("edit_date") if source_kind == "edited" else None}
    # Native forward has attribution/protected-content semantics distinct from upload.
    operation = bridge.operation(identities, target, semantic, principal, action="native-header", caption=header)
    await bridge.deliver(operation, send_header)
    operation = bridge.operation(identities, target, semantic, principal, action="native-forward", caption=header)
    return await bridge.deliver(operation, send_media)
