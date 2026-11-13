import asyncio
from collections import OrderedDict
import json
import os
from pathlib import Path
import re
import shutil
import tempfile
import time
from telethon import types
from telethon.errors import FloodWaitError, RPCError
from telegram.error import BadRequest, Forbidden, RetryAfter
from .store import digest, encode
from .filesystems import guard_archive
from types import SimpleNamespace
from tg_forwarder.shared_media import media_identity, AmbiguousDelivery, SharedUnavailable, definite_rejection


class Cancelled(RuntimeError):
    pass


class Ambiguous(RuntimeError):
    pass


class RateGate:
    """Persisted local delay plus optional fail-closed Redis account admission gate."""
    def __init__(self, store, bridge=None):
        self.store, self.bridge = store, bridge
    async def wait(self):
        while True:
            delay = float(self.store.get_meta("rate_until", "0")) - time.time()
            if delay <= 0:
                if self.bridge is not None:
                    try:
                        await self.bridge.wait()
                    except Exception:
                        raise SharedUnavailable("Shared account gate unavailable") from None
                return
            await asyncio.sleep(min(delay, 5))
    def delay(self, seconds):
        until = max(float(self.store.get_meta("rate_until", "0")), time.time()+seconds)
        self.store.meta("rate_until", until)
        if self.bridge is not None:
            self.bridge.delay(seconds)
        return until


class Archive:
    def __init__(self, root, reserve_bytes, expected_mount=None, expected_fstypes=("nfs","nfs4")):
        self.expected_mount, self.expected_fstypes = expected_mount, expected_fstypes
        self.root = Path(root).absolute()
        self.guard()
        root = Path(root).absolute()
        # Reject symlink roots AND ancestors; never resolve away a bad boundary.
        for p in (root, *root.parents):
            if p.is_symlink():
                raise ValueError("archive root/ancestor cannot be a symlink")
        root.mkdir(parents=True, exist_ok=True)
        self.root, self.reserve = root, reserve_bytes

    def guard(self):
        guard_archive(self.root,self.expected_mount,self.expected_fstypes)

    def path(self, *parts):
        self.guard()
        current = self.root
        for part in parts:
            part = str(part)
            if not re.fullmatch(r"[A-Za-z0-9_.-]+", part) or part in (".", ".."):
                raise ValueError("unsafe archive path")
            current = current / part
            if current.is_symlink():
                raise ValueError("archive symlink rejected")
        if not current.absolute().is_relative_to(self.root):
            raise ValueError("archive escape")
        return current

    def directory(self, snapshot):
        date = snapshot["main"][0]["date"]
        if not re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", date):
            raise ValueError("invalid date")
        return self.path(str(int(snapshot["peer"])), date+"_"+str(int(snapshot["post"])))

    def file(self, snapshot, i):
        base = self.directory(snapshot)
        section = "main" if i["section"] == "main" else "discussion"
        album = str(int(i["album"])) if i["album"] else "single"
        return self.path(*base.relative_to(self.root).parts, section, album,
                         str(int(i["chat"]))+"_"+str(int(i["id"]))+("_"+str(int(i["media_id"])) if i.get("media_id") else "")+(".jpg" if i["kind"] == "photo" else ".mp4"))

    def space(self, required=0):
        self.guard()
        if shutil.disk_usage(self.root).free - required < self.reserve:
            raise OSError("磁碟可用空間不足（保留空間限制）")

    def manifest(self, snapshot, tid, data):
        base = self.directory(snapshot)
        base.mkdir(parents=True, exist_ok=True)
        path = self.path(*base.relative_to(self.root).parts, "manifest_"+tid+".json")
        fd, temp = tempfile.mkstemp(prefix=".manifest-", dir=base)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                def sanitized(value):
                    if isinstance(value, dict):
                        return {k:sanitized(v) for k,v in value.items() if k != "access_hash"}
                    if isinstance(value, list):
                        return [sanitized(v) for v in value]
                    return value
                f.write(encode(sanitized(data)))
                f.flush()
                os.fsync(f.fileno())
            if path.is_symlink():
                raise ValueError("manifest symlink rejected")
            os.replace(temp, path)
        finally:
            if os.path.isfile(temp) and not os.path.islink(temp):
                os.unlink(temp)


class Engine:
    def __init__(self, config, store, client, gate, notify=None, delivery=None, bridge=None, bot_id=None):
        self.config, self.store, self.client, self.gate = config, store, client, gate
        self.archive = Archive(config.root, config.reserve_bytes, config.expected_archive_mount, config.expected_archive_fstypes)
        self.notify = notify
        self.delivery = delivery
        self.bridge, self.bot_id = bridge, bot_id
        if bridge is not None and (not isinstance(bot_id, int) or bot_id <= 0):
            raise ValueError("Shared control delivery requires verified bot identity")
        self.last_notice = {}
        # Different concurrent tasks can reference the same disk identity.
        self.download_lock = asyncio.Lock()
        self.send_lock = asyncio.Lock()

    def check(self, task):
        row = self.store.task(task["id"], task["owner"], task["chat"])
        if row["state"] == "cancelled":
            raise Cancelled("已取消")
        if task["owner"] not in self.config.allowed:
            raise PermissionError("任務擁有人不再獲授權")

    async def notice(self, task, final=False):
        now = time.monotonic()
        if self.notify and (final or now-self.last_notice.get(task["id"], 0) >= 10):
            self.last_notice[task["id"]] = now
            try:
                await self.notify(self.store.task(task["id"], task["owner"], task["chat"]))
            except Exception:
                pass  # Notification failure must never cause media resend.

    def validate(self, payload):
        values = payload["items"]
        if not values or len(values) > self.config.max_items:
            raise ValueError("媒體數量超過上限／選取為空")
        known = sum(i.get("size") or 0 for i in values)
        if known > self.config.max_bytes:
            raise ValueError("媒體大小超過上限")
        if payload["action"] not in ("download", "forward", "both", "cached_photos"):
            raise ValueError("unknown action")
        if payload["action"] != "download":
            if tuple(payload.get("target") or ()) not in self.config.targets:
                raise PermissionError("目的地不在允許清單")
            if any(i["protected"] for i in values):
                raise PermissionError("來源禁止轉發（protected）；不會嘗試原生轉發或繞過")
        self.archive.space(0 if payload["action"] == "cached_photos" else known)

    @staticmethod
    def peer(i):
        if i.get("access_hash") is not None and i["chat"] < -1000000000000:
            return types.InputPeerChannel(-i["chat"]-1000000000000, i["access_hash"])
        return i["chat"]

    def cached_photo_payload(self, task):
        original = self.store.task(task["id"],task["owner"],task["chat"])
        source = json.loads(original["payload"])
        if original["state"] != "done" or source["action"] not in ("download", "both"):
            raise ValueError("只可傳送已完成下載任務的圖片")
        if original["chat"] != original["owner"] or (original["chat"],None) not in self.config.targets:
            raise PermissionError("只可傳送至已明確允許的原私人對話，不包含群組／話題權限")
        photos = [i for i in source["items"] if i["kind"] == "photo"]
        payload = {"action":"cached_photos", "origin_task":original["id"], "target":[original["chat"],None],
                   "snapshot":source["snapshot"], "items":photos}
        self.validate(payload)
        return payload

    def cached_file(self, selected, used=0):
        key = digest([selected["chat"],selected["id"],selected.get("media_id", "")])
        cached = self.store.db.execute("SELECT * FROM cache WHERE key=?",(key,)).fetchone()
        if cached is None:
            raise ValueError("已下載圖片快取記錄遺失；此動作不會重新下載")
        try:
            path = self.archive.path(*Path(cached["path"]).relative_to(self.archive.root).parts)
            size = path.stat().st_size
            if not path.is_file() or size <= 0 or size != cached["size"] or (selected.get("size") is not None and selected["size"] != size):
                raise ValueError()
        except (OSError,ValueError):
            raise ValueError("已下載圖片檔案遺失／無效；檔案保留，不會重新下載") from None
        if used + size > self.config.max_bytes:
            raise ValueError("媒體大小超過上限")
        return path,size

    def validate_cached_task(self, task, payload):
        original = self.store.task(payload.get("origin_task"),task["owner"],task["chat"])
        expected = self.cached_photo_payload(original)
        if encode(expected) != encode(payload):
            raise ValueError("已下載圖片任務與原始任務不符")
        used = 0
        for selected in payload["items"]:
            _,size = self.cached_file(selected,used)
            used += size

    async def download(self, task, payload, i, used):
        key = digest([i["chat"], i["id"], i.get("media_id", "")])
        async with self.download_lock:
            cached = self.store.db.execute("SELECT * FROM cache WHERE key=?", (key,)).fetchone()
            if cached:
                path = Path(cached["path"])
                try:
                    path = self.archive.path(*path.relative_to(self.archive.root).parts)
                except ValueError:
                    raise ValueError("unsafe cache path")
                if path.is_file() and cached["size"] > 0 and path.stat().st_size == cached["size"] and (not i.get("size") or i["size"] == cached["size"]):
                    if used + cached["size"] > self.config.max_bytes:
                        raise ValueError("媒體大小超過上限")
                    await self.publish(i, path)
                    return path, cached["size"]
            self.check(task)
            if self.bridge is not None:
                imported = await self.import_shared(task, payload, i, key, used)
                if imported is not None:
                    return imported
            await self.gate.wait()
            msg = await self.client.get_messages(self.peer(i), ids=i["id"])
            if not msg or not getattr(msg, "media", None):
                raise ValueError("媒體已刪除／不可用")
            # Reject changed identity/type rather than downloading a replacement outside snapshot.
            from .source import item
            current = item(msg, i["section"])
            if current["kind"] != i["kind"] or current["album"] != i["album"] or current["media_id"] != i.get("media_id", ""):
                raise ValueError("來源媒體已更改，請重新查詢")
            if payload["action"] != "download" and current["protected"]:
                raise PermissionError("來源現時禁止轉發")
            path = self.archive.file(payload["snapshot"], i)
            path.parent.mkdir(parents=True, exist_ok=True)
            # Recheck every component after mkdir; archive is operator-owned, not shared-writable.
            self.archive.path(*path.relative_to(self.archive.root).parts)
            if path.exists():
                # Orphan (crash before cache commit): do not overwrite unverifiable bytes.
                raise ValueError("未記錄既有檔案，需管理員核對，未覆寫")
            fd, temp = tempfile.mkstemp(prefix=".download-", dir=path.parent)
            expected_total = [None]
            async def progress(done, total):
                if total and total > 0:
                    expected_total[0] = total
                self.check(task)
                if used + done > self.config.max_bytes or (total and used+total > self.config.max_bytes):
                    raise ValueError("媒體大小超過上限")
                self.archive.space()
                await self.notice(task)
            try:
                with os.fdopen(fd, "wb") as f:
                    await self.gate.wait()
                    result = await msg.download_media(file=f, progress_callback=progress)
                    if result is None:
                        raise ValueError("Telegram 未返回有效下載結果，未記錄成功")
                    f.flush()
                    os.fsync(f.fileno())
                size = os.stat(temp).st_size
                if size <= 0 or (expected_total[0] is not None and size != expected_total[0]) or (i.get("size") is not None and size != i["size"]) or (current.get("size") is not None and size != current["size"]):
                    raise ValueError("下載為空／長度不符，可能截斷；未記錄成功")
                if used+size > self.config.max_bytes:
                    raise ValueError("媒體大小超過上限")
                self.check(task)
                self.archive.space()
                # link() is atomic no-clobber; then remove ONLY this task-owned tempfile.
                os.link(temp, path, follow_symlinks=False)
                self.store.db.execute("INSERT OR REPLACE INTO cache VALUES(?,?,?)", (key, str(path), size))
                await self.publish(i, path)
                return path, size
            finally:
                if os.path.isfile(temp) and not os.path.islink(temp):
                    os.unlink(temp)

    async def publish(self, selected, path):
        if self.bridge is not None:
            try:
                await self.bridge.publish(media_identity(selected["chat"], selected["id"], selected.get("media_id", "")), path)
            except Exception:
                # Local successful cache/bytes remain authoritative and untouched.
                raise ValueError("共享媒體發佈失敗（Redis／共享磁碟／每檔大小限制）；本機下載檔已保留，可重試") from None

    async def import_shared(self, task, payload, selected, key, used):
        identity = media_identity(selected["chat"], selected["id"], selected.get("media_id", ""))
        # Query is mandatory/fail-closed when shared mode is enabled.
        try:
            source = await self.bridge.cached(identity)
        except Exception:
            raise SharedUnavailable("Shared source cache unavailable") from None
        if source is None:
            return None
        size = source.stat().st_size
        if size <= 0 or (selected.get("size") is not None and size != selected["size"]):
            raise ValueError("共享媒體長度不符")
        if used + size > self.config.max_bytes:
            raise ValueError("媒體大小超過上限")
        self.archive.space(size)
        path = self.archive.file(payload["snapshot"], selected)
        path.parent.mkdir(parents=True, exist_ok=True)
        self.archive.path(*path.relative_to(self.archive.root).parts)
        with tempfile.TemporaryDirectory(prefix=".shared-", dir=path.parent) as staging:
            temp = Path(staging) / "media"
            try:
                imported = await self.bridge.materialize(identity, temp)
            except Exception:
                raise SharedUnavailable("Shared source materialization failed") from None
            if imported is None or temp.is_symlink() or not temp.is_file() or temp.stat().st_size != size:
                raise ValueError("共享媒體匯入不完整")
            self.check(task)
            self.archive.space()
            with temp.open("rb") as stream:
                os.fsync(stream.fileno())
            os.link(temp, path, follow_symlinks=False)
            self.store.db.execute("INSERT OR REPLACE INTO cache VALUES(?,?,?)", (key, str(path), size))
        return path, size

    def operation_key(self, payload, selected):
        base = ["forward", payload.get("target"), selected["chat"], selected["id"], selected.get("media_id", "")]
        if payload["action"] == "cached_photos":
            base = ["cached-photo-send-v1",payload["origin_task"],*base]
        if self.bridge is not None:
            base += [self.bot_id, selected["caption"], selected["kind"]]
        return digest(base)

    async def send(self, task, payload, group, files):
        async with self.send_lock:
            return await self._send(task, payload, group, files)

    async def _send(self, task, payload, group, files):
        target = payload["target"]
        keys = [self.operation_key(payload, i) for i in group]
        states = [self.store.operation(k)[0] for k in keys]
        if any(state in ("sending", "ambiguous") for state in states):
            raise Ambiguous("上次傳送結果未確認；不會盲目重送，需核對目的地")
        pending = [(i, f, k) for i,f,k,state in zip(group, files, keys, states) if state != "done"]
        if not pending:
            return []
        group, files, keys = map(list, zip(*pending))
        self.check(task)
        if payload["action"] == "cached_photos":
            await self.gate.wait()
        else:
            # Check current chat-level protection as well as snapshot/message flag.
            await self.gate.wait()
            entity = await self.client.get_entity(self.peer(group[0]))
            if getattr(entity, "noforwards", False):
                raise PermissionError("來源現時禁止轉發")
            for selected in group:
                await self.gate.wait()
                current = await self.client.get_messages(self.peer(selected), ids=selected["id"])
                if not current or getattr(current, "noforwards", False):
                    raise PermissionError("來源已刪除／不可用或現時禁止轉發；下載檔保留")
        self.check(task)
        if self.delivery is None:
            raise RuntimeError("Bot delivery is not configured")
        with self.store.transaction():
            for key in keys:
                self.store.mark(key, "sending", {"task": task["id"]})
        try:
            if self.bridge is None:
                sent = await self.delivery.send(target, group, files)
            else:
                operation = self.bridge.operation(
                    [media_identity(i["chat"], i["id"], i.get("media_id", "")) for i in group],
                    target[0], target[1], "bot:"+str(self.bot_id)+":control-upload-v1",
                    action=("cached-photos-v1:"+payload["origin_task"] if payload["action"] == "cached_photos" else "upload"), caption=encode([[i["id"], i["caption"], i["kind"]] for i in group]))
                async def deliver():
                    try:
                        result = await self.delivery.send(target, group, files)
                    except ValueError:
                        # BotDelivery's local bounds reject before any Telegram API call.
                        raise BadRequest("Local media delivery bounds rejected") from None
                    values = result if isinstance(result, (list, tuple)) else [result]
                    mids = [getattr(m, "message_id", None) or getattr(m, "id", None) for m in values]
                    if len(mids) != len(group) or any(not isinstance(mid, int) or mid <= 0 for mid in mids):
                        raise AmbiguousDelivery("Incomplete bot delivery receipt")
                    return SimpleNamespace(sent_message_ids=mids, sent_message_id=mids[0], delivery_kind="control-upload")
                sent = await self.bridge.deliver(operation, deliver)
                mids = getattr(sent, "sent_message_ids", None)
                if not isinstance(mids, list) or len(mids) != len(keys) or any(not isinstance(mid, int) or mid <= 0 for mid in mids):
                    raise AmbiguousDelivery("Invalid shared delivery receipt")
                sent = [SimpleNamespace(message_id=mid) for mid in mids]
        except (FloodWaitError, RetryAfter, BadRequest, Forbidden, RPCError, ValueError) as exc:
            if self.bridge is not None and not definite_rejection(exc):
                for key in keys:
                    self.store.mark(key, "ambiguous", {"task": task["id"]})
                raise Ambiguous("傳送結果未確認；共享 claim 保留，需核對目的地") from None
            for key in keys:
                self.store.mark(key, "failed")
            raise
        except BaseException:
            for key in keys:
                self.store.mark(key, "ambiguous", {"task": task["id"]})
            raise Ambiguous("傳送結果未確認；需核對目的地，未自動重送")
        ids = [getattr(m, "message_id", None) or getattr(m, "id", None) for m in (sent if isinstance(sent, (list, tuple)) else [sent])]
        if payload["action"] == "cached_photos" and (len(ids) != len(keys) or any(type(mid) is not int or mid <= 0 for mid in ids)):
            for key in keys:
                self.store.mark(key,"ambiguous")
            raise Ambiguous("傳送回應無效；需核對目的地，不會盲目重送")
        with self.store.transaction():
            for key, mid in zip(keys, ids):
                self.store.mark(key, "done", mid)
        if len(ids) != len(keys):
            for key in keys[len(ids):]:
                self.store.mark(key, "ambiguous")
            raise Ambiguous("傳送回應數量不符；需核對目的地")
        return ids

    async def execute(self, task):
        payload = json.loads(task["payload"])
        manifest = {"task": task["id"], "snapshot": payload["snapshot"], "action": payload["action"],
                    "target": payload.get("target"), "selection": payload["items"], "items": [], "state": "running"}
        try:
            self.check(task)
            self.validate(payload)
            if payload["action"] == "cached_photos":
                self.validate_cached_task(task,payload)
            groups = OrderedDict()
            for i in payload["items"]:
                # Photos/videos are compatible; no document/audio album mixing.
                k = (i["chat"], i["album"] or "single_"+str(i["id"]))
                groups.setdefault(k, []).append(i)
            used, count = 0, 0
            for group in groups.values():
                group.sort(key=lambda i: i["id"])
                files = []
                for i in group:
                    self.check(task)
                    if payload["action"] == "cached_photos":
                        path,size = self.cached_file(i,used)
                    else:
                        path, size = await self.download(task, payload, i, used)
                    used += size
                    files.append(path)
                    manifest["items"].append({**i, "path": str(path.relative_to(self.archive.root)), "download": "done", "bytes": size})
                    count += 1
                    self.store.set_task(task["id"], progress=count)
                    self.archive.manifest(payload["snapshot"], task["id"], manifest)
                    await self.notice(task)
                if payload["action"] != "download":
                    for start in range(0, len(group), 10):
                        ids = await self.send(task, payload, group[start:start+10], files[start:start+10])
                        for entry in manifest["items"][len(manifest["items"])-len(group)+start:len(manifest["items"])-len(group)+start+10]:
                            entry["forward"] = "done"
                            entry["destination_ids"] = ids
                self.archive.manifest(payload["snapshot"], task["id"], manifest)
            self.store.set_task(task["id"], state="done", error="")
        except asyncio.CancelledError:
            raise
        except Cancelled as exc:
            self.store.set_task(task["id"], state="cancelled", error=str(exc))
        except Ambiguous as exc:
            self.store.set_task(task["id"], state="ambiguous", error=str(exc))
        except (FloodWaitError, RetryAfter) as exc:
            delay = exc.seconds if isinstance(exc, FloodWaitError) else (exc.retry_after.total_seconds() if hasattr(exc.retry_after, "total_seconds") else exc.retry_after)
            due = self.gate.delay(delay)
            state = "delayed" if task["attempt"] < self.config.retries else "failed"
            self.store.set_task(task["id"], state=state, due=due, error="FloodWait: "+str(delay)+" 秒")
        except (Forbidden, BadRequest) as exc:
            self.store.set_task(task["id"], state="failed", error="目的地權限／媒體限制拒絕（"+type(exc).__name__+"）；下載檔保留，未改用原生轉發")
        except Exception as exc:
            state = "delayed" if task["attempt"] < self.config.retries and not isinstance(exc, (ValueError, PermissionError, OSError, BadRequest, Forbidden)) else "failed"
            self.store.set_task(task["id"], state=state, due=time.time()+min(60, 2**task["attempt"]), error=(str(exc)[:300] if isinstance(exc, (ValueError, PermissionError, OSError)) else type(exc).__name__))
        finally:
            row = self.store.task(task["id"], task["owner"], task["chat"])
            manifest["state"], manifest["error"] = row["state"], row["error"]
            done = {(i["chat"], i["id"]) for i in manifest["items"]}
            manifest["unfinished"] = [{"chat": i["chat"], "id": i["id"], "error": row["error"]} for i in payload["items"] if (i["chat"],i["id"]) not in done]
            if payload["action"] != "download":
                manifest["forward_operations"] = [{"chat": i["chat"], "id": i["id"], "state": self.store.operation(self.operation_key(payload, i))[0] or "pending"} for i in payload["items"]]
            try:
                self.archive.manifest(payload["snapshot"], task["id"], manifest)
            except (ValueError, OSError):
                pass
            await self.notice(task, final=True)

    async def worker(self):
        while True:
            task = self.store.claim()
            if task:
                await self.execute(task)
            else:
                await asyncio.sleep(0.5)
