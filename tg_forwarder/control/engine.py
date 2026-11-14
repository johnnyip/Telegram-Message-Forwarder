import asyncio
from collections import OrderedDict
import json
import os
from pathlib import Path
import re
import shutil
import stat
import tempfile
import time
from telethon import types
from telethon.errors import FloodWaitError, RPCError
from telegram.error import BadRequest, Forbidden, RetryAfter
from .store import digest, encode
from .filesystems import guard_archive
from tg_forwarder.canonical_media import relative as original_relative, filename as original_filename, truncate_filename, validate_title, secure_path
from tg_forwarder.shared_media import _io, _join
from types import SimpleNamespace
from tg_forwarder.shared_media import media_identity, AmbiguousDelivery, SharedUnavailable, definite_rejection


class Cancelled(RuntimeError):
    pass


class Ambiguous(RuntimeError):
    pass


class PhotoReuseUnavailable(ValueError):
    """Only a NEW authenticated inbound download may fetch its current Bot copy."""


class PhotoReuseMissing(PhotoReuseUnavailable):
    pass


class PhotoReuseLegacyLayout(PhotoReuseUnavailable):
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
    def __init__(self, root, reserve_bytes, expected_mount=None, expected_fstypes=("nfs","nfs4"), archive_timezone="UTC", *, flat=False, metadata_dir=None, named=False):
        self.named=named
        self.flat, self.metadata_dir = flat or named, Path(metadata_dir).absolute() if metadata_dir else None
        if self.flat and self.metadata_dir is None:raise ValueError("Flat archive requires private metadata directory")
        self.timezone = archive_timezone
        self.expected_mount, self.expected_fstypes = expected_mount, expected_fstypes
        self.root = Path(root).absolute()
        self.guard()
        root = Path(root).absolute()
        # Reject symlink roots AND ancestors; never resolve away a bad boundary.
        for p in (root, *root.parents):
            if p.is_symlink():
                raise ValueError("archive root/ancestor cannot be a symlink")
        root.mkdir(parents=True, exist_ok=True,mode=0o700)
        self.root, self.reserve = root, reserve_bytes

    def guard(self):
        guard_archive(self.root,self.expected_mount,self.expected_fstypes)

    def path(self, *parts):
        self.guard()
        current = self.root
        for part in parts:
            part = str(part)
            if any(c in part for c in ("/", "\\", "\x00")) or part in ("", ".", ".."):
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
        if self.flat:
            if not i.get('archive_meta'):
                raise ValueError('Original filename metadata required for flat layout')
            if self.named:
                meta=i['archive_meta']
                title=meta.get('folder_title') or meta.get('title')
                if not title:raise ValueError('Confirmed chat title required')
                topic=meta.get('topic_title')
                if meta.get('topic_id') and not topic:raise ValueError('Confirmed topic title required')
                label=title+(' - '+topic if topic else '')
                label=truncate_filename(validate_title(label))
                return self.path(label,original_filename(i))
            return self.path(original_filename(i))
        if i.get("archive_meta"):
            rel = original_relative(i,self.timezone)
            return self.path(*rel.parts)
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
        if self.flat:
            from .filesystems import guard_state
            base = self.metadata_dir
            guard_state(base)
            if base.is_relative_to(self.root):raise ValueError('Flat metadata must be outside public media root')
            base.mkdir(parents=True,exist_ok=True,mode=0o700)
            for private in (base.parent,base):
                info=private.lstat()
                if not stat.S_ISDIR(info.st_mode) or info.st_uid!=os.getuid() or info.st_mode & 0o077:
                    raise ValueError('Manifest state directory must be private and owned')
            if not re.fullmatch(r'[A-Za-z0-9_-]{1,128}',tid):raise ValueError('Invalid manifest task ID')
            path=base/('manifest_'+tid+'.json')
        else:
            base = self.path(".metadata")
            base.mkdir(parents=True, exist_ok=True,mode=0o700)
            path = self.path(".metadata", "manifest_"+tid+".json")
        fd, temp = tempfile.mkstemp(prefix=".manifest-", dir=base)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                def sanitized(value):
                    if isinstance(value, dict):
                        return {k:sanitized(v) for k,v in value.items() if k not in ("access_hash", "file_id")}
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
        self.archive = Archive(config.root, config.reserve_bytes, config.expected_archive_mount, config.expected_archive_fstypes, config.archive_timezone, flat=getattr(config,"archive_flat",False), metadata_dir=config.db.parent/"manifests", named=getattr(config,"archive_named",False))
        self.notify = notify
        self.delivery = delivery
        self.bridge, self.bot_id = bridge, bot_id
        if bridge is not None:
            bridge.register_root("control", config.root)
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

    def validate(self, payload, check_space=True):
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
        if check_space:self.archive.space(0 if payload["action"]=="cached_photos" else known)

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
        self.validate(payload,check_space=False)
        return payload

    def cached_file(self, selected, used=0):
        key = digest([selected["chat"],selected["id"],selected.get("media_id", "")])
        cached = self.store.db.execute("SELECT * FROM cache WHERE key=?",(key,)).fetchone()
        if cached is None:
            raise ValueError("已下載圖片快取記錄遺失；此動作不會重新下載")
        return self._cached_path(dict(cached),selected,used)

    def _cached_path(self,cached,selected,used=0):
        try:
            path = self.archive.path(*Path(cached["path"]).relative_to(self.archive.root).parts)
            path = secure_path(self.archive.root,path.relative_to(self.archive.root),existing=True)
            size = path.stat().st_size
            if not path.is_file() or size <= 0 or size != cached["size"] or (selected.get("size") is not None and selected["size"] != size):
                raise ValueError()
        except (OSError,ValueError):
            raise ValueError("已下載圖片檔案遺失／無效；檔案保留，不會重新下載") from None
        if used + size > self.config.max_bytes:
            raise ValueError("媒體大小超過上限")
        return path,size

    async def cached_file_async(self,selected,used=0):
        key=digest([selected["chat"],selected["id"],selected.get("media_id","")])
        cached=self.store.db.execute("SELECT * FROM cache WHERE key=?",(key,)).fetchone()
        if cached is None:raise ValueError("已下載圖片快取記錄遺失；此動作不會重新下載")
        return await _io(self._cached_path,dict(cached),selected,used)

    async def validate_cached_task(self, task, payload):
        original = self.store.task(payload.get("origin_task"),task["owner"],task["chat"])
        expected = self.cached_photo_payload(original)
        if encode(expected) != encode(payload):
            raise ValueError("已下載圖片任務與原始任務不符")
        used = 0
        for selected in payload["items"]:
            _,size = await self.cached_file_async(selected,used)
            used += size

    async def primary_destination(self, snapshot, selected):
        primary=await _io(self.archive.file,snapshot,selected)
        if self.archive.named:
            meta=selected['archive_meta']
            binding=[meta.get('folder_chat',selected['chat']),meta.get('topic_id')]
            relative=primary.parent.relative_to(self.archive.root)
            name='archive_folder:'+digest(str(relative))
            prior=self.store.get_meta(name,'')
            if prior and json.loads(prior)!=binding:
                suffix=' - '+digest(binding)[:12]
                label=truncate_filename(str(relative),230-len(suffix))+suffix
                primary=await _io(self.archive.path,label,primary.name)
                name='archive_folder:'+digest(label)
                prior=self.store.get_meta(name,'')
                if prior and json.loads(prior)!=binding:raise ValueError('Named folder identity collision')
            self.store.meta(name,encode(binding))
        return primary

    async def flat_destination(self, snapshot, selected, key):
        primary=await self.primary_destination(snapshot,selected)
        # Existing cached identities are handled before this path. Unknown existing
        # names are NEVER adopted by size; preserve them and disambiguate only once.
        occupied = await _io(lambda:primary.exists() or primary.is_symlink()) or self.store.db.execute('SELECT 1 FROM cache WHERE path=? AND key!=?',(str(primary),key)).fetchone()
        if not occupied:return primary
        suffix=' - '+key[:16]
        stem=truncate_filename(primary.stem+primary.suffix,230-len(suffix))
        base=Path(stem)
        alternate=await _io(self.archive.path,*primary.parent.relative_to(self.archive.root).parts,base.stem+suffix+base.suffix)
        if await _io(lambda:alternate.exists() or alternate.is_symlink()) or self.store.db.execute('SELECT 1 FROM cache WHERE path=? AND key!=?',(str(alternate),key)).fetchone():
            raise ValueError('Flat filename collision requires verification; existing file not overwritten')
        return alternate

    def finalize(self, temporary, path, identity=None):
        self.archive.path(*path.relative_to(self.archive.root).parts)
        if identity is not None:
            current=os.lstat(temporary)
            if not stat.S_ISREG(current.st_mode) or (current.st_dev,current.st_ino)!=identity:
                raise ValueError('Owned download temporary replaced')
        os.link(temporary,path,follow_symlinks=False)
        if identity is not None:
            published=os.lstat(path)
            if not stat.S_ISREG(published.st_mode) or (published.st_dev,published.st_ino)!=identity:
                raise ValueError('Finalized download inode changed; not registered')
        directory=os.open(path.parent,os.O_RDONLY|os.O_DIRECTORY)
        try:os.fsync(directory)
        finally:os.close(directory)

    async def download_temporary(self, parent):
        """A cancelled thread handoff must not abandon its created FD/path."""
        owned = []
        def acquire():
            resource = tempfile.mkstemp(prefix=".download-", dir=parent)
            owned.append(resource)  # Recorded before the worker can hand off.
            return resource
        def discard():
            if not owned:return
            fd, name = owned.pop()
            try:
                info = os.fstat(fd)
            finally:
                os.close(fd)
            path = Path(name)
            try:current = path.lstat()
            except FileNotFoundError:return
            if (current.st_dev,current.st_ino) == (info.st_dev,info.st_ino) and not path.is_symlink():
                path.unlink()
        try:
            return await _io(acquire)
        except BaseException:
            # _io has joined acquisition. Join cleanup too, even if this task
            # receives repeated cancellation while waiting for the IO gate.
            await _join(asyncio.create_task(_io(discard)))
            raise

    def photo_alias_identity(self, task, payload, selected):
        # Caller performs the strict owner/bot/action/media_id binding FIRST.
        if not self.archive.flat or selected.get('source_kind') != 'bot-inbound' or selected.get('kind')!='photo':return None
        from .inbound import photo_id_from_file_id
        size=selected.get('size')
        unique=selected.get('file_unique_id')
        if type(size) is not int or size<=0 or not isinstance(unique,str) or not unique:return None
        photo=photo_id_from_file_id(selected['file_id'])
        declared=selected.get('archive_meta',{}).get('photo_id')
        if declared is not None and photo!=declared:
            raise PermissionError('Received photo immutable file binding mismatch')
        # file_unique_id identifies the selected Telegram PhotoSize variant; size
        # and decoded immutable ID further narrow it, never changing file_reference.
        variant=selected.get('photo_variant')
        if variant is not None and (not isinstance(variant,list) or len(variant)!=2 or any(type(v) is not int or v<=0 for v in variant)):
            raise ValueError('Invalid received photo variant')
        meta=selected.get('archive_meta',{})
        namespace=['root-flat']
        if self.archive.named:
            title=meta.get('folder_title') or meta.get('title')
            namespace=['named',meta.get('folder_chat',selected['chat']),meta.get('topic_id'),title,meta.get('topic_title')]
        return [task['owner'],self.bot_id,unique,photo,'photo',size,variant,namespace]

    async def reusable_photo_path(self, cached, selected, used):
        def lookup():
            path=self.archive.path(*Path(cached['path']).relative_to(self.archive.root).parts)
            try:path.lstat()
            except FileNotFoundError:raise PhotoReuseMissing('Known photo missing; current received copy required') from None
            # Permission, symlink, ownership, writable mode, size and corruption
            # errors remain HARD failures. Never infer missing from ValueError.
            return self._cached_path(cached,selected,used)
        return await _io(lookup)

    async def alias_file(self, identity, selected, key, used):
        name='flat_photo_alias:'+digest(identity)
        raw=self.store.get_meta(name,'')
        if not raw:return None
        record=json.loads(raw)
        if record.get('identity')!=identity:raise ValueError('Physical photo alias identity mismatch')
        cached=self.store.db.execute('SELECT * FROM cache WHERE key=?',(record['cache_key'],)).fetchone()
        if cached is None:
            # Missing DB reference does not excuse corruption of still-present
            # bytes for which this exact identity retains a physical proof.
            path,_=await self.reusable_photo_path({'path':record['path'],'size':record['size']},selected,used)
            from tg_forwarder.canonical_media import fingerprint
            if await _io(fingerprint,path)!=record['proof']:raise ValueError('Physical photo alias bytes changed; no download fallback')
            raise PhotoReuseMissing('Known photo cache reference missing')
        if cached['path']!=record['path'] or cached['size']!=record['size']:
            raise ValueError('Physical photo alias cache binding missing/changed; no download fallback')
        path,size=await self.reusable_photo_path(dict(cached),selected,used)
        from tg_forwarder.canonical_media import fingerprint
        proof=await _io(fingerprint,path)
        if proof!=record['proof']:raise ValueError('Physical photo alias bytes changed; no download fallback')
        if self.archive.named:
            intended=await self.primary_destination({},selected)
            if path.parent!=intended.parent:
                raise PhotoReuseLegacyLayout('Known photo uses old layout; current received copy required')
        self.commit_photo_cache(key,path,size,identity,record)
        return path,size

    def commit_photo_cache(self, key, path, size, identity=None, alias=None, *, stale_alias=None):
        with self.store.transaction():
            self.store.db.execute('INSERT OR REPLACE INTO cache VALUES(?,?,?)',(key,str(path),size))
            if identity is not None:
                name='flat_photo_alias:'+digest(identity)
                existing=self.store.get_meta(name,'')
                if stale_alias is not None and existing!=stale_alias:
                    raise ValueError('Stale physical alias changed concurrently; fresh rebind refused')
                if existing:
                    value=json.loads(existing)
                    if value.get('identity')!=identity or (stale_alias is None and (value['path']!=str(path) or value['proof']!=alias['proof'])):
                        raise ValueError('Physical photo alias already bound to another canonical file')
                self.store.meta(name,encode(alias))

    async def remember_photo(self, key, path, size, identity, *, stale_alias=None):
        if identity is None:
            self.store.db.execute('INSERT OR REPLACE INTO cache VALUES(?,?,?)',(key,str(path),size))
            return
        from tg_forwarder.canonical_media import fingerprint
        proof=await _io(fingerprint,path)
        self.commit_photo_cache(key,path,size,identity,dict(identity=identity,cache_key=key,path=str(path),size=size,proof=proof),stale_alias=stale_alias)

    async def bootstrap_photo_alias(self, task, payload, selected, identity, key, used):
        # Bounded private DB evidence only: last 32 task rows, at most 10 items
        # per pure inbound task. No filesystem walk, filename adoption or API call.
        rows=self.store.db.execute('SELECT owner,chat,state,payload FROM tasks ORDER BY rowid DESC LIMIT 32').fetchall()
        for row in rows:
            if row['owner']!=task['owner'] or row['chat']!=task['chat'] or row['state']!='done':continue
            old=json.loads(row['payload'])
            if old.get('source_kind')!='bot-inbound' or old.get('bot_id')!=self.bot_id or old.get('action')!='download':continue
            items=old.get('items',[])
            if not isinstance(items,list) or len(items)>10:continue
            for item in items:
                if item.get('chat')!=task['owner'] or item.get('bot_id')!=self.bot_id or item.get('media_id')!='bot-inbound:'+str(self.bot_id)+':'+str(item.get('file_unique_id')):continue
                previous=self.photo_alias_identity(task,old,item)
                if not previous or previous[:6]!=identity[:6]:continue
                if previous[6]!=identity[6] and (previous[6] is not None or previous[3] is None):continue
                if previous[7][:3]!=identity[7][:3]:continue
                oldkey=digest([item['chat'],item['id'],item.get('media_id','')])
                cached=self.store.db.execute('SELECT * FROM cache WHERE key=?',(oldkey,)).fetchone()
                if cached is None:continue
                path,size=await self.reusable_photo_path(dict(cached),selected,used)
                # A retained old-layout proof is optional, never invented for legacy
                # rows. When present it MUST pass before classifying layout stale.
                for candidate in (previous,previous[:7]+[['root-flat']]):
                    raw=self.store.get_meta('flat_photo_alias:'+digest(candidate),'')
                    if not raw:continue
                    retained=json.loads(raw)
                    if retained.get('identity')!=candidate or retained.get('path')!=str(path) or retained.get('size')!=size:
                        raise ValueError('Retained physical alias binding mismatch')
                    from tg_forwarder.canonical_media import fingerprint
                    if await _io(fingerprint,path)!=retained['proof']:
                        raise ValueError('Retained physical alias bytes changed; no download fallback')
                    proof_cache=self.store.db.execute('SELECT path,size FROM cache WHERE key=?',(retained['cache_key'],)).fetchone()
                    if proof_cache is None:raise PhotoReuseMissing('Retained physical alias cache reference missing')
                    if proof_cache['path']!=str(path) or proof_cache['size']!=size:raise ValueError('Retained physical alias cache binding mismatch')
                if self.archive.named:
                    intended=await self.primary_destination({},selected)
                    if path.parent!=intended.parent:raise PhotoReuseLegacyLayout('Known photo uses old layout; current received copy required')
                await self.remember_photo(oldkey,path,size,identity)
                return await self.alias_file(identity,selected,key,used)
        return None

    async def download(self, task, payload, i, used):
        inbound = i.get("source_kind") == "bot-inbound"
        if inbound:
            from .inbound import OWNER
            if (task["owner"] != OWNER or task["chat"] != OWNER or i["chat"] != OWNER
                    or i.get("bot_id") != self.bot_id or payload.get("bot_id") != self.bot_id
                    or payload["action"] != "download" or i["kind"] != "photo"
                    or i["media_id"] != "bot-inbound:"+str(self.bot_id)+":"+i["file_unique_id"]):
                raise PermissionError("Received photo principal/action mismatch")
        key = digest([i["chat"], i["id"], i.get("media_id", "")])
        async with self.download_lock:
            identity=self.photo_alias_identity(task,payload,i) if inbound else None
            stale_alias=None
            observed_alias=self.store.get_meta('flat_photo_alias:'+digest(identity),'') if identity is not None else None
            try:
                if identity is not None:
                    self.check(task)
                    found=await self.alias_file(identity,i,key,used)
                    if found is None:
                        found=await self.bootstrap_photo_alias(task,payload,i,identity,key,used)
                    if found is not None:
                        path,size=found
                        await self.publish(i,path)
                        return path,size
                cached = self.store.db.execute("SELECT * FROM cache WHERE key=?", (key,)).fetchone()
                if cached:
                    def lookup():
                        path=Path(cached["path"])
                        try:path=self.archive.path(*path.relative_to(self.archive.root).parts)
                        except ValueError:raise ValueError("unsafe cache path") from None
                        if path.exists():path=secure_path(self.archive.root,path.relative_to(self.archive.root),existing=True)
                        if path.is_file() and cached["size"]>0 and path.stat().st_size==cached["size"] and (not i.get("size") or i["size"]==cached["size"]):
                            return path,cached["size"]
                        return None
                    found=await self.reusable_photo_path(dict(cached),i,used) if identity is not None else await _io(lookup)
                    if found:
                        path,size=found
                        if used+size>self.config.max_bytes:raise ValueError("媒體大小超過上限")
                        if identity is not None:
                            if self.archive.named:
                                intended=await self.primary_destination({},i)
                                if path.parent!=intended.parent:raise PhotoReuseLegacyLayout('Known photo uses old layout; current received copy required')
                            await self.remember_photo(key,path,size,identity)
                        await self.publish(i,path)
                        return path,size
            except PhotoReuseUnavailable:
                if not inbound or payload['action']!='download':raise
                stale_alias=observed_alias
            self.check(task)
            # Original downloader skips a valid finalized destination before downloading.
            if i.get("archive_meta") and not self.archive.flat:
                candidate = await _io(self.archive.file,payload["snapshot"],i)
                def existing():
                    if not candidate.exists():return None
                    path=secure_path(self.archive.root,candidate.relative_to(self.archive.root),existing=True)
                    size=path.stat().st_size
                    if i.get("size") is not None and size!=i["size"]:raise ValueError("Existing original-format file size mismatch; not overwritten")
                    return path,size
                found=await _io(existing)
                if found:
                    path,size=found
                    if used+size>self.config.max_bytes:raise ValueError("媒體大小超過上限")
                    if self.store.db.execute("SELECT 1 FROM cache WHERE path=? AND key!=?",(str(path),key)).fetchone():
                        raise ValueError("Original-format filename collision; existing identity differs")
                    self.store.db.execute("INSERT OR REPLACE INTO cache VALUES(?,?,?)",(key,str(path),size))
                    await self.publish(i,path)
                    return path,size
            if self.bridge is not None and not inbound and (not self.archive.flat or i.get("archive_meta")):
                imported = await self.import_shared(task, payload, i, key, used)
                if imported is not None:
                    return imported
            await self.gate.wait()
            if inbound:
                current = i
                msg = await self.inbound_media(task, i, used)
            else:
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
            naming = i
            if not i.get("archive_meta"):
                await self.gate.wait()
                entity = await self.client.get_entity(self.peer(i))
                metadata = {**current["archive_meta"],"title":getattr(entity,"title",None) or str(i["chat"])}
                naming = {**i,"archive_meta":metadata}
            if self.archive.flat:
                path = await self.flat_destination(payload['snapshot'],naming,key)
            else:
                path = await _io(self.archive.file,payload["snapshot"], naming)
            await _io(lambda:path.parent.mkdir(parents=True,exist_ok=True))
            # Recheck components after mkdir; flat roots may be public-writable, so
            # publication/cleanup additionally bind to this task-owned temporary inode.
            await _io(lambda:self.archive.path(*path.relative_to(self.archive.root).parts))
            if await _io(path.exists):
                # Orphan (crash before cache commit): do not overwrite unverifiable bytes.
                raise ValueError("未記錄既有檔案，需管理員核對，未覆寫")
            fd, temp = await self.download_temporary(path.parent)
            owned_info=os.fstat(fd)
            owned_identity=(owned_info.st_dev,owned_info.st_ino)
            expected_total = [None]
            async def progress(done, total):
                if total and total > 0:
                    expected_total[0] = total
                self.check(task)
                if used + done > self.config.max_bytes or (total and used+total > self.config.max_bytes):
                    raise ValueError("媒體大小超過上限")
                await _io(self.archive.space)
                await self.notice(task)
            try:
                with os.fdopen(fd, "wb") as f:
                    await self.gate.wait()
                    result = await msg.download_media(file=f, progress_callback=progress)
                    if result is None:
                        raise ValueError("Telegram 未返回有效下載結果，未記錄成功")
                    await _io(f.flush)
                    await _io(lambda:os.fsync(f.fileno()))
                size = await _io(lambda:os.stat(temp).st_size)
                if size <= 0 or (expected_total[0] is not None and size != expected_total[0]) or (i.get("size") is not None and size != i["size"]) or (current.get("size") is not None and size != current["size"]):
                    raise ValueError("下載為空／長度不符，可能截斷；未記錄成功")
                if used+size > self.config.max_bytes:
                    raise ValueError("媒體大小超過上限")
                self.check(task)
                await _io(self.archive.space)
                # link() is atomic no-clobber; then remove ONLY this task-owned tempfile.
                await _io(self.finalize,temp,path,owned_identity)
                await self.remember_photo(key,path,size,identity,stale_alias=stale_alias)
                await self.publish(i, path)
                return path, size
            finally:
                def cleanup():
                    try:current=os.lstat(temp)
                    except FileNotFoundError:return
                    if stat.S_ISREG(current.st_mode) and (current.st_dev,current.st_ino)==owned_identity:os.unlink(temp)
                await _io(cleanup)

    async def inbound_media(self, task, selected, used):
        """PTB reads directly into the existing task-owned tempfile lifecycle."""
        from .inbound import CLOUD_DOWNLOAD_LIMIT
        bot = getattr(self.delivery, "bot", None)
        if bot is None or self.bot_id is None:
            raise ValueError("Received photo bot transport unavailable")
        remote = await bot.get_file(selected["file_id"])
        if remote.file_unique_id != selected["file_unique_id"]:
            raise ValueError("Received photo file identity mismatch")
        expected = remote.file_size
        if expected is not None and (expected <= 0 or expected > CLOUD_DOWNLOAD_LIMIT
                or (selected.get("size") is not None and expected != selected["size"])):
            raise ValueError("Received photo size mismatch / Bot API 20 MB limit")
        await _io(self.archive.space, expected or 0)
        engine = self
        class Media:
            async def download_media(self, file, progress_callback):
                # PTB retrieves a bounded cloud response before out.write(). Retain
                # those bytes only until an owned filesystem thread writes them;
                # do not block the event loop on a private NFS tempfile write.
                chunks, received = [], [0]
                class BoundedOutput:
                    def write(self, data):
                        engine.check(task)
                        received[0] += len(data)
                        if received[0] > min(CLOUD_DOWNLOAD_LIMIT, engine.config.max_bytes-used):
                            raise ValueError("Received photo exceeds download limit")
                        chunks.append(data)
                        return len(data)
                await remote.download_to_memory(out=BoundedOutput())
                for chunk in chunks:
                    engine.check(task)
                    await _io(file.write, chunk)
                await progress_callback(file.tell(), expected)
                return file
        return Media()

    async def publish(self, selected, path):
        if self.bridge is not None:
            try:
                await self.bridge.publish_reference(media_identity(selected["chat"], selected["id"], selected.get("media_id", "")), path)
            except Exception:
                # Local successful cache/bytes remain authoritative and untouched.
                raise ValueError("共享索引發佈失敗（Redis／路徑驗證）；原下載檔已保留，可重試") from None

    async def import_shared(self, task, payload, selected, key, used):
        identity = media_identity(selected["chat"], selected["id"], selected.get("media_id", ""))
        # Query is mandatory/fail-closed when shared mode is enabled.
        try:
            source = await self.bridge.cached(identity)
        except Exception:
            raise SharedUnavailable("Shared source cache unavailable") from None
        if source is None:
            return None
        try:
            rel = source.relative_to(self.archive.root)
        except ValueError:
            pass
        else:
            path = await _io(lambda:secure_path(self.archive.root,rel,existing=True))
            size = await _io(lambda:path.stat().st_size)
            if selected.get("size") is not None and size != selected["size"]:
                raise ValueError("Canonical reference size mismatch")
            if used+size > self.config.max_bytes:raise ValueError("媒體大小超過上限")
            self.store.db.execute("INSERT OR REPLACE INTO cache VALUES(?,?,?)",(key,str(path),size))
            return path,size
        size = await _io(lambda:source.stat().st_size)
        if size <= 0 or (selected.get("size") is not None and size != selected["size"]):
            raise ValueError("共享媒體長度不符")
        if used + size > self.config.max_bytes:
            raise ValueError("媒體大小超過上限")
        await _io(self.archive.space,size)
        path = await self.flat_destination(payload['snapshot'],selected,key) if self.archive.flat else await _io(self.archive.file,payload["snapshot"], selected)
        await _io(lambda:path.parent.mkdir(parents=True,exist_ok=True))
        await _io(lambda:self.archive.path(*path.relative_to(self.archive.root).parts))
        staging=await _io(lambda:tempfile.TemporaryDirectory(prefix=".shared-",dir=path.parent))
        try:
            temp = Path(staging.name) / "media"
            try:
                imported = await self.bridge.materialize(identity, temp)
            except Exception:
                raise SharedUnavailable("Shared source materialization failed") from None
            def validate_import():
                if imported is None or temp.is_symlink() or not temp.is_file() or temp.stat().st_size!=size:
                    raise ValueError("共享媒體匯入不完整")
                self.archive.space()
                with temp.open("rb") as stream:os.fsync(stream.fileno())
            await _io(validate_import)
            info=await _io(lambda:os.lstat(temp))
            owned_identity=(info.st_dev,info.st_ino)
            self.check(task)
            await _io(self.finalize,temp,path,owned_identity)
            self.store.db.execute("INSERT OR REPLACE INTO cache VALUES(?,?,?)", (key, str(path), size))
        finally:
            await _io(staging.cleanup)
        await self.publish(selected,path)
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
        inbound = payload.get("source_kind") == "bot-inbound"
        manifest = {"task": task["id"], "snapshot": payload["snapshot"], "action": payload["action"],
                    "target": payload.get("target"), "selection": payload["items"], "items": [], "state": "running"}
        try:
            self.check(task)
            self.validate(payload,check_space=False)
            await _io(self.archive.space,0 if payload["action"]=="cached_photos" else sum(i.get("size") or 0 for i in payload["items"]))
            if payload["action"] == "cached_photos":
                await self.validate_cached_task(task,payload)
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
                        path,size = await self.cached_file_async(i,used)
                    else:
                        path, size = await self.download(task, payload, i, used)
                    used += size
                    files.append(path)
                    manifest["items"].append({**i, "path": str(path.relative_to(self.archive.root)), "download": "done", "bytes": size})
                    count += 1
                    self.store.set_task(task["id"], progress=count)
                    await _io(self.archive.manifest,payload["snapshot"], task["id"], manifest)
                    await self.notice(task)
                if payload["action"] != "download":
                    for start in range(0, len(group), 10):
                        ids = await self.send(task, payload, group[start:start+10], files[start:start+10])
                        for entry in manifest["items"][len(manifest["items"])-len(group)+start:len(manifest["items"])-len(group)+start+10]:
                            entry["forward"] = "done"
                            entry["destination_ids"] = ids
                await _io(self.archive.manifest,payload["snapshot"], task["id"], manifest)
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
            self.store.set_task(task["id"], state="failed", error=(("收到相片下載被拒絕（"+type(exc).__name__+"）；Bot API 單檔上限 20 MB，原接收記錄保留") if inbound else "目的地權限／媒體限制拒絕（"+type(exc).__name__+"）；下載檔保留，未改用原生轉發"))
        except Exception as exc:
            state = "delayed" if task["attempt"] < self.config.retries and not isinstance(exc, (ValueError, PermissionError, OSError, BadRequest, Forbidden)) else "failed"
            self.store.set_task(task["id"], state=state, due=time.time()+min(60, 2**task["attempt"]), error=(("Received photo download failed ("+type(exc).__name__+"); private API details omitted") if inbound else (str(exc)[:300] if isinstance(exc, (ValueError, PermissionError, OSError)) else type(exc).__name__)))
        finally:
            row = self.store.task(task["id"], task["owner"], task["chat"])
            manifest["state"], manifest["error"] = row["state"], row["error"]
            done = {(i["chat"], i["id"]) for i in manifest["items"]}
            manifest["unfinished"] = [{"chat": i["chat"], "id": i["id"], "error": row["error"]} for i in payload["items"] if (i["chat"],i["id"]) not in done]
            if payload["action"] != "download":
                manifest["forward_operations"] = [{"chat": i["chat"], "id": i["id"], "state": self.store.operation(self.operation_key(payload, i))[0] or "pending"} for i in payload["items"]]
            try:
                await _io(self.archive.manifest,payload["snapshot"], task["id"], manifest)
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
