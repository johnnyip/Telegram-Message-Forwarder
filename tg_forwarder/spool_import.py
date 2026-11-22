"""Authenticated control -> Redis request -> spool-owner archive handoff.

No user-controlled absolute paths; no NFS ledger access from the control bot.
Pending/unknown delivery references retain their source, never become success.
"""
import asyncio
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import stat
import time
import uuid
from .spool_lifecycle import Spool, signature, rename_exclusive, code, key
from .shared_media import _io, configured_bridge

PREFIX = '/app/downloads/_spool/'
PATTERN = re.compile(r'new/-?[0-9]{1,20}/[a-f0-9]{32}_[^/\\\x00-\x1f]{1,220}\Z')


def relative_path(text):
    if PREFIX not in text:
        return None
    # The notice places its path on its own line, optionally backtick-wrapped.
    matches = [line.strip().strip('`') for line in text.splitlines() if PREFIX in line]
    if len(matches) != 1 or not matches[0].startswith(PREFIX):
        raise ValueError('請每次轉發一條完整 spool 保存通知')
    relative = matches[0][len(PREFIX):]
    if not PATTERN.fullmatch(relative) or any(p in ('.', '..') for p in relative.split('/')):
        raise ValueError('不接受此 spool path；只接受已登記 new 檔案')
    return relative


def request(owner, relative, nonce=None):
    if type(owner) is not int or owner <= 0 or not PATTERN.fullmatch(relative):
        raise ValueError('Invalid spool import request')
    value = {'owner': owner, 'relative': relative}
    if nonce is not None:
        if type(nonce) is not int or nonce <= 0:raise ValueError('Invalid import nonce')
        value['nonce']=nonce
    return key(value), value


def open_dir(path):
    path = Path(path)
    if not path.is_absolute():raise ValueError('Archive root must be absolute')
    fd = os.open('/', os.O_RDONLY | os.O_DIRECTORY)
    try:
        for part in path.parts[1:]:
            child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
            os.close(fd); fd = child
        return fd
    except BaseException:
        os.close(fd); raise


def hash_file(fd):
    os.lseek(fd, 0, os.SEEK_SET); h = hashlib.sha256()
    while True:
        block = os.read(fd, 1024 * 1024)
        if not block:break
        h.update(block)
    return h.hexdigest()


def check_archive(fd, expected):
    s = os.fstat(fd)
    if not stat.S_ISREG(s.st_mode) or s.st_nlink != 1 or s.st_size != expected['size']:
        raise ValueError('Archive identity/size changed; spool retained')
    if hash_file(fd) != expected['sha256']:
        raise ValueError('Archive checksum changed; spool retained')
    return signature(os.fstat(fd))


def archive_file(spool_root, archive_root, relative, *, max_bytes=2*1024**3, reserve=1024**3):
    """Native spool-owner only. Durable destination BEFORE any source deletion."""
    if not PATTERN.fullmatch(relative):raise ValueError('Invalid spool relative path')
    spool = Spool(spool_root); archive_root = Path(archive_root)
    # Do not create a missing operator mount/root or silently use local fallback.
    afd = open_dir(archive_root)
    lock = None; parent = None; src = None; dest = None
    try:
        lock = os.open(spool.guard()/'gc.lock', os.O_CREAT|os.O_RDWR|os.O_NOFOLLOW, 0o600)
        li = os.fstat(lock)
        if not stat.S_ISREG(li.st_mode) or li.st_nlink != 1 or li.st_mode & 0o077:
            raise ValueError('Unsafe spool import lock')
        fcntl.flock(lock, fcntl.LOCK_EX)
        token = Path(relative).name.split('_', 1)[0]
        with spool.db() as c:
            row = spool.row(c, token)
            if row['path'] != relative:raise ValueError('Spool token/path mismatch')
            exported = row.get('archive_export')
        if exported and exported['root'] != str(archive_root):
            raise ValueError('Existing archive root changed; reconciliation required')
        # Flat deterministic, token-bearing name; never clobber another file.
        name = Path(relative).name
        if not exported:
            if row['state'] not in ('referenced','cleanup_pending','retained'):
                raise ValueError('Spool 已過期、未完成或狀態不確定；未搬走')
            parent, leaf = spool.open_parent(relative)
            src = os.open(leaf, os.O_RDONLY|os.O_NOFOLLOW, dir_fd=parent)
            before = os.fstat(src)
            if signature(before) != row['proof'] or not stat.S_ISREG(before.st_mode) or before.st_nlink != 1:
                raise ValueError('Spool file changed; retained')
            if not 0 < before.st_size <= max_bytes:raise ValueError('Spool import size limit')
            if shutil.disk_usage(archive_root).free < before.st_size + reserve:
                raise ValueError('下載目錄空間不足；spool 保留')
            exported = {'root':str(archive_root), 'name':name, 'size':before.st_size,
                        'sha256':hash_file(src), 'state':'intent', 'created':time.time()}
            if signature(os.fstat(src)) != row['proof']:raise ValueError('Spool changed during hash')
            with spool.db() as c:
                row = spool.row(c, token); row['archive_export'] = exported; spool.put(c,row)
        # A crash after publication is reconciled ONLY against the durable digest.
        try:dest = os.open(name, os.O_RDONLY|os.O_NOFOLLOW, dir_fd=afd)
        except FileNotFoundError:
            if exported['state'] != 'intent':raise ValueError('已收檔紀錄存在，但下載檔已移走；不重新搬檔')
            if src is None:
                parent, leaf = spool.open_parent(relative)
                src = os.open(leaf, os.O_RDONLY|os.O_NOFOLLOW, dir_fd=parent)
                if signature(os.fstat(src)) != row['proof']:raise ValueError('Spool source changed')
            temp = '.spool-import-'+token+'-'+uuid.uuid4().hex+'.part'
            out = os.open(temp, os.O_CREAT|os.O_EXCL|os.O_WRONLY|os.O_NOFOLLOW,0o600,dir_fd=afd)
            try:
                os.lseek(src,0,os.SEEK_SET)
                with os.fdopen(os.dup(src),'rb') as source, os.fdopen(out,'wb') as target:
                    shutil.copyfileobj(source,target,1024*1024);target.flush();os.fsync(target.fileno())
                if signature(os.fstat(src)) != row['proof']:raise ValueError('Spool changed during copy')
                check = os.open(temp,os.O_RDONLY|os.O_NOFOLLOW,dir_fd=afd)
                try:check_archive(check,exported)
                finally:os.close(check)
                rename_exclusive(temp,afd,name,afd);os.fsync(afd)
            finally:
                try:os.unlink(temp,dir_fd=afd)
                except FileNotFoundError:pass
            dest=os.open(name,os.O_RDONLY|os.O_NOFOLLOW,dir_fd=afd)
        proof = check_archive(dest, exported)
        if exported.get('proof') and proof != exported['proof']:
            raise ValueError('Archive inode replaced; no source deletion')
        exported.update(state='verified',proof=proof)
        with spool.db() as c:
            row = spool.row(c,token);row['archive_export']=exported;spool.put(c,row)
            refs=[json.loads(r[0]) for r in c.execute('SELECT data FROM refs WHERE token=?',(token,))]
            eligible=bool(refs) and all(r['state']=='terminal' for r in refs) and row['shared_safe']
            if eligible and row['state'] in ('cleanup_pending','retained','referenced'):
                row['state']='archive_cleanup_claimed';spool.put(c,row)
        removed = row['state']=='deleted'
        if row['state']=='archive_cleanup_claimed':
            current_root=open_dir(archive_root)
            try:
                if signature(os.fstat(current_root))[:2] != signature(os.fstat(afd))[:2]:
                    raise ValueError('Archive root replaced; source retained')
            finally:os.close(current_root)
            if signature(os.stat(name,dir_fd=afd,follow_symlinks=False)) != proof:
                raise ValueError('Archive path replaced; source retained')
            # Revalidate destination on EVERY retry, then use durable quarantine.
            if check_archive(dest,exported) != proof:raise ValueError('Archive changed before cleanup')
            spool.claim_remove(row,relative)
            with spool.db() as c:
                row=spool.row(c,token);row.update(state='deleted',deleted_at=time.time());spool.put(c,row)
            removed=True
        return {'name':name,'size':exported['size'],'sha256':exported['sha256'],
                'state':'moved' if removed else 'copied_pending','spool_removed':removed}
    finally:
        for fd in (dest,src,parent,lock,afd):
            if fd is not None:os.close(fd)


class ImportQueue:
    def __init__(self, bridge):
        self.redis=bridge.redis; self.prefix=bridge.prefix+'spool-import:'

    async def submit(self, owner, relative, nonce=None):
        if not await self.redis.get(self.prefix+'ready'):
            raise ValueError('Spool 收檔 worker 未就緒；未搬走檔案')
        ident,payload=request(owner,relative,nonce)
        # Atomic bounded intake; completed results are never silently replayed.
        accepted=await self.redis.eval("if redis.call('EXISTS',KEYS[2])==1 then return 0 end; if redis.call('ZCARD',KEYS[1])>=128 and redis.call('ZSCORE',KEYS[1],ARGV[1])==false then return -1 end; redis.call('SET',KEYS[3],ARGV[2], 'NX'); redis.call('ZADD',KEYS[1], 'NX', ARGV[3], ARGV[1]); return 1",
            3,self.prefix+'queue',self.prefix+'result:'+ident,self.prefix+'request:'+ident,ident,code(payload),time.time())
        if accepted == -1:raise ValueError('Spool 收檔佇列已滿；請稍後再試')
        return ident

    async def result(self, ident):
        raw=await self.redis.get(self.prefix+'result:'+ident)
        return json.loads(raw) if raw else None

    async def tick(self, spool_root, archive_root, allowed):
        ids=await self.redis.zrange(self.prefix+'queue',0,0)
        if not ids:return False
        ident=ids[0]
        raw=await self.redis.get(self.prefix+'request:'+ident)
        try:
            value=json.loads(raw);expected,_=request(value['owner'],value['relative'],value.get('nonce'))
            if expected!=ident or value['owner'] not in allowed:raise ValueError('Unauthorized import request')
            result=await _io(archive_file,spool_root,archive_root,value['relative'])
        except (OSError,ValueError,KeyError,TypeError) as exc:
            # Explicit error, not successful move; preserve source on uncertainty.
            result={'state':'error','error':type(exc).__name__,'message':str(exc)[:240]}
        await self.redis.set(self.prefix+'result:'+ident,code(result),ex=30*86400)
        await self.redis.expire(self.prefix+'request:'+ident,30*86400)
        await self.redis.zrem(self.prefix+'queue',ident)
        return True


async def worker_loop(log):
    root=os.getenv('SPOOL_IMPORT_ARCHIVE_ROOT')
    if not root:return
    from .control.config import allowed_ids
    allowed=allowed_ids(os.getenv('SPOOL_IMPORT_ALLOWED_USER_IDS',''))
    if not allowed:raise ValueError('SPOOL_IMPORT_ALLOWED_USER_IDS required')
    bridge=await configured_bridge()
    if bridge is None:raise ValueError('Spool import requires shared Redis')
    queue=ImportQueue(bridge)
    while True:
        try:
            await queue.redis.set(queue.prefix+'ready','1',ex=300)
            await queue.tick(Path(os.getenv('DOWNLOAD_DIR','./downloads'))/'_spool',root,allowed)
        except asyncio.CancelledError:raise
        except Exception as exc:log({'op':'spool_import_worker_error','class':type(exc).__name__})
        await asyncio.sleep(1)
