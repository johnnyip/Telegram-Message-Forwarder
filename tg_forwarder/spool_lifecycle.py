"""New-write spool ownership, durable target receipts and independent bounded GC.

Never discovers ownership by walking files, age, journal TTL, or queue offsets.
SQLite receipts are retained after deletion; a terminal replay never transports.
"""
import asyncio
import ctypes
import errno
from contextlib import contextmanager
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import stat
import time
import uuid
from types import SimpleNamespace
from .shared_media import _io, _join
from .canonical_media import trusted_file_owner_uids


def code(value):return json.dumps(value,sort_keys=True,separators=(',',':'))
def key(value):return hashlib.sha256(code(value).encode()).hexdigest()
def signature(s):return [s.st_dev,s.st_ino,s.st_size,s.st_mtime_ns,s.st_ctime_ns,s.st_uid,s.st_mode]
def rename_exclusive(source,source_fd,destination,destination_fd):
    # Linux native no-clobber atomic rename: never link then unlink a mutable
    # source pathname. Unsupported platforms fail closed, no unsafe fallback.
    libc=ctypes.CDLL(None,use_errno=True);fn=getattr(libc,'renameat2',None)
    if fn is None:raise OSError(errno.ENOTSUP,'Atomic no-clobber rename unavailable')
    fn.argtypes=[ctypes.c_int,ctypes.c_char_p,ctypes.c_int,ctypes.c_char_p,ctypes.c_uint];fn.restype=ctypes.c_int
    if fn(source_fd,os.fsencode(source),destination_fd,os.fsencode(destination),1):
        error=ctypes.get_errno();raise OSError(error,os.strerror(error))

def snapshots(job):return job.get('snapshots') or ([job['snapshot']] if job.get('snapshot') else [])

_ROOT=None
_MANAGERS={}

def configure(root):
    global _ROOT
    _ROOT=Path(root).absolute()

def manager(root=None):
    root=Path(root or _ROOT or (Path(os.getenv('DOWNLOAD_DIR','./downloads')).expanduser()/'_spool')).absolute()
    if root not in _MANAGERS:_MANAGERS[root]=Spool(root)
    return _MANAGERS[root]


def owned(job):return any(s.get('spool_token') for s in snapshots(job))


class ClaimUncertain(ValueError):pass


class Spool:
    def __init__(self,root):
        self.root=Path(root).absolute();self.leases={}

    def guard(self):
        for p in (self.root,*self.root.parents):
            if p.is_symlink():raise ValueError('Spool root/ancestor symlink')
        if not self.root.is_dir():raise ValueError('Spool mount/root unavailable')
        state=self.root/'.lifecycle'
        try:state.mkdir(mode=0o700);created=True
        except FileExistsError:created=False
        if created:
            rootfd=os.open(self.root,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
            try:os.fsync(rootfd)
            finally:os.close(rootfd)
        info=state.lstat()
        if not stat.S_ISDIR(info.st_mode) or info.st_uid not in ({os.getuid()} | trusted_file_owner_uids()) or info.st_mode&0o077:raise ValueError('Unsafe private spool ledger')
        return state

    @contextmanager
    def db(self):
        state=self.guard();path=state/'receipts.sqlite3'
        if path.is_symlink():raise ValueError('Spool DB symlink')
        try:fd=os.open(path,os.O_CREAT|os.O_EXCL|os.O_WRONLY|os.O_NOFOLLOW,0o600);os.close(fd)
        except FileExistsError:pass
        info=path.lstat()
        if not stat.S_ISREG(info.st_mode) or info.st_uid not in ({os.getuid()} | trusted_file_owner_uids()) or info.st_nlink!=1 or info.st_mode&0o077:raise ValueError('Unsafe spool DB')
        # Serialize NAS writers before SQLite's short busy timer. Preserve
        # DELETE journaling, FULL durability and immutable delivery receipts.
        lock=os.open(state/'db.lock',os.O_CREAT|os.O_RDWR|os.O_NOFOLLOW,0o600)
        c=None
        try:
            info=os.fstat(lock)
            if not stat.S_ISREG(info.st_mode) or info.st_nlink!=1 or info.st_uid not in ({os.getuid()} | trusted_file_owner_uids()) or info.st_mode&0o077:
                raise ValueError('Unsafe spool transaction lock')
            deadline=time.monotonic()+120
            while True:
                try:
                    fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
                    break
                except BlockingIOError:
                    if time.monotonic()>=deadline:raise TimeoutError('Spool transaction contention; retained')
                    time.sleep(0.025)
            c=sqlite3.connect(path,timeout=30);c.row_factory=sqlite3.Row
            c.execute('PRAGMA synchronous=FULL');c.execute('PRAGMA journal_mode=DELETE')
            c.executescript('CREATE TABLE IF NOT EXISTS files(token TEXT PRIMARY KEY,data TEXT NOT NULL); CREATE TABLE IF NOT EXISTS refs(token TEXT,ref TEXT,data TEXT NOT NULL,PRIMARY KEY(token,ref)); CREATE TABLE IF NOT EXISTS targets(ref TEXT,target TEXT,data TEXT NOT NULL,PRIMARY KEY(ref,target));')
            c.execute('BEGIN IMMEDIATE');yield c;c.commit()
        finally:
            if c is not None:c.close()
            os.close(lock)

    def row(self,c,token):
        r=c.execute('SELECT data FROM files WHERE token=?',(token,)).fetchone()
        if not r:raise ValueError('Unknown spool ownership')
        return json.loads(r[0])

    def put(self,c,row):c.execute('INSERT OR REPLACE INTO files VALUES(?,?)',(row['token'],code(row)))

    def open_parent(self,relative,create=False):
        rel=Path(relative)
        if rel.is_absolute() or not rel.parts or any(p in ('','.','..') for p in rel.parts):raise ValueError('Unsafe spool relative path')
        fd=os.open('/',os.O_RDONLY|os.O_DIRECTORY)
        try:
            for part in self.root.parts[1:]:
                child=os.open(part,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW,dir_fd=fd);os.close(fd);fd=child
            device=os.fstat(fd).st_dev
            for part in rel.parts[:-1]:
                if create:
                    try:os.mkdir(part,0o700,dir_fd=fd)
                    except FileExistsError:pass
                child=os.open(part,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW,dir_fd=fd)
                if os.fstat(child).st_dev!=device:os.close(child);raise ValueError('Spool device changed')
                os.close(fd);fd=child
            return fd,rel.name
        except BaseException:os.close(fd);raise

    def allocate(self,source,chat,name):
        if not re.fullmatch(r'[a-zA-Z0-9_-]{1,64}',source) or not re.fullmatch(r'-?[0-9]+',str(chat)) or Path(name).name!=name or '\\' in name or '\0' in name:raise ValueError('Unsafe owned spool name')
        for ancestor in (self.root,*self.root.parents):
            if ancestor.is_symlink():raise ValueError('Spool acquisition root symlink')
        self.root.mkdir(parents=True,exist_ok=True)
        state=self.guard();token=uuid.uuid4().hex
        lease=os.open(state/(token+'.lease'),os.O_CREAT|os.O_EXCL|os.O_RDWR|os.O_NOFOLLOW,0o600);fcntl.flock(lease,fcntl.LOCK_EX)
        final=Path(source)/str(chat)/(token+'_'+name);part=final.with_name('.'+final.name+'.part')
        parent=None;fd=None
        try:
            parent,leaf=self.open_parent(part,True)
            fd=os.open(leaf,os.O_CREAT|os.O_EXCL|os.O_RDWR|os.O_NOFOLLOW,0o600,dir_fd=parent)
            info=os.fstat(fd);os.fsync(fd);os.fsync(parent)
            row={'token':token,'part':str(part),'path':str(final),'identity':[info.st_dev,info.st_ino],'state':'acquiring','created':time.time(),'proof':signature(info),'shared_safe':False}
            with self.db() as c:self.put(c,row)
            self.leases[token]=lease
            return token,fd,self.root/part
        except BaseException:
            if fd is not None:os.close(fd)
            os.close(lease)
            # Ambiguous ownership registration failures retain the private inode.
            raise
        finally:
            if parent is not None:os.close(parent)

    def release(self,token):
        fd=self.leases.pop(token,None)
        if fd is not None:os.close(fd)

    def finalize(self,token,shared_safe):
        with self.db() as c:
            row=self.row(c,token);parent,part=self.open_parent(row['part']);other,final=self.open_parent(row['path'])
            try:
                info=os.stat(part,dir_fd=parent,follow_symlinks=False)
                if [info.st_dev,info.st_ino]!=row['identity'] or not stat.S_ISREG(info.st_mode) or info.st_nlink!=1 or info.st_size<=0:raise ValueError('Owned acquisition changed')
                fd=os.open(part,os.O_RDONLY|os.O_NOFOLLOW,dir_fd=parent)
                try:os.fsync(fd)
                finally:os.close(fd)
                rename_exclusive(part,parent,final,other);os.fsync(other);os.fsync(parent)
                after=os.stat(final,dir_fd=other,follow_symlinks=False)
                if not stat.S_ISREG(after.st_mode) or after.st_nlink!=1 or signature(after)[:4]+signature(after)[5:]!=signature(info)[:4]+signature(info)[5:]:
                    raise ValueError('Publication claim changed; preserved final path for reconciliation')
                row.update(state='acquired',proof=signature(after),shared_safe=bool(shared_safe));self.put(c,row)
            finally:os.close(parent);os.close(other)
        return self.root/row['path']

    def claim_remove(self,row,relative,*,strict=True,max_bytes=None):
        """Move first, verify the exclusively claimed inode, then unlink privately.

        A fsynced intent is independent of the caller SQLite transaction. A crash
        never converts a path observation into permission to unlink its successor.
        Mismatches stay in the recorded private quarantine, never auto-rollback
        over a possibly new source file. Same-uid malicious ledger forgery is not
        a security boundary; directory replacement/ordinary path races are.
        """
        state=self.guard();base=state/'quarantine'
        try:base.mkdir(mode=0o700);created=True
        except FileExistsError:created=False
        if created:
            statefd=os.open(state,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
            try:os.fsync(statefd)
            finally:os.close(statefd)
        base_info=base.lstat()
        if not stat.S_ISDIR(base_info.st_mode) or base_info.st_uid not in ({os.getuid()} | trusted_file_owner_uids()) or base_info.st_mode&0o077:raise ValueError('Unsafe quarantine root')
        basefd=os.open(base,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
        qfd=None;parent=None
        name=row['token']+'-'+key(relative)[:16]
        try:
            if (os.fstat(basefd).st_dev,os.fstat(basefd).st_ino)!=(base_info.st_dev,base_info.st_ino):raise ValueError('Quarantine root replaced')
            try:os.mkdir(name,0o700,dir_fd=basefd);new=True;os.fsync(basefd)
            except FileExistsError:new=False
            qfd=os.open(name,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW,dir_fd=basefd)
            qi=os.fstat(qfd)
            if qi.st_uid not in ({os.getuid()} | trusted_file_owner_uids()) or qi.st_mode&0o077:raise ValueError('Unsafe quarantine claim directory')
            def save(data):
                temp='intent-'+uuid.uuid4().hex
                fd=os.open(temp,os.O_CREAT|os.O_EXCL|os.O_WRONLY|os.O_NOFOLLOW,0o600,dir_fd=qfd)
                try:
                    raw=code(data).encode();offset=0
                    while offset<len(raw):offset+=os.write(fd,raw[offset:])
                    os.fsync(fd)
                finally:os.close(fd)
                os.replace(temp,'intent.json',src_dir_fd=qfd,dst_dir_fd=qfd);os.fsync(qfd)
            def check_directory():
                current=os.stat(name,dir_fd=basefd,follow_symlinks=False)
                if (current.st_dev,current.st_ino)!=(qi.st_dev,qi.st_ino) or not stat.S_ISDIR(current.st_mode) or current.st_uid!=qi.st_uid or current.st_mode!=qi.st_mode:raise ValueError('Quarantine directory replaced; retained')
            parent,leaf=self.open_parent(relative)
            if new:
                try:before=os.stat(leaf,dir_fd=parent,follow_symlinks=False)
                except FileNotFoundError:
                    save({'token':row['token'],'relative':str(relative),'directory':[qi.st_dev,qi.st_ino],'expected':row['proof'],'state':'missing-unclaimed','quarantine':str(base/name/'payload')})
                    raise ClaimUncertain('Owned source missing before any verified claim; retained')
                if max_bytes is not None and before.st_size>max_bytes:raise OSError(errno.EFBIG,'Owned partial exceeds pass byte budget; retained')
                if not stat.S_ISREG(before.st_mode) or before.st_nlink!=1 or [before.st_dev,before.st_ino]!=row['identity']:raise ValueError('Cleanup ownership changed; retained')
                if strict and signature(before)!=row['proof']:raise ValueError('Cleanup attributes changed; retained')
                if not strict and [before.st_uid,before.st_mode]!=row['proof'][-2:]:raise ValueError('Partial ownership attributes changed')
                intent={'token':row['token'],'relative':str(relative),'directory':[qi.st_dev,qi.st_ino],
                        'expected':signature(before),'state':'intent','quarantine':str(base/name/'payload')}
                save(intent);check_directory()
                # Destination is a fresh private directory, not a reusable name
                # in the externally mutable source directory.
                rename_exclusive(leaf,parent,'payload',qfd)
                os.fsync(parent);os.fsync(qfd)
            else:
                fd=os.open('intent.json',os.O_RDONLY|os.O_NOFOLLOW,dir_fd=qfd)
                try:
                    with os.fdopen(fd,'r') as stream:intent=json.load(stream)
                except BaseException:raise
                if intent.get('token')!=row['token'] or intent.get('relative')!=str(relative) or intent.get('directory')!=[qi.st_dev,qi.st_ino] or intent.get('expected',[])[:2]!=row['identity']:raise ValueError('Unproven quarantine claim; retained')
            if max_bytes is not None and intent['expected'][2]>max_bytes:raise OSError(errno.EFBIG,'Claim exceeds pass byte budget; retained')
            check_directory()
            if intent['state']=='missing-unclaimed':raise ClaimUncertain('Missing unclaimed source requires reconciliation')
            if intent['state']=='uncertain':raise ClaimUncertain('Quarantine mismatch retained for operator reconciliation: '+intent['quarantine'])
            try:claimed=os.stat('payload',dir_fd=qfd,follow_symlinks=False)
            except FileNotFoundError:
                if intent['state'] in ('verified','removed'):
                    intent['state']='removed';save(intent);return intent['expected'][2]
                raise ClaimUncertain('Unverified claim missing payload; retained')
            expected=intent['expected'];actual=signature(claimed)
            # rename changes ctime; compare every invariant across the move.
            if not stat.S_ISREG(claimed.st_mode) or claimed.st_nlink!=1 or actual[:4]+actual[5:]!=expected[:4]+expected[5:]:
                intent.update(state='uncertain',observed=actual,reason='claimed-inode-or-attributes-mismatch');save(intent)
                raise ClaimUncertain('Foreign/changed inode quarantined without unlink: '+intent['quarantine'])
            if intent['state']=='verified' and actual!=intent.get('verified_proof'):
                intent.update(state='uncertain',observed=actual,reason='verified-claim-changed');save(intent)
                raise ClaimUncertain('Verified quarantine inode changed; retained')
            intent.update(state='verified',verified_proof=actual);save(intent);check_directory()
            # No externally mutable source pathname is ever unlinked here.
            os.unlink('payload',dir_fd=qfd);os.fsync(qfd)
            intent['state']='removed';save(intent)
            return claimed.st_size
        finally:
            if parent is not None:os.close(parent)
            if qfd is not None:os.close(qfd)
            os.close(basefd)

    def acquisition_failed(self,token):
        # Caller joined the acquisition; no publication intent may exist.
        self.release(token)
        with self.db() as c:
            row=self.row(c,token)
            if row['state']!='acquiring' or c.execute('SELECT 1 FROM refs WHERE token=?',(token,)).fetchone():return
            try:
                self.claim_remove(row,row['part'],strict=False)
                row['state']='deleted';self.put(c,row)
            except (OSError,ValueError) as exc:
                row.update(state='cleanup_uncertain',cleanup_error=type(exc).__name__,cleanup_reason=str(exc));self.put(c,row)

    def retain_unpublished(self,tokens,reason):
        with self.db() as c:
            for token in tokens:
                row=self.row(c,token)
                if row['state']=='acquired' and not c.execute('SELECT 1 FROM refs WHERE token=?',(token,)).fetchone():
                    row.update(state='failed_recoverable',reason=reason);self.put(c,row)
        for token in tokens:self.release(token)

    def bind(self,job,targets):
        values=snapshots(job)
        if not values or any(not s.get('spool_token') for s in values):raise ValueError('Mixed unknown/owned spool selection')
        expected=sorted(set(code([route,target]) for route,target in targets))
        if not expected:raise ValueError('No authoritative target selection')
        tokens=[s['spool_token'] for s in values]
        ref=key([tokens,job.get('source_kind'),job.get('route_names'),expected])
        if job.get('spool_ref') not in (None,ref):raise ValueError('Spool reference/target changed')
        with self.db() as c:
            for s in values:
                row=self.row(c,s['spool_token'])
                if str(self.root/row['path'])!=s['path']:raise ValueError('Spool path/token mismatch')
                prior=c.execute('SELECT data FROM refs WHERE token=? AND ref=?',(row['token'],ref)).fetchone()
                if not prior:
                    if row['state']!='acquired':raise ValueError('Spool reference cannot adopt queued/unknown file')
                    c.execute('INSERT INTO refs VALUES(?,?,?)',(row['token'],ref,code({'state':'publication_pending','targets':expected,'future':bool(job.get('info',{}).get('future_comments')),'cleanup_requested':False})))
                elif json.loads(prior[0])['targets']!=expected:raise ValueError('Spool targets changed')
                if row['state']=='acquired':row['state']='referenced';self.put(c,row)
        for token in tokens:self.release(token)
        job['spool_ref']=ref;job['spool_targets']=[json.loads(v) for v in expected]
        return ref

    def start(self,job,targets,cleanup):
        self.bind(job,targets)
        with self.db() as c:
            for s in snapshots(job):
                r=c.execute('SELECT data FROM refs WHERE token=? AND ref=?',(s['spool_token'],job['spool_ref'])).fetchone();data=json.loads(r[0])
                data['cleanup_requested']=bool(cleanup)
                if data['state']!='terminal':data['state']='inflight'
                c.execute('UPDATE refs SET data=? WHERE token=? AND ref=?',(code(data),s['spool_token'],job['spool_ref']))

    def reconcile(self,c,now,limit):
        # Durable per-target receipts, not an expiring legacy journal, arbitrate
        # crash-after-delivery/before-cleanup. Future outbox refs need explicit
        # sender terminal release; canceled/uncertain refs remain recoverable.
        rows=c.execute("SELECT token,ref,data FROM refs WHERE json_extract(data,'$.state')!='terminal' AND json_extract(data,'$.cleanup_requested')=1 AND json_extract(data,'$.future')=0 AND (SELECT COUNT(*) FROM targets WHERE targets.ref=refs.ref AND json_extract(targets.data,'$.state')='complete')=json_array_length(refs.data,'$.targets') LIMIT ?",(limit,)).fetchall()
        for r in rows:
            data=json.loads(r['data'])
            if data['state']=='terminal' or data.get('future') or not data.get('cleanup_requested'):continue
            complete=True
            for target in data['targets']:
                receipt=c.execute('SELECT data FROM targets WHERE ref=? AND target=?',(r['ref'],target)).fetchone()
                if not receipt or json.loads(receipt[0])['state']!='complete':complete=False;break
            if not complete:continue
            data['state']='terminal';c.execute('UPDATE refs SET data=? WHERE token=? AND ref=?',(code(data),r['token'],r['ref']))
            row=self.row(c,r['token'])
            if row['state'] not in ('cleanup_pending','deleted','retained'):
                row.update(state='cleanup_pending' if row['shared_safe'] else 'retained',due=now+86400);self.put(c,row)

    def reference_state(self,job,state):
        with self.db() as c:
            for s in snapshots(job):
                row=self.row(c,s['spool_token']);r=c.execute('SELECT data FROM refs WHERE token=? AND ref=?',(row['token'],job['spool_ref'])).fetchone()
                if not r:raise ValueError('Missing durable publication intent')
                data=json.loads(r[0])
                if data['state']!='terminal':data['state']=state
                c.execute('UPDATE refs SET data=? WHERE token=? AND ref=?',(code(data),row['token'],job['spool_ref']))

    def begin_target(self,job,route,target):
        ref=job['spool_ref'];target=code([route,target])
        if [route,json.loads(target)[1]] not in job['spool_targets']:raise ValueError('Unexpected spool target')
        with self.db() as c:
            r=c.execute('SELECT data FROM targets WHERE ref=? AND target=?',(ref,target)).fetchone()
            if r:
                prior=json.loads(r[0])
                if prior['state']=='complete':return prior['receipt']
                raise RuntimeError('Uncertain/failed spool transport retained; reconcile before resend')
            c.execute('INSERT INTO targets VALUES(?,?,?)',(ref,target,code({'state':'inflight','created':time.time()})))
        return None

    def target_result(self,job,route,target,result):
        count=len(snapshots(job));fields=('sent_message_id','sent_message_ids','delivery_kind','message_thread_id','general_message_id','topic_message_id','general_message_ids','topic_message_ids','preserve_local_copy')
        receipt={k:getattr(result,k,None) for k in fields}
        complete=bool(result) and not receipt['preserve_local_copy'] and receipt['delivery_kind'] in ('file','album')
        if count==1:complete=complete and type(receipt['sent_message_id']) is int and receipt['sent_message_id']>0 and (not receipt['message_thread_id'] or bool(receipt['topic_message_id']))
        else:
            def ids(v):return isinstance(v,list) and len(v)==count and len(set(v))==count and all(type(i) is int and i>0 for i in v)
            complete=complete and ids(receipt['sent_message_ids']) and (not receipt['message_thread_id'] or ids(receipt['topic_message_ids'])) and (not receipt['general_message_ids'] or ids(receipt['general_message_ids']))
        with self.db() as c:
            c.execute('UPDATE targets SET data=? WHERE ref=? AND target=?',(code({'state':'complete' if complete else 'failed_recoverable','receipt':receipt}),job['spool_ref'],code([route,target])))
        return complete

    def complete(self,job):
        if not job.get('spool_ref') or not job.get('spool_targets'):return False
        with self.db() as c:
            for route,target in job['spool_targets']:
                row=c.execute('SELECT data FROM targets WHERE ref=? AND target=?',(job['spool_ref'],code([route,target]))).fetchone()
                if not row or json.loads(row[0])['state']!='complete':return False
            for s in snapshots(job):
                row=c.execute('SELECT data FROM refs WHERE token=? AND ref=?',(s['spool_token'],job['spool_ref'])).fetchone()
                if not row or json.loads(row[0])['targets']!=sorted(code(v) for v in job['spool_targets']):return False
        return True

    def finish(self,job,paths,grace=86400):
        if not self.complete(job):return False
        with self.db() as c:
            for s in snapshots(job):
                row=self.row(c,s['spool_token']);r=c.execute('SELECT data FROM refs WHERE token=? AND ref=?',(row['token'],job['spool_ref'])).fetchone();data=json.loads(r[0]);data['state']='terminal'
                c.execute('UPDATE refs SET data=? WHERE token=? AND ref=?',(code(data),row['token'],job['spool_ref']))
                if row['state'] not in ('cleanup_pending','deleted'):
                    row.update(state='cleanup_pending' if s['path'] in paths and row['shared_safe'] else 'retained',due=time.time()+grace)
                    self.put(c,row)
        return True

    def abandoned(self,c,now,limit,max_bytes,started,seconds):
        rows=c.execute("SELECT data FROM files WHERE json_extract(data,'$.state') IN ('acquiring','acquired') AND json_extract(data,'$.created')<? LIMIT ?",(now-72*3600,limit)).fetchall()
        count=0;bytes_=0
        for item in rows:
            if time.monotonic()-started>=seconds:break
            row=json.loads(item[0])
            if c.execute('SELECT 1 FROM refs WHERE token=?',(row['token'],)).fetchone():continue
            lease=os.open(self.guard()/(row['token']+'.lease'),os.O_RDWR|os.O_NOFOLLOW)
            opened=[]
            try:
                try:fcntl.flock(lease,fcntl.LOCK_EX|fcntl.LOCK_NB)
                except BlockingIOError:continue
                if bytes_+row['proof'][2]>max_bytes:continue
                size=0
                relative=row['path'] if row['state']=='acquired' else row['part']
                size=self.claim_remove(row,relative,strict=row['state']=='acquired',max_bytes=max_bytes-bytes_)
                row.update(state='deleted',deleted_at=now,reason='dead-lease-unpublished');self.put(c,row);count+=1;bytes_+=size
            except (OSError,ValueError) as exc:
                row.update(state='cleanup_uncertain',cleanup_error=type(exc).__name__,cleanup_reason=str(exc));self.put(c,row)
            finally:
                for parent,_ in opened:os.close(parent)
                os.close(lease)
        return count,bytes_

    def scan(self,*,limit=64,max_bytes=256*1024*1024,seconds=2,now=None):
        now=time.time() if now is None else now;started=time.monotonic();deleted=0;bytes_=0;errors=0
        lock=os.open(self.guard()/'gc.lock',os.O_CREAT|os.O_RDWR|os.O_NOFOLLOW,0o600)
        try:
            try:fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
            except BlockingIOError:return {'owner':False,'deleted':0,'bytes':0}
            with self.db() as c:
                self.reconcile(c,now,min(limit,64))
                abandoned,bytes_=self.abandoned(c,now,min(limit,16),max_bytes,started,seconds)
                rows=[json.loads(r[0]) for r in c.execute('SELECT data FROM files WHERE json_extract(data,\'$.state\')=\'cleanup_pending\' AND json_extract(data,\'$.due\')<=? ORDER BY rowid LIMIT ?',(now,max(0,min(limit,64)-abandoned)))]
            for old in rows:
                if time.monotonic()-started>=seconds or bytes_+old['proof'][2]>max_bytes:break
                if old.get('due',now+1)>now:continue
                try:
                    with self.db() as c:
                        row=self.row(c,old['token'])
                        if row['state']!='cleanup_pending' or not row['shared_safe']:continue
                        refs=[json.loads(v[0]) for v in c.execute('SELECT data FROM refs WHERE token=?',(row['token'],))]
                        if not refs or any(r['state']!='terminal' for r in refs):continue
                        bytes_+=self.claim_remove(row,row['path'])
                        row['state']='deleted';row['deleted_at']=now;self.put(c,row);deleted+=1
                except (OSError,ValueError,sqlite3.Error) as exc:
                    errors+=1
                    try:
                        with self.db() as c:
                            row=self.row(c,old['token']);row.update(due=now+3600,cleanup_error=True,cleanup_reason=str(exc))
                            if isinstance(exc,ClaimUncertain):row['state']='cleanup_uncertain'
                            self.put(c,row)
                    except (OSError,ValueError,sqlite3.Error):pass
            with self.db() as c:
                states={r[0]:{'files':r[1],'bytes':r[2]} for r in c.execute("SELECT json_extract(data,'$.state'),COUNT(*),SUM(json_extract(data,'$.proof[2]')) FROM files GROUP BY json_extract(data,'$.state')")}
            return {'known_states':states,'unknown_walk':False,'owner':True,'deleted':deleted,'bytes':bytes_,'errors':errors,'abandoned':abandoned,'scanned':len(rows),'complete':len(rows)<min(limit,64),'at':now}
        finally:os.close(lock)


async def publish_intent(job,targets):
    if owned(job):await _io(manager().bind,job,targets)

async def begin_job(job,ctx):
    if not owned(job):return False
    targets=[]
    for name in job['route_names']:
        route=ctx.route_map.get(name)
        if not route:raise ValueError('Owned spool route removed; retained')
        targets.extend((name,t) for t in route['targets'])
    if job.get('spool_targets') is not None and sorted(code(v) for v in job['spool_targets'])!=sorted(code(v) for v in targets):raise ValueError('Owned spool target policy changed; retained')
    await _io(manager().start,job,targets,ctx.delete_after_send)
    return await completed(job)

async def target_send(job,route,target,send):
    if not owned(job):return await send()
    m=manager();prior=await _io(m.begin_target,job,route,target)
    if prior:return SimpleNamespace(**prior)
    result=await send()
    if not await _io(m.target_result,job,route,target,result):raise RuntimeError('Incomplete spool target receipt; recoverable file retained')
    return result

async def finish(job,paths):
    if not owned(job):return False
    return await _io(manager().finish,job,paths)

async def completed(job):
    return owned(job) and await _io(manager().complete,job)

async def gc_loop(log,interval=3600):
    while True:
        try:log({'type':'spool_gc',**await _io(manager().scan)})
        except (OSError,ValueError,sqlite3.Error) as exc:log({'type':'spool_gc_error','error':type(exc).__name__})
        await asyncio.sleep(interval)


async def classify(job,state):
    if owned(job) and job.get('spool_ref'):await _io(manager().reference_state,job,state)
