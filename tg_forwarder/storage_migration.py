"""Operator-only offline, explicit-plan migration; dry-run by default. No source calls.

Apply requires quiescing every reader/writer. It takes the existing control state lock;
never run against a live control service. No task/operation/receipt payload is rewritten.
"""
import argparse
import asyncio
import fcntl
import hashlib
import errno
import stat
import json
import os
from pathlib import Path
import sqlite3
import tempfile
from .canonical_media import secure_path, fingerprint, relative
from .control.filesystems import guard_state
from .control.store import digest as cache_digest
from .shared_media import _io, _owned, configured_bridge, digest


def atomic_json(path, value):
    path=Path(path)
    if path.is_symlink():raise ValueError('Metadata symlink')
    path.parent.mkdir(parents=True,exist_ok=True,mode=0o700)
    fd,temp=tempfile.mkstemp(prefix='.migration-',dir=path.parent)
    try:
        with os.fdopen(fd,'w') as stream:
            json.dump(value,stream,ensure_ascii=False,sort_keys=True)
            stream.flush();os.fsync(stream.fileno())
        os.replace(temp,path)
        sync_dir(path.parent)
    finally:
        if os.path.exists(temp):os.unlink(temp)


def sync_dir(path):
    fd=os.open(path,os.O_RDONLY|os.O_DIRECTORY)
    try:os.fsync(fd)
    finally:os.close(fd)


def rewrite(value, replacements):
    if isinstance(value,dict):
        return {k:(replacements.get(v,v) if k=="path" and isinstance(v,str) else rewrite(v,replacements)) for k,v in value.items()}
    if isinstance(value,list):return [rewrite(v,replacements) for v in value]
    return value


def has_old_path(value, old):
    if isinstance(value,dict):
        return any((k=="path" and isinstance(v,str) and v in old) or has_old_path(v,old) for k,v in value.items())
    return isinstance(value,list) and any(has_old_path(v,old) for v in value)


class Migration:
    def __init__(self, plan):
        self.plan=plan
        if plan.get('version')!=1 or plan.get('owner_uid')!=5268216341:raise ValueError('Explicit owner/version required')
        selected=plan['selected']
        self.identity=[str(selected['chat']),int(selected['id']),str(selected['media_id'])]
        if self.identity!=plan['identity']:raise ValueError('Identity mismatch')
        if len(plan['sha256'])!=64 or any(c not in '0123456789abcdef' for c in plan['sha256']):raise ValueError('Explicit SHA256 required')
        self.key=cache_digest([selected['chat'],selected['id'],selected['media_id']])
        self.old_root=Path(plan['old_root']).absolute()
        self.root=Path(plan['canonical_root']).absolute()
        self.shared_root=Path(plan['shared_root']).absolute()
        self.db=Path(plan['db']).absolute()
        self.destination=secure_path(self.root,relative(selected,plan.get('timezone','UTC')))
        self.old=secure_path(self.old_root,Path(plan['old_archive']).absolute().relative_to(self.old_root))
        self.shared=secure_path(self.shared_root,digest(self.identity))
        if str(self.shared)!=plan['shared_object']:raise ValueError('Shared object is not exactly this identity')
        if self.destination in (self.old,self.shared):raise ValueError('Migration destination must be distinct')
        self.journal=self.db.parent/('storage-migration-'+digest(plan)+'.json')
        self.manifests=[]
        self.relocated_manifests={}
        self.metadata_proofs={}
        if self.journal.exists():
            secure_path(self.db.parent,self.journal.name,existing=True)
            journal=json.loads(self.journal.read_text())
            if journal.get('plan')!=plan:raise ValueError('Migration journal plan mismatch')
            self.metadata_proofs=journal.get('metadata_proofs',{})
        if not plan.get('task_ids'):raise ValueError('Explicit task IDs required')
        if len(plan.get('manifests',[]))>1000:raise ValueError('Manifest batch bound')
        changes=self.path_changes()
        for name in plan.get('manifests',[]):
            old=Path(name).absolute()
            secure_path(self.old_root,old.relative_to(self.old_root))
            if old.name not in {'manifest_'+tid+'.json' for tid in plan['task_ids']}:
                raise ValueError('Manifest filename must match an explicitly verified task')
            target=secure_path(self.root,Path('.metadata')/old.name)
            if target in self.relocated_manifests.values():raise ValueError('Duplicate manifest destination')
            self.manifests.append(old);self.relocated_manifests[old]=target
            proof=self.metadata_proofs.get(str(old))
            if old.exists():
                secure_path(self.old_root,old.relative_to(self.old_root),existing=True)
                value=json.loads(old.read_text())
                candidate={'source_sha256':fingerprint(old)['sha256'],
                           'target_sha256':hashlib.sha256(self.manifest_bytes(rewrite(value,changes))).hexdigest()}
                if proof is not None and proof!=candidate:raise ValueError('Original manifest changed')
                self.metadata_proofs[str(old)]=candidate
            elif proof is None:
                raise ValueError('Missing original manifest without durable relocation proof')
        self.prune_dirs=[]
        if len(plan.get('prune_dirs',[]))>32:raise ValueError('Directory cleanup batch bound')
        for name in plan.get('prune_dirs',[]):
            directory=Path(name).absolute()
            rel=directory.relative_to(self.old_root)
            if not rel.parts or len(rel.parts)>16:raise ValueError('Cannot prune root/unbounded ancestry')
            secure_path(self.old_root,rel)
            if not any(directory in old.parents for old in [self.old,*self.manifests]):
                raise ValueError('Prune directory is not a planned old-file ancestor')
            self.prune_dirs.append(directory)
        self.verify_metadata(require_relocated=False)

    def path_changes(self):
        return {str(self.old):str(self.destination),str(self.old.relative_to(self.old_root)):str(self.destination.relative_to(self.root))}

    @staticmethod
    def manifest_bytes(value):
        return json.dumps(value,ensure_ascii=False,sort_keys=True).encode('utf-8')

    def verify_metadata(self, *, require_relocated):
        for old,target in self.relocated_manifests.items():
            proof=self.metadata_proofs[str(old)]
            secure_path(self.old_root,old.relative_to(self.old_root))
            secure_path(self.root,target.relative_to(self.root))
            if old.exists():
                secure_path(self.old_root,old.relative_to(self.old_root),existing=True)
                if fingerprint(old)['sha256']!=proof['source_sha256']:raise ValueError('Original manifest changed')
            if target.exists():
                secure_path(self.root,target.relative_to(self.root),existing=True)
                if fingerprint(target)['sha256']!=proof['target_sha256']:raise ValueError('Relocated manifest collision/hash mismatch')
            elif require_relocated or not old.exists():
                raise ValueError('Verified relocated manifest is missing')

    def verify(self):
        guard_state(self.db)
        self.verify_metadata(require_relocated=False)
        available=[]
        for path in (self.old,self.shared,self.destination):
            root=self.root if path==self.destination else self.old_root if path==self.old else self.shared_root
            secure_path(root,path.relative_to(root))
            if path.exists():
                secure_path(root,path.relative_to(root),existing=True)
                result=fingerprint(path)
                if result['sha256']!=self.plan['sha256']:raise ValueError('Hash mismatch; no changes permitted')
                available.append(path)
        if not available:raise ValueError('No verified copy remains')
        with sqlite3.connect('file:'+str(self.db)+'?mode=ro',uri=True) as db:
            row=db.execute('SELECT path,size FROM cache WHERE key=?',(self.key,)).fetchone()
            if row is None or row[0] not in (str(self.old),str(self.destination)):raise ValueError('Cache row differs from explicit plan')
            if fingerprint(available[0])['size']!=row[1]:raise ValueError('Cache size mismatch')
            for tid in self.plan['task_ids']:
                task=db.execute('SELECT owner,payload FROM tasks WHERE id=?',(tid,)).fetchone()
                if task is None or task[0]!=self.plan['owner_uid']:raise ValueError('Task owner mismatch')
                items=json.loads(task[1])['items']
                if not any([str(i['chat']),i['id'],str(i['media_id'])]==self.identity for i in items):raise ValueError('Task source mismatch')
        return {'destination':str(self.destination),'copies':len(available),'sha256':self.plan['sha256'],'apply':False}

    def publish(self):
        self.verify()
        if self.destination.exists():return
        self.destination.parent.mkdir(parents=True,exist_ok=True)
        secure_path(self.root,self.destination.relative_to(self.root))
        source=self.old if self.old.exists() else self.shared
        # Same-filesystem atomic move workflow. No cross-filesystem copy fallback.
        try:os.link(source,self.destination,follow_symlinks=False)
        except FileExistsError:pass
        if fingerprint(self.destination)['sha256']!=self.plan['sha256']:raise ValueError('Publish collision')
        with self.destination.open('rb') as stream:os.fsync(stream.fileno())
        sync_dir(self.destination.parent)

    def update_db(self):
        self.verify()
        with sqlite3.connect(self.db) as db:
            db.execute('BEGIN IMMEDIATE')
            db.execute('UPDATE cache SET path=? WHERE key=? AND path IN (?,?)',
                (str(self.destination),self.key,str(self.old),str(self.destination)))

    def update_manifests(self):
        self.verify_metadata(require_relocated=False)
        for old,target in self.relocated_manifests.items():
            if target.exists():continue
            value=rewrite(json.loads(old.read_text()),self.path_changes())
            data=self.manifest_bytes(value)
            if hashlib.sha256(data).hexdigest()!=self.metadata_proofs[str(old)]['target_sha256']:
                raise ValueError('Relocation bytes changed')
            target.parent.mkdir(parents=True,exist_ok=True,mode=0o700)
            secure_path(self.root,target.relative_to(self.root))
            fd,temp=tempfile.mkstemp(prefix='.manifest-migration-',dir=target.parent)
            try:
                with os.fdopen(fd,'wb') as stream:
                    stream.write(data);stream.flush();os.fsync(stream.fileno())
                try:os.link(temp,target,follow_symlinks=False)
                except FileExistsError:pass
                sync_dir(target.parent)
                self.verify_metadata(require_relocated=False)
            finally:
                os.unlink(temp)
        self.verify_metadata(require_relocated=True)

    def assert_references(self):
        self.verify()
        with sqlite3.connect('file:'+str(self.db)+'?mode=ro',uri=True) as db:
            if db.execute('SELECT path FROM cache WHERE key=?',(self.key,)).fetchone()[0]!=str(self.destination):raise ValueError('Canonical DB reference missing')
            if db.execute('SELECT 1 FROM cache WHERE path IN (?,?)',(str(self.old),str(self.shared))).fetchone():raise ValueError('Old cache reference still exists')
        self.verify_metadata(require_relocated=True)
        for path in self.relocated_manifests.values():
            value=json.loads(path.read_text())
            if has_old_path(value,{str(self.old),str(self.old.relative_to(self.old_root))}):raise ValueError('Old manifest reference still exists')

    def retire(self):
        self.assert_references()
        for old in self.manifests:
            if old.exists():
                self.verify_metadata(require_relocated=True)
                old.unlink();sync_dir(old.parent)
        for path in (self.old,self.shared):
            if path.exists():
                if fingerprint(path)['sha256']!=self.plan['sha256']:raise ValueError('Retirement hash changed')
                secure_path(self.root,self.destination.relative_to(self.root),existing=True)
                if fingerprint(self.destination)['sha256']!=self.plan['sha256']:raise ValueError('Canonical copy unverified')
                path.unlink();sync_dir(path.parent)
        for directory in sorted(set(self.prune_dirs),key=lambda p:len(p.parts),reverse=True):
            secure_path(self.old_root,directory.relative_to(self.old_root))
            if not directory.exists():continue
            info=directory.lstat()
            if not stat.S_ISDIR(info.st_mode) or info.st_uid not in (os.getuid(),self.old_root.stat().st_uid):
                raise ValueError('Prune directory is not owned')
            with os.scandir(directory) as entries:
                if next(entries,None) is not None:continue
            if directory.lstat().st_ino!=info.st_ino:raise ValueError('Prune directory changed')
            try:directory.rmdir()
            except OSError as exc:
                if exc.errno not in (errno.ENOTEMPTY,errno.EEXIST):raise
            else:sync_dir(directory.parent)

    async def run(self, *, apply=False, quiescent=False, retire_old=False, bridge=None, crash_after=None):
        if not apply:return await _io(self.verify)
        if not quiescent or bridge is None:raise ValueError('Apply requires quiescence and configured shared index')
        async def commit():
            await _io(self.verify)
            lockpath=Path(str(self.db)+'.lock')
            if lockpath.is_symlink():raise ValueError('State lock symlink')
            lock=await _io(lambda:lockpath.open('a'))
            try:
                await _io(lambda:fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB))
                bridge.register_root('control',self.root)
                async def phase(name):
                    await _io(atomic_json,self.journal,{'plan':self.plan,'phase':name,'metadata_proofs':self.metadata_proofs})
                    if crash_after==name:raise RuntimeError('Simulated phase interruption')
                await phase('validated')
                await _io(self.publish);await phase('published')
                await _io(self.update_db);await phase('db_updated')
                await _io(self.update_manifests);await phase('metadata_relocated');await phase('manifests_updated')
                raw=await bridge.redis.get(bridge.prefix+'source:'+digest(self.identity))
                if raw:
                    entry=json.loads(raw)
                    if entry.get('v',1)==1 and entry.get('path')!=self.shared.name:raise ValueError('Unexpected old shared reference')
                    if entry.get('v')==2 and (entry.get('root')!='control' or entry.get('path')!=str(self.destination.relative_to(self.root))):raise ValueError('Unexpected canonical shared reference')
                await bridge.publish_reference(self.identity,self.destination)
                await phase('index_updated')
                await _io(self.assert_references)
                if await bridge.cached(self.identity)!=self.destination:raise ValueError('Canonical shared lookup failed')
                await phase('verified')
                if retire_old:
                    await _io(self.retire);await phase('retired')
                return {'destination':str(self.destination),'apply':True,'retired':retire_old}
            finally:await _io(lock.close)
        return await _owned(commit())


def cli():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--plan',required=True)
    parser.add_argument('--apply',action='store_true')
    parser.add_argument('--quiescent',action='store_true')
    parser.add_argument('--retire-old',action='store_true')
    args=parser.parse_args()
    async def main():
        plan=await _io(lambda:json.loads(Path(args.plan).read_text()))
        migration=await _io(Migration,plan)
        if args.apply:
            from .control.runtime import environment
            await _io(environment)  # Same protected-file/no-override policy; no client or Store.
        bridge=await configured_bridge() if args.apply else None
        result=await migration.run(apply=args.apply,quiescent=args.quiescent,retire_old=args.retire_old,bridge=bridge)
        print(json.dumps(result,ensure_ascii=False))
    asyncio.run(main())


if __name__=='__main__':cli()
