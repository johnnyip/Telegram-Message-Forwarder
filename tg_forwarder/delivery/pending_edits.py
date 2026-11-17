"""Durable edit outbox on the existing guarded spool ledger.
No source re-download/forward. Retain events for late fanout mappings; per-copy
idempotence and ambiguous-reply fencing are owned by edit_updates.
"""
import asyncio
import hashlib
import json
import time
from ..spool_lifecycle import manager
from ..shared_media import _io
from .edit_updates import edit_forwarded_text, edit_forwarded_media_caption, edit_forwarded_album_caption, _copy_ids


def table(c):
    c.execute("CREATE TABLE IF NOT EXISTS pending_edits (id TEXT PRIMARY KEY,payload TEXT NOT NULL,created REAL NOT NULL,expires REAL NOT NULL,due REAL NOT NULL,state TEXT NOT NULL)")
    c.execute("CREATE INDEX IF NOT EXISTS pending_edits_due ON pending_edits(state,due)")
    c.execute("CREATE INDEX IF NOT EXISTS pending_edits_expiry ON pending_edits(state,expires)")


def enqueue(info, text, kind, routes, now=None):
    now = time.time() if now is None else now
    payload = {'info': info, 'text': text, 'kind': kind, 'routes': routes}
    raw = json.dumps(payload, sort_keys=True, default=str)
    identity = hashlib.sha256(raw.encode()).hexdigest()
    with manager().db() as c:
        table(c)
        c.execute("INSERT OR IGNORE INTO pending_edits VALUES (?,?,?,?,?,'pending')", (identity,raw,now,now+86400,now))
    return identity


def pending(now=None):
    now = time.time() if now is None else now
    with manager().db() as c:
        table(c)
        expired = c.execute("UPDATE pending_edits SET state='expired' WHERE state='pending' AND expires<=?", (now,)).rowcount
        c.execute("DELETE FROM pending_edits WHERE state!='pending' AND expires<?", (now-7*86400,))
        rows = c.execute("SELECT id,payload FROM pending_edits WHERE state='pending' AND due<=? ORDER BY created,id LIMIT 16", (now,)).fetchall()
    return rows,expired


def defer(identity, state='pending', now=None):
    now = time.time() if now is None else now
    with manager().db() as c:
        c.execute("UPDATE pending_edits SET due=?,state=? WHERE id=?", (now+30,state,identity))


def route_signature(routes, chat, skip_types=(), ignore_users=(), ignore_ids=()):
    """Snapshot the effective edit-forwarding policy for safe durable replay."""
    return {
        'routes': sorted([[r['name'], sorted(r['targets'])] for r in routes if chat in r.get('sources',[])]),
        'skip_types': sorted(str(value) for value in skip_types),
        'ignore_users': sorted(str(value) for value in ignore_users),
        'ignore_ids': sorted(int(value) for value in ignore_ids),
    }


def acknowledged(mapping,event):
    route_data=event['routes'].get('routes',[]) if isinstance(event['routes'],dict) else event['routes']
    needed={str(t) for _,targets in route_data for t in targets}
    targets=mapping.get('targets',{})
    if not needed or not needed.issubset(targets):return False
    for target in needed:
        payload=targets[target];ids=_copy_ids(payload)
        if not ids:return False
        for mid in ids:
            progress=payload.get('edit_progress',{}).get(str(mid),{})
            if progress.get('source_text',payload.get('source_text'))==event['text']:continue
            # A newer applied revision supersedes an older queued event.
            old=progress.get('edit_date','');new=event['info'].get('edit_date','')
            try:
                from datetime import datetime
                if old and new and datetime.fromisoformat(old)>=datetime.fromisoformat(new):continue
            except (ValueError,TypeError):pass
            return False
    return True


async def replay_once(bot, log, routes, original_fetcher, skip_types=(), ignore_users=(), ignore_ids=()):
    from .edit_updates import get_message_mapping, get_album_mapping
    rows,expired = await _io(pending)
    if expired:
        log({'type':'warn','op':'pending_edits_expired','count':expired})
    async def run_event(identity,raw):
        try:
            event = json.loads(raw); info = event['info']; kind = event['kind']
            if route_signature(routes,info['chat_id'],skip_types,ignore_users,ignore_ids) != event['routes']:
                await _io(defer, identity, 'policy_changed')
                log({'type':'warn','op':'pending_edit_policy_changed','event':identity})
                return
            mapping = await (get_album_mapping(info['chat_id'],info['grouped_id']) if kind=='album' else get_message_mapping(info['chat_id'],info['msg_id']))
            if mapping.get('targets'):
                # Never edit unexpected destinations left by an obsolete route.
                route_data=event['routes'].get('routes',[]) if isinstance(event['routes'],dict) else event['routes']
                allowed={str(t) for _,targets in route_data for t in targets}
                if not set(mapping['targets']).issubset(allowed):
                    await _io(defer,identity,'policy_changed')
                    return
                fn={'text':edit_forwarded_text,'caption':edit_forwarded_media_caption,'album':edit_forwarded_album_caption}[kind]
                await fn(bot,info,event['text'],log,original_fetcher=original_fetcher)
                current=await (get_album_mapping(info['chat_id'],info['grouped_id']) if kind=='album' else get_message_mapping(info['chat_id'],info['msg_id']))
                if acknowledged(current,event):
                    await _io(defer,identity,'done')
                    log({'type':'info','op':'pending_edit_done','event':identity})
                    return
            # Keep pending after success: another fanout target may publish its
            # mapping later. Per-copy stored progress makes revisits no-ops.
            await _io(defer, identity)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log({'type':'warn','op':'pending_edit_retry','event':identity,'error':type(exc).__name__})
            await _io(defer, identity)

    groups={}
    for identity,raw in rows:
        try:
            event=json.loads(raw);info=event['info']
            key=(info['chat_id'], event['kind']=='album', info.get('grouped_id') if event['kind']=='album' else info['msg_id'])
        except (ValueError,KeyError,TypeError):key=identity
        groups.setdefault(key,[]).append((identity,raw))
    gate=asyncio.Semaphore(4)
    async def lane(events):
        async with gate:
            for identity,raw in events:await run_event(identity,raw)
    outcomes=await asyncio.gather(*(lane(events) for events in groups.values()),return_exceptions=True)
    for outcome in outcomes:
        if isinstance(outcome,asyncio.CancelledError):raise outcome
        if isinstance(outcome,Exception):log({'type':'warn','op':'pending_edit_lane_retry','error':type(outcome).__name__})


async def replay_loop(bot,log,routes,original_fetcher,skip_types=(),ignore_users=(),ignore_ids=()):
    while True:
        try:
            await replay_once(bot,log,routes,original_fetcher,skip_types,ignore_users,ignore_ids)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log({'type':'warn','op':'pending_edit_worker_retry','error':type(exc).__name__})
        await asyncio.sleep(5)
