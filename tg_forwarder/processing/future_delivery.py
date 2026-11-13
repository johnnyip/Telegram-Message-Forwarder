"""Future-only validation and durable evidence; legacy delivery semantics are unchanged."""
import asyncio
import hashlib
import json
import os
import stat
from pathlib import Path
from ..shared_media import configured_bridge, digest
from ..storage.edit_mapping import get_redis, REDIS_PREFIX

PREFIX = REDIS_PREFIX + ':future_comments:'
BLOCKED = PREFIX + 'blocked'


def snapshots(job):
    if job.get('job_type') == 'media_file':
        return [job['snapshot']]
    if job.get('job_type') == 'media_album_file':
        return job['snapshots']
    raise ValueError('Future comments require prepared photo files')


def selection(job):
    context = job['info']['future_comments']
    expected = context['expected']
    values = snapshots(job)
    if not expected or len(expected) > 10 or len({digest(i) for i in expected}) != len(expected):
        raise ValueError('Invalid future selection')
    if [s.get('shared_identity') for s in values] != expected:
        raise ValueError('Incomplete/revised future photo selection')
    if any(s.get('media_type') != 'photo' for s in values):
        raise ValueError('Future comments are photo-only')
    return values


def file_proof(path):
    path = Path(path)
    if path.is_symlink():
        raise ValueError('Unsafe future spool')
    with os.fdopen(os.open(path,os.O_RDONLY | os.O_NOFOLLOW),'rb') as stream:
        before = os.fstat(stream.fileno())
        if not stat.S_ISREG(before.st_mode) or before.st_size <= 0:
            raise ValueError('Invalid future spool')
        h = hashlib.sha256()
        for chunk in iter(lambda: stream.read(1024*1024), b''):
            h.update(chunk)
        after = os.fstat(stream.fileno())
        if (before.st_dev,before.st_ino,before.st_size,before.st_mtime_ns) != (after.st_dev,after.st_ino,after.st_size,after.st_mtime_ns):
            raise ValueError('Future spool changed during verification')
        return {'size':after.st_size,'sha256':h.hexdigest()}


async def seal(job):
    for item in selection(job):
        item['future_proof'] = await asyncio.to_thread(file_proof,item['path'])


async def validate_files(job):
    for item in selection(job):
        if item.get('future_proof') != await asyncio.to_thread(file_proof,item['path']):
            raise ValueError('Missing/revised future spool; reconcile before resuming')


def evidence_key(job):
    return PREFIX + 'delivery:' + digest({'context':job['info']['future_comments'], 'routes':job['route_names']})


async def record(job, *, redis=None):
    redis = redis or await get_redis()
    if redis is None:
        raise RuntimeError('Future delivery state unavailable')
    raw = await redis.get(evidence_key(job))
    if not raw:
        raise RuntimeError('Future prepared evidence missing')
    return redis,json.loads(raw)


EVIDENCE_CAS = """-- future_evidence_cas_v1
if redis.call('GET',KEYS[1]) == ARGV[1] then
    redis.call('SET',KEYS[1],ARGV[2]); return 1
end
return 0
"""


async def transition(redis, key, status, *, expected_job=None):
    """CAS the latest evidence, never an independently read cancellation snapshot.

    Only a successful prepared->in_flight transition authorizes transport. A
    cancellation after handoff records intent without erasing uncertainty; actual
    confirmed completion (only called after receipt proof) dominates cancellation.
    Payload bytes are immutable; a canceled record can never reacquire transport.
    """
    if status not in {'in_flight','complete','canceled'}:
        raise ValueError('Invalid future evidence transition')
    binding = expected_job
    for _ in range(8):
        raw = await redis.get(key)
        if not raw:
            raise RuntimeError('Future prepared evidence missing')
        value = json.loads(raw)
        if binding is None:
            binding = value.get('job')
        if binding is None or value.get('job') != binding:
            raise RuntimeError('Future prepared payload changed')
        current = value.get('status')
        if current not in {'prepared','in_flight','complete','canceled'}:
            raise RuntimeError('Invalid future evidence state')
        updated = dict(value)
        if current == 'complete' or current == status:
            return {'accepted':False,'status':current,'value':value}
        if status == 'in_flight':
            if current != 'prepared':
                return {'accepted':False,'status':current,'value':value}
            updated.update(status='in_flight',previous_status='prepared')
        elif status == 'canceled' and current == 'in_flight':
            # Handoff already won. Do not turn uncertainty into "never sent".
            if value.get('cancel_requested'):
                return {'accepted':False,'status':current,'value':value}
            updated['cancel_requested'] = True
        else:
            updated.update(status=status,previous_status=current)
        encoded = json.dumps(updated,sort_keys=True)
        if await redis.eval(EVIDENCE_CAS,1,key,raw,encoded):
            return {'accepted':True,'status':updated['status'],'value':updated}
    raise RuntimeError('Future evidence contention; reconciliation required')


async def terminal(job, status):
    redis = await get_redis()
    if redis is None:
        raise RuntimeError('Future delivery state unavailable')
    return await transition(redis,evidence_key(job),status)


async def receipts_complete(job, ctx):
    """Use exactly legacy_send's operation codec; never treat empty dispatch as success."""
    values = selection(job)
    bridge = await configured_bridge()
    if bridge is None:
        return False
    info = job['info']
    caption = (ctx.build_header_from_info(info,is_edit=False)+'\n'+values[0].get('caption_text','').strip()).strip()
    topic = {'legacy_sender':info.get('sender_id'), 'policy':os.getenv('FORUM_TOPIC_DELIVERY_MODE','mirror'),'edit_revision':None}
    targets = []
    for name in job['route_names']:
        route = ctx.route_map.get(name)
        if not route:
            return False
        targets.extend(route['targets'])
    if not targets:
        return False
    for target in targets:
        operation = bridge.operation([s['shared_identity'] for s in values],target,topic,
                                     os.getenv('MEDIA_DELIVERY_IDENTITY'), action='upload',caption=caption)
        receipt = await bridge.receipt(operation)
        if not receipt or receipt.get('preserve_local_copy') or receipt.get('delivery_kind') not in {'file','album'}:
            return False
        count = len(values)
        def complete(ids):
            return isinstance(ids,list) and len(ids)==count and len(set(ids))==count and all(type(i) is int and i>0 for i in ids)
        if receipt.get('delivery_kind') == 'album':
            if not complete(receipt.get('sent_message_ids')):
                return False
            if receipt.get('message_thread_id') and not complete(receipt.get('topic_message_ids')):
                return False
            if receipt.get('general_message_ids') and not complete(receipt['general_message_ids']):
                return False
        elif count != 1 or not receipt.get('sent_message_id'):
            return False
        elif receipt.get('message_thread_id') and not receipt.get('topic_message_id'):
            return False
    return True


async def block(reason, *, redis=None):
    redis = redis or await get_redis()
    if redis is None:
        raise RuntimeError('Future blocked state unavailable')
    await redis.set(BLOCKED,json.dumps(reason,sort_keys=True),nx=True)


async def metadata_ready(client, topic):
    # All-topics metadata cannot request creation of an absent named topic.
    metadata = await asyncio.wait_for(client.fetch_all_metadata(),20)
    if topic not in metadata.topics() or not metadata.partitions_for_topic(topic):
        raise RuntimeError('Future Kafka topic absent/inaccessible; operator provisioning required')
