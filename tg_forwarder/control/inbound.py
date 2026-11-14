"""Received Bot API photos, not original-post references. No network or auth ownership."""
import base64
import hashlib
import re
import struct
import json
import time
from .store import digest, encode
from .config import actor, positive

OWNER = 5268216341
CLOUD_DOWNLOAD_LIMIT = 20_000_000


def photo_id_from_file_id(value):
    """Independent bounded reader of Telegram photo-ID wire layout.

    Legacy layout cross-checked against Pyrogram v2.0.106 file_id.py;
    compact photo layout against pinned TDLib below:
    zero-RLE/base64, type/DC, optional TL bytes, media ID/hash, photo suffix.
    Only known v2/v4 photo forms are accepted; no credential/client creation.
    This is a metadata decoder, NOT a way to grant MTProto file access.
    """
    try:
        if not isinstance(value, str) or not re.fullmatch(r'[A-Za-z0-9_-]{8,2048}', value):
            return None
        packed = base64.b64decode(value + '=' * (-len(value) % 4), altchars=b'-_', validate=True)
        raw = bytearray(); n = 0
        while n < len(packed):
            byte = packed[n]; n += 1
            if byte:
                raw.append(byte)
            else:
                if n == len(packed) or not packed[n]:return None
                raw.extend(b'\0' * packed[n]); n += 1
            if len(raw) > 4096:return None
        if len(raw) < 25:return None
        major = raw[-1]
        if major not in (2,4):return None
        minor = raw[-2] if major == 4 else 0
        body = raw[:-2] if major == 4 else raw[:-1]
        flags, dc = struct.unpack_from('<ii', body)
        if flags & ~(2 | (1 << 25)) or flags & 0xff != 2 or not 1 <= dc <= 5:return None
        offset = 8
        if flags & (1 << 25):
            length = body[offset]; offset += 1
            if length == 254:
                length = int.from_bytes(body[offset:offset+3], 'little'); offset += 3
                header = 4
            elif length < 254:header = 1
            else:return None
            offset += length + (-(length + header) % 4)
        media_id, = struct.unpack_from('<q', body, offset)
        # ID/hash + volume + source (v4) + supported photo-location payload.
        # TDLib Version::RemovePhotoVolumeAndLocalId == 32 removes BOTH
        # volume_id before source and local_id after it. For a thumbnail,
        # the modern suffix is exactly source/file_type/size (3 int32).
        # tdlib/td c8828d50f5eac2fc7089ce1089d7ce9095c61b5e:
        # FileLocation.hpp:51-64, PhotoSizeSource.hpp:35-44, Version.h:48.
        if major == 4 and minor >= 32:
            if len(body) != offset + 28:return None
            source, file_type, size = struct.unpack_from('<iii', body, offset+16)
            if source != 1 or file_type != 2 or not 32 <= size <= 126:return None
            return media_id if media_id > 0 else None
        tail = offset + 24
        if major == 2:
            if len(body) != tail + 12:return None
        else:
            source, = struct.unpack_from('<i', body, tail)
            if source not in (0,1) or len(body) != tail + 16:return None
            if source == 1:
                file_type, size, _ = struct.unpack_from('<iii', body, tail+4)
                if file_type != 2 or not 32 <= size <= 126:return None
        return media_id if media_id > 0 else None
    except (ValueError, IndexError, struct.error):
        return None


class ReceivedMediaLimit(ValueError):
    pass


def video_header(value):
    """Type/DC header only; NOT an immutable media-ID or bot-principal decoder."""
    try:
        if not isinstance(value,str) or not re.fullmatch(r'[A-Za-z0-9_-]{8,2048}',value):return None
        packed=base64.b64decode(value+'='*(-len(value)%4),altchars=b'-_',validate=True)
        raw=bytearray();n=0
        while n<len(packed):
            byte=packed[n];n+=1
            if byte:raw.append(byte)
            else:
                if n==len(packed) or not packed[n]:return None
                raw.extend(b'\0'*packed[n]);n+=1
            if len(raw)>4096:return None
        if len(raw)<25 or raw[-1] not in (2,4):return None
        flags,dc=struct.unpack_from('<ii',raw)
        if flags & ~(4|(1<<25)) or flags & 0xff!=4 or not 1<=dc<=5:return None
        return {'type':4,'dc':dc,'major':raw[-1]}
    except (ValueError,IndexError,struct.error):return None


def video_binding(item):
    return digest([item['chat'],item['bot_id'],item['update_id'],item['id'],'video',item['file_id'],item['file_unique_id'],item['size'],item['video_meta']])


def validate_video(item,bot_id):
    if (item.get('kind')!='video' or item.get('source_kind')!='bot-inbound' or item.get('chat')!=OWNER or item.get('bot_id')!=bot_id
            or item.get('media_id')!='bot-inbound-video:'+str(bot_id)+':'+str(item.get('file_unique_id'))
            or video_header(item.get('file_id')) is None or item.get('received_binding')!=video_binding(item)):
        raise PermissionError('Received video typed/principal/file binding mismatch')
    if item.get('size') is not None and (type(item['size']) is not int or not 0<item['size']<=CLOUD_DOWNLOAD_LIMIT):raise ReceivedMediaLimit('收到影片超過 Bot API 20 MB 下載上限')


def video_payload(update,allowed,bot_id):
    message=update.message;owner=actor(message,allowed)
    if owner!=OWNER or message.chat.id!=owner or message.chat.type!='private':raise PermissionError('Received video requires owner private chat')
    positive(bot_id);positive(update.update_id);positive(message.message_id,2**31-1)
    video=message.video
    if video is None or not video.file_unique_id or len(video.file_unique_id)>512 or video_header(video.file_id) is None:raise ValueError('Invalid typed received video header/identity')
    if video.file_size is not None and (video.file_size<=0 or video.file_size>CLOUD_DOWNLOAD_LIMIT):raise ReceivedMediaLimit('收到影片超過 Bot API 20 MB 下載上限')
    receiving=(getattr(message.chat,'title',None) or getattr(message.chat,'full_name',None) or ' '.join(v for v in (getattr(message.from_user,'first_name',None),getattr(message.from_user,'last_name',None)) if v) or getattr(message.chat,'username',None) or '私人對話')
    meta={'title':str(owner),'folder_title':receiving,'folder_chat':owner,'datetime':message.date.isoformat(),'extension':'.mp4'}
    source=getattr(getattr(message,'forward_origin',None),'chat',None)
    if source is not None and type(getattr(source,'id',None)) is int and getattr(source,'type',None) in ('channel','supergroup','group') and getattr(source,'title',None):meta.update(folder_title=source.title,folder_chat=source.id,origin_chat_id=source.id,origin_title=source.title)
    original=video.file_name
    if original:
        if len(original)>1024 or '\0' in original:raise ValueError('Invalid received video filename')
        meta['filename']=original
    else:meta['filename']='video_'+str(message.message_id)+'.mp4'
    duration=video.duration.total_seconds() if hasattr(video.duration,'total_seconds') else video.duration
    dimensions={'width':video.width,'height':video.height,'duration':duration,'mime_type':video.mime_type,'file_name':original}
    if any(type(v) is not int or v<=0 for v in (video.width,video.height)) or not isinstance(duration,(int,float)) or duration<0:raise ValueError('Invalid video metadata')
    item={'chat':owner,'id':message.message_id,'media_id':'bot-inbound-video:'+str(bot_id)+':'+video.file_unique_id,'source_kind':'bot-inbound','bot_id':bot_id,
        'file_id':video.file_id,'file_unique_id':video.file_unique_id,'update_id':update.update_id,'kind':'video','size':video.file_size,'album':message.media_group_id,
        'date':message.date.date().isoformat(),'section':'main','caption':message.caption or '', 'protected':bool(message.has_protected_content),'archive_meta':meta,
        'video_meta':dimensions,'naming_codec':'received-video-filename-v1' if original else 'received-video-fallback-v1'}
    item['received_binding']=video_binding(item)
    return {'action':'download','target':None,'source_kind':'bot-inbound','bot_id':bot_id,'snapshot':{'source_kind':'bot-inbound','peer':owner,'post':message.message_id,'main':[item],'discussion':[]},'items':[item]}


def payload_for(update, allowed, bot_id):
    """One actual received message -> one immutable durable download job."""
    message = update.message
    if message is not None and message.video:return video_payload(update,allowed,bot_id)
    if message is None:raise ValueError('Received photo required')
    owner = actor(message, allowed)
    if owner != OWNER or message.chat.id != owner or message.chat.type != 'private':
        raise PermissionError('Received photos require owner private chat')
    positive(bot_id)
    positive(update.update_id)
    positive(message.message_id, 2**31-1)
    photos = message.photo
    if not photos:raise ValueError('Received photo required')
    photo = max(photos, key=lambda p:(p.width*p.height,p.file_size or 0))
    if not photo.file_id or not photo.file_unique_id or len(photo.file_id)>2048 or len(photo.file_unique_id)>512:
        raise ValueError('Invalid received photo identity')
    if photo.file_size is not None and (photo.file_size <= 0 or photo.file_size > CLOUD_DOWNLOAD_LIMIT):
        raise ValueError('收到相片超過 Bot API 20 MB 下載上限')
    photo_id = photo_id_from_file_id(photo.file_id)
    receiving=(getattr(message.chat,'title',None) or getattr(message.chat,'full_name',None)
        or ' '.join(v for v in (getattr(message.from_user,'first_name',None),getattr(message.from_user,'last_name',None)) if v)
        or getattr(message.chat,'username',None) or '私人對話')
    meta = {'title':str(owner),'folder_title':receiving,'folder_chat':owner,'datetime':message.date.isoformat(),'extension':'.jpg'}
    origin=getattr(message,'forward_origin',None)
    source=getattr(origin,'chat',None)
    if source is not None and type(getattr(source,'id',None)) is int and getattr(source,'type',None) in ('channel','supergroup','group') and getattr(source,'title',None):
        meta.update(folder_title=source.title,folder_chat=source.id,origin_chat_id=source.id,origin_title=source.title)
    # Origin affects a label ONLY; actor/bot namespace remain the receiving owner.
    if photo_id:meta['photo_id'] = photo_id
    else:
        # Deliberately NOT labelled original-compatible when a codec is unknown.
        meta['filename'] = 'bot-photo-'+hashlib.sha256(photo.file_unique_id.encode()).hexdigest()[:24]+'.jpg'
    item = {'chat':owner,'id':message.message_id,'media_id':'bot-inbound:'+str(bot_id)+':'+photo.file_unique_id,
            'source_kind':'bot-inbound','bot_id':bot_id,'file_id':photo.file_id,'file_unique_id':photo.file_unique_id,
            'photo_variant':[photo.width,photo.height],
            'update_id':update.update_id,'kind':'photo','size':photo.file_size,'album':message.media_group_id,
            'date':message.date.date().isoformat(),'section':'main','caption':message.caption or '',
            'protected':bool(message.has_protected_content),'archive_meta':meta,
            'naming_codec':'telegram-photo-v2-v4' if photo_id else 'stable-fallback-v1'}
    return {'action':'download','target':None,'source_kind':'bot-inbound','bot_id':bot_id,
            'snapshot':{'source_kind':'bot-inbound','peer':owner,'post':message.message_id,'main':[item],'discussion':[]},
            'items':[item]}


def register_payload(store,payload,bot_id):
    """Native registration; caller owns the SQLite transaction, no API/offset mutation."""
    album=payload['items'][0].get('album')
    if album is not None and (not isinstance(album,str) or not album or len(album)>256):raise ValueError('Invalid incoming album')
    key=digest(['inbound-notice',OWNER,bot_id,OWNER,album if album else 'single:'+str(payload['items'][0]['id'])])
    payload={**payload,'inbound_notice':key}
    existing=store.db.execute('SELECT * FROM inbound_notices WHERE key=?',(key,)).fetchone()
    members=json.loads(existing['payload']) if existing else []
    task=store.insert_task(OWNER,OWNER,0,payload)
    # An already-DONE historical task is never replayed/notified retroactively.
    if json.loads(task['payload']).get('inbound_notice')!=key:return task
    if payload['items'][0]['kind']=='video':
        records=json.loads(store.get_meta('inbound-unsupported:'+key,'[]'))
        for record in records:
            if record['id']==payload['items'][0]['id'] and record['kind']=='video' and record.get('state')=='awaiting-task':
                if record.get('binding')!=payload['items'][0]['received_binding'] or record.get('update_id')!=payload['items'][0]['update_id']:raise ValueError('Pending video registration binding mismatch')
        records=[r for r in records if not (r['id']==payload['items'][0]['id'] and r['kind']=='video')]
        store.meta('inbound-unsupported:'+key,encode(records))
    if task['id'] in members:return task
    unsupported=json.loads(store.get_meta('inbound-unsupported:'+key,'[]'))
    if len(members)+len(unsupported)>=10:raise ValueError('Incoming album exceeds 10 items')
    members.append(task['id'])
    store.db.execute("INSERT INTO inbound_notices(key,owner,chat,bot,payload,due,state) VALUES(?,?,?,?,?,?,'pending') ON CONFLICT(key) DO UPDATE SET payload=excluded.payload,due=excluded.due,generation=inbound_notices.generation+1,state=CASE WHEN inbound_notices.state IN ('sending','ambiguous','failed') THEN inbound_notices.state ELSE 'pending' END",
        (key,OWNER,OWNER,bot_id,encode(members),time.time()+(2 if album else 0)))
    return task



def enqueue(store,config,update,bot_id):
    payload=payload_for(update,config.allowed,bot_id)
    with store.transaction():return register_payload(store,payload,bot_id)


def record_pending_video(store,config,raw_payload,bot_id):
    """Trusted retained raw update -> membership placeholder, NEVER a task/update."""
    from telegram import Update
    from .store import Store
    update=Update.de_json(json.loads(raw_payload),None)
    if update.message is None or not update.message.video or not update.message.media_group_id:return False
    payload=payload_for(update,config.allowed,bot_id);item=payload['items'][0]
    key=digest(['inbound-notice',OWNER,bot_id,OWNER,item['album']])
    with store.transaction():
        row=store.db.execute('SELECT * FROM inbound_notices WHERE key=? AND owner=? AND chat=? AND bot=? AND reply>0',(key,OWNER,OWNER,bot_id)).fetchone()
        if row is None:return False
        members=json.loads(row['payload'])
        task=store.db.execute('SELECT id,payload FROM tasks WHERE key=?',(Store.task_key(OWNER,OWNER,payload),)).fetchone()
        if task is not None:
            stored=json.loads(task['payload'])
            if task['id'] not in members or stored.get('inbound_notice')!=key or stored['items'][0].get('received_binding')!=item['received_binding']:raise ValueError('Retained video task binding mismatch')
            return False
        records=json.loads(store.get_meta('inbound-unsupported:'+key,'[]'))
        entry={'id':item['id'],'album':item['album'],'kind':'video','size':item['size'],'state':'awaiting-task',
            'update_id':update.update_id,'payload_sha256':hashlib.sha256(raw_payload.encode()).hexdigest(),'binding':item['received_binding']}
        existing=next((r for r in records if r['id']==item['id']),None)
        if existing==entry:return True
        if existing is not None:records.remove(existing)
        if len(records)+len(members)>=10:raise ValueError('Received album exceeds 10 items')
        records.append(entry)
        store.meta('inbound-unsupported:'+key,encode(sorted(records,key=lambda r:r['id'])))
        store.db.execute("UPDATE inbound_notices SET state='pending',generation=generation+1 WHERE key=?",(key,))
    return True


def record_unsupported(store,config,update,bot_id,*,require_existing=False):
    message=update.message
    if message is None or not message.media_group_id:return False
    kind=next((k for k in ('video','document','animation','audio') if getattr(message,k,None)),None)
    if kind is None:return False
    if kind=='video' and (message.video.file_size is None or 0<message.video.file_size<=CLOUD_DOWNLOAD_LIMIT):return False
    owner=actor(message,config.allowed)
    if owner!=OWNER or message.chat.id!=owner or message.chat.type!='private':raise PermissionError('Owner private album required')
    positive(bot_id)
    album=message.media_group_id
    if not isinstance(album,str) or not album or len(album)>256:raise ValueError('Invalid received album')
    key=digest(['inbound-notice',owner,bot_id,owner,album])
    with store.transaction():
        row=store.db.execute('SELECT * FROM inbound_notices WHERE key=?',(key,)).fetchone()
        if require_existing and (row is None or not row['reply']):return False
        records=json.loads(store.get_meta('inbound-unsupported:'+key,'[]'))
        if any(r['id']==message.message_id for r in records):return True
        members=json.loads(row['payload']) if row else []
        if len(records)+len(members)>=10:raise ValueError('Received album exceeds 10 items')
        records.append({'id':message.message_id,'album':album,'kind':kind,'size':getattr(getattr(message,kind),'file_size',None)})
        store.meta('inbound-unsupported:'+key,encode(sorted(records,key=lambda r:r['id'])))
        store.db.execute("INSERT INTO inbound_notices(key,owner,chat,bot,payload,due,state) VALUES(?,?,?,?,?,?,'pending') ON CONFLICT(key) DO UPDATE SET due=excluded.due,generation=inbound_notices.generation+1,state=CASE WHEN inbound_notices.state IN ('sending','ambiguous','failed') THEN inbound_notices.state ELSE 'pending' END",
            (key,owner,owner,bot_id,encode(members),time.time()+2))
    return True
