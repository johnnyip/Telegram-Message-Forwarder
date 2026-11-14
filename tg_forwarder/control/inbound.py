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


def payload_for(update, allowed, bot_id):
    """One actual received message -> one immutable durable download job."""
    message = update.message
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


def enqueue(store, config, update, bot_id):
    payload = payload_for(update, config.allowed, bot_id)
    album=payload['items'][0].get('album')
    if album is not None and (not isinstance(album,str) or not album or len(album)>256):raise ValueError('Invalid incoming album')
    key=digest(['inbound-notice',OWNER,bot_id,OWNER,album if album else 'single:'+str(update.message.message_id)])
    payload['inbound_notice']=key
    with store.transaction():
        existing=store.db.execute('SELECT * FROM inbound_notices WHERE key=?',(key,)).fetchone()
        members=json.loads(existing['payload']) if existing else []
        task=store.insert_task(OWNER,OWNER,0,payload)
        # An already-DONE historical task is never replayed/notified retroactively.
        if json.loads(task['payload']).get('inbound_notice')!=key:return task
        if task['id'] in members:return task
        if len(members)>=10:raise ValueError('Incoming album exceeds 10 items')
        members.append(task['id'])
        store.db.execute("INSERT INTO inbound_notices(key,owner,chat,bot,payload,due,state) VALUES(?,?,?,?,?,?,'pending') ON CONFLICT(key) DO UPDATE SET payload=excluded.payload,due=excluded.due,generation=inbound_notices.generation+1,state=CASE WHEN inbound_notices.state IN ('sending','ambiguous','failed') THEN inbound_notices.state ELSE 'pending' END",
            (key,OWNER,OWNER,bot_id,encode(members),time.time()+(2 if album else 0)))
        return task
