"""Thin Telethon adapter to tangyoha downloader defaults; no Pyrogram client.

Naming helpers adapted from tangyoha/telegram_media_downloader
fe7ccdc2cc5fce5b04a464bc66cfb1266bf7ef45, utils/format.py and
media_downloader.py. Copyright (c) 2019 Dineshkarthik R, MIT;
see docs/licenses/tangyoha-downloader-MIT.txt. Photo codec is a stdlib
behavior adapter to Pyrogram's documented DOCUMENT unique-ID format.
"""
import base64
from datetime import datetime, timezone
import hashlib
import os
from pathlib import Path
import re
import stat
import struct
import unicodedata
from zoneinfo import ZoneInfo


def validate_title(value):
    return re.sub(r'[/\\:*?"<>|\n]', '_', str(value))


def photo_unique_id(photo_id):
    raw = struct.pack('<iq', 2, int(photo_id))
    result = bytearray(); zeros = 0
    for byte in raw:
        if byte == 0:
            zeros += 1
        else:
            if zeros:
                result.extend((0, zeros)); zeros = 0
            result.append(byte)
    if zeros:
        result.extend((0, zeros))
    return base64.urlsafe_b64encode(result).decode().rstrip('=')


def truncate_filename(path, limit=230):
    # tangyoha/telegram_media_downloader utils/format.py, MIT, pinned below.
    parent, base = os.path.split(os.path.normpath(path))
    stem, extension = os.path.splitext(base)
    maximum = limit - len(extension.encode("utf-8"))
    stem = unicodedata.normalize("NFC", stem)
    return os.path.join(parent, stem.encode()[:maximum].decode("utf-8", errors="ignore") + extension)


def filename(selected):
    meta = selected['archive_meta']
    original = meta.get('filename')
    stem, extension = None, meta.get('extension') or ('.jpg' if selected['kind']=='photo' else '.mp4')
    if original:
        stem, suffix = os.path.splitext(os.path.basename(os.path.normpath(original)))
        extension = suffix or extension
    if not stem and selected['kind']=='photo' and meta.get('photo_id'):
        stem = photo_unique_id(meta['photo_id'])
    generated = str(selected['id']) + (' - '+stem if stem else '')
    return truncate_filename(validate_title(generated)+extension)


def relative(selected, tz='UTC'):
    meta = selected['archive_meta']
    date = datetime.fromisoformat(meta['datetime']) if meta.get('datetime') else None
    if date is not None and date.tzinfo is None:
        date = date.replace(tzinfo=timezone.utc)
    title = validate_title(meta.get('title') or str(selected['chat']))
    if title in ('','.','..') or '\x00' in title:
        raise ValueError('Invalid archive chat title')
    return Path(title) / (date.astimezone(ZoneInfo(tz)).strftime('%Y_%m') if date else '0') / filename(selected)


def trusted_file_owner_uids():
    """Explicit public NFS identity mapping; never infer trust from a parent."""
    raw = os.getenv('MEDIA_ARCHIVE_FILE_OWNER_UIDS', '')
    if not raw:
        return frozenset()
    parts = raw.split(',')
    if len(parts) > 16 or any(not re.fullmatch(r'(0|[1-9][0-9]{0,9})', v.strip()) or int(v) > 2**32-2 for v in parts):
        raise ValueError('Invalid MEDIA_ARCHIVE_FILE_OWNER_UIDS')
    return frozenset(int(v.strip()) for v in parts)


def secure_path(root, relative_path, *, existing=False):
    trusted_uids = trusted_file_owner_uids()
    root, rel = Path(root).absolute(), Path(relative_path)
    if rel.is_absolute() or not rel.parts or any(p in ('','.','..') or '\x00' in p for p in rel.parts):
        raise ValueError('Unsafe canonical relative path')
    for component in (root,*root.parents):
        if component.is_symlink(): raise ValueError('Canonical root symlink rejected')
    path = root
    for part in rel.parts:
        path = path/part
        if path.is_symlink(): raise ValueError('Canonical symlink rejected')
    if existing:
        info=path.lstat()
        if not stat.S_ISREG(info.st_mode) or info.st_size <= 0 or info.st_uid not in ({os.getuid(),root.stat().st_uid} | trusted_uids) or info.st_mode & 0o022:
            raise ValueError('Canonical file must be owned, regular, nonempty, not writable by others')
    return path


def fingerprint(path):
    flags=os.O_RDONLY|getattr(os,'O_NOFOLLOW',0)
    with os.fdopen(os.open(path,flags),'rb') as stream:
        before=os.fstat(stream.fileno())
        if not stat.S_ISREG(before.st_mode) or before.st_size <= 0: raise ValueError('Invalid file')
        digest=hashlib.sha256()
        while block:=stream.read(1024*1024):digest.update(block)
        after=os.fstat(stream.fileno())
        if (before.st_ino,before.st_size,before.st_mtime_ns)!=(after.st_ino,after.st_size,after.st_mtime_ns):raise ValueError('File changed while hashing')
        return {'sha256':digest.hexdigest(),'size':after.st_size}
