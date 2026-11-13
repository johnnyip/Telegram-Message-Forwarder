"""Explicit local retained-update recovery; no API, polling or offset mutation."""
import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import sqlite3
from telegram import Update
from .config import Config, positive
from .filesystems import guard_state
from .inbound import OWNER, payload_for
from .store import Store


def recover(config, plan, *, apply=False, quiescent=False):
    if (set(plan) != {'version','owner_uid','bot_id','db','updates'} or plan['version'] != 1
            or plan['owner_uid'] != OWNER or OWNER not in config.allowed):
        raise ValueError('Explicit owner/version/allowlist required')
    bot_id = positive(plan['bot_id'])
    entries = plan['updates']
    if not isinstance(entries,list) or not 1 <= len(entries) <= 32:
        raise ValueError('Recovery requires 1..32 exact update entries')
    if any(set(v) != {'update_id','payload_sha256'} for v in entries):
        raise ValueError('Exact update identity and payload hash required')
    ids = [positive(v['update_id']) for v in entries]
    if len(set(ids)) != len(ids):raise ValueError('Duplicate update IDs')
    db = Path(plan['db'])
    if not db.is_absolute() or db != config.db.absolute() or db.is_symlink() or not db.is_file():
        raise ValueError('Exact existing configured state DB required')
    guard_state(db)
    if apply and not quiescent:raise ValueError('Stop control and use --quiescent')
    if apply:
        token_bot = os.getenv('TELEGRAM_BOT_TOKEN','').partition(':')[0]
        if token_bot != str(bot_id):raise ValueError('Recovery bot principal differs from configured token')
    def prepare(conn):
        principal = conn.execute("SELECT value FROM meta WHERE key='bot_principal'").fetchone()
        if principal and principal[0] != str(bot_id):raise ValueError('State bot principal mismatch')
        prepared = []
        for entry in entries:
            row = conn.execute('SELECT payload,state FROM updates WHERE id=?',(entry['update_id'],)).fetchone()
            if row is None or row[1] != 'done':raise ValueError('Exact retained done update required')
            if hashlib.sha256(row[0].encode()).hexdigest() != entry['payload_sha256']:
                raise ValueError('Retained payload hash mismatch')
            raw = json.loads(row[0])
            if raw.get('update_id') != entry['update_id'] or set(raw) != {'update_id','message'}:
                raise ValueError('Exact received-message update required')
            prepared.append(payload_for(Update.de_json(raw, None),config.allowed,bot_id))
        return prepared
    if not apply:
        with sqlite3.connect(db.as_uri()+'?mode=ro',uri=True) as conn:
            payloads = prepare(conn)
        return {'mode':'dry-run','validated_updates':len(payloads),'bot_id':bot_id, 'naming_codecs':{codec:sum(p['items'][0]['naming_codec']==codec for p in payloads) for codec in sorted({p['items'][0]['naming_codec'] for p in payloads})}}
    with open(str(db)+'.lock','a') as lock:
        try:fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        except BlockingIOError:raise ValueError('Control state already owned') from None
        store = Store(db)
        try:
            with store.transaction():
                payloads = prepare(store.db)
                tasks = [store.insert_task(OWNER,OWNER,0,p) for p in payloads]
            return {'mode':'enqueue-only','validated_updates':len(tasks),'task_ids':[t['id'] for t in tasks]}
        finally:store.close()


def cli():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--plan',required=True,help='Private JSON: version, owner_uid, bot_id, db, updates [{update_id,payload_sha256}]')
    parser.add_argument('--apply',action='store_true')
    parser.add_argument('--quiescent',action='store_true')
    args = parser.parse_args()
    os.umask(0o077)
    try:
        if args.apply:
            from .runtime import environment
            config = environment()
        else:config = Config.from_env()  # no secret file, Redis, clients or API
        plan = json.loads(Path(args.plan).read_text())
        print(json.dumps(recover(config,plan,apply=args.apply,quiescent=args.quiescent)))
    except Exception as exc:
        print('Recovery refused ('+type(exc).__name__+'); no private payload/API details logged')
        raise SystemExit(1) from None


if __name__ == '__main__':cli()
