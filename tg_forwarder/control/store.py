"""SQLite transactions separate source/cache identity from action/target completion."""
import hashlib
import json
import secrets
import sqlite3
import time
from .filesystems import guard_state


def encode(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def digest(value):
    return hashlib.sha256(encode(value).encode()).hexdigest()


class Store:
    def __init__(self, path):
        guard_state(path)
        self.db = sqlite3.connect(str(path), isolation_level=None)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA busy_timeout=5000")
        self.db.executescript("""
        CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS updates(id INTEGER PRIMARY KEY,payload TEXT NOT NULL,state TEXT NOT NULL DEFAULT 'pending',attempt INTEGER DEFAULT 0,due REAL DEFAULT 0);
        CREATE TABLE IF NOT EXISTS previews(id INTEGER PRIMARY KEY,owner INTEGER,chat INTEGER,reply INTEGER,payload TEXT,state TEXT DEFAULT 'pending',attempt INTEGER DEFAULT 0,due REAL DEFAULT 0);
        CREATE TABLE IF NOT EXISTS queries(id TEXT PRIMARY KEY,owner INTEGER,chat INTEGER,reply INTEGER,expires REAL,payload TEXT);
        CREATE TABLE IF NOT EXISTS callbacks(id TEXT PRIMARY KEY,owner INTEGER,chat INTEGER,reply INTEGER,expires REAL,payload TEXT,claimed INTEGER DEFAULT 0);
        CREATE TABLE IF NOT EXISTS tasks(id TEXT PRIMARY KEY,owner INTEGER,chat INTEGER,reply INTEGER,key TEXT UNIQUE,payload TEXT,state TEXT,attempt INTEGER DEFAULT 0,due REAL DEFAULT 0,error TEXT DEFAULT '',progress INTEGER DEFAULT 0);
        CREATE TABLE IF NOT EXISTS operations(key TEXT PRIMARY KEY,state TEXT,payload TEXT);
        CREATE TABLE IF NOT EXISTS cache(key TEXT PRIMARY KEY,path TEXT,size INTEGER);
        CREATE TABLE IF NOT EXISTS watches(id TEXT PRIMARY KEY,owner INTEGER,chat INTEGER,payload TEXT,cursor INTEGER,due REAL,state TEXT,error TEXT DEFAULT '');
        """)

        columns = {r[1] for r in self.db.execute("PRAGMA table_info(watches)")}
        for name, definition in (("generation", "INTEGER NOT NULL DEFAULT 0"), ("settle_signature", "TEXT NOT NULL DEFAULT ''"), ("settle_since", "REAL NOT NULL DEFAULT 0")):
            if name not in columns:
                self.db.execute("ALTER TABLE watches ADD COLUMN "+name+" "+definition)

    def close(self):
        self.db.close()

    def get_meta(self, key, default="0"):
        row = self.db.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        return row[0] if row else default

    def meta(self, key, value):
        self.db.execute("INSERT OR REPLACE INTO meta VALUES(?,?)", (key, str(value)))

    def ingest(self, update):
        # Commit received update and next offset together BEFORE acknowledging polling.
        with self.transaction():
            self.db.execute("INSERT OR IGNORE INTO updates(id,payload) VALUES(?,?)", (update["update_id"], encode(update)))
            self.meta("offset", max(int(self.get_meta("offset")), update["update_id"] + 1))

    def transaction(self):
        return Transaction(self.db)

    def capability(self, owner, chat, reply, payload, ttl=3600):
        token = secrets.token_urlsafe(18)
        self.db.execute("INSERT INTO callbacks(id,owner,chat,reply,expires,payload) VALUES(?,?,?,?,?,?)",
                        (token, owner, chat, reply, time.time()+ttl, encode(payload)))
        return token

    def use(self, token, owner, chat, reply):
        with self.transaction():
            row = self.db.execute("SELECT * FROM callbacks WHERE id=?", (token,)).fetchone()
            if not row or (row["owner"], row["chat"], row["reply"]) != (owner, chat, reply) or row["expires"] < time.time():
                raise PermissionError("按鈕已過期／不屬於你")
            # Non-consuming navigation can be repeated; task key handles action claims atomically.
            return json.loads(row["payload"])

    def query(self, owner, chat, reply, snapshot):
        qid = secrets.token_urlsafe(12)
        self.db.execute("INSERT INTO queries VALUES(?,?,?,?,?,?)", (qid, owner, chat, reply, time.time()+3600, encode(snapshot)))
        return qid

    def get_query(self, qid, owner, chat, reply):
        row = self.db.execute("SELECT * FROM queries WHERE id=?", (qid,)).fetchone()
        if not row or (row["owner"], row["chat"], row["reply"]) != (owner, chat, reply) or row["expires"] < time.time():
            raise PermissionError("查詢已過期／不屬於你")
        return json.loads(row["payload"])

    def create_task(self, owner, chat, reply, payload):
        with self.transaction():
            return self.insert_task(owner, chat, reply, payload)

    def insert_task(self, owner, chat, reply, payload):
        """Caller holds BEGIN IMMEDIATE; permits task+watch cursor atomic commit."""
        key = digest([owner, chat, sorted((i["chat"], i["id"], i.get("media_id", "")) for i in payload["items"]), payload["action"], payload.get("target")])
        if payload["action"] == "cached_photos":
            key = digest(["cached-photos-v1",payload["origin_task"],key])
        task = secrets.token_hex(8)
        self.db.execute("INSERT OR IGNORE INTO tasks(id,owner,chat,reply,key,payload,state) VALUES(?,?,?,?,?,?,?)",
                        (task, owner, chat, reply, key, encode(payload), "queued"))
        return dict(self.db.execute("SELECT * FROM tasks WHERE key=?", (key,)).fetchone())

    def task(self, tid, owner, chat):
        row = self.db.execute("SELECT * FROM tasks WHERE id=? AND owner=? AND chat=?", (tid, owner, chat)).fetchone()
        if not row:
            raise PermissionError("任務不存在／不屬於你")
        return dict(row)

    def tasks(self, owner, chat):
        return [dict(r) for r in self.db.execute("SELECT * FROM tasks WHERE owner=? AND chat=? ORDER BY rowid DESC LIMIT 20", (owner, chat))]

    def set_task(self, tid, **fields):
        if not set(fields) <= {"state", "attempt", "due", "error", "progress"}:
            raise ValueError("invalid task fields")
        self.db.execute("UPDATE tasks SET " + ",".join(k+"=?" for k in fields) + " WHERE id=?", (*fields.values(), tid))

    def claim(self):
        with self.transaction():
            r = self.db.execute("SELECT * FROM tasks WHERE state IN ('queued','delayed') AND due<=? ORDER BY rowid LIMIT 1", (time.time(),)).fetchone()
            if not r:
                return None
            self.set_task(r["id"], state="running", attempt=r["attempt"]+1)
            return dict(self.db.execute("SELECT * FROM tasks WHERE id=?", (r["id"],)).fetchone())

    def operation(self, key):
        r = self.db.execute("SELECT state,payload FROM operations WHERE key=?", (key,)).fetchone()
        return (r[0], json.loads(r[1])) if r else (None, None)

    def mark(self, key, state, payload=None):
        self.db.execute("INSERT OR REPLACE INTO operations VALUES(?,?,?)", (key, state, encode(payload)))

    def recover(self):
        # In-flight send cannot be safely retried after process death.
        self.db.execute("UPDATE operations SET state='ambiguous' WHERE state='sending'")
        self.db.execute("UPDATE tasks SET state='queued' WHERE state='running'")
        self.db.execute("UPDATE updates SET state='pending' WHERE state='processing'")
        self.db.execute("UPDATE previews SET state='pending' WHERE state='running'")

    def watch(self, owner, chat, payload, cursor):
        payload = dict(payload)
        payload["ref"] = {**payload["ref"], "comment": None}
        wid = digest([owner, chat, payload["ref"], payload["action"], payload.get("target"), payload["content"]])[:20]
        self.db.execute("INSERT INTO watches(id,owner,chat,payload,cursor,due,state,error) VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET payload=excluded.payload,cursor=CASE WHEN watches.state='stopped' THEN excluded.cursor ELSE MAX(watches.cursor,excluded.cursor) END,state='active',due=excluded.due,error='',generation=watches.generation+1,settle_signature='',settle_since=0",
                        (wid, owner, chat, encode(payload), cursor, time.time(), "active", ""))
        return wid

    def stop_watch(self, wid, owner, chat):
        self.db.execute("UPDATE watches SET state='stopped',generation=generation+1 WHERE id=? AND owner=? AND chat=?", (wid,owner,chat))

    def commit_watch(self, observation, *, cursor=None, task=None, subscription=None,
                     signature="", since=0, error="", due=None, state="active"):
        """CAS generation+cursor after awaits; unsubscribe wins before task insertion."""
        with self.transaction():
            row = self.db.execute("SELECT * FROM watches WHERE id=?", (observation["id"],)).fetchone()
            if not row or row["state"] != "active" or row["generation"] != observation["generation"] or row["cursor"] != observation["cursor"]:
                return False
            if task:
                self.insert_task(row["owner"], row["chat"], 0, task)
            self.db.execute("UPDATE watches SET cursor=?,payload=?,settle_signature=?,settle_since=?,error=?,due=?,state=? WHERE id=?",
                (row["cursor"] if cursor is None else cursor, encode(subscription) if subscription else row["payload"],
                 signature,since,error,time.time() if due is None else due,state,row["id"]))
            return True


class Transaction:
    def __init__(self, db):
        self.db = db
    def __enter__(self):
        self.db.execute("BEGIN IMMEDIATE")
    def __exit__(self, kind, value, tb):
        self.db.execute("ROLLBACK" if kind else "COMMIT")
