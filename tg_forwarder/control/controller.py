"""Actual Bot API update handlers: auth is checked before parsing ANY input."""
import asyncio
import json
from tg_forwarder.shared_media import _io
import logging
import time
from telegram import InlineKeyboardButton as Button, InlineKeyboardMarkup as Markup
from telethon.errors import FloodWaitError
from telegram.error import BadRequest, Forbidden, RetryAfter
from .config import actor
from .input import from_message, SourceRef
from .source import scopes
from .store import encode, digest

LABELS = {"single": "單項", "album": "所選相簿", "discussion": "整串留言", "all": "原帖＋留言"}


class Controller:
    def __init__(self, config, store, resolver, engine, bot):
        self.config, self.store, self.resolver, self.engine, self.bot = config, store, resolver, engine, bot
        self.inbound_notice_lock=asyncio.Lock()

    def button(self, text, owner, chat, reply, payload):
        return Button(text, callback_data=self.store.capability(owner, chat, reply, payload))

    async def edit(self, chat, reply, text, rows=()):
        return await self.bot.edit_message_text(chat_id=chat, message_id=reply, text=text,
            reply_markup=Markup(rows) if rows else None)

    async def menu(self, owner, chat, reply, qid, content="all"):
        snap = self.store.get_query(qid, owner, chat, reply)
        choices = scopes(snap, content)
        rows = []
        for name, items in choices.items():
            base = {"query": qid, "scope": name, "content": content}
            rows.append([self.button(LABELS[name]+str(len(items))+" 下載", owner, chat, reply, {**base, "do": "download"}),
                         self.button("動作…", owner, chat, reply, {**base, "do": "actions"})])
        rows.append([self.button(label, owner, chat, reply, {"query": qid, "content": kind, "do": "filter"})
                     for kind, label in (("photo", "只要相"), ("video", "只要片"), ("all", "全部"))])
        counts = self.snapshot_summary(snap, content)
        warning = "\n⚠ partial／未知："+snap["warning"] if snap["partial"] else ""
        text = "媒體快照："+counts+warning+"\n最高 Telegram 可用質素（非原檔保證）。留言監察預設關閉。"
        await self.edit(chat, reply, text, rows)

    async def handle(self, update):
        cb = getattr(update, "callback_query", None)
        message = cb.message if cb else getattr(update, "message", None)
        if message is None:
            return
        # callback author is cb.from_user, never the bot-authored callback message.
        principal = cb if cb else message
        try:
            owner = actor(principal, self.config.allowed)
            if cb and getattr(message, "sender_chat", None):
                raise PermissionError("不支援匿名控制")
            chat, reply = message.chat.id, message.message_id
            if cb:
                data = self.store.use(cb.data, owner, chat, reply)
                await self.bot.answer_callback_query(cb.id)
                await self.callback(owner, chat, reply, data)
                return
            text = (getattr(message, "text", None) or "").strip()
            parts = text.split()
            command = parts[0].split("@")[0] if parts else ""
            if command in ("/start", "/help"):
                await self.bot.send_message(chat_id=chat, text="私人對話轉發／上傳相片會自動下載；或貼原帖／留言 t.me 連結。\n/tasks 任務；/watches 監察；/health 授權檢查。")
                return
            if command == "/health":
                await self.bot.send_message(chat_id=chat, text="控制台正常；你的數字 ID 已獲授權。獨立下載佇列；未執行下載／轉發。")
                return
            if command in ("/tasks", "/task", "/cancel", "/retry", "/resume"):
                await self.task_command(owner, chat, command, parts[1:])
                return
            if command in ("/watches", "/unwatch"):
                await self.watch_command(owner, chat, command, parts[1:])
                return
            if getattr(message, "photo", None):
                from .inbound import enqueue
                enqueue(self.store, self.config, update, self.engine.bot_id)
                return
            ref = from_message(message)
            media_group = getattr(message, "media_group_id", None)
            group_key = "input-album:"+str(owner)+":"+str(chat)+":"+str(media_group) if media_group else None
            if group_key and self.store.get_meta(group_key, ""):
                return
            # Persist placeholder before resolution; retrying a durable inbox row edits same reply.
            key = "preview:"+str(update.update_id)
            prior = self.store.get_meta(key, "")
            if prior:
                preview = int(prior)
            else:
                sent = await self.bot.send_message(chat_id=chat, text="正在解析原帖／相簿／留言…", reply_to_message_id=reply)
                preview = sent.message_id
                self.store.meta(key, preview)
            if group_key:
                self.store.meta(group_key, preview)
            self.store.db.execute("INSERT OR IGNORE INTO previews(id,owner,chat,reply,payload) VALUES(?,?,?,?,?)",
                                  (update.update_id, owner, chat, preview, encode(ref.as_dict())))
        except PermissionError as exc:
            if cb:
                await self.bot.answer_callback_query(cb.id, text=str(exc)[:180], show_alert=True)
            # Do not respond to unauthorized messages, disclose tasks, or resolve source.
        except ValueError as exc:
            if cb:
                rows = []
                if "data" in locals() and data.get("query"):
                    rows = [[self.back_button(owner, chat, reply, {"query": data["query"], "content": data.get("content", "all")})]]
                await self.edit(message.chat.id, message.message_id, str(exc), rows)
            else:
                await self.bot.send_message(chat_id=message.chat.id, text=str(exc))

    async def preview_notice(self, row, text):
        """Notification failure never escapes a preview worker or kills other queues."""
        try:
            await self.edit(row["chat"],row["reply"],text)
        except RetryAfter as exc:
            seconds = exc.retry_after.total_seconds() if hasattr(exc.retry_after,"total_seconds") else exc.retry_after
            self.engine.gate.delay(float(seconds))
        except Exception:
            pass

    async def preview_tick(self):
        row = self.store.db.execute("SELECT * FROM previews WHERE state='pending' AND due<=? ORDER BY id LIMIT 1", (time.time(),)).fetchone()
        if not row:
            return
        self.store.db.execute("UPDATE previews SET state='running',attempt=attempt+1 WHERE id=?", (row["id"],))
        try:
            if row["owner"] not in self.config.allowed:
                self.store.db.execute("UPDATE previews SET state='denied' WHERE id=?",(row["id"],))
                return
            key = "preview-query:"+str(row["id"])
            qid = self.store.get_meta(key, "")
            if not qid:
                snap = await self.resolver.resolve(SourceRef(**json.loads(row["payload"])))
                qid = self.store.query(row["owner"], row["chat"], row["reply"], snap)
                self.store.meta(key,qid)
            await self.menu(row["owner"], row["chat"], row["reply"], qid)
            self.store.db.execute("UPDATE previews SET state='done' WHERE id=?", (row["id"],))
        except (FloodWaitError, RetryAfter) as exc:
            seconds = exc.seconds if isinstance(exc,FloodWaitError) else (exc.retry_after.total_seconds() if hasattr(exc.retry_after,"total_seconds") else exc.retry_after)
            due = self.engine.gate.delay(float(seconds))
            state = "pending" if row["attempt"]+1 < self.config.retries else "failed"
            self.store.db.execute("UPDATE previews SET state=?,due=? WHERE id=?", (state,due,row["id"]))
            await self.preview_notice(row,"Telegram 限流；延遲 "+str(seconds)+" 秒（"+state+"）。")
        except (BadRequest, Forbidden):
            # Deleted/inaccessible preview: do not send unsolicited replacements or re-resolve.
            self.store.db.execute("UPDATE previews SET state='failed' WHERE id=?",(row["id"],))
        except Exception as exc:
            retry = self.store.get_meta("preview-query:"+str(row["id"]), "") and row["attempt"]+1 < self.config.retries
            self.store.db.execute("UPDATE previews SET state=?,due=? WHERE id=?",("pending" if retry else "failed",time.time()+5,row["id"]))
            await self.preview_notice(row,"來源／預覽不可用："+type(exc).__name__+"。請核對原帖及權限。")

    async def preview_loop(self):
        while True:
            try:
                await self.preview_tick()
                await self.inbound_notice_tick()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logging.getLogger(__name__).warning("preview worker isolated failure class=%s",type(exc).__name__)
            await asyncio.sleep(0.25)

    async def callback(self, owner, chat, reply, data):
        # Every entry comes exclusively through actor + bound persisted capability.
        if data.get("kind") == "task":
            await self.task_callback(owner, chat, reply, data)
            return
        qid = data["query"]
        snap = self.store.get_query(qid, owner, chat, reply)
        content = data.get("content", "all")
        if data["do"] in ("filter", "back"):
            await self.menu(owner, chat, reply, qid, content)
            return
        selected = scopes(snap, content).get(data.get("scope"))
        if not selected:
            raise ValueError("選取範圍已失效")
        base = {"query": qid, "scope": data["scope"], "content": content}
        if data["do"] == "actions":
            rows = [[self.button("下載", owner, chat, reply, {**base, "do": "download"})]]
            for target in self.config.targets:
                label = str(target[0])+(" topic "+str(target[1]) if target[1] else "")
                rows.append([self.button("轉發→"+label, owner, chat, reply, {**base, "do": "forward", "target": target}),
                             self.button("下載＋轉發", owner, chat, reply, {**base, "do": "both", "target": target})])
            if snap.get("thread"):
                rows.append([self.button("開啟此帖新留言監察…", owner, chat, reply, {**base, "do": "watch_menu"})])
            rows.append([self.back_button(owner, chat, reply, base)])
            await self.edit(chat, reply, "選擇動作。轉發會上載實際相簿，保留順序及說明；受保護來源不可轉發。", rows)
            return
        if data["do"] == "watch_menu":
            if not snap.get("thread") or snap["partial"]:
                raise ValueError("討論串未完整，不可開啟監察")
            rows = [[self.button("確認：只下載新留言", owner, chat, reply, {**base, "do": "watch", "action": "download"})]]
            for target in self.config.targets:
                rows.append([self.button("確認：新留言下載＋轉發→"+str(target[0]), owner, chat, reply,
                                         {**base, "do": "watch", "action": "both", "target": target})])
            rows.append([self.back_button(owner, chat, reply, base, "actions")])
            await self.edit(chat, reply, "只監察此帖未來留言（唔係整個頻道）。不補發現有留言。", rows)
            return
        if data["do"] == "watch":
            if not snap.get("thread") or snap["partial"]:
                raise ValueError("討論串未完整，不可開啟監察")
            action = data["action"]
            target = data.get("target")
            if action != "download" and tuple(target or ()) not in self.config.targets:
                raise PermissionError("目的地未獲允許")
            payload = self.watch_subscription(snap, action, target, content)
            wid = self.store.watch(owner, chat, payload, snap["cursor"])
            await self.edit(chat, reply, "已開啟此帖新留言監察："+wid+"\n/watches 查看；/unwatch ID 取消。", [[self.back_button(owner, chat, reply, base)]])
            return
        action = data["do"]
        if action not in ("download", "forward", "both"):
            raise ValueError("不支援動作")
        payload = {"snapshot": snap, "items": selected, "action": action, "target": data.get("target")}
        await _io(self.engine.validate,payload)
        unknown = any(i.get("size") is None for i in selected)
        if (len(selected) >= self.config.confirm_items or unknown or snap["partial"]) and not data.get("confirmed"):
            await self.edit(chat, reply, "較大／未知大小／partial 任務：只處理已列出 "+str(len(selected))+" 項，仍受數量／空間上限限制。確認？",
                [[self.button("確認執行", owner, chat, reply, {**data, "confirmed": True})],
                 [self.back_button(owner, chat, reply, base, "actions")]])
            return
        task = self.store.create_task(owner, chat, reply, payload)
        await self.render_task(owner, chat, reply, task)

    @staticmethod
    def media_counts(values):
        photos = sum(i.get("kind") == "photo" for i in values)
        videos = sum(i.get("kind") == "video" for i in values)
        return str(photos)+"相＋"+str(videos)+"片"

    def snapshot_summary(self, snap, content="all"):
        selected = snap["selected"]
        kind = selected[0].get("kind") if selected else None
        selected_type = {"photo": "相片", "video": "影片"}.get(kind, "文字／其他")
        comment = bool(snap.get("ref", {}).get("comment"))
        lines = [("所選留言單項：" if comment else "所選原帖單項：")+selected_type]
        choices = scopes(snap, content)
        for name, values in choices.items():
            label = "所選留言相簿" if name == "album" and comment else LABELS[name]
            lines.append(label+str(len(values))+"（"+self.media_counts(values)+"）")
        if not choices:
            lines.append("此篩選無媒體")
        return "\n".join(lines)

    def back_button(self, owner, chat, reply, base, action="back"):
        return self.button("← 返回動作" if action == "actions" else "← 返回範圍", owner, chat, reply, {**base, "do": action})

    def task_counts(self, task):
        p = json.loads(task["payload"])
        total = len(p["items"])
        downloaded = min(total, task["progress"])
        if p["action"] == "download":
            completed = downloaded
        else:
            completed = sum(self.store.operation(self.engine.operation_key(p, i))[0] == "done" for i in p["items"])
        return p, total, completed, downloaded

    async def task_buttons(self, owner, chat, reply, task):
        base = {"kind": "task", "task": task["id"]}
        buttons = [self.button("詳情／更新", owner, chat, reply, {**base, "do": "details"})]
        if task["state"] in ("queued", "running", "delayed"):
            buttons.append(self.button("取消", owner, chat, reply, {**base, "do": "cancel"}))
        elif task["state"] == "failed":
            buttons.append(self.button("重試失敗項目", owner, chat, reply, {**base, "do": "retry"}))
        elif task["state"] == "cancelled":
            buttons.append(self.button("繼續", owner, chat, reply, {**base, "do": "resume"}))
        if task["state"] == "done":
            try:
                payload = self.engine.cached_photo_payload(task)
                for selected in payload["items"]:
                    await self.engine.cached_file_async(selected)
            except (ValueError,PermissionError,OSError):
                pass
            else:
                buttons.append(self.button("📤 傳送已下載圖片到此對話（"+str(len(payload["items"]))+"）",owner,chat,reply,{**base,"do":"cached_photos"}))
        return buttons

    async def notify_task(self, task):
        payload=json.loads(task['payload'])
        key=payload.get('inbound_notice')
        if key:
            self.store.db.execute("UPDATE inbound_notices SET state=CASE WHEN state='sending' THEN 'sending' ELSE 'pending' END,generation=generation+1 WHERE key=? AND owner=? AND chat=? AND bot=? AND state NOT IN ('ambiguous','failed')",
                (key,task['owner'],task['chat'],self.engine.bot_id))
            return
        if task['reply'] or task['state'] in ('done','failed','ambiguous'):
            await self.render_task(task['owner'],task['chat'],task['reply'] or None,task)

    async def inbound_notice_tick(self):
        async with self.inbound_notice_lock:
            row=self.store.db.execute("SELECT * FROM inbound_notices WHERE state IN ('pending','sending') AND due<=? AND bot=? ORDER BY due LIMIT 1",(time.time(),self.engine.bot_id)).fetchone()
            if row is None:return
            if row['owner'] not in self.config.allowed or row['chat']!=row['owner']:
                self.store.db.execute("UPDATE inbound_notices SET state='failed' WHERE key=?",(row['key'],));return
            if row['state']=='sending':
                self.store.db.execute("UPDATE inbound_notices SET state=? WHERE key=?",('pending' if row['reply'] else 'ambiguous',row['key']));return
            ids=json.loads(row['payload'])
            if not ids or len(ids)>10:raise ValueError('Invalid inbound notification membership')
            tasks=[self.store.task(tid,row['owner'],row['chat']) for tid in ids]
            for task in tasks:
                p=json.loads(task['payload'])
                if p.get('inbound_notice')!=row['key'] or p.get('bot_id')!=row['bot'] or p.get('source_kind')!='bot-inbound':raise PermissionError('Inbound notice binding mismatch')
            done=sum(t['state']=='done' for t in tasks)
            errors=sum(t['state'] in ('failed','ambiguous','cancelled') for t in tasks)
            text='收到相簿' if json.loads(tasks[0]['payload'])['items'][0].get('album') else '收到相片'
            text+='：'+str(len(tasks))+' 相；已完成 '+str(done)+'/'+str(len(tasks))+'；錯誤／取消 '+str(errors)
            text+='\n'+('下載完成' if done==len(tasks) else '下載處理中／可在任務詳情查看')
            reply=row['reply']
            try:
                if not reply:
                    # Persist uncertainty BEFORE first send. Crash/unknown outcome
                    # never triggers another first-send attempt on recovery.
                    self.store.db.execute("UPDATE inbound_notices SET state='sending' WHERE key=?",(row['key'],))
                    sent=await self.bot.send_message(chat_id=row['chat'],text=text)
                    reply=getattr(sent,'message_id',None)
                    if type(reply) is not int or reply<=0:raise RuntimeError('Unconfirmed inbound notice send')
                    self.store.db.execute('UPDATE inbound_notices SET reply=? WHERE key=?',(reply,row['key']))
                rows=[]
                for task in tasks:
                    buttons=await self.task_buttons(row['owner'],row['chat'],reply,task)
                    buttons[0]=self.button('詳情 '+task['id'][-6:],row['owner'],row['chat'],reply,{'kind':'task','task':task['id'],'do':'details'})
                    rows.append(buttons)
                await self.edit(row['chat'],reply,text,rows)
                self.store.db.execute("UPDATE inbound_notices SET state=CASE WHEN generation=? THEN 'sent' ELSE 'pending' END WHERE key=?",(row['generation'],row['key']))
            except RetryAfter as exc:
                seconds=exc.retry_after.total_seconds() if hasattr(exc.retry_after,'total_seconds') else exc.retry_after
                self.engine.gate.delay(float(seconds))
                self.store.db.execute("UPDATE inbound_notices SET state='pending',due=? WHERE key=?",(time.time()+float(seconds)+1,row['key']))
            except asyncio.CancelledError:
                raise
            except Exception:
                self.store.db.execute("UPDATE inbound_notices SET state=? WHERE key=?",('failed' if reply else 'ambiguous',row['key']))

    async def render_task(self, owner, chat, reply, task):
        # Re-read by owner/chat; notification payloads and stale buttons are not authority.
        task = self.store.task(task["id"], owner, chat)
        p, total, completed, downloaded = self.task_counts(task)
        if not reply:
            sent = await self.bot.send_message(chat_id=chat, text="正在讀取任務…")
            reply = sent.message_id
        action = {"download": "下載", "forward": "轉發", "both": "下載＋轉發", "cached_photos": "傳送已下載圖片（不重新下載）"}[p["action"]]
        text = "任務 "+task["id"]+"："+task["state"]+"\n"+action+"；"+self.media_counts(p["items"])
        text += "\n已完成 "+str(completed)+"/"+str(total)+"；已下載 "+str(downloaded)+"/"+str(total)
        if task["state"] in ("done", "failed", "cancelled", "ambiguous"):
            text += "\n失敗／未完成餘下 "+str(total-completed)+" 項"
        text += "\n嘗試 "+str(task["attempt"])
        if task["error"]:
            text += "\n"+task["error"]
        if task["state"] == "delayed":
            text += "\n可重試時間："+time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(task["due"]))
        if task["state"] == "ambiguous":
            text += "\n傳送結果未確認，必須核對目的地，禁止盲目重送。"
        rows = [await self.task_buttons(owner, chat, reply, task),
                [self.button("我的任務", owner, chat, reply, {"kind": "task", "do": "list"})]]
        await self.edit(chat, reply, text[:4000], rows)

    async def render_tasks(self, owner, chat, reply=None):
        tasks = self.store.tasks(owner, chat)[:20]
        if not tasks:
            if reply:
                await self.edit(chat, reply, "暫無任務")
            else:
                await self.bot.send_message(chat_id=chat, text="暫無任務")
            return
        if not reply:
            sent = await self.bot.send_message(chat_id=chat, text="正在讀取任務…")
            reply = sent.message_id
        lines, rows = [], []
        for task in tasks:
            _, total, done, _ = self.task_counts(task)
            lines.append(task["id"]+" "+task["state"]+" "+str(done)+"/"+str(total))
            buttons = await self.task_buttons(owner, chat, reply, task)
            buttons[0] = self.button("詳情 "+task["id"], owner, chat, reply,
                {"kind": "task", "task": task["id"], "do": "details"})
            rows.append(buttons)
        await self.edit(chat, reply, "我的任務（最多20項）\n"+"\n".join(lines), rows)

    def change_task(self, owner, chat, tid, action):
        task = self.store.task(tid, owner, chat)
        if action == "cancel" and task["state"] in ("queued", "running", "delayed"):
            self.store.set_task(tid, state="cancelled")
        elif action in ("retry", "resume"):
            if task["state"] not in ("failed", "cancelled"):
                raise ValueError("只可重試 failed／cancelled；ambiguous 必須先人工核對，禁止盲目重送")
            self.store.set_task(tid, state="queued", due=0, attempt=0, error="")
        return self.store.task(tid, owner, chat)

    async def task_callback(self, owner, chat, reply, data):
        if data.get("do") == "list":
            await self.render_tasks(owner, chat, reply)
            return
        if data.get("do") not in ("details", "cancel", "retry", "resume", "cached_photos"):
            raise ValueError("不支援任務動作")
        task = self.store.task(data.get("task"), owner, chat)
        if data["do"] == "cached_photos":
            payload = self.engine.cached_photo_payload(task)
            cached = self.store.create_task(owner,chat,reply,payload)
            await self.render_task(owner,chat,reply,cached)
            return
        if data["do"] in ("retry", "resume") and task["state"] not in ("failed", "cancelled"):
            # Stale controls refresh the authoritative state without resetting an active task.
            await self.render_task(owner, chat, reply, task)
            return
        if data["do"] != "details":
            task = self.change_task(owner, chat, task["id"], data["do"])
        await self.render_task(owner, chat, reply, task)

    async def task_command(self, owner, chat, command, args):
        if command == "/tasks" and not args:
            await self.render_tasks(owner, chat)
            return
        if len(args) != 1:
            raise ValueError(command+" TASK_ID")
        task = self.store.task(args[0], owner, chat)
        if command in ("/cancel", "/retry", "/resume"):
            task = self.change_task(owner, chat, task["id"], command[1:])
        await self.render_task(owner, chat, None, task)

    async def watch_command(self, owner, chat, command, args):
        if command == "/unwatch":
            if len(args) != 1:
                raise ValueError("/unwatch WATCH_ID")
            self.store.stop_watch(args[0], owner, chat)
        rows = self.store.db.execute("SELECT id,state,error FROM watches WHERE owner=? AND chat=?", (owner, chat)).fetchall()
        await self.bot.send_message(chat_id=chat, text="\n".join(r["id"]+" "+r["state"]+" "+r["error"] for r in rows)[:4000] or "未開啟任何監察")

    @staticmethod
    def watch_subscription(snap, action, target, content):
        import copy
        base = copy.deepcopy(snap)
        ref = {"peer": snap["ref"]["peer"], "post": snap["post"], "comment": None}
        base.update(ref=ref, selected=base["main"][:1], selected_album=base["main"], discussion=[])
        return {"ref": ref, "thread": snap["thread"], "snapshot": base,
                "action": action, "target": target, "content": content}

    async def watch_tick(self):
        rows = self.store.db.execute("SELECT * FROM watches WHERE state='active' AND due<=?", (time.time(),)).fetchall()
        for watch in rows:
            if watch["owner"] not in self.config.allowed:
                self.store.commit_watch(watch, state="denied", error="owner removed")
                continue
            try:
                p = json.loads(watch["payload"])
                if not p.get("thread") or not p.get("snapshot"):
                    # One-time upgrade of old subscriptions; selected comment is never required.
                    ref = SourceRef(**{**p["ref"], "comment": None})
                    initial = await self.resolver.resolve(ref)
                    if initial["partial"] or not initial.get("thread"):
                        raise ValueError("partial："+initial["warning"])
                    p = self.watch_subscription(initial,p["action"],p.get("target"),p["content"])
                snap = await self.resolver.new_comments(p,watch["cursor"])
                if snap["partial"]:
                    raise ValueError("partial："+snap["warning"])
                latest = snap["discussion"][-1] if snap["discussion"] else None
                waiting, signature, since = False, "", 0
                if latest and latest["album"]:
                    tail = [(i["id"],i.get("media_id","")) for i in snap["discussion"] if i["album"] == latest["album"]]
                    signature = digest([latest["album"],tail])
                    since = watch["settle_since"] if signature == watch["settle_signature"] else time.time()
                    if time.time()-since < self.config.watch_settle_seconds:
                        waiting = True
                        first_tail = min(mid for mid,_ in tail)
                        snap["discussion"] = [i for i in snap["discussion"] if i["id"] < first_tail]
                        snap["cursor"] = max((i["id"] for i in snap["discussion"]), default=watch["cursor"])
                values = [i for i in snap["discussion"] if i["kind"] and (p["content"] == "all" or i["kind"] == p["content"])]
                task = None
                if values:
                    task = {"snapshot": snap, "items": values, "action": p["action"], "target": p.get("target")}
                    await _io(self.engine.validate,task)
                    if len(values) >= self.config.confirm_items or any(i.get("size") is None for i in values):
                        raise ValueError("新留言任務超過確認門檻／未知大小，請手動查詢；游標未前進")
                self.store.commit_watch(watch,cursor=snap["cursor"],task=task,subscription=p,
                    signature=signature if waiting else "",since=since if waiting else 0,
                    error="等待最新相簿穩定／邊界" if waiting else "",
                    due=time.time()+(self.config.watch_settle_seconds if waiting else self.config.watch_interval))
            except FloodWaitError as exc:
                due = self.engine.gate.delay(exc.seconds)
                self.store.commit_watch(watch,due=due,error="FloodWait "+str(exc.seconds)+"s")
            except Exception as exc:
                self.store.commit_watch(watch,due=time.time()+self.config.watch_interval,
                    error=str(exc)[:300] if isinstance(exc,ValueError) else type(exc).__name__)

    async def watch_loop(self):
        while True:
            await self.watch_tick()
            await asyncio.sleep(5)
