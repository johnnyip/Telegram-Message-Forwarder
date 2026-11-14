"""Actual Bot API update handlers: auth is checked before parsing ANY input."""
import asyncio
import json
from pathlib import Path
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
            if getattr(message, "photo", None) or getattr(message,'video',None):
                from .inbound import enqueue,ReceivedMediaLimit,record_unsupported
                try:enqueue(self.store,self.config,update,self.engine.bot_id)
                except ReceivedMediaLimit:
                    if not record_unsupported(self.store,self.config,update,self.engine.bot_id):raise
                return
            if getattr(message,'media_group_id',None):
                from .inbound import record_unsupported
                if record_unsupported(self.store,self.config,update,self.engine.bot_id):return
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
        if data.get('kind')=='inbound_album':
            await self.album_callback(owner,chat,reply,data)
            return
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
        if self.location_allowed(owner,chat) and json.loads(task['payload']).get('action') in ('download','both') and (task['progress'] or task['state']=='done'):
            buttons.append(self.button('📂 下載位置',owner,chat,reply,{**base,'do':'locations','revision':digest(task['payload']),'bot':self.engine.bot_id,'page':0}))
        return buttons

    async def notify_task(self, task):
        payload=json.loads(task['payload'])
        key=('cached-operation:'+task['id']) if payload.get('origin_notice') else payload.get('inbound_notice')
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
            operation=row['key'].startswith('cached-operation:')
            if operation:
                ids=json.loads(row['payload'])
                if len(ids)!=1:raise ValueError('Invalid cached operation notice')
                tasks=[self.store.task(ids[0],row['owner'],row['chat'])]
                payload=json.loads(tasks[0]['payload'])
                if payload.get('action')!='cached_photos' or not payload.get('origin_notice') or row['key']!='cached-operation:'+tasks[0]['id']:raise PermissionError('Cached operation notice mismatch')
                _,total,completed,_=self.task_counts(tasks[0])
                text='傳送已下載相簿：'+tasks[0]['state']+'；已傳送 '+str(completed)+'/'+str(total)
            else:
                _,tasks,unsupported,revision=self.engine.album_snapshot(row['owner'],row['chat'],row['key'])
                text=await self.album_text(row,tasks,unsupported)
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
                if operation:
                    self.store.set_task(tasks[0]['id'],reply=reply)
                    rows=[await self.task_buttons(row['owner'],row['chat'],reply,tasks[0])]
                else:
                    rows=await self.album_buttons(row,tasks,unsupported,revision,reply)
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

    async def album_text(self,row,tasks,unsupported):
        total=len(tasks)+len(unsupported);done=sum(t['state']=='done' for t in tasks)
        errors=sum(t['state'] in ('failed','ambiguous','cancelled') for t in tasks)
        text='收到相簿／相片：'+str(total)+' 項；已完成 '+str(done)+'/'+str(total)+'；錯誤／取消 '+str(errors)
        if unsupported:
            counts={kind:sum(v['kind']==kind for v in unsupported) for kind in sorted({v['kind'] for v in unsupported})}
            names={'video':'影片','document':'文件','animation':'動畫','audio':'音訊'}
            text+='\n'+ '；'.join('未下載'+names.get(k,k)+' '+str(v) for k,v in counts.items())
            awaiting=sum(v.get('state')=='awaiting-task' for v in unsupported)
            if awaiting:text+='\n影片尚未下載／待補做 '+str(awaiting)
            if awaiting<len(unsupported):text+='\n其餘媒體超過影片 Cloud 20 MB 上限／類型未支援。'
            text+='\n不會當作整組完成或只傳送部分媒體。'
        terminal=done+errors+len(unsupported)==total and not any(v.get('state')=='awaiting-task' for v in unsupported)
        text+='\n'+('下載完成' if done==total else ('部分完成' if done else '下載失敗／尚未下載') if terminal else '下載處理中／可在相簿詳情查看')
        if self.store.get_meta('album-view:'+row['key'],'')=='details':
            for task in sorted(tasks,key=lambda t:json.loads(t['payload'])['items'][0]['id']):
                item=json.loads(task['payload'])['items'][0]
                line=str(item['id'])+' '+task['state']
                if task['state']=='done':
                    try:
                        path,_=await self.engine.cached_file_async(item)
                        line+=' '+str(path.relative_to(self.engine.archive.root))[:220]
                    except (ValueError,OSError):line+=' 快取不可用（不重新下載）'
                text+='\n'+line
            for item in unsupported:text+='\n'+str(item['id'])+' '+item['kind']+' 尚未下載'
        return text[:3900]

    async def album_buttons(self,row,tasks,unsupported,revision,reply):
        base={'kind':'inbound_album','notice':row['key'],'revision':revision}
        detail=self.store.get_meta('album-view:'+row['key'],'')=='details'
        buttons=[self.button('相簿摘要' if detail else '相簿詳情',row['owner'],row['chat'],reply,{**base,'do':'summary' if detail else 'details'})]
        if tasks and not unsupported and all(t['state']=='done' for t in tasks):
            try:
                payload=self.engine.cached_album_payload(row['owner'],row['chat'],row['key'],revision)
                used=0
                for item in payload['items']:
                    _,size=await self.engine.cached_file_async(item,used);used+=size
            except (ValueError,PermissionError,OSError):pass
            else:buttons.append(self.button('📤 傳送已下載相簿到此對話（'+str(len(payload['items']))+'）',row['owner'],row['chat'],reply,{**base,'do':'cached_photos'}))
        return [buttons]

    async def album_callback(self,owner,chat,reply,data):
        revision=data.get('revision')
        if not isinstance(revision,str) or len(revision)!=64:raise ValueError('Exact album revision required')
        row,tasks,unsupported,revision=self.engine.album_snapshot(owner,chat,data.get('notice'),reply=reply,revision=data.get('revision'))
        action=data.get('do')
        if action in ('details','summary'):
            self.store.meta('album-view:'+row['key'],'details' if action=='details' else '')
            await self.edit(chat,reply,await self.album_text(row,tasks,unsupported),await self.album_buttons(row,tasks,unsupported,revision,reply))
        elif action=='cached_photos':
            payload=self.engine.cached_album_payload(owner,chat,row['key'],revision)
            used=0
            for item in payload['items']:
                _,size=await self.engine.cached_file_async(item,used);used+=size
            # One operation, one separate durable status anchor. No upload occurs
            # in this callback; only the existing worker may execute cached_photos.
            with self.store.transaction():
                task=self.store.insert_task(owner,chat,0,payload)
                key='cached-operation:'+task['id']
                self.store.db.execute("INSERT OR IGNORE INTO inbound_notices(key,owner,chat,bot,payload,due,state) VALUES(?,?,?,?,?,0,'pending')",(key,owner,chat,self.engine.bot_id,encode([task['id']])))
        else:raise ValueError('Unsupported album action')

    def refresh_inbound_templates(self):
        from telegram import Update
        from .inbound import OWNER,record_unsupported,record_pending_video
        if OWNER not in self.config.allowed or not self.engine.bot_id:return
        marker='inbound-ui-template:'+str(self.engine.bot_id)+':'+str(OWNER)
        if self.store.get_meta(marker,'')=='album-actions-video-v3':return
        known={r['key'] for r in self.store.db.execute("SELECT key FROM inbound_notices WHERE owner=? AND chat=? AND bot=? AND reply>0 AND state NOT IN ('sending','ambiguous','failed') ORDER BY rowid DESC LIMIT 2",(OWNER,OWNER,self.engine.bot_id))}
        # Exact persisted received updates only; never poll/replay/execute media.
        rows=self.store.db.execute("SELECT payload FROM updates WHERE state='done' ORDER BY id DESC LIMIT 32").fetchall()
        for item in rows:
            try:
                update=Update.de_json(json.loads(item['payload']),None)
                msg=update.message
                if msg is None or not msg.media_group_id:continue
                key=digest(['inbound-notice',OWNER,self.engine.bot_id,OWNER,msg.media_group_id])
                if key not in known:continue
                if msg.video and (msg.video.file_size is None or 0<msg.video.file_size<=20_000_000):
                    record_pending_video(self.store,self.config,item['payload'],self.engine.bot_id)
                else:record_unsupported(self.store,self.config,update,self.engine.bot_id,require_existing=True)
            except (ValueError,PermissionError):continue
        with self.store.transaction():
            self.store.db.execute("UPDATE inbound_notices SET state='pending',generation=generation+1 WHERE key IN (SELECT key FROM inbound_notices WHERE owner=? AND chat=? AND bot=? AND reply>0 AND state NOT IN ('sending','ambiguous','failed') ORDER BY rowid DESC LIMIT 2)",(OWNER,OWNER,self.engine.bot_id))
            self.store.meta(marker,'album-actions-video-v3')

    def location_allowed(self,owner,chat):
        from .inbound import OWNER
        bot=self.engine.bot_id
        return (owner==OWNER and chat==owner and owner in self.config.allowed and type(bot) is int and bot>0
            and self.store.get_meta('bot_principal','') in ('',str(bot)))

    async def location_inventory(self,task):
        payload=json.loads(task['payload'])
        cache=[]
        for item in payload['items']:
            key=digest([item['chat'],item['id'],item.get('media_id','')])
            row=self.store.db.execute('SELECT * FROM cache WHERE key=?',(key,)).fetchone()
            cache.append((item,dict(row) if row else None))
        def inspect():
            values=[]
            for item,row in cache:
                relative=None;status='未有快取路徑'
                if row:
                    try:
                        relative=Path(row['path']).relative_to(self.engine.archive.root)
                        path=self.engine.archive.path(*relative.parts)
                        try:path.lstat()
                        except FileNotFoundError:status='檔案遺失'
                        else:
                            try:self.engine._cached_path(row,item,0)
                            except (ValueError,OSError):status='不安全／大小不符'
                            else:status='可用'
                    except (ValueError,OSError):relative=None;status='不安全路徑'
                values.append((item['kind'],str(relative) if relative is not None else None,status))
            return values
        return await _io(inspect)  # metadata/stat ONLY, no content/source calls.

    async def render_locations(self,owner,chat,reply,task,data):
        if not self.location_allowed(owner,chat) or data.get('bot')!=self.engine.bot_id:raise PermissionError('下載位置的擁有人／Bot 不符')
        task=self.store.task(task['id'],owner,chat)
        payload=json.loads(task['payload'])
        if data.get('revision')!=digest(task['payload']) or payload.get('action') not in ('download','both'):raise ValueError('任務內容已更新，請重新開啟下載位置')
        page=data.get('page',0)
        if type(page) is not int or page<0:raise ValueError('Invalid location page')
        values=await self.location_inventory(task)
        per_page=10;pages=max(1,(len(values)+per_page-1)//per_page)
        if page>=pages:raise ValueError('Invalid location page')
        clean=lambda value:''.join(c if c.isprintable() else '�' for c in str(value))
        available=sum(v[2]=='可用' for v in values);missing=sum(v[2]=='檔案遺失' for v in values)
        text='📂 下載位置 · '+task['id']+'\n'+self.media_counts(payload['items'])+'；可用 '+str(available)+'/'+str(len(values))+'；檔案遺失 '+str(missing)+'；其餘未確認 '+str(len(values)-available-missing)
        if task['state']=='done':text+='\n歷史任務已完成；以上是目前檔案檢查，並非本次重新下載。'
        text+='\n程式／Docker 內根目錄：'+clean(self.engine.archive.root)[:180]+'\n以下為相對下載路徑（不代表 NAS 主機掛載點）：'
        groups={}
        for _,relative,status in values:
            if relative is not None:
                parent=str(Path(relative).parent);counts=groups.setdefault(parent,[0,0]);counts[0]+=1;counts[1]+=status=='可用'
        for folder,counts in list(sorted(groups.items()))[:6]:text+='\n📁 '+clean(folder)[:150]+'：'+str(counts[1])+'/'+str(counts[0])+' 可用'
        if len(groups)>6:text+='\n另有 '+str(len(groups)-6)+' 個資料夾（見檔案分頁）'
        text+='\n\n第 '+str(page+1)+'/'+str(pages)+' 頁'
        for index,(kind,relative,status) in enumerate(values[page*per_page:(page+1)*per_page],page*per_page+1):
            text+='\n'+str(index)+'. '+('相片' if kind=='photo' else '影片' if kind=='video' else kind)+' ['+status+'] '+clean(relative if relative is not None else '未顯示未驗證路徑')[:180]
        base={'kind':'task','task':task['id'],'do':'locations','revision':data['revision'],'bot':data['bot']}
        nav=[]
        if page:nav.append(self.button('← 上頁',owner,chat,reply,{**base,'page':page-1}))
        if page+1<pages:nav.append(self.button('下頁 →',owner,chat,reply,{**base,'page':page+1}))
        repair=[]
        if missing and payload.get('source_kind')!='bot-inbound' and task['state']=='done' and not payload.get('missing_repair'):
            repair=[[self.button('🔄 補下載遺失項目（'+str(missing)+'）',owner,chat,reply,{**base,'do':'missing_repair','inventory':digest(values),'head':self.store.get_meta('missing-repair-head:'+task['id'],'')})]]
        rows=([nav] if nav else [])+repair+[[self.button('← 任務詳情',owner,chat,reply,{'kind':'task','task':task['id'],'do':'details'})]]
        await self.bot.edit_message_text(chat_id=chat,message_id=reply,text=text.encode('utf-16-le')[:7800].decode('utf-16-le',errors='ignore'),parse_mode=None,reply_markup=Markup(rows))

    async def repair_missing(self,owner,chat,reply,task,data):
        if not self.location_allowed(owner,chat) or data.get('bot')!=self.engine.bot_id:raise PermissionError('Repair principal mismatch')
        task=self.store.task(task['id'],owner,chat);original=json.loads(task['payload'])
        if task['state']!='done' or original.get('source_kind')=='bot-inbound' or original.get('missing_repair') or original['action'] not in ('download','both') or data.get('revision')!=digest(task['payload']):raise ValueError('Only an exact historical source download may repair missing files')
        headkey='missing-repair-head:'+task['id'];head=self.store.get_meta(headkey,'')
        if data.get('head')!=head:raise ValueError('已有較新的補下載任務，請更新詳情；不會重複建立')
        if head:
            previous=self.store.task(head,owner,chat)
            if previous['state']!='done':
                await self.render_task(owner,chat,reply,previous)
                return  # Active/failed/cancelled/ambiguous uses native existing controls, never reset.
        values=await self.location_inventory(task)
        if data.get('inventory')!=digest(values):raise ValueError('檔案狀態已變更，請更新下載位置再確認補下載')
        selected=[item for item,value in zip(original['items'],values) if value[2]=='檔案遺失' and value[1] is not None]
        if not selected:raise ValueError('目前沒有已驗證遺失檔案；不會覆寫現有／不安全檔案')
        entries=[]
        for item,value in zip(original['items'],values):
            if value[2]!='檔案遺失' or value[1] is None:continue
            key=digest([item['chat'],item['id'],item.get('media_id','')])
            cached=self.store.db.execute('SELECT * FROM cache WHERE key=?',(key,)).fetchone()
            if cached is None or cached['path']!=str(self.engine.archive.root/Path(value[1])) or cached['size']<=0 or (item.get('size') is not None and cached['size']!=item['size']):raise ValueError('Missing-file cache binding changed')
            entries.append({'key':key,'relative':value[1],'size':cached['size']})
        payload={'action':'download','target':None,'snapshot':original['snapshot'],'items':selected,
            'missing_repair':{'origin':task['id'],'revision':data['revision'],'previous':head,'entries':entries}}
        await _io(self.engine.validate,payload)
        with self.store.transaction():
            if self.store.get_meta(headkey,'')!=head:raise ValueError('Repair changed concurrently')
            repair=self.store.insert_task(owner,chat,reply,payload)
            self.store.meta(headkey,repair['id'])
        await self.render_task(owner,chat,reply,repair)

    async def refresh_task_location_cards(self):
        from .inbound import OWNER
        if not self.location_allowed(OWNER,OWNER):return
        marker='task-location-ui:'+str(self.engine.bot_id)+':'+str(OWNER)+':v2:'
        rows=self.store.db.execute("SELECT * FROM tasks WHERE owner=? AND chat=? AND reply>0 AND state IN ('done','failed','cancelled') ORDER BY rowid DESC LIMIT 32",(OWNER,OWNER)).fetchall()
        blocked={r[0] for r in self.store.db.execute("SELECT DISTINCT reply FROM tasks WHERE owner=? AND chat=? AND reply>0 AND state IN ('ambiguous','queued','running','delayed')",(OWNER,OWNER))}
        candidates={}
        def eligible(task):
            payload=json.loads(task['payload'])
            return payload.get('action') in ('download','both') and not payload.get('inbound_notice') and not payload.get('origin_notice')
        for task in rows:
            if eligible(task):candidates.setdefault(task['reply'],task)
        # Persisted native capabilities also identify deduplicated submission cards.
        # Legacy navigation predates revision fields; permit it only under the retained
        # verified principal. Explicit bot/revision fields must always match exactly.
        principal=self.store.get_meta('bot_principal','')==str(self.engine.bot_id)
        caps=self.store.db.execute("SELECT * FROM callbacks WHERE owner=? AND chat=? AND reply>0 ORDER BY rowid DESC LIMIT 256",(OWNER,OWNER)).fetchall()
        for cap in caps:
            try:
                data=json.loads(cap['payload'])
                if data.get('kind')!='task' or data.get('do') not in ('details','locations','cancel','retry','resume','cached_photos','missing_repair'):continue
                task=self.store.task(data['task'],OWNER,OWNER)
                if task['state'] not in ('done','failed','cancelled') or not eligible(task):continue
                if data.get('bot',self.engine.bot_id)!=self.engine.bot_id or data.get('revision',digest(task['payload']))!=digest(task['payload']):continue
                if ('bot' not in data or 'revision' not in data) and not principal:continue
                candidates.setdefault(cap['reply'],task)
            except (KeyError,ValueError,PermissionError,TypeError):continue
        for reply,task in list(candidates.items())[:32]:
            if reply in blocked:continue
            # A capability for an active operation must also protect its anchor.
            conflict=False
            for cap in caps:
                if cap['reply']!=reply:continue
                try:
                    data=json.loads(cap['payload'])
                    other=self.store.task(data['task'],OWNER,OWNER) if data.get('kind')=='task' and data.get('task') else None
                    if other and other['state'] in ('ambiguous','queued','running','delayed'):conflict=True;break
                except (KeyError,ValueError,PermissionError,TypeError):continue
            if conflict:continue
            stamp=digest([task['id'],task['payload'],task['state'],task['progress']])
            cardmarker=marker+str(reply)
            if self.store.get_meta(cardmarker,'')==stamp:continue
            try:await self.render_task(OWNER,OWNER,reply,task)
            except RetryAfter as exc:
                seconds=exc.retry_after.total_seconds() if hasattr(exc.retry_after,'total_seconds') else exc.retry_after
                self.engine.gate.delay(float(seconds));return
            except Exception:continue  # Failed cards remain retryable; never send replacements.
            self.store.meta(cardmarker,stamp)

    async def render_task(self, owner, chat, reply, task, *, detail=False):
        # Re-read by owner/chat; notification payloads and stale buttons are not authority.
        task = self.store.task(task["id"], owner, chat)
        p, total, completed, downloaded = self.task_counts(task)
        if not reply:
            sent = await self.bot.send_message(chat_id=chat, text="正在讀取任務…")
            reply = sent.message_id
        action = {"download": "下載", "forward": "轉發", "both": "下載＋轉發", "cached_photos": "傳送已下載圖片（不重新下載）"}[p["action"]]
        text = "任務 "+task["id"]+"："+task["state"]+"\n"+action+"；"+self.media_counts(p["items"])
        if task['state']=='done' and p['action'] in ('download','both'):
            text+='\n歷史已完成 '+str(completed)+'/'+str(total)+'；不代表目前檔案仍存在或本次有重新下載。'
            if self.location_allowed(owner,chat):
                inventory=await self.location_inventory(task)
                text+='\n目前可用 '+str(sum(v[2]=='可用' for v in inventory))+'/'+str(total)+'；遺失 '+str(sum(v[2]=='檔案遺失' for v in inventory))+'（📂 下載位置查看／補下載）'
                uncertain=sum(v[2] not in ('可用','檔案遺失') for v in inventory)
                if uncertain:text+='；待核對 '+str(uncertain)
        else:text += "\n已完成 "+str(completed)+"/"+str(total)+"；已下載 "+str(downloaded)+"/"+str(total)
        if p.get('missing_repair'):text+='\n補下載遺失項目；原歷史任務 '+p['missing_repair']['origin']+' 保留；不會重新轉發。'
        stats=json.loads(self.store.get_meta('task-last-run:'+task['id'],'null'))
        if stats:text+='\n最近一次執行（attempt '+str(stats['attempt'])+'）：新下載 '+str(stats['fresh'])+'；重用／略過 '+str(stats['reused'])
        elif task['state']=='done':text+='\n沒有本次重新下載紀錄；並非新下載成功。'
        if task["state"] in ("done", "failed", "cancelled", "ambiguous"):
            text += "\n失敗／未完成餘下 "+str(total-completed)+" 項"
        if detail:text += "\n嘗試 "+str(task["attempt"])
        if task["error"]:
            text += "\n"+(task["error"] if detail else task["error"][:180])
        if task["state"] == "delayed":
            text += "\n可重試時間："+time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(task["due"]))
        if task["state"] == "ambiguous":
            text += "\n傳送結果未確認，必須核對目的地，禁止盲目重送。"
        live=self.engine.progress_text(task)
        if live:text+='\n'+live
        if not detail:
            text=text.replace('任務 '+task['id']+'：'+task['state']+'\n'+action+'；',action+'｜'+task['state']+'\n',1)
            text=text.replace('；不代表目前檔案仍存在或本次有重新下載。','')
            text=text.replace('\n沒有本次重新下載紀錄；並非新下載成功。','')
            if p.get('missing_repair'):
                text=text.replace('補下載遺失項目；原歷史任務 '+p['missing_repair']['origin']+' 保留；不會重新轉發。','補下載遺失項目（不轉發）')
            text=text.replace('；不代表目前檔案仍存在或本次有重新下載。','；非本次新下載').replace('（📂 下載位置查看／補下載）','').replace('最近一次執行（attempt '+str(task['attempt'])+'）：',('本次紀錄：' if task['state']=='running' else '最近執行：')).replace('沒有本次重新下載紀錄；並非新下載成功。','沒有本次新下載紀錄。')
            text=text.replace('\n失敗／未完成餘下 0 項','')
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
            self.store.retry(tid,owner,chat)
        return self.store.task(tid, owner, chat)

    async def task_callback(self, owner, chat, reply, data):
        if data.get("do") == "list":
            await self.render_tasks(owner, chat, reply)
            return
        if data.get("do") not in ("details", "cancel", "retry", "resume", "cached_photos", "locations", "missing_repair"):
            raise ValueError("不支援任務動作")
        task = self.store.task(data.get("task"), owner, chat)
        if data['do']=='missing_repair':
            await self.repair_missing(owner,chat,reply,task,data)
            return
        if data['do']=='locations':
            await self.render_locations(owner,chat,reply,task,data)
            return
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
        await self.render_task(owner, chat, reply, task, detail=data['do']=='details')

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
