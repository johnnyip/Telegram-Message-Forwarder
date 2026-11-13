"""Bot-only archive delivery. No native forward fallback, no Kafka spool cleanup."""
from contextlib import ExitStack
from telegram import InputMediaPhoto, InputMediaVideo, InputFile
from telegram.error import BadRequest, Forbidden, NetworkError, RetryAfter
from .engine import Ambiguous


class BotDelivery:
    def __init__(self, bot, max_bytes=49*1024*1024):
        self.bot, self.max_bytes = bot, max_bytes

    async def send(self, target, group, files):
        if any(p.stat().st_size > self.max_bytes for p in files):
            raise ValueError("媒體超過 Bot API 上載上限；下載檔保留，不改用原生轉發")
        if any(len(i["caption"]) > 1024 for i in group):
            raise ValueError("來源說明超過 Bot API 1024 字限制；下載檔及完整說明保留，未截斷轉發")
        if any(i["kind"] == "photo" and p.stat().st_size > 10*1024*1024 for i,p in zip(group,files)):
            raise ValueError("相片超過 Bot API 10 MiB 上限；下載檔保留")
        with ExitStack() as stack:
            streams = [InputFile(stack.enter_context(open(p, "rb")), filename=p.name, read_file_handle=False) for p in files]
            args = {"chat_id": target[0], "message_thread_id": target[1]}
            if len(files) > 1:
                media = [(InputMediaPhoto if i["kind"] == "photo" else InputMediaVideo)(stream, caption=i["caption"])
                         for i,stream in zip(group, streams)]
                return await self.bot.send_media_group(media=media, **args)
            i = group[0]
            if i["kind"] == "photo":
                return await self.bot.send_photo(photo=streams[0], caption=i["caption"], **args)
            return await self.bot.send_video(video=streams[0], caption=i["caption"], **args)
