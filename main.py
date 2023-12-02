from pyrogram import Client
from datetime import datetime
from os.path import splitext
import asyncio
import os
import time
import mimetypes
from dotenv import load_dotenv

# Load the environment variables from .env file
load_dotenv()

app = Client(
    "my_account",
    api_id=os.getenv("API_ID"),
    api_hash=os.getenv("API_HASH")
)

# list of chat IDs you are interested in
media_types = ["audio", "photo", "video", "document", "voice", "video_note", "animation"]


@app.on_message()
async def handle_message(client, message):
    # Check if the message is from a chat in your list
    target_chat = os.getenv("CHAT_TARGET")
    chat_ids = os.getenv("CHAT_IDS")    
    delay_seconds = int(os.getenv("DELAY_SECONDS"))

    # Get chat id array
    if chat_ids is not None:
        # Convert the string of IDs to a list of integers
        chat_ids = [int(id) for id in chat_ids.split(',')]
    else:
        chat_ids = []

    # Get target_chat id array
    if target_chat is not None:
        target_chat = [int(id) if id.isdigit() else id for id in target_chat.split(',')]
    else:
        target_chat = []

    if message.chat.id in chat_ids:
        # Create the file path
        file_path_prefix = f"/app/downloads/{message.chat.id}/"
        file_path_prefix = file_path_prefix.replace(" ", "_")
        # print(message)


        # Check the type of the message and download if it's a type we're interested in
        if message.audio: 
            file_path = await client.download_media(message=message, file_name=f"{file_path_prefix}/{message.id}_{message.from_user.username}.mp3")
            print(f"[{datetime.now()}]Downloaded audio file to {file_path}")
            # Send the downloaded media to another user or group
            for chat in target_chat:
                time.sleep(delay_seconds)
                await client.send_audio(chat_id=chat, audio=file_path)
                print(f"[{datetime.now()}]File sent to chat: {chat}")

        elif message.photo:
            file_path = await client.download_media(message=message, file_name=f"{file_path_prefix}/{message.id}_{message.from_user.username}.jpg")
            print(f"[{datetime.now()}]Downloaded photo file to {file_path}")
            for chat in target_chat:
                time.sleep(delay_seconds)
                await client.send_photo(chat_id=chat, photo=file_path)
                print(f"[{datetime.now()}]File sent to chat: {chat}")

        elif message.video:
            file_path = await client.download_media(message=message, file_name=f"{file_path_prefix}/{message.id}_{message.from_user.username}.mp4")
            print(f"[{datetime.now()}]Downloaded video file to {file_path}")
            for chat in target_chat:
                time.sleep(delay_seconds)
                await client.send_video(chat_id=chat, video=file_path)
                print(f"[{datetime.now()}]File sent to chat: {chat}")

        elif message.document:
            # Get file extension based on the MIME type, if the filename is not set
            file_path = await client.download_media(message=message, file_name=f"{file_path_prefix}/{message.id}_{message.from_user.username}_{message.document.file_name}")
            print(f"[{datetime.now()}]Downloaded document file to {file_path}")
            for chat in target_chat:
                time.sleep(delay_seconds)
                await client.send_document(chat_id=chat, document=file_path)
                print(f"[{datetime.now()}]File sent to chat: {chat}")

        elif message.animation:
            file_path = await client.download_media(message=message, file_name=f"{file_path_prefix}/{message.id}_{message.from_user.username}.gif")
            print(f"[{datetime.now()}]Downloaded animation file to {file_path}")
            for chat in target_chat:
                time.sleep(delay_seconds)
                await client.send_animation(chat_id=chat, animation=file_path)
                print(f"[{datetime.now()}]File sent to chat: {chat}")



    else:
        print(f"[{datetime.now()}]Message from chat {message.chat.id} ignored: {message.text}")


app.run()
