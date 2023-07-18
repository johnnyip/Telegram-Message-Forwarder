from pyrogram import Client
from datetime import datetime
from os.path import splitext
import asyncio
import os
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

    if chat_ids is not None:
        # Convert the string of IDs to a list of integers
        chat_ids = [int(id) for id in chat_ids.split(',')]
    else:
        # If there's no environment variable, use an empty list
        chat_ids = []

    if message.chat.id in chat_ids and message.from_user.username != "airwicka":
        # print(message)
        # Format the date and time
        # media_datetime = message.date.strftime('%Y-%m-%d_%H-%M-%S'
        
        # Create the file path
        # file_path_prefix = f"{message.chat.title}_{message.date}_{message.from_user.username}"
        file_path_prefix = f"{message.id}_{message.from_user.username}"
        file_path_prefix = file_path_prefix.replace(" ", "_")

        # Check the type of the message and download if it's a type we're interested in
        if message.audio: 
            file_path = await client.download_media(message=message, file_name=f"{file_path_prefix}.mp3")
            print(f"Downloaded file to {file_path}")
            # Send the downloaded media to another user or group
            await client.send_audio(chat_id=target_chat, audio=file_path)
        elif message.photo:
            file_path = await client.download_media(message=message, file_name=f"{file_path_prefix}.jpg")
            print(f"Downloaded file to {file_path}")
            await client.send_photo(chat_id=target_chat, photo=file_path)
        elif message.video:
            file_path = await client.download_media(message=message, file_name=f"{file_path_prefix}.mp4")
            print(f"Downloaded file to {file_path}")
            await client.send_video(chat_id=target_chat, video=file_path)
        elif message.document:
            # Get file extension based on the MIME type, if the filename is not set
            ext = mimetypes.guess_extension(message.document.mime_type) if message.document.file_name is None else ''
            file_path = await client.download_media(message=message, file_name=f"{file_path_prefix}{ext}")
            print(f"Downloaded file to {file_path}")
            await client.send_document(chat_id=target_chat, document=file_path)
        elif message.animation:
            file_path = await client.download_media(message=message, file_name=f"{file_path_prefix}.gif")
            print(f"Downloaded file to {file_path}")
            await client.send_animation(chat_id=target_chat, animation=file_path)


    else:
        print(f"Message from chat {message.chat.id} ignored")


app.run()
