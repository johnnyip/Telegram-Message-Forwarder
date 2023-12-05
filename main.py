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
    target_chat = [int(id) if id.isdigit() else id for id in os.getenv("CHAT_TARGET").split(',')] if os.getenv("CHAT_TARGET") is not None else []
    chat_ids = [int(id) for id in os.getenv("CHAT_IDS").split(',')] if os.getenv("CHAT_IDS") is not None else []
    ignore_username = [username.strip() for username in os.getenv("CHAT_IGNORE_USERNAME").split(",")] if os.getenv("CHAT_IGNORE_USERNAME") else []
    delay_seconds = int(os.getenv("DELAY_SECONDS", 5))
    # print(target_chat)
    # print(chat_ids)
    # print(ignore_username)
    # print(message)
    # print(message.from_user)

    # print(f"1: {message.chat.id in chat_ids}")
    # print(f"2: {message.from_user is not None}")
    # print(f"3: {message.from_user.username not in ignore_username}")

    if message.chat.id in chat_ids:

        if (message.from_user is not None and message.from_user.username in ignore_username):
            print(f"[{datetime.now()}]Message from ignored user {message.from_user.username} chat {message.chat.id}: {message.text}")
        
        else:
            # Create the file path
            file_path_prefix = f"/app/downloads/{message.chat.id}/"
            file_path_prefix = file_path_prefix.replace(" ", "_")


            # Check the type of the message and download if it's a type we're interested in
            if message.audio: 
                file_path = await client.download_media(message=message, file_name=f"{file_path_prefix}/{message.id}_{message.from_user.username}.mp3")
                print(f"[{datetime.now()}]Downloaded audio file to {file_path}")
                # Send the downloaded media to another user or group
                for chat in target_chat:
                    time.sleep(delay_seconds)
                    print(f"[{datetime.now()}]Forwarding audio to chat: {chat}")
                    if (message.chat.has_protected_content == False):
                        await client.forward_messages(chat_id=chat)
                    else:
                        await client.send_audio(chat_id=chat, audio=file_path)
                    print(f"[{datetime.now()}]File sent to chat: {chat}")




            elif message.photo:
                
                file_path = await client.download_media(message=message, file_name=f"{file_path_prefix}/{message.id}_{message.from_user.username}.jpg")
                print(f"[{datetime.now()}]Downloaded photo file to {file_path}")
                for chat in target_chat:
                    time.sleep(delay_seconds)
                    print(f"[{datetime.now()}]Forwarding photo to chat: {chat}")
                    if (message.chat.has_protected_content == False):
                        await client.forward_messages(chat_id=chat)
                    else:
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
                if (message.chat.has_protected_content == False):
                    for chat in target_chat:
                        time.sleep(delay_seconds)
                        print(f"[{datetime.now()}]Forwarding document to chat: {chat}")
                        await client.forward_messages(chat_id=chat,from_chat_id=message.chat.id,message_ids=message.id)

                else:
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
                print(f"[{datetime.now()}]Text message from user {message.from_user.username} chat {message.chat.id} ignored: {message.text}")


    else:
        print(f"[{datetime.now()}]Message from chat {message.chat.id} ignored: {message.text}")


app.run()



#python main.py