#!/usr/bin/env python3
"""
Audio Converter Bot - main.py
Uses Pyrogram + aiohttp + Motor (MongoDB) to accept media, probe audio tracks,
convert selected track(s) with optional watermark mixing, and upload result.
"""

import os
import re
import math
import time
import json
import signal
import asyncio
import logging
import aiohttp
import shlex
import uuid
import shutil
from aiohttp import web
from datetime import datetime
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv
from pyrogram import Client, filters, enums
from pyrogram.errors import FloodWait, MessageIdInvalid, MessageNotModified
from pyrogram.types import Message, InlineKeyboardButton, InlineKeyboardMarkup, CallbackQuery

# -------------------------------------------------------------------------------- #
# CONFIGURATION
# -------------------------------------------------------------------------------- #

load_dotenv()

logging.basicConfig(level=logging.INFO, format='[%(asctime)s - %(levelname)s] - %(message)s')
LOGGER = logging.getLogger(__name__)
logging.getLogger("pyrogram").setLevel(logging.WARNING)

class Config:
    API_ID = int(os.environ.get("API_ID", 0))
    API_HASH = os.environ.get("API_HASH", "")
    BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
    OWNER_ID = int(os.environ.get("OWNER_ID", 0))

    MONGO_URI = os.environ.get("MONGO_URI", "")
    PORT = int(os.environ.get("PORT", 8080))
    
    # Directory for temporary file downloads
    DOWNLOAD_DIR = os.environ.get("DOWNLOAD_DIR", f"/tmp/audio_bot_downloads_{uuid.uuid4()}/")

    # For Keep-Alive Pinger
    STREAM_URL = os.environ.get("STREAM_URL", "").rstrip('/')
    PING_INTERVAL = int(os.environ.get("PING_INTERVAL", 1200))
    ON_HEROKU = 'DYNO' in os.environ

    # New settings
    # Limit concurrent FFMPEG jobs to avoid overloading server
    MAX_CONCURRENT_JOBS = int(os.environ.get("MAX_CONCURRENT_JOBS", 1)) 
    # Telegram's 2GB limit for bot uploads
    TELEGRAM_MAX_FILE_SIZE = int(os.environ.get("TELEGRAM_MAX_FILE_SIZE", 2 * 1024 * 1024 * 1024))
    # Auto-clear conversation state after 5 minutes (300 seconds)
    CONVERSATION_CLEAR_DELAY = int(os.environ.get("CONVERSATION_CLEAR_DELAY", 300))

# --- VALIDATE ESSENTIAL CONFIGURATIONS ---
required_vars = [
    Config.API_ID, Config.API_HASH, Config.BOT_TOKEN, Config.OWNER_ID,
    Config.MONGO_URI, Config.PORT
]
if not all(required_vars):
    LOGGER.critical("FATAL: One or more required variables are missing. Cannot start.")
    exit(1)

# Ensure download directory exists
if not os.path.isdir(Config.DOWNLOAD_DIR):
    try:
        os.makedirs(Config.DOWNLOAD_DIR, exist_ok=True)
        LOGGER.info(f"Created download directory: {Config.DOWNLOAD_DIR}")
    except OSError as e:
        LOGGER.critical(f"Failed to create download directory {Config.DOWNLOAD_DIR}: {e}")
        exit(1)

# -------------------------------------------------------------------------------- #
# GLOBAL VARIABLES
# -------------------------------------------------------------------------------- #

# Semaphore to limit concurrent FFMPEG jobs
ffmpeg_semaphore = asyncio.Semaphore(Config.MAX_CONCURRENT_JOBS)

# Dictionary to track download progress for each message
DOWNLOAD_PROGRESS = {}

# -------------------------------------------------------------------------------- #
# DATABASE
# -------------------------------------------------------------------------------- #

db_client = AsyncIOMotorClient(Config.MONGO_URI)
db = db_client['AudioBotDB']
user_conversations_col = db['conversations']
bot_settings_collection = db['settings'] # For saving watermark
admin_collection = db['admins'] # For saving other admins

# --- Conversation State ---
async def get_user_conversation(chat_id):
    return await user_conversations_col.find_one({"_id": chat_id})

async def update_user_conversation(chat_id, data):
    if data:
        await user_conversations_col.update_one(
            {"_id": chat_id}, {"$set": data}, upsert=True
        )
    else:
        await user_conversations_col.delete_one({"_id": chat_id})

async def clear_conversation_after_delay(chat_id, delay=Config.CONVERSATION_CLEAR_DELAY):
    """
    Waits for a specified delay and then clears the user's conversation state.
    """
    await asyncio.sleep(delay)
    await update_user_conversation(chat_id, None)
    LOGGER.info(f"Auto-cleared conversation state for chat_id: {chat_id}")

# --- Watermark Settings ---
async def get_watermark_settings():
    """Fetches watermark file_id and volume."""
    settings = await bot_settings_collection.find_one({"_id": "watermark"})
    if not settings:
        return {"file_id": None, "volume": 0.2} # Default volume
    return {
        "file_id": settings.get("file_id"),
        "volume": float(settings.get("volume", 0.2))
    }

async def set_watermark_file_id(file_id):
    await bot_settings_collection.update_one(
        {"_id": "watermark"}, {"$set": {"file_id": file_id}}, upsert=True
    )

async def set_watermark_volume(volume: float):
    await bot_settings_collection.update_one(
        {"_id": "watermark"}, {"$set": {"volume": float(volume)}}, upsert=True
    )

async def delete_watermark():
    await bot_settings_collection.update_one(
        {"_id": "watermark"}, {"$unset": {"file_id": ""}}
    )

# --- Admin Management ---
async def get_admin_list():
    """Returns a list of admin user IDs, excluding the OWNER_ID."""
    admins_cursor = admin_collection.find({"_id": {"$ne": Config.OWNER_ID}})
    return [admin["_id"] async for admin in admins_cursor]

async def add_admin(user_id: int):
    if user_id == Config.OWNER_ID:
        return
    await admin_collection.update_one(
        {"_id": user_id}, {"$set": {"date_added": datetime.utcnow()}}, upsert=True
    )

async def remove_admin(user_id: int):
    await admin_collection.delete_one({"_id": user_id})

# -------------------------------------------------------------------------------- #
# BOT FILTERS
# -------------------------------------------------------------------------------- #

async def admin_filter_func(_, __, message_or_query):
    # message_or_query: Might be Message or CallbackQuery
    user_id = None
    if isinstance(message_or_query, CallbackQuery):
        user_id = message_or_query.from_user.id
    elif isinstance(message_or_query, Message):
        user_id = message_or_query.from_user.id
    else:
        # fallback
        user_id = getattr(message_or_query, 'from_user', None).id if getattr(message_or_query, 'from_user', None) else None

    if user_id == Config.OWNER_ID:
        return True
    
    admin_list = await get_admin_list()
    return user_id in admin_list

admin_filter = filters.create(admin_filter_func)

# -------------------------------------------------------------------------------- #
# FFMPEG & SHELL UTILITIES
# -------------------------------------------------------------------------------- #

async def run_shell_command(command):
    """Runs a shell command asynchronously (NOT for ffmpeg monitoring)."""
    process = await asyncio.create_subprocess_shell(
        command,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )
    stdout, stderr = await process.communicate()
    if process.returncode != 0:
        LOGGER.error(f"Command failed: {command}\nError: {stderr.decode().strip()}")
        raise Exception(f"Command failed: {stderr.decode().strip()}")
    return stdout.decode().strip()

async def probe_media(file_path):
    """Runs ffprobe to get stream info (audio streams)."""
    command = (
        f"ffprobe -v error -show_streams -of json "
        f"{shlex.quote(file_path)}"
    )
    try:
        result_json = await run_shell_command(command)
        return json.loads(result_json).get("streams", [])
    except Exception as e:
        LOGGER.error(f"Failed to probe file {file_path}: {e}")
        return []

async def get_media_duration(file_path):
    """Runs ffprobe to get media duration (seconds)."""
    command = (
        f"ffprobe -v error -show_entries format=duration -of default=noprint_wrappers=1:nokey=1 "
        f"{shlex.quote(file_path)}"
    )
    try:
        duration_str = await run_shell_command(command)
        return float(duration_str)
    except Exception as e:
        LOGGER.warning(f"Failed to get duration for {file_path}: {e}")
        return 0.0

async def run_ffmpeg_with_progress(args_list, total_duration_seconds, status_msg, update_every=5.0):
    """
    Runs ffmpeg via create_subprocess_exec and monitors progress using '-progress pipe:1'.
    args_list should be a list (no shell string). This function will append '-progress pipe:1'
    if it's not present.
    """
    # Ensure -progress pipe:1 is included for progress reporting
    if "-progress" not in args_list:
        args_list.extend(["-progress", "pipe:1", "-nostats"])

    LOGGER.info(f"Running FFMPEG: {' '.join(args_list)}")
    
    process = await asyncio.create_subprocess_exec(
        *args_list,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )

    last_update = 0.0
    stderr_acc = bytearray()

    # ffmpeg will write progress KV pairs to stdout when using -progress pipe:1
    while True:
        line = await process.stdout.readline()
        if not line:
            break
        
        try:
            text = line.decode('utf-8', errors='ignore').strip()
        except Exception:
            text = ''
        
        if not text:
            continue

        # parse key=value
        if '=' in text:
            try:
                k, v = text.split('=', 1)
                k = k.strip()
                v = v.strip()
                
                # out_time_ms is milliseconds according to ffmpeg's -progress
                if k == 'out_time_ms':
                    try:
                        out_ms = int(v)
                    except ValueError:
                        out_ms = 0
                    out_seconds = out_ms / 1000.0  # CORRECT: ms -> seconds
                    if total_duration_seconds > 0:
                        percent = min(100.0, (out_seconds / total_duration_seconds) * 100.0)
                    else:
                        percent = 0.0
                    
                    now = time.time()
                    if now - last_update > update_every:
                        try:
                            await status_msg.edit_text(f"⚙️ **Converting... Please wait.**\n\n**Progress:** {percent:.1f}%")
                        except (MessageNotModified, FloodWait):
                            pass
                        except Exception as e:
                            LOGGER.warning(f"Failed to edit progress message: {e}")
                        last_update = now
                
                elif k == 'progress' and v == 'end':
                    try:
                        await status_msg.edit_text("⚙️ **Converting...**\n\nFinalizing... 99.9%")
                    except Exception:
                        pass
            except Exception as e:
                LOGGER.warning(f"Error parsing progress line '{text}': {e}")

    # Read remaining stderr for error logging
    stderr_data = await process.stderr.read()
    if stderr_data:
        stderr_acc.extend(stderr_data)

    rc = await process.wait()
    
    if rc != 0:
        err_text = stderr_acc.decode('utf-8', errors='ignore')[:4000]
        LOGGER.error(f"FFMPEG failed (rc={rc}): {err_text}\nCommand: {' '.join(args_list)}")
        raise RuntimeError(f"FFMPEG conversion failed. See logs for details.\nError: {err_text}")
    
    LOGGER.info("FFMPEG conversion successful.")
    return True

# -------------------------------------------------------------------------------- #
# TELEGRAM BOT HANDLERS
# -------------------------------------------------------------------------------- #

bot = Client("AudioBot", api_id=Config.API_ID, api_hash=Config.API_HASH, bot_token=Config.BOT_TOKEN)

# --- Progress Callback for Upload/Download ---
async def progress_callback(current, total, message, action):
    """
    Dynamically updates the status message with progress.
    Uses a global dict to track last update time per message.
    """
    global DOWNLOAD_PROGRESS
    try:
        percent = (current / total) * 100 if total else 0.0
    except Exception:
        percent = 0.0
    current_time = time.time()
    msg_id = getattr(message, "id", None) or 0

    last_update_time = DOWNLOAD_PROGRESS.get(msg_id, 0)
        
    if current_time - last_update_time > 3:
        try:
            await message.edit_text(f"**{action}...** {percent:.1f}%")
            DOWNLOAD_PROGRESS[msg_id] = current_time
        except (MessageNotModified, FloodWait):
            pass
        except Exception as e:
            LOGGER.warning(f"Error updating progress: {e}")

# --- Start & Main Menu ---
@bot.on_message(filters.command("start") & filters.private & admin_filter)
async def start_command(client, message):
    buttons = [
        [InlineKeyboardButton("🎧 Audio Tools", callback_data="audio_tools_menu")]
    ]
    if message.from_user.id == Config.OWNER_ID:
        buttons.append([InlineKeyboardButton("👨‍💼 Admin Management", callback_data="admin_menu")])
    
    await message.reply_text(
        "**🎧 Welcome to the Audio Converter Bot!**\n\n"
        "I can help you convert audio files, extract tracks, and add watermarks.",
        reply_markup=InlineKeyboardMarkup(buttons)
    )
    await update_user_conversation(message.chat.id, None)

@bot.on_callback_query(filters.regex("^main_menu$") & admin_filter)
async def main_menu_cb(client, cb: CallbackQuery):
    await cb.answer()
    buttons = [
        [InlineKeyboardButton("🎧 Audio Tools", callback_data="audio_tools_menu")]
    ]
    if cb.from_user.id == Config.OWNER_ID:
        buttons.append([InlineKeyboardButton("👨‍💼 Admin Management", callback_data="admin_menu")])
        
    try:
        await cb.message.edit_text(
            "**🎧 Welcome to the Audio Converter Bot!**\n\n"
            "I can help you convert audio files, extract tracks, and add watermarks.",
            reply_markup=InlineKeyboardMarkup(buttons)
        )
    except MessageNotModified:
        pass
    await update_user_conversation(cb.message.chat.id, None)

@bot.on_callback_query(filters.regex("^audio_tools_menu$") & admin_filter)
async def audio_tools_menu_cb(client, cb: CallbackQuery):
    await cb.answer()
    await cb.message.edit_text(
        "**🎧 Audio Tools**\n\nSelect an option:",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🎵 Convert Audio File", callback_data="convert_audio_start")],
            [InlineKeyboardButton("⚙️ Watermark Settings", callback_data="watermark_settings")],
            [InlineKeyboardButton("⬅️ Back to Main Menu", callback_data="main_menu")]
        ])
    )

@bot.on_callback_query(filters.regex("^cancel_conv$") & admin_filter)
async def cancel_conversation_handler(client, cb: CallbackQuery):
    await cb.answer("Operation Cancelled.")
    chat_id = cb.message.chat.id
    
    # Clean up temp files if any
    conv = await get_user_conversation(chat_id)
    if conv:
        job_dir = conv.get("job_dir")
        if job_dir and os.path.isdir(job_dir):
            try:
                shutil.rmtree(job_dir)
                LOGGER.info(f"Cleaned up job directory: {job_dir}")
            except Exception as e:
                LOGGER.error(f"Failed to cleanup job directory {job_dir}: {e}")

    # Clear conversation state after a delay
    asyncio.create_task(clear_conversation_after_delay(chat_id))
    
    try:
        await cb.message.delete()
    except Exception:
        pass
    await start_command(client, cb.message)

# --- Watermark Settings Flow ---
@bot.on_callback_query(filters.regex("^watermark_settings$") & admin_filter)
async def watermark_settings_cb(client, cb: CallbackQuery):
    await cb.answer()
    settings = await get_watermark_settings()
    watermark_id = settings.get("file_id")
    current_volume = settings.get("volume", 0.2)
    
    text = "**⚙️ Watermark Settings**\n\n"
    buttons = []
    
    if watermark_id:
        text += f"A watermark file is currently saved.\n**Current Volume:** `{int(current_volume * 100)}%`\n\n"
        buttons.append([InlineKeyboardButton("🔊 Change Volume", callback_data="watermark_set_volume")])
        buttons.append([InlineKeyboardButton("🗑️ Delete Current Watermark", callback_data="watermark_delete")])
    else:
        text += "No watermark file is set. Please upload an audio file (e.g., MP3, AAC, M4A) to use as a watermark."
    
    buttons.append([InlineKeyboardButton("⬆️ Upload New Watermark", callback_data="watermark_upload_start")])
    buttons.append([InlineKeyboardButton("⬅️ Back", callback_data="audio_tools_menu")])
    
    await cb.message.edit_text(text, reply_markup=InlineKeyboardMarkup(buttons))

@bot.on_callback_query(filters.regex("^watermark_set_volume$") & admin_filter)
async def watermark_set_volume_cb(client, cb: CallbackQuery):
    await cb.answer()
    await cb.message.edit_text(
        "**🔊 Set Watermark Volume**\n\n"
        "Select the volume for the watermark mix (Default is 20%).",
        reply_markup=InlineKeyboardMarkup([
            [
                InlineKeyboardButton("20%", callback_data="wm_vol_0.2"),
                InlineKeyboardButton("50%", callback_data="wm_vol_0.5"),
                InlineKeyboardButton("100%", callback_data="wm_vol_1.0")
            ],
            [InlineKeyboardButton("⬅️ Back", callback_data="watermark_settings")]
        ])
    )

@bot.on_callback_query(filters.regex(r"^wm_vol_(\d\.\d)$") & admin_filter)
async def watermark_save_volume_cb(client, cb: CallbackQuery):
    volume = float(cb.data.split("_")[-1])
    await set_watermark_volume(volume)
    await cb.answer(f"Watermark volume set to {int(volume * 100)}%")
    # Go back to settings menu
    await watermark_settings_cb(client, cb)

@bot.on_callback_query(filters.regex("^watermark_upload_start$") & admin_filter)
async def watermark_upload_start_cb(client, cb: CallbackQuery):
    await cb.answer()
    await update_user_conversation(cb.message.chat.id, {"stage": "awaiting_watermark_audio"})
    await cb.message.edit_text(
        "**⬆️ Upload Watermark**\n\nPlease send the audio file you want to use as a watermark (e.g., a short MP3 or AAC file).",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="watermark_settings")]])
    )

@bot.on_callback_query(filters.regex("^watermark_delete$") & admin_filter)
async def watermark_delete_cb(client, cb: CallbackQuery):
    await cb.answer("Deleting watermark...")
    await delete_watermark()
    await cb.message.edit_text(
        "✅ **Watermark Deleted**\n\nThe saved watermark file has been removed.",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="watermark_settings")]])
    )

# --- Admin Management Flow (OWNER_ID only) ---
@bot.on_callback_query(filters.regex("^admin_menu$") & filters.user(Config.OWNER_ID))
async def admin_menu_cb(client, cb: CallbackQuery):
    await cb.answer()
    await cb.message.edit_text(
        "**👨‍💼 Admin Management**\n\n"
        "Here you can add or remove other users who can use this bot.",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("➕ Add Admin", callback_data="admin_add")],
            [InlineKeyboardButton("➖ Remove Admin", callback_data="admin_remove_list")],
            [InlineKeyboardButton("⬅️ Back", callback_data="main_menu")]
        ])
    )

@bot.on_callback_query(filters.regex("^admin_add$") & filters.user(Config.OWNER_ID))
async def admin_add_start_cb(client, cb: CallbackQuery):
    await cb.answer()
    await update_user_conversation(cb.message.chat.id, {"stage": "awaiting_admin_id"})
    await cb.message.edit_text(
        "**➕ Add Admin**\n\n"
        "Please send the User ID of the user you want to add as an admin.",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="admin_menu")]])
    )

@bot.on_callback_query(filters.regex("^admin_remove_list$") & filters.user(Config.OWNER_ID))
async def admin_remove_list_cb(client, cb: CallbackQuery):
    await cb.answer()
    admins = await get_admin_list()
    if not admins:
        return await cb.message.edit_text(
            "**➖ Remove Admin**\n\nNo other admins found.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")]])
        )
    
    buttons = []
    text = "**➖ Remove Admin**\n\nSelect an admin to remove:\n"
    for admin_id in admins:
        try:
            user = await client.get_users(admin_id)
            name = user.first_name or f"User {admin_id}"
        except Exception:
            name = f"User {admin_id}"
        text += f"\n▪️ {name} (`{admin_id}`)"
        buttons.append([InlineKeyboardButton(f"❌ {name}", callback_data=f"admin_remove_{admin_id}")])
    
    buttons.append([InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")])
    await cb.message.edit_text(text, reply_markup=InlineKeyboardMarkup(buttons))

@bot.on_callback_query(filters.regex(r"^admin_remove_(\d+)$") & filters.user(Config.OWNER_ID))
async def admin_remove_confirm_cb(client, cb: CallbackQuery):
    user_id = int(cb.data.split("_")[-1])
    await remove_admin(user_id)
    await cb.answer(f"Admin {user_id} removed.", show_alert=True)
    # Refresh list
    await admin_remove_list_cb(client, cb)


# --- Audio Conversion Flow ---
@bot.on_callback_query(filters.regex("^convert_audio_start$") & admin_filter)
async def convert_audio_start_cb(client, cb: CallbackQuery):
    await cb.answer()
    job_id = str(uuid.uuid4())
    job_dir = os.path.join(Config.DOWNLOAD_DIR, job_id)
    os.makedirs(job_dir, exist_ok=True)
    
    await update_user_conversation(cb.message.chat.id, {
        "stage": "awaiting_media_file",
        "job_id": job_id,
        "job_dir": job_dir
    })
    await cb.message.edit_text(
        "**🎵 Convert Audio File**\n\nPlease send the video or audio file you want to convert.",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="cancel_conv")]])
    )

@bot.on_message(filters.private & (filters.audio | filters.video | filters.document | filters.text) & admin_filter)
async def message_handler_router(client, message: Message):
    chat_id = message.chat.id
    conv = await get_user_conversation(chat_id)
    if not conv:
        return
        
    stage = conv.get("stage")

    if stage == "awaiting_watermark_audio":
        if not (message.audio or (message.document and getattr(message.document, "mime_type", "").startswith("audio"))):
            await message.reply_text("That's not an audio file. Please send an audio file.")
            return
        await handle_watermark_upload(client, message, conv)
    
    elif stage == "awaiting_media_file":
        if not (message.audio or message.video or message.document):
            await message.reply_text("Invalid file type. Please send a media file.")
            return
        await handle_media_file(client, message, conv)

    elif stage == "awaiting_admin_id" and message.text:
        if message.from_user.id != Config.OWNER_ID:
            return
        await handle_admin_add(client, message, conv)

async def handle_watermark_upload(client, message: Message, conv: dict):
    status_msg = await message.reply_text("📥 **Saving watermark...**")
    file_id = message.audio.file_id if message.audio else message.document.file_id
    
    await set_watermark_file_id(file_id)
    
    await update_user_conversation(message.chat.id, None) # Clear stage
    await status_msg.edit_text(
        "✅ **Watermark Saved!**\n\nThis audio will now be used for mixing.",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back to Settings", callback_data="watermark_settings")]])
    )

async def handle_admin_add(client, message: Message, conv: dict):
    try:
        user_id = int(message.text.strip())
        await add_admin(user_id)
        await message.reply_text(
            f"✅ **Admin Added!**\n\nUser `{user_id}` can now use the bot.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back to Admin Menu", callback_data="admin_menu")]])
        )
        await update_user_conversation(message.chat.id, None) # Clear stage
    except ValueError:
        await message.reply_text("Invalid User ID. Please send numbers only.")
    except Exception as e:
        await message.reply_text(f"Error adding admin: {e}")


async def handle_media_file(client, message: Message, conv: dict):
    media = message.audio or message.video or message.document
    job_dir = conv.get("job_dir")
    
    if not job_dir or not os.path.isdir(job_dir):
        await message.reply_text("Error: Job directory not found. Please start over.", quote=True)
        asyncio.create_task(clear_conversation_after_delay(message.chat.id, delay=5))
        return

    status_msg = await message.reply_text("📥 **Downloading file...** 0%", quote=True)
    
    try:
        input_file_path = await message.download(
            file_name=os.path.join(job_dir, f"input_file_{uuid.uuid4()}"),
            progress=progress_callback,
            progress_args=(status_msg, "Downloading")
        )
    except Exception as e:
        LOGGER.error(f"File download failed: {e}", exc_info=True)
        try:
            await status_msg.edit_text(f"❌ **Download Failed!**\n\n`{e}`")
        except Exception:
            pass
        asyncio.create_task(clear_conversation_after_delay(message.chat.id))
        return
    finally:
        # Clear progress tracker for this message (status_msg may change later)
        DOWNLOAD_PROGRESS.pop(status_msg.id, None)

    await status_msg.edit_text("🔬 **Probing file...**\n\nPlease wait, analyzing media streams.")
    
    audio_streams = await probe_media(input_file_path)
    
    if not audio_streams:
        await status_msg.edit_text("❌ **Error!**\n\nNo audio streams were found in this file.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="cancel_conv")]])
        )
        return

    # Check if original file was audio-only
    is_audio_only = message.audio or (message.document and getattr(message.document, "mime_type", "").startswith("audio"))
    if not is_audio_only:
        # If it was a video, check if it *only* has audio streams
        all_streams_probe_cmd = f"ffprobe -v error -show_streams -of json {shlex.quote(input_file_path)}"
        try:
            all_streams_json = await run_shell_command(all_streams_probe_cmd)
            all_streams = json.loads(all_streams_json).get("streams", [])
            video_streams = [s for s in all_streams if s.get('codec_type') == 'video']
            if not video_streams:
                is_audio_only = True
        except Exception:
            pass # Assume it has video

    await update_user_conversation(message.chat.id, {
        "input_file_path": input_file_path,
        "audio_streams": audio_streams,
        "is_audio_only": is_audio_only,
        "job_dir": job_dir
    })

    buttons = []
    text = "**🔬 Analysis Complete!**\n\nPlease select the audio track to convert:\n\n"
    
    for stream in audio_streams:
        index = stream.get("index")
        codec = stream.get("codec_name", "unknown")
        lang = (stream.get("tags") or {}).get("language", "und")
        channels = stream.get("channels", "N/A")
        layout = stream.get("channel_layout", "N/A")
        
        label = f"Track {index}: {codec.upper()} ({layout} {channels}ch) [{lang.upper()}]"
        text += f"▪️ {label}\n"
        buttons.append([InlineKeyboardButton(label, callback_data=f"track_{index}")])
    
    buttons.append([InlineKeyboardButton("❌ Cancel", callback_data="cancel_conv")])
    await update_user_conversation(message.chat.id, {"stage": "awaiting_track_selection"})
    await status_msg.edit_text(text, reply_markup=InlineKeyboardMarkup(buttons))

@bot.on_callback_query(filters.regex(r"^track_(\d+)$") & admin_filter)
async def track_selection_cb(client, cb: CallbackQuery):
    await cb.answer()
    chat_id = cb.message.chat.id
    conv = await get_user_conversation(chat_id)
    if not conv or conv.get("stage") != "awaiting_track_selection":
        return await cb.answer("Session expired. Please start over.", show_alert=True)
    
    track_index = int(cb.data.split("_")[1])
    
    # Find the correct stream object by its index
    stream_obj = next((s for s in conv.get('audio_streams', []) if s.get('index') == track_index), None)
    
    if not stream_obj:
        return await cb.answer("Error: Selected stream not found. Please try again.", show_alert=True)

    await update_user_conversation(chat_id, {
        "selected_track_index": track_index,
        "selected_stream_obj": stream_obj, # Store the stream object
        "stage": "awaiting_format_selection"
    })
    
    buttons = [
        [InlineKeyboardButton("🎵 AAC (Stereo, 192k)", callback_data="format_aac_stereo")],
        [InlineKeyboardButton("🎧 AAC (5.1 Keep Channels)", callback_data="format_aac_5_1")],
        [InlineKeyboardButton("🎵 MP3 (Stereo, 192k)", callback_data="format_mp3_stereo")],
    ]
    
    # New Feature: Add "Copy" button if source is already AAC
    if stream_obj.get("codec_name") == "aac":
        buttons.insert(0, [InlineKeyboardButton("✨ Copy AAC Stream (Fastest)", callback_data="format_aac_copy")])
    
    buttons.extend([
        [InlineKeyboardButton("⬅️ Back (Reselect File)", callback_data="convert_audio_start")],
        [InlineKeyboardButton("❌ Cancel", callback_data="cancel_conv")]
    ])
    
    await cb.message.edit_text(
        f"✅ **Track {track_index} ({stream_obj.get('codec_name', '').upper()}) selected.**\n\nNow, choose the target format:",
        reply_markup=InlineKeyboardMarkup(buttons)
    )

@bot.on_callback_query(filters.regex(r"^format_(.+)"))
async def format_selection_cb(client, cb: CallbackQuery):
    await cb.answer()
    chat_id = cb.message.chat.id
    conv = await get_user_conversation(chat_id)
    if not conv or conv.get("stage") != "awaiting_format_selection":
        return await cb.answer("Session expired. Please start over.", show_alert=True)
    
    format_choice = cb.data.split("_", 1)[1]
    
    format_options = {
        "aac_stereo": {"codec": "aac", "channels": 2, "bitrate": "192k"},
        "aac_5_1": {"codec": "aac", "channels": 6, "bitrate": "320k"}, # Keep 6 channels
        "mp3_stereo": {"codec": "libmp3lame", "channels": 2, "bitrate": "192k"},
        "aac_copy": {"codec": "copy", "channels": "copy", "bitrate": "copy"} # Smart copy
    }
    
    if format_choice not in format_options:
        return await cb.answer("Invalid format.", show_alert=True)

    await update_user_conversation(chat_id, {"format": format_options[format_choice]})
    
    settings = await get_watermark_settings()
    watermark_file_id = settings.get("file_id")
    
    # If using "copy" mode, we cannot apply a watermark
    if format_choice == "aac_copy":
        await cb.answer("Copy mode selected. Watermark will be skipped.", show_alert=True)
        await update_user_conversation(chat_id, {"watermark": False})
        await ask_for_output_type(cb, conv)
    elif watermark_file_id:
        await update_user_conversation(chat_id, {"stage": "awaiting_watermark_selection"})
        await cb.message.edit_text(
            "✅ **Format selected.**\n\nDo you want to mix the saved watermark into this audio?",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("💧 Yes, Add Watermark", callback_data="watermark_yes")],
                [InlineKeyboardButton("❌ No Watermark", callback_data="watermark_no")],
                [InlineKeyboardButton("❌ Cancel", callback_data="cancel_conv")]
            ])
        )
    else:
        # No watermark set, skip to output selection
        await update_user_conversation(chat_id, {"watermark": False})
        await ask_for_output_type(cb, conv) # Function to ask for output type

@bot.on_callback_query(filters.regex(r"^watermark_(yes|no)$") & admin_filter)
async def watermark_selection_cb(client, cb: CallbackQuery):
    await cb.answer()
    chat_id = cb.message.chat.id
    conv = await get_user_conversation(chat_id)
    if not conv or conv.get("stage") != "awaiting_watermark_selection":
        return await cb.answer("Session expired. Please start over.", show_alert=True)
        
    use_watermark = (cb.data == "watermark_yes")
    await update_user_conversation(chat_id, {"watermark": use_watermark})
    await ask_for_output_type(cb, conv)

async def ask_for_output_type(cb, conv):
    """Helper function to ask for Audio Only or Remux."""
    chat_id = cb.message.chat.id
    
    if conv.get("is_audio_only"):
        # Original file was audio-only, so we can only output audio
        await update_user_conversation(chat_id, {
            "output_type": "audio",
            "stage": "processing"
        })
        try:
            await cb.message.edit_text("✅ **Ready!**\n\nStarting conversion (Audio Only)...")
        except Exception:
            pass
        await start_conversion_process(cb)
    else:
        # Original was a video, ask for output type
        await update_user_conversation(chat_id, {"stage": "awaiting_output_selection"})
        await cb.message.edit_text(
            "✅ **Settings complete.**\n\nWhat output do you want?",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🎵 Audio Only (.m4a/.mp3)", callback_data="output_audio")],
                [InlineKeyboardButton("🎬 Remux Video (.mkv)", callback_data="output_remux")],
                [InlineKeyboardButton("❌ Cancel", callback_data="cancel_conv")]
            ])
        )

@bot.on_callback_query(filters.regex(r"^output_(audio|remux)$") & admin_filter)
async def output_selection_cb(client, cb: CallbackQuery):
    await cb.answer()
    chat_id = cb.message.chat.id
    conv = await get_user_conversation(chat_id)
    if not conv or conv.get("stage") != "awaiting_output_selection":
        return await cb.answer("Session expired. Please start over.", show_alert=True)
        
    output_type = cb.data.split("_")[1]
    
    await update_user_conversation(chat_id, {
        "output_type": output_type,
        "stage": "processing"
    })
    
    await cb.message.edit_text(f"✅ **Ready!**\n\nStarting conversion (Output: {output_type})...")
    await start_conversion_process(cb)


# --- FFMPEG Command Builder ---

async def build_ffmpeg_args(conv, input_file, watermark_file):
    """
    Builds the final FFMPEG argument list (not a string)
    to be passed to create_subprocess_exec.
    """
    track_index = conv["selected_track_index"]
    stream_obj = conv["selected_stream_obj"]
    fmt = conv["format"]
    use_watermark = conv.get("watermark", False)
    output_type = conv["output_type"]
    job_dir = conv["job_dir"]
    
    output_ext = "mp3" if fmt['codec'] == 'libmp3lame' else "m4a"
    if output_type == "remux":
        output_ext = "mkv"
    elif fmt['codec'] == 'copy':
        output_ext = "m4a" # Assume AAC copy

    final_output_file = os.path.join(job_dir, f"output.{output_ext}")
    
    args = ["ffmpeg", "-y", "-hide_banner", "-i", input_file]
    
    filter_complex_parts = []
    # Default: direct input map (no brackets). If filter produced a label we'll use [aud_out]
    audio_map_label = f"0:a:{track_index}"
    
    if use_watermark and watermark_file and fmt['codec'] != 'copy':
        settings = await get_watermark_settings()
        wm_volume = settings.get("volume", 0.2)
        
        # second input is watermark
        args.extend(["-i", watermark_file])
        
        # Convert watermark if necessary by filter-chain implicitly handled by amix.
        filter_complex_parts.append(f"[0:a:{track_index}]volume=1.0[main]")
        filter_complex_parts.append(f"[1:a]volume={wm_volume}[wm]")
        filter_complex_parts.append(f"[main][wm]amix=inputs=2:duration=first[aud_out]")
        
        audio_map_label = "[aud_out]" # filter output label
    
    if filter_complex_parts:
        args.extend(["-filter_complex", ";".join(filter_complex_parts)])

    # --- Output Mapping ---
    
    # Map the (potentially filtered) audio
    args.extend(["-map", audio_map_label])
    
    # Audio Codec settings
    if fmt['codec'] == 'copy':
        args.extend(["-c:a", "copy"])
    else:
        args.extend([
            "-c:a", fmt['codec'],
            "-b:a", fmt['bitrate'],
            "-ac", str(fmt['channels']),
            "-ar", "48000", # Standardize sample rate
        ])
    
    # Add language metadata to the audio stream (safe retrieval)
    lang = (stream_obj or {}).get("tags", {}).get("language", "und")
    args.extend([f"-metadata:s:a:0", f"language={lang}"])

    # If remux, map video and subtitles (append mappings after audio map)
    if output_type == "remux":
        args.extend([
            "-map", "0:v:0?",  # Map first video stream (if it exists)
            "-map", "0:s?"    # Map all subtitle streams (if they exist)
        ])
        # Copy video/subs
        args.extend([
            "-c:v", "copy",
            "-c:s", "copy"
        ])

    # Final output file
    args.append(final_output_file)
    
    return args, final_output_file


# --- The Main Conversion Process ---
async def start_conversion_process(cb: CallbackQuery):
    chat_id = cb.message.chat.id
    status_msg = cb.message
    
    # Acquire semaphore to limit concurrency
    async with ffmpeg_semaphore:
        conv = await get_user_conversation(chat_id)
        if not conv or conv.get("stage") != "processing":
            return
            
        try:
            # 1. Get all variables from conversation
            job_dir = conv["job_dir"]
            input_file = conv["input_file_path"]
            fmt = conv["format"]
            use_watermark = conv.get("watermark", False)
            output_type = conv["output_type"]
            
            watermark_file = None
            
            # 2. Download watermark if needed
            if use_watermark and fmt['codec'] != 'copy':
                settings = await get_watermark_settings()
                watermark_file_id = settings.get("file_id")
                
                if not watermark_file_id:
                    raise Exception("Watermark file not found in database, but was selected.")
                
                try:
                    await status_msg.edit_text("Downloading watermark...")
                except Exception:
                    pass

                try:
                    watermark_file = await bot.download_media(
                        watermark_file_id,
                        file_name=os.path.join(job_dir, "watermark_audio")
                    )
                except Exception as e:
                    LOGGER.error(f"Failed to download watermark: {e}", exc_info=True)
                    raise Exception("Failed to download watermark file.") from e

            # 3. Get total duration for progress reporting
            try:
                await status_msg.edit_text("Preparing conversion...")
            except Exception:
                pass
            total_duration = await get_media_duration(input_file)

            # 4. Build the FFMPEG command (args list)
            ffmpeg_args, final_output_file = await build_ffmpeg_args(conv, input_file, watermark_file)
            
            # 5. Run conversion
            await run_ffmpeg_with_progress(ffmpeg_args, total_duration, status_msg)
            
            # 6. Check output file size
            try:
                await status_msg.edit_text("✅ **Conversion Complete!**\n\nVerifying file size...")
            except Exception:
                pass
            
            if not os.path.exists(final_output_file):
                raise Exception("Conversion finished, but no output file was found.")

            output_file_size = os.path.getsize(final_output_file)
            
            if output_file_size > Config.TELEGRAM_MAX_FILE_SIZE:
                size_gb = output_file_size / (1024**3)
                await status_msg.edit_text(
                    f"❌ **Upload Failed!**\n\n"
                    f"Converted file is **{size_gb:.2f} GB**, which is over Telegram's limit."
                )
                return # Do not upload

            # 7. Upload the result
            try:
                await status_msg.edit_text(f"✅ **Conversion Complete!**\n\nUploading result...")
            except Exception:
                pass
            
            await bot.send_document(
                chat_id,
                document=final_output_file,
                caption=f"**Conversion Complete!**\nFormat: `{fmt['codec']}`\nWatermark: `{'Yes' if use_watermark else 'No'}`",
                progress=progress_callback,
                progress_args=(status_msg, "Uploading")
            )
            try:
                await status_msg.delete()
            except Exception:
                pass

        except Exception as e:
            LOGGER.error(f"Full conversion process failed: {e}", exc_info=True)
            try:
                await status_msg.edit_text(f"❌ **Error!**\n\nProcess failed: `{str(e)}`")
            except Exception:
                pass
        
        finally:
            # 8. Cleanup
            try:
                job_dir = conv.get("job_dir")
                if job_dir and os.path.isdir(job_dir):
                    shutil.rmtree(job_dir)
                    LOGGER.info(f"Cleaned up job directory: {job_dir}")
            except Exception as e:
                LOGGER.error(f"Failed to cleanup job directory {job_dir}: {e}")
            
            # Clear progress tracker for this message
            DOWNLOAD_PROGRESS.pop(status_msg.id, None)
            
            # Schedule conversation state cleanup
            asyncio.create_task(clear_conversation_after_delay(chat_id))


# -------------------------------------------------------------------------------- #
# WEB SERVER (FOR KEEP-ALIVE)
# -------------------------------------------------------------------------------- #

routes = web.RouteTableDef()

@routes.get("/", allow_head=True)
async def root_route_handler(request):
    return web.Response(text="Audio Bot is alive!", content_type='text/html')

async def web_server():
    web_app = web.Application(client_max_size=30_000_000)
    web_app.add_routes(routes)
    return web_app
            
async def ping_server():
    """Pings the server to keep it alive on platforms like Heroku/Render."""
    if not Config.ON_HEROKU or not Config.STREAM_URL:
        LOGGER.info("Pinger disabled (Not on Heroku or STREAM_URL not set).")
        return
        
    app_url = Config.STREAM_URL

    LOGGER.info(f"Pinger started for {app_url}.")
    while True:
        await asyncio.sleep(Config.PING_INTERVAL)
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
                async with session.get(app_url) as resp:
                    LOGGER.info(f"Pinged server with status: {resp.status}")
        except Exception as e:
            LOGGER.warning(f"Failed to ping server: {e}")

# -------------------------------------------------------------------------------- #
# APPLICATION LIFECYCLE
# -------------------------------------------------------------------------------- #

if __name__ == "__main__":
    async def main_startup_shutdown_logic():
        LOGGER.info("Application starting up...")
        
        await bot.start()
        bot_info = await bot.get_me()
        LOGGER.info(f"Audio Bot @{bot_info.username} started.")
        
        # Start keep-alive pinger
        asyncio.create_task(ping_server())
        
        # Start web server
        web_app = await web_server()
        runner = web.AppRunner(web_app)
        await runner.setup()
        site = web.TCPSite(runner, "0.0.0.0", Config.PORT)
        await site.start()
        LOGGER.info(f"Web server started on port {Config.PORT}.")
        
        try:
            await bot.send_message(Config.OWNER_ID, "**✅ Audio Bot has restarted and all services are online!**")
        except Exception as e:
            LOGGER.warning(f"Could not send startup message: {e}")
                
        await asyncio.Event().wait()

    loop = asyncio.get_event_loop()

    async def shutdown_handler(sig):
        LOGGER.info(f"Received exit signal {sig.name}... shutting down.")
        
        if bot and bot.is_connected:
            LOGGER.info("Stopping bot...")
            await bot.stop()
        
        tasks = [t for t in asyncio.all_tasks(loop) if t is not asyncio.current_task()]
        if tasks:
            LOGGER.info(f"Cancelling {len(tasks)} outstanding tasks...")
            [task.cancel() for task in tasks]
            await asyncio.gather(*tasks, return_exceptions=True)
        
        loop.stop()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(
                sig,
                lambda s=sig: asyncio.create_task(shutdown_handler(s))
            )
        except NotImplementedError:
            # Windows loop may not support add_signal_handler
            pass

    try:
        LOGGER.info("Application starting event loop...")
        loop.run_until_complete(main_startup_shutdown_logic())
        loop.run_forever()
    except Exception as e:
        LOGGER.critical(f"A critical error forced the application to stop: {e}", exc_info=True)
    finally:
        LOGGER.info("Event loop stopped. Final cleanup.")
        # Cleanup temp dir on final exit
        try:
            if os.path.isdir(Config.DOWNLOAD_DIR):
                shutil.rmtree(Config.DOWNLOAD_DIR)
                LOGGER.info(f"Cleaned up main download directory: {Config.DOWNLOAD_DIR}")
        except Exception as e:
            LOGGER.error(f"Failed to cleanup main download directory: {e}")
                
        if loop.is_running():
            loop.stop()
        if not loop.is_closed():
            loop.close()
        LOGGER.info("Shutdown complete.")
