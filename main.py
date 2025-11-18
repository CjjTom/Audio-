#!/usr/bin/env python3
"""
Audio Converter Bot - main.py (English Version)
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
from datetime import datetime, timedelta
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
    MAX_CONCURRENT_JOBS = int(os.environ.get("MAX_CONCURRENT_JOBS", 1)) 
    TELEGRAM_MAX_FILE_SIZE = int(os.environ.get("TELEGRAM_MAX_FILE_SIZE", 2 * 1024 * 1024 * 1024))
    CONVERSATION_CLEAR_DELAY = int(os.environ.get("CONVERSATION_CLEAR_DELAY", 300))
    # Progress interval (seconds)
    PROGRESS_UPDATE_INTERVAL = float(os.environ.get("PROGRESS_UPDATE_INTERVAL", 2.0))
    # Force owner watermark by default
    OWNER_WATERMARK_MANDATORY = os.environ.get("OWNER_WATERMARK_MANDATORY", "1") == "1"

# Validate
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
# GLOBALS
# -------------------------------------------------------------------------------- #

ffmpeg_semaphore = asyncio.Semaphore(Config.MAX_CONCURRENT_JOBS)
DOWNLOAD_PROGRESS = {}
JOB_TRACKERS = {}  # job_id -> dict: {cancelled:bool, last_update_time:float, bytes_processed:int,...}

# -------------------------------------------------------------------------------- #
# DATABASE
# -------------------------------------------------------------------------------- #

db_client = AsyncIOMotorClient(Config.MONGO_URI)
db = db_client['AudioBotDB']
user_conversations_col = db['conversations']
bot_settings_collection = db['settings'] # Owner watermark and global settings
admin_collection = db['admins'] # Per-admin watermark records
jobs_collection = db['jobs'] # Save job metadata

# Conversation helpers
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
    await asyncio.sleep(delay)
    await update_user_conversation(chat_id, None)
    LOGGER.info(f"Auto-cleared conversation state for chat_id: {chat_id}")

# -------------------------------------------------------------------------------- #
# Watermark storage: owner + per-admin
# -------------------------------------------------------------------------------- #

async def get_owner_watermark():
    doc = await bot_settings_collection.find_one({"_id": "owner_watermark"})
    if not doc:
        # default: no file, default volume 0.2 and default position start+end within first hour
        return {"file_id": None, "volume": 0.2, "position_mode": "start_end", "custom_seconds": 0, "max_within_seconds": 3600}
    return {
        "file_id": doc.get("file_id"),
        "volume": float(doc.get("volume", 0.2)),
        "position_mode": doc.get("position_mode", "start_end"),
        "custom_seconds": int(doc.get("custom_seconds", 0)),
        "max_within_seconds": int(doc.get("max_within_seconds", 3600))
    }

async def set_owner_watermark_file(file_id):
    await bot_settings_collection.update_one({"_id": "owner_watermark"}, {"$set": {"file_id": file_id}}, upsert=True)

async def set_owner_watermark_volume(volume: float):
    await bot_settings_collection.update_one({"_id": "owner_watermark"}, {"$set": {"volume": float(volume)}}, upsert=True)

async def set_owner_watermark_position(mode: str, custom_seconds: int = 0):
    await bot_settings_collection.update_one(
        {"_id": "owner_watermark"},
        {"$set": {"position_mode": mode, "custom_seconds": int(custom_seconds)}},
        upsert=True
    )

async def delete_owner_watermark():
    await bot_settings_collection.update_one({"_id": "owner_watermark"}, {"$unset": {"file_id": ""}})

# Per-admin watermark
async def set_admin_watermark(user_id: int, file_id: str, volume: float=0.2, position_mode:str="start_end", custom_seconds:int=0):
    await admin_collection.update_one(
        {"_id": user_id},
        {"$set": {
            "watermark": {"file_id": file_id, "volume": float(volume), "position_mode": position_mode, "custom_seconds": int(custom_seconds)}
        }},
        upsert=True
    )

async def get_admin_watermark(user_id: int):
    doc = await admin_collection.find_one({"_id": user_id})
    if not doc:
        return None
    return doc.get("watermark")

# -------------------------------------------------------------------------------- #
# Admin list management (owner controls)
# -------------------------------------------------------------------------------- #

async def get_admin_list():
    cursor = admin_collection.find({"_id": {"$ne": Config.OWNER_ID}})
    return [doc["_id"] async for doc in cursor]

async def add_admin(user_id: int):
    if user_id == Config.OWNER_ID:
        return
    await admin_collection.update_one({"_id": user_id}, {"$set": {"date_added": datetime.utcnow()}}, upsert=True)

async def remove_admin(user_id: int):
    await admin_collection.delete_one({"_id": user_id})

# -------------------------------------------------------------------------------- #
# Filters
# -------------------------------------------------------------------------------- #

async def admin_filter_func(_, __, message_or_query):
    user_id = None
    if isinstance(message_or_query, CallbackQuery):
        user_id = message_or_query.from_user.id
    elif isinstance(message_or_query, Message):
        user_id = message_or_query.from_user.id
    else:
        user_id = getattr(message_or_query, 'from_user', None).id if getattr(message_or_query, 'from_user', None) else None

    if user_id == Config.OWNER_ID:
        return True

    admins = await get_admin_list()
    return user_id in admins

admin_filter = filters.create(admin_filter_func)

# -------------------------------------------------------------------------------- #
# Utilities: shell, ffprobe, duration
# -------------------------------------------------------------------------------- #

async def run_shell_command(command):
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
    command = f"ffprobe -v error -show_streams -of json {shlex.quote(file_path)}"
    try:
        result_json = await run_shell_command(command)
        return json.loads(result_json).get("streams", [])
    except Exception as e:
        LOGGER.error(f"Failed to probe file {file_path}: {e}")
        return []

async def get_media_duration(file_path):
    command = f"ffprobe -v error -show_entries format=duration -of default=noprint_wrappers=1:nokey=1 {shlex.quote(file_path)}"
    try:
        duration_str = await run_shell_command(command)
        return float(duration_str)
    except Exception as e:
        LOGGER.warning(f"Failed to get duration for {file_path}: {e}")
        return 0.0

# -------------------------------------------------------------------------------- #
# Progress UI Helpers
# -------------------------------------------------------------------------------- #

def human_size(num_bytes: int) -> str:
    # Returns string with MB/KB etc.
    step = 1024.0
    if num_bytes < step:
        return f"{num_bytes} B"
    for unit in ["KB", "MB", "GB", "TB"]:
        num_bytes /= step
        if num_bytes < step:
            return f"{num_bytes:.2f} {unit}"
    return f"{num_bytes:.2f} PB"

def progress_bar(percent: float, length: int = 20) -> str:
    filled = int(math.floor((percent / 100.0) * length))
    bar = "█" * filled + "░" * (length - filled)
    return f"[{bar}]"

def format_time(seconds: float) -> str:
    if seconds is None or math.isinf(seconds):
        return "--:--:--"
    seconds = max(0, int(seconds))
    return str(timedelta(seconds=seconds))

async def safe_edit(message, text, reply_markup=None):
    try:
        await message.edit_text(text, reply_markup=reply_markup)
    except MessageNotModified:
        pass
    except Exception as e:
        LOGGER.warning(f"Failed to edit message: {e}")

# -------------------------------------------------------------------------------- #
# Run ffmpeg with progress parsing + cancel support
# -------------------------------------------------------------------------------- #

async def run_ffmpeg_with_progress(args_list, total_duration_seconds, status_msg, job_id, update_every=Config.PROGRESS_UPDATE_INTERVAL):
    """
    Runs ffmpeg with -progress pipe:1, parses out_time_ms, total_size where available,
    and updates the status_msg with the requested progress block. Uses JOB_TRACKERS[job_id]
    to support cancellation.
    """
    # Ensure tracker entry
    JOB_TRACKERS.setdefault(job_id, {"cancelled": False, "last_update": 0.0, "bytes_processed": 0, "start_ts": time.time(), "last_bytes": 0})

    if "-progress" not in args_list:
        args_list.extend(["-progress", "pipe:1", "-nostats"])

    LOGGER.info(f"Running ffmpeg: {' '.join(args_list)}")
    process = await asyncio.create_subprocess_exec(
        *args_list,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )

    last_update = 0.0
    out_time_ms = 0
    total_size = 0
    bytes_written = 0
    last_bytes = 0
    last_time = time.time()

    # prepare cancel button
    cancel_kb = InlineKeyboardMarkup([[InlineKeyboardButton("⛔ Cancel", callback_data=f"cancel_job|{job_id}")]])

    # read stdout lines (ffmpeg -progress)
    try:
        while True:
            # Check cancellation frequently
            if JOB_TRACKERS[job_id]["cancelled"]:
                LOGGER.info(f"Job {job_id} cancelled by user. Terminating ffmpeg...")
                try:
                    process.send_signal(signal.SIGINT)
                    await asyncio.wait_for(process.wait(), timeout=10.0)
                except Exception:
                    process.terminate()
                raise asyncio.CancelledError("Conversion cancelled by user.")

            line = await process.stdout.readline()
            if not line:
                break
            text = line.decode('utf-8', errors='ignore').strip()
            if not text:
                continue

            # parse key=value
            if '=' in text:
                k, v = text.split('=', 1)
                k = k.strip(); v = v.strip()
                if k == "out_time_ms":
                    try:
                        out_time_ms = int(v)
                    except:
                        out_time_ms = 0
                elif k == "total_size":
                    try:
                        total_size = int(v)
                    except:
                        total_size = 0
                elif k == "progress" and v == "end":
                    # set percent to 99.9% for finalization stage
                    percent = 99.9
                    now = time.time()
                    if now - JOB_TRACKERS[job_id]["last_update"] > update_every:
                        # compute speed from bytes delta
                        now_time = time.time()
                        dt = now_time - JOB_TRACKERS[job_id].get("last_time", JOB_TRACKERS[job_id]["start_ts"])
                        dbytes = total_size - JOB_TRACKERS[job_id].get("last_bytes", 0)
                        speed = dbytes / dt if dt > 0 else 0.0
                        bar = progress_bar(percent, length=20)
                        txt = (
                            f"**Converting Progress:** {bar}\n\n"
                            f"📊 **Percentage:** {percent:.2f}%\n\n"
                            f"✅ **Processed:** {human_size(total_size)}\n\n"
                            f"🚀 **Speed:** {human_size(int(speed))}/s\n\n"
                            f"⏳ **ETA:** --:--:--"
                        )
                        await safe_edit(status_msg, txt, reply_markup=cancel_kb)
                        JOB_TRACKERS[job_id]["last_update"] = now
                        JOB_TRACKERS[job_id]["last_time"] = now_time
                        JOB_TRACKERS[job_id]["last_bytes"] = total_size
                # else continue; we will compute percent from out_time_ms

            # compute percent and display at intervals
            percent = 0.0
            if total_duration_seconds and out_time_ms:
                processed_seconds = out_time_ms / 1000.0
                percent = min(100.0, (processed_seconds / total_duration_seconds) * 100.0)

            now = time.time()
            if now - JOB_TRACKERS[job_id]["last_update"] > update_every:
                # compute speed approximately using total_size / elapsed
                elapsed = now - JOB_TRACKERS[job_id]["start_ts"]
                size_for_speed = total_size if total_size else JOB_TRACKERS[job_id].get("bytes_processed", 0)
                speed = (size_for_speed / elapsed) if elapsed > 0 else 0.0
                # approximate downloaded/converted bytes shown
                converted_bytes = total_size if total_size else JOB_TRACKERS[job_id].get("bytes_processed", 0)
                bar = progress_bar(percent, length=20)
                # ETA compute
                eta_seconds = None
                if speed > 0 and total_size:
                    remaining = max(0, total_size - converted_bytes)
                    eta_seconds = remaining / speed
                elif speed > 0 and total_duration_seconds:
                    # estimate size from percent
                    if percent > 0:
                        eta_seconds = (total_duration_seconds * (100.0 - percent) / percent)
                else:
                    eta_seconds = None

                eta_str = format_time(eta_seconds) if eta_seconds is not None else "--:--:--"

                txt = (
                    f"**Download Progress:** {bar}\n\n"
                    f"📊 **Percentage:** {percent:.2f}%\n\n"
                    f"✅ **Downloaded:** {human_size(int(converted_bytes))} / {human_size(int(total_size) if total_size else 0)}\n\n"
                    f"🚀 **Speed:** {human_size(int(speed))}/s\n\n"
                    f"⏳ **ETA:** {eta_str}"
                )
                await safe_edit(status_msg, txt, reply_markup=cancel_kb)
                JOB_TRACKERS[job_id]["last_update"] = now
                JOB_TRACKERS[job_id]["last_bytes"] = converted_bytes
                JOB_TRACKERS[job_id]["last_time"] = now

    except asyncio.CancelledError as ce:
        LOGGER.info(f"ffmpeg cancelled for job {job_id}: {ce}")
        # cleanup process
        try:
            if process and process.returncode is None:
                process.terminate()
        except Exception:
            pass
        raise
    except Exception as e:
        # read remaining stderr
        stderr_acc = await process.stderr.read()
        err_text = stderr_acc.decode('utf-8', errors='ignore')[:4000]
        LOGGER.error(f"FFMPEG runtime error: {e}\nStderr: {err_text}")
        try:
            process.terminate()
        except:
            pass
        raise RuntimeError(f"FFMPEG runtime error: {e}\n{err_text}")
    finally:
        # wait for process to finish if not already
        try:
            rc = await process.wait()
        except Exception:
            rc = None

    if rc and rc != 0:
        stderr_data = await process.stderr.read()
        err_text = stderr_data.decode('utf-8', errors='ignore')[:4000]
        LOGGER.error(f"FFMPEG failed (rc={rc}): {err_text}")
        raise RuntimeError(f"FFMPEG failed. {err_text}")

    LOGGER.info("FFMPEG finished successfully.")
    return True

# -------------------------------------------------------------------------------- #
# TELEGRAM BOT
# -------------------------------------------------------------------------------- #

bot = Client("AudioBot", api_id=Config.API_ID, api_hash=Config.API_HASH, bot_token=Config.BOT_TOKEN)

# progress callback for download/upload
async def progress_callback(current, total, message, action):
    global DOWNLOAD_PROGRESS
    try:
        percent = (current / total) * 100 if total else 0.0
    except:
        percent = 0.0
    now = time.time()
    msg_id = getattr(message, "id", None) or 0
    last = DOWNLOAD_PROGRESS.get(msg_id, {"ts": 0})
    if now - last.get("ts", 0) > 3:
        # build progress block similar to conversion
        bar = progress_bar(percent, length=20)
        speed = 0.0
        # naive speed: if we have previous
        prev_bytes = last.get("bytes", 0)
        prev_time = last.get("ts", now)
        dt = now - prev_time if now - prev_time > 0 else 1.0
        dbytes = current - prev_bytes
        speed = dbytes / dt if dt > 0 else 0.0
        eta = None
        if speed > 0:
            eta = (total - current) / speed
        eta_str = format_time(eta) if eta is not None else "--:--:--"

        txt = (
            f"**Download Progress:** {bar}\n\n"
            f"📊 **Percentage:** {percent:.2f}%\n\n"
            f"✅ **Downloaded:** {human_size(int(current))} / {human_size(int(total))}\n\n"
            f"🚀 **Speed:** {human_size(int(speed))}/s\n\n"
            f"⏳ **ETA:** {eta_str}"
        )
        try:
            await message.edit_text(txt)
            DOWNLOAD_PROGRESS[msg_id] = {"ts": now, "bytes": current}
        except (MessageNotModified, FloodWait):
            pass
        except Exception as e:
            LOGGER.warning(f"progress_callback edit error: {e}")

# -------------------------------------------------------------------------------- #
# Bot handlers: start, menus, watermark settings, admin management, conversion flow
# -------------------------------------------------------------------------------- #

@bot.on_message(filters.command("start") & filters.private & admin_filter)
async def start_command(client, message):
    buttons = [
        [InlineKeyboardButton("🎧 Audio Tools", callback_data="audio_tools_menu")]
    ]
    if message.from_user.id == Config.OWNER_ID:
        buttons.append([InlineKeyboardButton("👨‍💼 Admin Management", callback_data="admin_menu")])
    await message.reply_text(
        "**🎧 Audio Converter Bot**\n\n"
        "This bot accepts audio files, converts them with optional watermark mixing, and returns the processed file.",
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
            "**🎧 Audio Converter Bot**\n\n"
            "Choose a menu option:",
            reply_markup=InlineKeyboardMarkup(buttons)
        )
    except MessageNotModified:
        pass
    await update_user_conversation(cb.message.chat.id, None)

@bot.on_callback_query(filters.regex("^audio_tools_menu$") & admin_filter)
async def audio_tools_menu_cb(client, cb: CallbackQuery):
    await cb.answer()
    await cb.message.edit_text(
        "**🎧 Audio Tools**\n\nChoose an option:",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🎵 Convert Audio", callback_data="convert_audio_start")],
            [InlineKeyboardButton("⚙️ Watermark Settings", callback_data="watermark_settings")],
            [InlineKeyboardButton("⬅️ Back to Main", callback_data="main_menu")]
        ])
    )

@bot.on_callback_query(filters.regex("^cancel_conv$") & admin_filter)
async def cancel_conversation_handler(client, cb: CallbackQuery):
    await cb.answer("Operation cancelled.")
    chat_id = cb.message.chat.id
    conv = await get_user_conversation(chat_id)
    if conv:
        job_dir = conv.get("job_dir")
        if job_dir and os.path.isdir(job_dir):
            try:
                shutil.rmtree(job_dir)
                LOGGER.info(f"Cleaned up job directory: {job_dir}")
            except Exception as e:
                LOGGER.error(f"Failed to cleanup job directory {job_dir}: {e}")
    asyncio.create_task(clear_conversation_after_delay(chat_id))
    try:
        await cb.message.delete()
    except Exception:
        pass
    await start_command(client, cb.message)

# Watermark settings menu
@bot.on_callback_query(filters.regex("^watermark_settings$") & admin_filter)
async def watermark_settings_cb(client, cb: CallbackQuery):
    await cb.answer()
    owner_wm = await get_owner_watermark()
    admin_wm = await get_admin_watermark(cb.from_user.id)
    text = "**⚙️ Watermark Settings**\n\n"
    if owner_wm.get("file_id"):
        text += f"🟢 Owner watermark is set.\nVolume: `{int(owner_wm.get('volume',0.2)*100)}%`\nPosition: `{owner_wm.get('position_mode')}`\n\n"
    else:
        text += "🔴 Owner watermark is not set.\n\n"
    if admin_wm:
        text += f"🧑‍💼 Your personal watermark is set (Volume {int(admin_wm.get('volume',0.2)*100)}%).\n\n"
    text += "You can upload, manage, or change watermark position.\n"
    buttons = [
        [InlineKeyboardButton("⬆️ Upload Owner Watermark", callback_data="owner_wm_upload")],
        [InlineKeyboardButton("🔊 Set Volume (Owner)", callback_data="owner_wm_volume")],
        [InlineKeyboardButton("📍 Set Position (Owner)", callback_data="owner_wm_position")],
        [InlineKeyboardButton("⬆️ Upload Your Watermark (Admin)", callback_data="admin_wm_upload")],
        [InlineKeyboardButton("🗑️ Delete Owner Watermark", callback_data="owner_wm_delete")],
        [InlineKeyboardButton("⬅️ Back", callback_data="audio_tools_menu")]
    ]
    await cb.message.edit_text(text, reply_markup=InlineKeyboardMarkup(buttons))

# Owner/ Admin watermark upload handlers
@bot.on_callback_query(filters.regex("^owner_wm_upload$") & filters.user(Config.OWNER_ID))
async def owner_wm_upload_start(client, cb: CallbackQuery):
    await cb.answer()
    await update_user_conversation(cb.message.chat.id, {"stage": "awaiting_owner_wm"})
    await cb.message.edit_text("📥 **Upload Watermark**\n\nPlease send a short audio file (mp3/m4a).", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="watermark_settings")]]))

@bot.on_callback_query(filters.regex("^admin_wm_upload$") & admin_filter)
async def admin_wm_upload_start(client, cb: CallbackQuery):
    await cb.answer()
    await update_user_conversation(cb.message.chat.id, {"stage": "awaiting_admin_wm"})
    await cb.message.edit_text("📥 **Upload Your Watermark**\n\nPlease send a short audio file (mp3/m4a).", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="watermark_settings")]]))

@bot.on_callback_query(filters.regex("^owner_wm_delete$") & filters.user(Config.OWNER_ID))
async def owner_wm_delete_cb(client, cb: CallbackQuery):
    await cb.answer("Watermark deleted.")
    await delete_owner_watermark()
    await watermark_settings_cb(client, cb)

@bot.on_callback_query(filters.regex("^owner_wm_volume$") & filters.user(Config.OWNER_ID))
async def owner_wm_volume_cb(client, cb: CallbackQuery):
    await cb.answer()
    await cb.message.edit_text("🔊 Choose owner watermark volume:", reply_markup=InlineKeyboardMarkup([
        [InlineKeyboardButton("10%", callback_data="owner_wm_vol_0.1"), InlineKeyboardButton("50%", callback_data="owner_wm_vol_0.5"), InlineKeyboardButton("80%", callback_data="owner_wm_vol_0.8")],
        [InlineKeyboardButton("100%", callback_data="owner_wm_vol_1.0"), InlineKeyboardButton("⬅️ Back", callback_data="watermark_settings")]
    ]))

@bot.on_callback_query(filters.regex(r"^owner_wm_vol_(\d\.\d)$") & filters.user(Config.OWNER_ID))
async def owner_wm_vol_save(client, cb: CallbackQuery):
    vol = float(cb.data.split("_")[-1])
    await set_owner_watermark_volume(vol)
    await cb.answer(f"Volume set to {int(vol*100)}%.")
    await watermark_settings_cb(client, cb)

@bot.on_callback_query(filters.regex("^owner_wm_position$") & filters.user(Config.OWNER_ID))
async def owner_wm_position_cb(client, cb: CallbackQuery):
    await cb.answer()
    await cb.message.edit_text("📍 Choose watermark position:", reply_markup=InlineKeyboardMarkup([
        [InlineKeyboardButton("Start + End (Default)", callback_data="owner_wm_pos_start_end")],
        [InlineKeyboardButton("Start Only", callback_data="owner_wm_pos_start")],
        [InlineKeyboardButton("End Only", callback_data="owner_wm_pos_end")],
        [InlineKeyboardButton("Custom (Seconds)", callback_data="owner_wm_pos_custom")],
        [InlineKeyboardButton("⬅️ Back", callback_data="watermark_settings")]
    ]))

@bot.on_callback_query(filters.regex("^owner_wm_pos_start_end$") & filters.user(Config.OWNER_ID))
async def owner_wm_pos_start_end(client, cb: CallbackQuery):
    await set_owner_watermark_position("start_end", 0)
    await cb.answer("Position set to Start+End.")
    await watermark_settings_cb(client, cb)

@bot.on_callback_query(filters.regex("^owner_wm_pos_start$") & filters.user(Config.OWNER_ID))
async def owner_wm_pos_start_only(client, cb: CallbackQuery):
    await set_owner_watermark_position("start_only", 0)
    await cb.answer("Position set to Start Only.")
    await watermark_settings_cb(client, cb)

@bot.on_callback_query(filters.regex("^owner_wm_pos_end$") & filters.user(Config.OWNER_ID))
async def owner_wm_pos_end_only(client, cb: CallbackQuery):
    await set_owner_watermark_position("end_only", 0)
    await cb.answer("Position set to End Only.")
    await watermark_settings_cb(client, cb)

@bot.on_callback_query(filters.regex("^owner_wm_pos_custom$") & filters.user(Config.OWNER_ID))
async def owner_wm_pos_custom_prompt(client, cb: CallbackQuery):
    await cb.answer()
    await update_user_conversation(cb.message.chat.id, {"stage": "awaiting_owner_wm_custom_seconds"})
    await cb.message.edit_text("🔢 Please send custom time in seconds (e.g., 1800 = 30 minutes).", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="watermark_settings")]]))

# Message handler for watermark uploads & custom seconds & admin watermark upload
@bot.on_message(filters.private & (filters.audio | filters.document | filters.text) & admin_filter)
async def message_router(client, message: Message):
    chat_id = message.chat.id
    conv = await get_user_conversation(chat_id)
    if not conv:
        return
    stage = conv.get("stage")

    if stage == "awaiting_owner_wm":
        # must be from owner
        if message.from_user.id != Config.OWNER_ID:
            await message.reply_text("You don't have permission.")
            return
        if not (message.audio or (message.document and getattr(message.document, "mime_type","").startswith("audio"))):
            await message.reply_text("Please send an audio file (mp3/m4a).")
            return
        file_id = message.audio.file_id if message.audio else message.document.file_id
        await set_owner_watermark_file(file_id)
        await update_user_conversation(chat_id, None)
        await message.reply_text("✅ Owner watermark saved.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="watermark_settings")]]))
        return

    if stage == "awaiting_admin_wm":
        # admin uploads their own watermark
        if not (message.audio or (message.document and getattr(message.document, "mime_type","").startswith("audio"))):
            await message.reply_text("Please send an audio file (mp3/m4a).")
            return
        file_id = message.audio.file_id if message.audio else message.document.file_id
        await set_admin_watermark(message.from_user.id, file_id, volume=0.2, position_mode="start_end", custom_seconds=0)
        await update_user_conversation(chat_id, None)
        await message.reply_text("✅ Your watermark saved.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="watermark_settings")]]))
        return

    if stage == "awaiting_owner_wm_custom_seconds":
        # owner provided custom seconds
        if message.from_user.id != Config.OWNER_ID:
            await message.reply_text("You don't have permission.")
            return
        try:
            secs = int(message.text.strip())
            await set_owner_watermark_position("custom", secs)
            await update_user_conversation(chat_id, None)
            await message.reply_text(f"✅ Custom time set to {secs} seconds.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="watermark_settings")]]))
        except Exception:
            await message.reply_text("Please send a valid number (digits only).")
        return

# Admin management callbacks
@bot.on_callback_query(filters.regex("^admin_menu$") & filters.user(Config.OWNER_ID))
async def admin_menu_cb(client, cb: CallbackQuery):
    await cb.answer()
    await cb.message.edit_text(
        "**👨‍💼 Admin Management**\n\nAdd or remove admins.",
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
    await cb.message.edit_text("➕ Send user ID to add as admin.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="admin_menu")]]))

@bot.on_callback_query(filters.regex("^admin_remove_list$") & filters.user(Config.OWNER_ID))
async def admin_remove_list_cb(client, cb: CallbackQuery):
    await cb.answer()
    admins = await get_admin_list()
    if not admins:
        return await cb.message.edit_text("➖ No other admins found.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")]]))
    buttons = []
    text = "**➖ Remove Admin**\n\nChoose an admin to remove:\n"
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
    await admin_remove_list_cb(client, cb)

# -------------------------------------------------------------------------------- #
# Conversion flow: start -> upload -> probe -> track selection -> format -> watermark choice -> output -> convert
# -------------------------------------------------------------------------------- #

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
    await cb.message.edit_text("🎵 **Send File**\n\nSend the file you want to process.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="cancel_conv")]]))

@bot.on_message(filters.private & (filters.audio | filters.video | filters.document | filters.text) & admin_filter)
async def message_handler_router(client, message: Message):
    chat_id = message.chat.id
    conv = await get_user_conversation(chat_id)
    if not conv:
        return
    stage = conv.get("stage")

    if stage == "awaiting_watermark_audio":
        # handled earlier
        pass

    elif stage == "awaiting_media_file":
        if not (message.audio or message.video or message.document):
            await message.reply_text("Please send a valid media file.")
            return
        await handle_media_file(client, message, conv)

    elif stage == "awaiting_admin_id" and message.text:
        if message.from_user.id != Config.OWNER_ID:
            return
        try:
            user_id = int(message.text.strip())
            await add_admin(user_id)
            await message.reply_text(f"✅ Admin added `{user_id}`", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")]]))
            await update_user_conversation(message.chat.id, None)
        except ValueError:
            await message.reply_text("Please send a valid user ID (digits only).")

async def handle_media_file(client, message: Message, conv: dict):
    media = message.audio or message.video or message.document
    job_dir = conv.get("job_dir")
    if not job_dir or not os.path.isdir(job_dir):
        await message.reply_text("Error: Job directory not found. Please try again.", quote=True)
        asyncio.create_task(clear_conversation_after_delay(message.chat.id, delay=5))
        return

    status_msg = await message.reply_text("📥 **Downloading... 0%**", quote=True)
    try:
        input_file_path = await message.download(
            file_name=os.path.join(job_dir, f"input_file_{uuid.uuid4()}"),
            progress=progress_callback,
            progress_args=(status_msg, "Downloading")
        )
    except Exception as e:
        LOGGER.error(f"File download failed: {e}", exc_info=True)
        try:
            await status_msg.edit_text(f"❌ **Download Failed**\n\n`{e}`")
        except:
            pass
        asyncio.create_task(clear_conversation_after_delay(message.chat.id))
        return
    finally:
        DOWNLOAD_PROGRESS.pop(status_msg.id, None)

    await status_msg.edit_text("🔬 **File Analysis**\n\nProbing...")
    audio_streams = await probe_media(input_file_path)
    if not audio_streams:
        await status_msg.edit_text("❌ **Error**\n\nNo audio streams found in this file.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="cancel_conv")]]))
        return

    is_audio_only = message.audio or (message.document and getattr(message.document, "mime_type","").startswith("audio"))
    if not is_audio_only:
        all_streams_probe_cmd = f"ffprobe -v error -show_streams -of json {shlex.quote(input_file_path)}"
        try:
            all_streams_json = await run_shell_command(all_streams_probe_cmd)
            all_streams = json.loads(all_streams_json).get("streams", [])
            video_streams = [s for s in all_streams if s.get('codec_type') == 'video']
            if not video_streams:
                is_audio_only = True
        except Exception:
            pass

    await update_user_conversation(message.chat.id, {
        "input_file_path": input_file_path,
        "audio_streams": audio_streams,
        "is_audio_only": is_audio_only,
        "job_dir": job_dir
    })

    buttons = []
    text = "**🔬 Analysis Complete!**\n\nAudio Tracks: \n\n"
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
    stream_obj = next((s for s in conv.get('audio_streams', []) if s.get('index') == track_index), None)
    if not stream_obj:
        return await cb.answer("Error: Selected track not found.", show_alert=True)

    await update_user_conversation(chat_id, {
        "selected_track_index": track_index,
        "selected_stream_obj": stream_obj,
        "stage": "awaiting_format_selection"
    })

    buttons = [
        [InlineKeyboardButton("🎵 AAC (Stereo, 192k)", callback_data="format_aac_stereo")],
        [InlineKeyboardButton("🎧 AAC (5.1 Keep Channels)", callback_data="format_aac_5_1")],
        [InlineKeyboardButton("🎵 MP3 (Stereo, 192k)", callback_data="format_mp3_stereo")],
    ]
    if stream_obj.get("codec_name") == "aac":
        buttons.insert(0, [InlineKeyboardButton("✨ Copy AAC Stream (Fastest)", callback_data="format_aac_copy")])

    buttons.extend([
        [InlineKeyboardButton("⬅️ Choose Different File", callback_data="convert_audio_start")],
        [InlineKeyboardButton("❌ Cancel", callback_data="cancel_conv")]
    ])

    await cb.message.edit_text(f"✅ Track {track_index} selected.\n\nChoose output format:", reply_markup=InlineKeyboardMarkup(buttons))

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
        "aac_5_1": {"codec": "aac", "channels": 6, "bitrate": "320k"},
        "mp3_stereo": {"codec": "libmp3lame", "channels": 2, "bitrate": "192k"},
        "aac_copy": {"codec": "copy", "channels": "copy", "bitrate": "copy"}
    }
    if format_choice not in format_options:
        return await cb.answer("Invalid format.", show_alert=True)

    await update_user_conversation(chat_id, {"format": format_options[format_choice]})

    owner_wm = await get_owner_watermark()
    admin_wm = await get_admin_watermark(cb.from_user.id)
    # Determine watermark default behavior
    watermark_available = (owner_wm.get("file_id") is not None) or (admin_wm is not None)
    # If copy mode chosen and watermark requested, we will force re-encode later.
    if format_choice == "aac_copy":
        # we allow user to still request watermark, but warn that copy will be overridden
        await update_user_conversation(chat_id, {"watermark": False})
        await cb.answer("Copy mode selected. Watermark is not required.", show_alert=True)
        # proceed to output selection
        await ask_for_output_type(cb, conv)
    else:
        # Ask whether to use watermark: default to owner WM if mandatory
        default_use = bool(Config.OWNER_WATERMARK_MANDATORY and owner_wm.get("file_id"))
        if watermark_available:
            await update_user_conversation(chat_id, {"stage": "awaiting_watermark_selection"})
            await cb.message.edit_text(
                "✅ Format selected.\n\nDo you want to mix watermark?",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("💧 Use Owner Watermark", callback_data="watermark_use_owner")],
                    [InlineKeyboardButton("🧑‍💼 Use My Watermark (Admin)", callback_data="watermark_use_admin")],
                    [InlineKeyboardButton("❌ No Watermark", callback_data="watermark_use_no")],
                    [InlineKeyboardButton("❌ Cancel", callback_data="cancel_conv")]
                ])
            )
        else:
            await update_user_conversation(chat_id, {"watermark": False})
            await ask_for_output_type(cb, conv)

@bot.on_callback_query(filters.regex(r"^watermark_use_(owner|admin|no)$") & admin_filter)
async def watermark_selection_cb(client, cb: CallbackQuery):
    await cb.answer()
    choice = cb.data.split("_")[-1]
    chat_id = cb.message.chat.id
    conv = await get_user_conversation(chat_id)
    if not conv or conv.get("stage") != "awaiting_watermark_selection":
        return await cb.answer("Session expired.", show_alert=True)

    if choice == "owner":
        await update_user_conversation(chat_id, {"watermark": True, "watermark_choice": "owner"})
        # ask about position specifics or use owner defaults
        owner_wm = await get_owner_watermark()
        await cb.message.edit_text("Owner watermark selected.\n\nChange position?", reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("Start + End (Default)", callback_data="wmpos_owner_start_end")],
            [InlineKeyboardButton("Start Only", callback_data="wmpos_owner_start")],
            [InlineKeyboardButton("End Only", callback_data="wmpos_owner_end")],
            [InlineKeyboardButton("Custom (Seconds)", callback_data="wmpos_owner_custom")],
            [InlineKeyboardButton("➡️ Continue", callback_data="wmpos_owner_done")]
        ]))
        await update_user_conversation(chat_id, {"stage": "awaiting_wm_position_choice"})
    elif choice == "admin":
        admin_wm = await get_admin_watermark(cb.from_user.id)
        if not admin_wm:
            await cb.answer("Your watermark is not saved. Please upload first.", show_alert=True)
            return await watermark_settings_cb(client, cb)
        await update_user_conversation(chat_id, {"watermark": True, "watermark_choice": "admin"})
        await cb.message.edit_text("Your watermark selected. Configure options:", reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("Start + End", callback_data="wmpos_admin_start_end"), InlineKeyboardButton("Start Only", callback_data="wmpos_admin_start")],
            [InlineKeyboardButton("End Only", callback_data="wmpos_admin_end"), InlineKeyboardButton("Custom", callback_data="wmpos_admin_custom")],
            [InlineKeyboardButton("➡️ Continue", callback_data="wmpos_admin_done")]
        ]))
        await update_user_conversation(chat_id, {"stage": "awaiting_wm_position_choice"})
    else:
        await update_user_conversation(chat_id, {"watermark": False})
        await ask_for_output_type(cb, conv)

# Watermark position callbacks (owner + admin)
@bot.on_callback_query(filters.regex(r"^wmpos_(owner|admin)_(start_end|start|end|custom|done)$") & admin_filter)
async def wmpos_choice_cb(client, cb: CallbackQuery):
    await cb.answer()
    parts = cb.data.split("_")
    who = parts[1] if len(parts) > 1 else "owner"
    option = parts[2]
    chat_id = cb.message.chat.id
    conv = await get_user_conversation(chat_id)
    if not conv:
        return await cb.answer("Session expired.", show_alert=True)
    if option == "start_end":
        await update_user_conversation(chat_id, {"wm_position_mode": "start_end", "wm_custom_seconds": 0})
        await cb.answer("Position set to Start+End.")
        # proceed to output selection
        await ask_for_output_type(cb, conv)
    elif option == "start":
        await update_user_conversation(chat_id, {"wm_position_mode": "start_only", "wm_custom_seconds": 0})
        await cb.answer("Position set to Start Only.")
        await ask_for_output_type(cb, conv)
    elif option == "end":
        await update_user_conversation(chat_id, {"wm_position_mode": "end_only", "wm_custom_seconds": 0})
        await cb.answer("Position set to End Only.")
        await ask_for_output_type(cb, conv)
    elif option == "custom":
        await update_user_conversation(chat_id, {"stage": "awaiting_wm_custom_seconds"})
        await cb.message.edit_text("🔢 Please send time in seconds (e.g., 1800 = 30 minutes).", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="cancel_conv")]]))
    elif option == "done":
        # done, use owner defaults (if any)
        await ask_for_output_type(cb, conv)

@bot.on_message(filters.private & filters.text & admin_filter)
async def custom_wm_seconds_handler(client, message: Message):
    conv = await get_user_conversation(message.chat.id)
    if not conv:
        return
    stage = conv.get("stage")
    if stage == "awaiting_wm_custom_seconds":
        try:
            secs = int(message.text.strip())
            await update_user_conversation(message.chat.id, {"wm_position_mode": "custom", "wm_custom_seconds": secs, "stage": "processing"})
            # proceed
            # call ask_for_output_type manually since we don't have original cb
            fake_cb = CallbackQuery(id=None, from_user=message.from_user, message=message, chat_instance=None)
            # use a tiny wrapper to call ask_for_output_type (we can craft conv)
            await ask_for_output_type(fake_cb, conv)
        except Exception:
            await message.reply_text("Please send a valid number (digits only).")

# Output selection
async def ask_for_output_type(cb, conv):
    chat_id = cb.message.chat.id
    if conv.get("is_audio_only"):
        await update_user_conversation(chat_id, {"output_type": "audio", "stage": "processing"})
        try:
            await cb.message.edit_text("✅ **Ready!**\n\nProcessing audio only...")
        except:
            pass
        await start_conversion_process(cb)
    else:
        await update_user_conversation(chat_id, {"stage": "awaiting_output_selection"})
        await cb.message.edit_text("✅ Settings complete.\n\nChoose output type:", reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🎵 Audio Only (.m4a/.mp3)", callback_data="output_audio")],
            [InlineKeyboardButton("🎬 Remux Video (.mkv)", callback_data="output_remux")],
            [InlineKeyboardButton("❌ Cancel", callback_data="cancel_conv")]
        ]))

@bot.on_callback_query(filters.regex(r"^output_(audio|remux)$") & admin_filter)
async def output_selection_cb(client, cb: CallbackQuery):
    await cb.answer()
    chat_id = cb.message.chat.id
    conv = await get_user_conversation(chat_id)
    if not conv or conv.get("stage") not in ["awaiting_output_selection", "awaiting_wm_position_choice", "processing"]:
        return await cb.answer("Session expired.", show_alert=True)
    output_type = cb.data.split("_")[1]
    await update_user_conversation(chat_id, {"output_type": output_type, "stage": "processing"})
    await cb.message.edit_text(f"✅ **Ready!**\n\nStarting conversion (Output: {output_type})...")
    await start_conversion_process(cb)

# -------------------------------------------------------------------------------- #
# FFmpeg args builder with watermark positioning (start, end, custom)
# -------------------------------------------------------------------------------- #

async def build_ffmpeg_args(conv, input_file, watermark_local_file):
    track_index = conv["selected_track_index"]
    stream_obj = conv["selected_stream_obj"]
    fmt = conv["format"]
    use_watermark = conv.get("watermark", False)
    watermark_choice = conv.get("watermark_choice", "owner") # owner or admin
    job_dir = conv["job_dir"]
    wm_position_mode = conv.get("wm_position_mode") or "start_end"
    wm_custom_seconds = int(conv.get("wm_custom_seconds", 0) or 0)

    output_ext = "mp3" if fmt['codec'] == 'libmp3lame' else "m4a"
    if conv.get("output_type") == "remux":
        output_ext = "mkv"
    elif fmt['codec'] == 'copy':
        output_ext = "m4a"

    final_output_file = os.path.join(job_dir, f"output.{output_ext}")

    args = ["ffmpeg", "-y", "-hide_banner", "-i", input_file]

    filter_complex_parts = []
    audio_map_label = f"0:a:{track_index}"

    # Determine which watermark file to use
    wm_file_to_use = None
    wm_volume = 0.2
    wm_owner_description = "none"
    if use_watermark:
        if watermark_choice == "owner":
            owner_wm = await get_owner_watermark()
            wm_file_to_use = owner_wm.get("file_id")
            wm_volume = owner_wm.get("volume", 0.2)
            wm_owner_description = "owner"
        else:
            admin_wm = await get_admin_watermark(conv.get("user_id") or 0)
            if admin_wm:
                wm_file_to_use = admin_wm.get("file_id")
                wm_volume = admin_wm.get("volume", 0.2)
                wm_owner_description = f"admin:{conv.get('user_id')}"

    # If copy selected but watermark requested, we will force a re-encode (can't mix in copy mode).
    force_reencode = False
    if fmt.get("codec") == "copy" and use_watermark:
        force_reencode = True

    # If watermark exists and we have a local watermark file (downloaded earlier)
    if use_watermark and watermark_local_file and not force_reencode:
        # prepare 2-input filter
        args.extend(["-i", watermark_local_file])
        # simple start: mix at start, no delay
        if wm_position_mode == "start_only":
            filter_complex_parts.append(f"[0:a:{track_index}]volume=1.0[main]")
            filter_complex_parts.append(f"[1:a]adelay=0|0,volume={wm_volume}[wm]")
            filter_complex_parts.append(f"[main][wm]amix=inputs=2:duration=first[aud_out]")
        elif wm_position_mode == "end_only":
            # compute delay later at runtime by substituting placeholder
            input_duration = await get_media_duration(input_file)
            # choose watermark duration
            try:
                wm_dur = await get_media_duration(watermark_local_file)
            except:
                wm_dur = 0
            delay_ms = max(0, int((input_duration - wm_dur) * 1000))
            filter_complex_parts.append(f"[0:a:{track_index}]volume=1.0[main]")
            filter_complex_parts.append(f"[1:a]adelay={delay_ms}|{delay_ms},volume={wm_volume}[wm]")
            filter_complex_parts.append(f"[main][wm]amix=inputs=2:duration=first[aud_out]")
        elif wm_position_mode == "custom":
            delay_ms = max(0, int(wm_custom_seconds * 1000))
            filter_complex_parts.append(f"[0:a:{track_index}]volume=1.0[main]")
            filter_complex_parts.append(f"[1:a]adelay={delay_ms}|{delay_ms},volume={wm_volume}[wm]")
            filter_complex_parts.append(f"[main][wm]amix=inputs=2:duration=first[aud_out]")
        else:  # default start_end
            # We'll mix twice: start and end.
            input_duration = await get_media_duration(input_file)
            try:
                wm_dur = await get_media_duration(watermark_local_file)
            except:
                wm_dur = 0
            # end delay ms = clamp so end occurs within first hour if necessary
            max_within = 3600
            end_target = input_duration if input_duration <= max_within else max_within
            delay_ms = max(0, int((end_target - wm_dur) * 1000))
            filter_complex_parts.append(f"[1:a]asplit=2[wm1][wm2]")
            filter_complex_parts.append(f"[0:a:{track_index}]volume=1.0[main]")
            filter_complex_parts.append(f"[wm1]volume={wm_volume}[wm_start]")
            # end delay compute
            filter_complex_parts.append(f"[wm2]adelay={delay_ms}|{delay_ms},volume={wm_volume}[wm_end]")
            filter_complex_parts.append(f"[main][wm_start][wm_end]amix=inputs=3:duration=first[aud_out]")

        audio_map_label = "[aud_out]"

    elif use_watermark and watermark_local_file and force_reencode:
        # Downloaded watermark but chosen copy mode: force reencode path
        args.extend(["-i", watermark_local_file])
        # same mixing strategy as above for default start_end
        input_duration = await get_media_duration(input_file)
        try:
            wm_dur = await get_media_duration(watermark_local_file)
        except:
            wm_dur = 0
        max_within = 3600
        end_target = input_duration if input_duration <= max_within else max_within
        delay_ms = max(0, int((end_target - wm_dur) * 1000))
        filter_complex_parts.append(f"[1:a]asplit=2[wm1][wm2]")
        filter_complex_parts.append(f"[0:a:{track_index}]volume=1.0[main]")
        filter_complex_parts.append(f"[wm1]volume={wm_volume}[wm_start]")
        filter_complex_parts.append(f"[wm2]adelay={delay_ms}|{delay_ms},volume={wm_volume}[wm_end]")
        filter_complex_parts.append(f"[main][wm_start][wm_end]amix=inputs=3:duration=first[aud_out]")
        audio_map_label = "[aud_out]"
        # override codec to ensure re-encode
        if fmt.get("codec") == "copy":
            fmt = {"codec": "aac", "channels": 2, "bitrate": "192k"}

    else:
        # no watermark -> map existing audio directly
        audio_map_label = f"0:a:{track_index}"

    if filter_complex_parts:
        args.extend(["-filter_complex", ";".join(filter_complex_parts)])

    # Map the audio
    args.extend(["-map", audio_map_label])

    # codec settings
    if fmt.get("codec") == "copy":
        args.extend(["-c:a", "copy"])
    else:
        args.extend(["-c:a", fmt['codec'], "-b:a", fmt['bitrate'], "-ac", str(fmt['channels']), "-ar", "48000"])

    lang = (stream_obj or {}).get("tags", {}).get("language", "und")
    args.extend([f"-metadata:s:a:0", f"language={lang}"])

    if conv.get("output_type") == "remux":
        args.extend(["-map", "0:v:0?", "-map", "0:s?"])
        args.extend(["-c:v", "copy", "-c:s", "copy"])

    args.append(final_output_file)
    return args, final_output_file

# -------------------------------------------------------------------------------- #
# Conversion process: download watermark, build args, run ffmpeg with progress, upload and save job metadata
# -------------------------------------------------------------------------------- #

async def start_conversion_process(cb: CallbackQuery):
    chat_id = cb.message.chat.id
    status_msg = cb.message

    async with ffmpeg_semaphore:
        conv = await get_user_conversation(chat_id)
        if not conv or conv.get("stage") != "processing":
            return

        job_id = conv.get("job_id") or str(uuid.uuid4())
        JOB_TRACKERS.setdefault(job_id, {"cancelled": False, "last_update": 0.0, "start_ts": time.time()})
        try:
            job_dir = conv["job_dir"]
            input_file = conv["input_file_path"]
            fmt = conv["format"]
            use_watermark = conv.get("watermark", False)
            watermark_choice = conv.get("watermark_choice", "owner")
            output_type = conv.get("output_type", "audio")
            user_id = cb.from_user.id

            # make sure user_id saved
            conv["user_id"] = user_id

            watermark_local = None
            wm_info = {"applied": False, "which": None, "position": None, "volume": None}

            # download watermark if needed
            if use_watermark:
                if watermark_choice == "owner":
                    owner_wm = await get_owner_watermark()
                    wm_file_id = owner_wm.get("file_id")
                    wm_vol = owner_wm.get("volume", 0.2)
                    wm_pos = conv.get("wm_position_mode") or owner_wm.get("position_mode", "start_end")
                    wm_custom = conv.get("wm_custom_seconds") or owner_wm.get("custom_seconds", 0)
                    wm_max_within = owner_wm.get("max_within_seconds", 3600)
                    wm_info.update({"which": "owner", "volume": wm_vol, "position": wm_pos, "custom": wm_custom, "max_within_seconds": wm_max_within})
                else:
                    admin_wm = await get_admin_watermark(user_id)
                    if admin_wm:
                        wm_file_id = admin_wm.get("file_id")
                        wm_vol = admin_wm.get("volume", 0.2)
                        wm_pos = conv.get("wm_position_mode") or admin_wm.get("position_mode", "start_end")
                        wm_custom = conv.get("wm_custom_seconds") or admin_wm.get("custom_seconds", 0)
                        wm_info.update({"which": "admin", "volume": wm_vol, "position": wm_pos, "custom": wm_custom})
                    else:
                        wm_file_id = None

                if not wm_file_id:
                    # fallback: no watermark available
                    use_watermark = False
                    wm_info["applied"] = False
                else:
                    # download watermark
                    try:
                        await status_msg.edit_text("📥 **Downloading watermark...**")
                    except:
                        pass
                    try:
                        watermark_local = await bot.download_media(wm_file_id, file_name=os.path.join(job_dir, "watermark_audio"))
                        wm_info["applied"] = True
                    except Exception as e:
                        LOGGER.error(f"Failed to download watermark: {e}", exc_info=True)
                        raise Exception("Watermark download failed.") from e

            await status_msg.edit_text("🔧 **Preparing conversion...**")

            total_duration = await get_media_duration(input_file)

            # Build ffmpeg args
            ffmpeg_args, final_output_file = await build_ffmpeg_args(conv, input_file, watermark_local)

            # run ffmpeg with progress
            await run_ffmpeg_with_progress(ffmpeg_args, total_duration, status_msg, job_id, update_every=Config.PROGRESS_UPDATE_INTERVAL)

            # Post-process: check final file
            if not os.path.exists(final_output_file):
                raise Exception("Conversion completed, but output file not found.")

            output_file_size = os.path.getsize(final_output_file)
            if output_file_size > Config.TELEGRAM_MAX_FILE_SIZE:
                size_gb = output_file_size / (1024**3)
                await status_msg.edit_text(f"❌ **Upload failed**\n\nConverted file is {size_gb:.2f} GB, which is over Telegram's limit.")
                return

            # Save job metadata to DB
            job_doc = {
                "_id": job_id,
                "chat_id": chat_id,
                "user_id": user_id,
                "input_file": input_file,
                "output_file": final_output_file,
                "output_size": output_file_size,
                "format": fmt,
                "watermark": wm_info,
                "timestamp": datetime.utcnow()
            }
            await jobs_collection.insert_one(job_doc)

            # Prepare final caption info block
            info_block = "✅ **Conversion Complete!**\n\n"
            info_block += f"Format: `{fmt.get('codec')}`\n"
            info_block += f"Watermark Applied: `{'Yes' if use_watermark else 'No'}`\n"
            if wm_info.get("applied"):
                info_block += f"Watermark: `{wm_info.get('which')}`\n"
                info_block += f"Position: `{wm_info.get('position')}`\n"
                info_block += f"Volume: `{int(wm_info.get('volume',0.2)*100)}%`\n"
            info_block += f"Output Size: `{human_size(output_file_size)}`\n"
            info_block += f"Job ID: `{job_id}`\n"

            await status_msg.edit_text("✅ **Conversion Complete!**\n\nUploading result...")

            # Upload output file
            await bot.send_document(
                chat_id,
                document=final_output_file,
                caption=info_block,
                progress=progress_callback,
                progress_args=(status_msg, "Uploading")
            )
            try:
                await status_msg.delete()
            except:
                pass

        except asyncio.CancelledError:
            # user cancelled; cleanup
            try:
                await status_msg.edit_text("⚠️ **Conversion Cancelled**\n\nCleaning up files...")
            except:
                pass
        except Exception as e:
            LOGGER.error(f"Conversion failed: {e}", exc_info=True)
            try:
                await status_msg.edit_text(f"❌ ** Error! **\n\nProcess failed: `{str(e)}`")
            except:
                pass
        finally:
            # cleanup job_dir
            try:
                job_dir = conv.get("job_dir")
                if job_dir and os.path.isdir(job_dir):
                    shutil.rmtree(job_dir)
                    LOGGER.info(f"Cleaned up job directory: {job_dir}")
            except Exception as e:
                LOGGER.error(f"Failed to cleanup job directory {job_dir}: {e}")
            DOWNLOAD_PROGRESS.pop(status_msg.id, None)
            asyncio.create_task(clear_conversation_after_delay(chat_id))

# Cancel job callback
@bot.on_callback_query(filters.regex(r"^cancel_job\|(.+)$") & admin_filter)
async def cancel_job_cb(client, cb: CallbackQuery):
    await cb.answer("Cancel request received.")
    job_id = cb.data.split("|",1)[1]
    tracker = JOB_TRACKERS.get(job_id)
    if tracker is None:
        await cb.message.edit_text("Job not found or already completed.")
        return
    tracker["cancelled"] = True
    await cb.message.edit_text("⛔ You cancelled the job. Please wait a moment; system is cleaning up...")

# -------------------------------------------------------------------------------- #
# Web server & ping
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
            LOGGER.warning(f"Pinger error: {e}")

# -------------------------------------------------------------------------------- #
# App lifecycle
# -------------------------------------------------------------------------------- #

if __name__ == "__main__":
    async def main_startup_shutdown_logic():
        LOGGER.info("Application starting up...")
        await bot.start()
        bot_info = await bot.get_me()
        LOGGER.info(f"Audio Bot @{bot_info.username} started.")
        asyncio.create_task(ping_server())
        web_app = await web_server()
        runner = web.AppRunner(web_app)
        await runner.setup()
        site = web.TCPSite(runner, "0.0.0.0", Config.PORT)
        await site.start()
        LOGGER.info(f"Web server started on port {Config.PORT}.")
        try:
            await bot.send_message(Config.OWNER_ID, "**✅ Audio Bot restarted — all services online!**")
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
            loop.add_signal_handler(sig, lambda s=sig: asyncio.create_task(shutdown_handler(s)))
        except NotImplementedError:
            pass

    try:
        LOGGER.info("Starting event loop...")
        loop.run_until_complete(main_startup_shutdown_logic())
        loop.run_forever()
    except Exception as e:
        LOGGER.critical(f"A critical error forced the application to stop: {e}", exc_info=True)
    finally:
        LOGGER.info("Event loop stopped. Final cleanup.")
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
