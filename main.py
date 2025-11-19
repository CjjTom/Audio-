#Audio Converter Bot - main.py (Updated)
# Uses Pyrogram + aiohttp + Motor (MongoDB) to accept media, probe audio tracks,
# convert selected track(s) with optional multi-position watermark mixing, and upload result
#
# CHANGES MADE:
# - Improved media detection (documents with audio/video extensions, forwarded files)
# - Preserve original filename for output (e.g., "The Boys S01E02.mp3")
# - Progress UI fixes: consistent labels ("Downloading...", "Converting...", "Uploading...") and percent math
# - New "All (Audio + Video)" output option (uploads audio first, then video; cleanup after both)
# - Multi-position watermark toggles (Start, Middle, End, Custom) and multi-apply in single FFmpeg pass
# - "➕ Send Audio/Video" quick-start button on /start
# - Admin-specific watermark storage expanded (file_id, volume, positions)
# - Automatic start of conversion when user sends a media file even if no prior session
# - Various small bug fixes and safer error handling
#
# NOTE: This file is intended to replace the original main.py completely (full file provided).
# Ensure necessary environment variables are set (API_ID, API_HASH, BOT_TOKEN, OWNER_ID, MONGO_URI, PORT).
#
# Keep audio-quality focused presets — no ultrafast/low-quality shortcuts were introduced.

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

logging.basicConfig(level=logging.DEBUG, format='[%(asctime)s - %(levelname)s] - %(message)s')
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

# -------------------------------------------------------------------------------- #
# STARTUP CHECKS
# -------------------------------------------------------------------------------- #
def check_ffmpeg_available():
    ff = shutil.which("ffmpeg")
    fp = shutil.which("ffprobe")
    if not ff or not fp:
        LOGGER.critical("ffmpeg or ffprobe not found in PATH. Conversions will fail. Please install ffmpeg.")
        return False
    LOGGER.info(f"ffmpeg found: {ff}, ffprobe found: {fp}")
    return True

# Call immediately
check_ffmpeg_available()

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
# Watermark storage: owner + per-admin (expanded to store positions list + volume)
# -------------------------------------------------------------------------------- #

async def get_owner_watermark():
    doc = await bot_settings_collection.find_one({"_id": "owner_watermark"})
    if not doc:
        # default: no file, default volume 0.2 and default positions start+end within first hour
        return {"file_id": None, "volume": 0.2, "positions": ["start","end"], "custom_seconds": 0, "max_within_seconds": 3600}
    return {
        "file_id": doc.get("file_id"),
        "volume": float(doc.get("volume", 0.2)),
        "positions": doc.get("positions", ["start","end"]),
        "custom_seconds": int(doc.get("custom_seconds", 0)),
        "max_within_seconds": int(doc.get("max_within_seconds", 3600))
    }

async def set_owner_watermark_file(file_id):
    await bot_settings_collection.update_one({"_id": "owner_watermark"}, {"$set": {"file_id": file_id}}, upsert=True)

async def set_owner_watermark_volume(volume: float):
    await bot_settings_collection.update_one({"_id": "owner_watermark"}, {"$set": {"volume": float(volume)}}, upsert=True)

async def set_owner_watermark_positions(positions: list, custom_seconds: int = 0):
    await bot_settings_collection.update_one(
        {"_id": "owner_watermark"},
        {"$set": {"positions": positions, "custom_seconds": int(custom_seconds)}},
        upsert=True
    )

async def delete_owner_watermark():
    await bot_settings_collection.update_one({"_id": "owner_watermark"}, {"$unset": {"file_id": ""}})

# Per-admin watermark (supports storing positions list, volume)
async def set_admin_watermark(user_id: int, file_id: str, volume: float=0.2, positions:list=None, custom_seconds:int=0):
    if positions is None:
        positions = ["start","end"]
    await admin_collection.update_one(
        {"_id": user_id},
        {"$set": {
            "watermark": {"file_id": file_id, "volume": float(volume), "positions": positions, "custom_seconds": int(custom_seconds), "date_added": datetime.utcnow()}
        }},
        upsert=True
    )

async def update_admin_watermark_positions(user_id:int, positions:list, custom_seconds:int=0):
    await admin_collection.update_one({"_id": user_id}, {"$set": {"watermark.positions": positions, "watermark.custom_seconds": int(custom_seconds)}}, upsert=False)

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
    """
    Returns True if user is owner or present in admins collection.
    Adds logging to help debug why admin_filter might block messages.
    """
    try:
        user_id = None
        if isinstance(message_or_query, CallbackQuery):
            user_id = message_or_query.from_user.id
        elif isinstance(message_or_query, Message):
            user_id = message_or_query.from_user.id
        else:
            user = getattr(message_or_query, 'from_user', None)
            user_id = user.id if user else None

        LOGGER.debug(f"admin_filter_func called for user_id={user_id}")

        if user_id is None:
            LOGGER.warning("admin_filter_func: could not determine user id.")
            return False

        if user_id == Config.OWNER_ID:
            LOGGER.debug("admin_filter_func: user is OWNER => allowed")
            return True

        # Get admin list from DB and check membership
        try:
            admins = await get_admin_list()
            is_admin = user_id in admins
            LOGGER.debug(f"admin_filter_func: user {user_id} is_admin={is_admin} admins_count={len(admins)}")
            return is_admin
        except Exception as e:
            LOGGER.error(f"admin_filter_func: DB error checking admin list: {e}", exc_info=True)
            return False

    except Exception as e:
        LOGGER.error(f"admin_filter_func: unexpected error: {e}", exc_info=True)
        return False

admin_filter = filters.create(admin_filter_func)

# -------------------------------------------------------------------------------- #
# Utilities: shell, ffprobe, duration, ext helpers
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

def safe_ext_from_filename(filename: str) -> str:
    if not filename:
        return ""
    _, ext = os.path.splitext(filename)
    return ext.lower().lstrip('.')  # returns 'm4a', 'mp3', 'mkv', etc.

def sanitize_filename(fn: str) -> str:
    # Keep it simple: remove problematic chars
    if not fn:
        return f"file_{uuid.uuid4()}"
    fn = str(fn)
    fn = re.sub(r'[/\\<>:"|?*\x00-\x1F]', '_', fn)
    return fn

# -------------------------------------------------------------------------------- #
# Progress UI Helpers (fixed math, consistent labels)
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
    # percent [0..100]
    percent = max(0.0, min(100.0, percent))
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
# Run ffmpeg with progress parsing + cancel support (improved)
# -------------------------------------------------------------------------------- #

async def run_ffmpeg_with_progress(args_list, total_duration_seconds, status_msg, job_id, update_every=Config.PROGRESS_UPDATE_INTERVAL):
    """
    Runs ffmpeg with -progress pipe:1, parses out_time_ms, updates the status_msg with progress.
    Uses JOB_TRACKERS[job_id] to support cancellation.
    """
    # Ensure tracker entry
    JOB_TRACKERS.setdefault(job_id, {"cancelled": False, "last_update": 0.0, "bytes_processed": 0, "start_ts": time.time(), "last_time": time.time(), "last_percent": 0.0})

    if "-progress" not in args_list:
        args_list.extend(["-progress", "pipe:1", "-nostats"])

    LOGGER.info(f"Running ffmpeg: {' '.join(args_list)}")
    process = await asyncio.create_subprocess_exec(
        *args_list,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )

    out_time_ms = 0
    total_size = 0
    percent = 0.0

    # prepare cancel button
    cancel_kb = InlineKeyboardMarkup([[InlineKeyboardButton("⛔ Cancel", callback_data=f"cancel_job|{job_id}")]])

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
                    # set percent to 100% for finalization stage (will be set after process completes)
                    percent = 100.0

            # compute percent using time if possible (preferred)
            if total_duration_seconds and out_time_ms:
                processed_seconds = out_time_ms / 1000.0
                percent = min(100.0, (processed_seconds / total_duration_seconds) * 100.0)
            else:
                # fallback: if total_size available, approximate using total_size / last known
                if total_size and JOB_TRACKERS[job_id].get("last_size"):
                    try:
                        last_known = JOB_TRACKERS[job_id]["last_size"]
                        percent = min(100.0, (total_size / (last_known if last_known else total_size)) * 100.0)
                    except:
                        percent = JOB_TRACKERS[job_id].get("last_percent", 0.0)

            now = time.time()
            if now - JOB_TRACKERS[job_id]["last_update"] > update_every:
                # compute speed roughly from bytes if possible
                elapsed = now - JOB_TRACKERS[job_id]["start_ts"]
                size_for_speed = total_size if total_size else JOB_TRACKERS[job_id].get("bytes_processed", 0)
                speed = (size_for_speed / elapsed) if elapsed > 0 else 0.0
                bar = progress_bar(percent, length=20)
                eta_seconds = None
                if speed > 0 and total_size:
                    remaining = max(0, total_size - size_for_speed)
                    eta_seconds = remaining / speed
                elif speed > 0 and total_duration_seconds and percent > 0:
                    eta_seconds = (total_duration_seconds * (100.0 - percent) / percent)

                eta_str = format_time(eta_seconds) if eta_seconds is not None else "--:--:--"

                txt = (
                    f"**Converting Progress:** {bar}\n\n"
                    f"📊 **Percentage:** {percent:.2f}%\n\n"
                    f"⏳ **Elapsed:** {format_time(now - JOB_TRACKERS[job_id]['start_ts'])}\n\n"
                    f"🚀 **Speed:** {human_size(int(speed))}/s\n\n"
                    f"⏳ **ETA:** {eta_str}"
                )
                await safe_edit(status_msg, txt, reply_markup=cancel_kb)
                JOB_TRACKERS[job_id]["last_update"] = now
                JOB_TRACKERS[job_id]["last_size"] = total_size
                JOB_TRACKERS[job_id]["last_percent"] = percent

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
        try:
            stderr_acc = await process.stderr.read()
            err_text = stderr_acc.decode('utf-8', errors='ignore')[:4000]
        except Exception:
            err_text = ""
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
        try:
            stderr_data = await process.stderr.read()
            err_text = stderr_data.decode('utf-8', errors='ignore')[:4000]
        except:
            err_text = ""
        LOGGER.error(f"FFMPEG failed (rc={rc}): {err_text}")
        raise RuntimeError(f"FFMPEG failed. {err_text}")

    LOGGER.info("FFMPEG finished successfully.")
    return True

# -------------------------------------------------------------------------------- #
# TELEGRAM BOT
# -------------------------------------------------------------------------------- #

bot = Client("AudioBot", api_id=Config.API_ID, api_hash=Config.API_HASH, bot_token=Config.BOT_TOKEN)

# progress callback for download/upload (and generic updates)
async def progress_callback(current, total, message, action):
    global DOWNLOAD_PROGRESS
    try:
        percent = (current / total) * 100 if total else 0.0
    except:
        percent = 0.0
    now = time.time()
    msg_id = getattr(message, "id", None) or 0
    last = DOWNLOAD_PROGRESS.get(msg_id, {"ts": 0, "bytes": 0})
    if now - last.get("ts", 0) > 2:
        # build progress block similar to conversion
        bar = progress_bar(percent, length=20)
        prev_bytes = last.get("bytes", 0)
        prev_time = last.get("ts", now)
        dt = now - prev_time if now - prev_time > 0 else 1.0
        dbytes = max(0, current - prev_bytes)
        speed = dbytes / dt if dt > 0 else 0.0
        eta = None
        if speed > 0 and total and total > current:
            eta = (total - current) / speed
        eta_str = format_time(eta) if eta is not None else "--:--:--"

        header = "Downloading"
        if isinstance(action, str):
            header = action
        txt = (
            f"**{header}** {bar}\n\n"
            f"📊 **Percentage:** {percent:.2f}%\n\n"
            f"✅ **Processed:** {human_size(int(current))} / {human_size(int(total))}\n\n"
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
        [InlineKeyboardButton("🎧 Audio Tools", callback_data="audio_tools_menu")],
        [InlineKeyboardButton("➕ Send Audio/Video", callback_data="quick_send")]
    ]
    if message.from_user.id == Config.OWNER_ID:
        buttons.append([InlineKeyboardButton("👨‍💼 Admin Management", callback_data="admin_menu")])
    await message.reply_text(
        "**🎧 Audio Converter Bot**\n\n"
        "This bot accepts audio/video files, converts them with optional watermark mixing, and returns the processed file.",
        reply_markup=InlineKeyboardMarkup(buttons)
    )
    await update_user_conversation(message.chat.id, None)

@bot.on_callback_query(filters.regex("^main_menu$") & admin_filter)
async def main_menu_cb(client, cb: CallbackQuery):
    await cb.answer()
    buttons = [
        [InlineKeyboardButton("🎧 Audio Tools", callback_data="audio_tools_menu")],
        [InlineKeyboardButton("➕ Send Audio/Video", callback_data="quick_send")]
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
            [InlineKeyboardButton("🎵 Convert Audio/Video", callback_data="convert_audio_start")],
            [InlineKeyboardButton("⚙️ Watermark Settings", callback_data="watermark_settings")],
            [InlineKeyboardButton("⬅️ Back to Main", callback_data="main_menu")]
        ])
    )

@bot.on_callback_query(filters.regex("^quick_send$") & admin_filter)
async def quick_send_cb(client, cb: CallbackQuery):
    await cb.answer()
    job_id = str(uuid.uuid4())
    job_dir = os.path.join(Config.DOWNLOAD_DIR, job_id)
    os.makedirs(job_dir, exist_ok=True)
    await update_user_conversation(cb.message.chat.id, {
        "stage": "awaiting_media_file",
        "job_id": job_id,
        "job_dir": job_dir
    })
    try:
        await cb.message.edit_text("🎵 **Send File**\n\nSend the file you want to process.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="cancel_conv")]]))
    except:
        pass

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

# Watermark settings menu (now includes position toggles)
@bot.on_callback_query(filters.regex("^watermark_settings$") & admin_filter)
async def watermark_settings_cb(client, cb: CallbackQuery):
    await cb.answer()
    owner_wm = await get_owner_watermark()
    admin_wm = await get_admin_watermark(cb.from_user.id)
    text = "**⚙️ Watermark Settings**\n\n"
    if owner_wm.get("file_id"):
        text += f"🟢 Owner watermark is set.\nVolume: `{int(owner_wm.get('volume',0.2)*100)}%`\nPositions: `{', '.join(owner_wm.get('positions',[]))}`\n\n"
    else:
        text += "🔴 Owner watermark is not set.\n\n"
    if admin_wm:
        text += f"🧑‍💼 Your personal watermark is set (Volume {int(admin_wm.get('volume',0.2)*100)}%). Positions: `{', '.join(admin_wm.get('positions',[]))}`\n\n"
    text += "You can upload, manage, or change watermark positions (toggle multiple positions)."
    owner_buttons = [
        [InlineKeyboardButton("⬆️ Upload Owner Watermark", callback_data="owner_wm_upload")],
        [InlineKeyboardButton("🔊 Set Volume (Owner)", callback_data="owner_wm_volume")],
        [InlineKeyboardButton("📍 Set Positions (Owner)", callback_data="owner_wm_positions")],
        [InlineKeyboardButton("🗑️ Delete Owner Watermark", callback_data="owner_wm_delete")],
    ]
    admin_buttons = [
        [InlineKeyboardButton("⬆️ Upload Your Watermark (Admin)", callback_data="admin_wm_upload")],
        [InlineKeyboardButton("📍 Set Positions (Admin)", callback_data="admin_wm_positions")]
    ]
    base = [
        [InlineKeyboardButton("⬅️ Back", callback_data="audio_tools_menu")]
    ]
    kb = InlineKeyboardMarkup(owner_buttons + admin_buttons + base)
    await cb.message.edit_text(text, reply_markup=kb)

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

# Owner positions toggle UI
@bot.on_callback_query(filters.regex("^owner_wm_positions$") & filters.user(Config.OWNER_ID))
async def owner_wm_positions_cb(client, cb: CallbackQuery):
    await cb.answer()
    owner_wm = await get_owner_watermark()
    current = set(owner_wm.get("positions", ["start","end"]))
    # build toggle buttons with checkmarks
    def mk_btn(name):
        mark = "✅" if name in current else "❌"
        return InlineKeyboardButton(f"{mark} {name.capitalize()}", callback_data=f"owner_togglepos_{name}")
    kb = InlineKeyboardMarkup([
        [mk_btn("start"), mk_btn("middle")],
        [mk_btn("end"), InlineKeyboardButton("Custom", callback_data="owner_pos_custom_prompt")],
        [InlineKeyboardButton("➡️ Done", callback_data="owner_pos_done"), InlineKeyboardButton("⬅️ Back", callback_data="watermark_settings")]
    ])
    await cb.message.edit_text("📍 Toggle positions to apply owner watermark at multiple timestamps:", reply_markup=kb)

@bot.on_callback_query(filters.regex(r"^owner_togglepos_(start|middle|end)$") & filters.user(Config.OWNER_ID))
async def owner_togglepos_cb(client, cb: CallbackQuery):
    await cb.answer()
    pos = cb.data.split("_")[-1]
    owner_wm = await get_owner_watermark()
    positions = set(owner_wm.get("positions", ["start","end"]))
    if pos in positions:
        positions.remove(pos)
    else:
        positions.add(pos)
    if not positions:
        positions = set(["start"])
    await set_owner_watermark_positions(list(positions))
    await owner_wm_positions_cb(client, cb)

@bot.on_callback_query(filters.regex("^owner_pos_custom_prompt$") & filters.user(Config.OWNER_ID))
async def owner_pos_custom_prompt(client, cb: CallbackQuery):
    await cb.answer()
    await update_user_conversation(cb.message.chat.id, {"stage": "awaiting_owner_pos_custom"})
    await cb.message.edit_text("🔢 Please send custom time in seconds (e.g., 1800 = 30 minutes).", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="watermark_settings")]]))

@bot.on_callback_query(filters.regex("^owner_pos_done$") & filters.user(Config.OWNER_ID))
async def owner_pos_done_cb(client, cb: CallbackQuery):
    await cb.answer()
    await watermark_settings_cb(client, cb)

# Admin positions toggle UI
@bot.on_callback_query(filters.regex("^admin_wm_positions$") & admin_filter)
async def admin_wm_positions_cb(client, cb: CallbackQuery):
    await cb.answer()
    admin_wm = await get_admin_watermark(cb.from_user.id) or {"positions":["start","end"]}
    current = set(admin_wm.get("positions", ["start","end"]))
    def mk_btn(name):
        mark = "✅" if name in current else "❌"
        return InlineKeyboardButton(f"{mark} {name.capitalize()}", callback_data=f"admin_togglepos_{name}")
    kb = InlineKeyboardMarkup([
        [mk_btn("start"), mk_btn("middle")],
        [mk_btn("end"), InlineKeyboardButton("Custom", callback_data="admin_pos_custom_prompt")],
        [InlineKeyboardButton("➡️ Done", callback_data="admin_pos_done"), InlineKeyboardButton("⬅️ Back", callback_data="watermark_settings")]
    ])
    await cb.message.edit_text("📍 Toggle positions to apply your watermark at multiple timestamps:", reply_markup=kb)

@bot.on_callback_query(filters.regex(r"^admin_togglepos_(start|middle|end)$") & admin_filter)
async def admin_togglepos_cb(client, cb: CallbackQuery):
    await cb.answer()
    pos = cb.data.split("_")[-1]
    admin_wm = await get_admin_watermark(cb.from_user.id) or {"positions":["start","end"], "custom_seconds":0}
    positions = set(admin_wm.get("positions", ["start","end"]))
    if pos in positions:
        positions.remove(pos)
    else:
        positions.add(pos)
    if not positions:
        positions = set(["start"])
    await update_admin_watermark_positions(cb.from_user.id, list(positions))
    await admin_wm_positions_cb(client, cb)

@bot.on_callback_query(filters.regex("^admin_pos_custom_prompt$") & admin_filter)
async def admin_pos_custom_prompt(client, cb: CallbackQuery):
    await cb.answer()
    await update_user_conversation(cb.message.chat.id, {"stage": "awaiting_admin_pos_custom"})
    await cb.message.edit_text("🔢 Please send custom time in seconds (e.g., 1800 = 30 minutes).", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="watermark_settings")]]))

@bot.on_callback_query(filters.regex("^admin_pos_done$") & admin_filter)
async def admin_pos_done_cb(client, cb: CallbackQuery):
    await cb.answer()
    await watermark_settings_cb(client, cb)

# Message handler for watermark uploads & custom seconds & admin watermark upload
@bot.on_message(filters.private & (filters.audio | filters.video | filters.document | filters.text) & admin_filter)
async def message_handler_router(client, message: Message):
    chat_id = message.chat.id
    LOGGER.info(f"Incoming message from {message.from_user.id} - type audio={bool(message.audio)} video={bool(message.video)} doc={bool(message.document)} text={bool(message.text)} forward={bool(message.forward_from or message.forward_sender_name)}")

    # Debug: show conversation doc for this chat
    try:
        conv = await get_user_conversation(chat_id)
        LOGGER.debug(f"Conversation for chat {chat_id}: {conv}")
    except Exception as e:
        LOGGER.error(f"Error reading conversation for chat {chat_id}: {e}", exc_info=True)
        conv = None

    # If no conversation state and user sent a media file, create a session automatically
    sent_media = None
    if message.audio:
        sent_media = ("audio", message.audio)
    elif message.video:
        sent_media = ("video", message.video)
    elif message.document:
        sent_media = ("document", message.document)
    # Also treat voice as audio
    elif getattr(message, "voice", None):
        sent_media = ("audio", message.voice)

    # Helper: determine if a document has an audio/video extension even if mime not set
    async def doc_is_media(doc):
        if not doc:
            return False
        mime = getattr(doc, "mime_type", "") or ""
        filename = getattr(doc, "file_name", "") or ""
        if mime.startswith("audio") or mime.startswith("video"):
            return True
        ext = safe_ext_from_filename(filename)
        if ext in ["mka","mkv","mp3","m4a","aac","opus","flac","wav","ogg","mp4","mov","webm","m2ts"]:
            return True
        return False

    # If no conv exists but user sent a media-like document, create job session so bot doesn't ignore
    if not conv and sent_media:
        # verify document file_type
        if sent_media[0] == "document":
            if not await doc_is_media(sent_media[1]):
                # not a media-like document -> ignore and show guidance
                try:
                    await message.reply_text(
                        "🔎 I did not find an active conversion session.\n\n"
                        "Please press *Convert Audio* in the bot menu first (Audio Tools → Convert Audio),\n"
                        "or press ➕ Send Audio/Video to start immediately, then send the file you want to process.",
                        quote=True
                    )
                except Exception as e:
                    LOGGER.warning(f"Failed to send session-missing guidance: {e}")
                return
        # Create a session automatically
        job_id = str(uuid.uuid4())
        job_dir = os.path.join(Config.DOWNLOAD_DIR, job_id)
        os.makedirs(job_dir, exist_ok=True)
        await update_user_conversation(chat_id, {
            "stage": "awaiting_media_file",
            "job_id": job_id,
            "job_dir": job_dir
        })
        conv = await get_user_conversation(chat_id)
        LOGGER.info(f"Auto-created session for chat {chat_id}, job {job_id}")

    # If still no conversation, tell the user to start conversion flow
    if not conv:
        try:
            await message.reply_text(
                "🔎 I did not find an active conversion session.\n\n"
                "Please press *Convert Audio* in the bot menu first (Audio Tools → Convert Audio),\n"
                "then send the file you want to process.",
                quote=True
            )
        except Exception as e:
            LOGGER.warning(f"Failed to send session-missing guidance: {e}")
        return

    stage = conv.get("stage")
    LOGGER.debug(f"message_handler_router: chat {chat_id} stage={stage}")

    # Owner uploading watermark
    if stage == "awaiting_owner_wm":
        if message.from_user.id != Config.OWNER_ID:
            await message.reply_text("You don't have permission.")
            return
        # Accept audio-like doc/video/audio
        doc = message.audio or getattr(message, "voice", None) or message.document
        if not doc or not await doc_is_media(doc):
            await message.reply_text("Please send an audio file (mp3/m4a).")
            return
        file_id = doc.file_id
        await set_owner_watermark_file(file_id)
        await update_user_conversation(chat_id, None)
        await message.reply_text("✅ Owner watermark saved.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="watermark_settings")]]))
        return

    # Admin uploading watermark
    if stage == "awaiting_admin_wm":
        doc = message.audio or getattr(message, "voice", None) or message.document
        if not doc or not await doc_is_media(doc):
            await message.reply_text("Please send an audio file (mp3/m4a).")
            return
        file_id = doc.file_id
        # Preserve admin defaults
        await set_admin_watermark(message.from_user.id, file_id, volume=0.2, positions=["start","end"], custom_seconds=0)
        await update_user_conversation(chat_id, None)
        await message.reply_text("✅ Your watermark saved.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="watermark_settings")]]))
        return

    # Custom owner position seconds
    if stage == "awaiting_owner_pos_custom":
        if message.from_user.id != Config.OWNER_ID:
            await message.reply_text("You don't have permission.")
            return
        try:
            secs = int(message.text.strip())
            owner_wm = await get_owner_watermark()
            positions = owner_wm.get("positions", ["start","end"])
            # store custom seconds as owner custom
            await set_owner_watermark_positions(positions, custom_seconds=secs)
            await update_user_conversation(chat_id, None)
            await message.reply_text(f"✅ Owner custom seconds set to {secs} seconds.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="watermark_settings")]]))
        except Exception:
            await message.reply_text("Please send a valid number (digits only).")
        return

    # Custom admin position seconds
    if stage == "awaiting_admin_pos_custom":
        try:
            secs = int(message.text.strip())
            admin_wm = await get_admin_watermark(message.from_user.id) or {"positions":["start","end"]}
            positions = admin_wm.get("positions", ["start","end"])
            await update_admin_watermark_positions(message.from_user.id, positions, custom_seconds=secs)
            await update_user_conversation(chat_id, None)
            await message.reply_text(f"✅ Custom seconds set to {secs} seconds for your watermark.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="watermark_settings")]]))
        except Exception:
            await message.reply_text("Please send a valid number (digits only).")
        return

    # Awaiting owner/admin/wm custom seconds handled above

    if stage == "awaiting_media_file":
        # Accept media file even as document with various containers
        if not (message.audio or message.video or message.document or getattr(message, "voice", None)):
            await message.reply_text("Please send a valid media file.")
            return
        await handle_media_file(client, message, conv)
        return

    if stage == "awaiting_wm_custom_seconds":
        try:
            secs = int(message.text.strip())
            await update_user_conversation(message.chat.id, {"wm_position_mode": "custom", "wm_custom_seconds": secs, "stage": "processing"})
            # proceed
            fake_cb = CallbackQuery(id=None, from_user=message.from_user, message=message, chat_instance=None)
            await ask_for_output_type(fake_cb, conv)
        except Exception:
            await message.reply_text("Please send a valid number (digits only).")
        return

    if stage == "awaiting_admin_id" and message.text:
        if message.from_user.id != Config.OWNER_ID:
            return
        try:
            user_id = int(message.text.strip())
            await add_admin(user_id)
            await message.reply_text(f"✅ Admin added `{user_id}`", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")]]))
            await update_user_conversation(message.chat.id, None)
        except ValueError:
            await message.reply_text("Please send a valid user ID (digits only).")

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

async def handle_media_file(client, message: Message, conv: dict):
    # This function downloads the incoming media, probes it, and asks the user to pick a track if multiple
    media = message.audio or message.video or message.document or getattr(message, "voice", None)
    job_dir = conv.get("job_dir")
    if not job_dir or not os.path.isdir(job_dir):
        await message.reply_text("Error: Job directory not found. Please try again.", quote=True)
        asyncio.create_task(clear_conversation_after_delay(message.chat.id, delay=5))
        return

    # capture original filename
    original_filename = None
    if message.document and getattr(message.document, "file_name", None):
        original_filename = message.document.file_name
    elif message.audio and getattr(message.audio, "file_name", None):
        original_filename = message.audio.file_name
    elif message.video and getattr(message.video, "file_name", None):
        original_filename = message.video.file_name
    else:
        # try from caption or fallback to message media file_unique_id
        original_filename = getattr(message, "caption", None) or f"file_{uuid.uuid4()}"
    original_filename = sanitize_filename(original_filename)

    # determine extension fallback
    ext = safe_ext_from_filename(original_filename)
    if not ext:
        # choose reasonable ext based on media type
        if message.video:
            ext = "mkv"
        elif message.audio or getattr(message, "voice", None):
            ext = "m4a"
        else:
            ext = "dat"
        original_filename = f"{original_filename}.{ext}"

    input_target_name = f"input_{uuid.uuid4()}_{os.path.basename(original_filename)}"
    input_file_path = os.path.join(job_dir, input_target_name)

    status_msg = await message.reply_text("📥 **Downloading... 0%**", quote=True)
    try:
        downloaded = await message.download(
            file_name=input_file_path,
            progress=progress_callback,
            progress_args=(status_msg, "Downloading")
        )
        # message.download returns path
        if downloaded:
            input_file_path = downloaded
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

    is_audio_only = message.audio or (message.document and (getattr(message.document, "mime_type","") or "").startswith("audio")) or getattr(message, "voice", None)
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

    # Save conv details
    await update_user_conversation(message.chat.id, {
        "input_file_path": input_file_path,
        "original_filename": original_filename,
        "audio_streams": audio_streams,
        "is_audio_only": is_audio_only,
        "job_dir": job_dir,
        "stage": "awaiting_track_selection"
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

    raw_choice = cb.data.split("_", 1)[1]
    # normalize mapping keys used earlier
    format_choice_map = {
        "aac_stereo": {"codec": "aac", "channels": 2, "bitrate": "192k"},
        "aac_5_1": {"codec": "aac", "channels": 6, "bitrate": "320k"},
        "mp3_stereo": {"codec": "libmp3lame", "channels": 2, "bitrate": "192k"},
        "aac_copy": {"codec": "copy", "channels": "copy", "bitrate": "copy"},
    }
    # map incoming cb values
    mapping = {
        "aac_stereo": "aac_stereo",
        "aac_5_1": "aac_5_1",
        "mp3_stereo": "mp3_stereo",
        "aac_copy": "aac_copy"
    }
    format_choice = raw_choice
    if format_choice not in format_choice_map:
        # sometimes cb contained other suffixes; fallback to detect 'copy'
        if "copy" in format_choice:
            format_choice = "aac_copy"
        else:
            return await cb.answer("Invalid format.", show_alert=True)

    await update_user_conversation(chat_id, {"format": format_choice_map[format_choice]})

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
        owner_wm = await get_owner_watermark()
        # set temporary positions in conversation (allow user to toggle additively)
        await update_user_conversation(chat_id, {"wm_positions": owner_wm.get("positions", ["start","end"]), "wm_custom_seconds": owner_wm.get("custom_seconds", 0)})
        # present toggle UI
        await cb.message.edit_text("Owner watermark selected.\n\nToggle positions (you can select multiple):", reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("Start", callback_data="wm_toggle_start"), InlineKeyboardButton("Middle", callback_data="wm_toggle_middle")],
            [InlineKeyboardButton("End", callback_data="wm_toggle_end"), InlineKeyboardButton("Custom", callback_data="wm_pos_custom_prompt")],
            [InlineKeyboardButton("➡️ Continue", callback_data="wm_pos_done")]
        ]))
        await update_user_conversation(chat_id, {"stage": "awaiting_wm_position_choice"})
    elif choice == "admin":
        admin_wm = await get_admin_watermark(cb.from_user.id)
        if not admin_wm:
            await cb.answer("Your watermark is not saved. Please upload first.", show_alert=True)
            return await watermark_settings_cb(client, cb)
        await update_user_conversation(chat_id, {"watermark": True, "watermark_choice": "admin"})
        await update_user_conversation(chat_id, {"wm_positions": admin_wm.get("positions", ["start","end"]), "wm_custom_seconds": admin_wm.get("custom_seconds", 0)})
        await cb.message.edit_text("Your watermark selected. Toggle positions (you can select multiple):", reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("Start", callback_data="wm_toggle_start"), InlineKeyboardButton("Middle", callback_data="wm_toggle_middle")],
            [InlineKeyboardButton("End", callback_data="wm_toggle_end"), InlineKeyboardButton("Custom", callback_data="wm_pos_custom_prompt")],
            [InlineKeyboardButton("➡️ Continue", callback_data="wm_pos_done")]
        ]))
        await update_user_conversation(chat_id, {"stage": "awaiting_wm_position_choice"})
    else:
        await update_user_conversation(chat_id, {"watermark": False})
        await ask_for_output_type(cb, conv)

# Watermark position toggle callbacks (generic for owner/admin when in selection stage)
@bot.on_callback_query(filters.regex(r"^wm_toggle_(start|middle|end)$") & admin_filter)
async def wm_toggle_generic_cb(client, cb: CallbackQuery):
    await cb.answer()
    choice = cb.data.split("_")[-1]
    chat_id = cb.message.chat.id
    conv = await get_user_conversation(chat_id)
    if not conv:
        return await cb.answer("Session expired.", show_alert=True)
    current = set(conv.get("wm_positions", []))
    if choice in current:
        current.remove(choice)
    else:
        current.add(choice)
    if not current:
        current.add("start")
    await update_user_conversation(chat_id, {"wm_positions": list(current)})
    # reflect selections in UI by adding marks to text (simple re-render)
    selected = ", ".join(sorted(list(current)))
    await cb.message.edit_text(f"Selected positions: `{selected}`\n\nToggle more or press Continue.", reply_markup=InlineKeyboardMarkup([
        [InlineKeyboardButton("Start", callback_data="wm_toggle_start"), InlineKeyboardButton("Middle", callback_data="wm_toggle_middle")],
        [InlineKeyboardButton("End", callback_data="wm_toggle_end"), InlineKeyboardButton("Custom", callback_data="wm_pos_custom_prompt")],
        [InlineKeyboardButton("➡️ Continue", callback_data="wm_pos_done")]
    ]))

@bot.on_callback_query(filters.regex("^wm_pos_custom_prompt$") & admin_filter)
async def wm_pos_custom_prompt(client, cb: CallbackQuery):
    await cb.answer()
    await update_user_conversation(cb.message.chat.id, {"stage": "awaiting_wm_custom_seconds"})
    await cb.message.edit_text("🔢 Please send custom time in seconds (e.g., 1800 = 30 minutes).", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="cancel_conv")]]))

@bot.on_callback_query(filters.regex("^wm_pos_done$") & admin_filter)
async def wm_pos_done_cb(client, cb: CallbackQuery):
    await cb.answer()
    chat_id = cb.message.chat.id
    conv = await get_user_conversation(chat_id)
    if not conv:
        return await cb.answer("Session expired.", show_alert=True)
    await ask_for_output_type(cb, conv)

@bot.on_message(filters.private & filters.text & admin_filter)
async def custom_wm_seconds_handler(client, message: Message):
    conv = await get_user_conversation(message.chat.id)
    if not conv:
        return
    stage = conv.get("stage")
    if stage == "awaiting_wm_custom_seconds" or stage == "awaiting_owner_pos_custom" or stage == "awaiting_admin_pos_custom":
        try:
            secs = int(message.text.strip())
            await update_user_conversation(message.chat.id, {"wm_position_mode": "custom", "wm_custom_seconds": secs, "stage": "processing"})
            # proceed
            fake_cb = CallbackQuery(id=None, from_user=message.from_user, message=message, chat_instance=None)
            await ask_for_output_type(fake_cb, conv)
        except Exception:
            await message.reply_text("Please send a valid number (digits only).")

# Output selection (now includes "all")
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
            [InlineKeyboardButton("🎵 Audio Only (.m4a/.mp3)", callback_data="output_audio"), InlineKeyboardButton("🎬 Remux Video (.mkv)", callback_data="output_remux")],
            [InlineKeyboardButton("📦 All (Audio + Video)", callback_data="output_all")],
            [InlineKeyboardButton("❌ Cancel", callback_data="cancel_conv")]
        ]))

@bot.on_callback_query(filters.regex(r"^output_(audio|remux|all)$") & admin_filter)
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
# FFmpeg args builder with multi-position watermark positioning
# -------------------------------------------------------------------------------- #

async def build_ffmpeg_args(conv, input_file, watermark_local_file, override_format=None, override_output_type=None):
    """
    Build FFmpeg args based on conversation. Supports multi-position watermarks list:
    conv['wm_positions'] -> list like ['start','middle','end','custom']
    custom seconds in conv['wm_custom_seconds'] if 'custom' selected
    """
    track_index = conv["selected_track_index"]
    stream_obj = conv["selected_stream_obj"]
    fmt = override_format if override_format else conv.get("format")
    use_watermark = conv.get("watermark", False)
    watermark_choice = conv.get("watermark_choice", "owner") # owner or admin
    job_dir = conv["job_dir"]
    wm_positions = conv.get("wm_positions", []) or []
    wm_custom_seconds = int(conv.get("wm_custom_seconds", 0) or 0)

    output_type = override_output_type if override_output_type else conv.get("output_type")
    output_ext = "mp3" if fmt['codec'] == 'libmp3lame' else "m4a"
    if output_type == "remux":
        output_ext = "mkv"
    elif fmt['codec'] == 'copy':
        # keep container if audio-only original exists
        output_ext = "m4a"

    # build final output basename from original filename
    original_filename = conv.get("original_filename") or f"output_{uuid.uuid4()}"
    base_name, _ = os.path.splitext(original_filename)
    final_basename = f"{base_name}.{output_ext}"
    final_output_file = os.path.join(job_dir, final_basename)

    args = ["ffmpeg", "-y", "-hide_banner", "-i", input_file]

    filter_complex_parts = []
    audio_map_label = f"0:a:{track_index}"

    # Determine which watermark file to use and its volume/positions
    wm_file_to_use = None
    wm_volume = 0.2
    wm_owner_description = "none"
    if use_watermark:
        if watermark_choice == "owner":
            owner_wm = await get_owner_watermark()
            wm_file_to_use = owner_wm.get("file_id")
            wm_volume = owner_wm.get("volume", 0.2)
            # default positions set if not explicitly set in conversation
            wm_positions = conv.get("wm_positions") or owner_wm.get("positions", ["start","end"])
            wm_custom_seconds = conv.get("wm_custom_seconds") or owner_wm.get("custom_seconds") or 0
            wm_owner_description = "owner"
        else:
            admin_wm = await get_admin_watermark(conv.get("user_id") or 0)
            if admin_wm:
                wm_file_to_use = admin_wm.get("file_id")
                wm_volume = admin_wm.get("volume", 0.2)
                wm_positions = conv.get("wm_positions") or admin_wm.get("positions", ["start","end"])
                wm_custom_seconds = conv.get("wm_custom_seconds") or admin_wm.get("custom_seconds") or 0
                wm_owner_description = f"admin:{conv.get('user_id')}"

    # If copy selected but watermark requested, we will force a re-encode (can't mix in copy mode).
    force_reencode = False
    if fmt.get("codec") == "copy" and use_watermark:
        force_reencode = True

    # If watermark exists and we have a local watermark file (downloaded earlier)
    if use_watermark and watermark_local_file:
        # plan: add watermark as second input and as many delayed streams as positions selected
        args.extend(["-i", watermark_local_file])
        # determine durations
        input_duration = await get_media_duration(input_file)
        try:
            wm_dur = await get_media_duration(watermark_local_file)
        except:
            wm_dur = 0.0

        # compute position timestamps in seconds for each selected position (start=0, middle center, end align)
        positions = []
        for p in wm_positions:
            if p == "start":
                positions.append(0.0)
            elif p == "middle":
                positions.append(max(0.0, (input_duration / 2.0) - (wm_dur / 2.0)))
            elif p == "end":
                positions.append(max(0.0, input_duration - wm_dur))
            elif p == "custom":
                positions.append(max(0.0, float(wm_custom_seconds)))
        # deduplicate and sort ascending
        unique_positions = sorted(set([int(max(0, p)) for p in positions]))

        # Build filter_complex:
        # - [1:a]asplit=N -> [wm0][wm1]...
        # - for each wm_i: adelay=pos_ms|pos_ms,volume=wm_volume -> [wm_i_out]
        # - [0:a:track_index]volume=1.0[main]
        # - then amix inputs=(1 + N) of all [main] + [wm_i_out]
        n_wms = len(unique_positions)
        if n_wms == 0:
            audio_map_label = f"0:a:{track_index}"
        else:
            # split watermark track into n copies
            filter_complex_parts.append(f"[1:a]asplit={n_wms}" + "".join([f"[wm{i}]" for i in range(n_wms)]))
            filter_complex_parts.append(f"[0:a:{track_index}]volume=1.0[main]")
            wm_out_labels = []
            for i, pos_sec in enumerate(unique_positions):
                delay_ms = int(round(pos_sec * 1000))
                # adelay expects channel-delays separated by |
                filter_complex_parts.append(f"[wm{i}]adelay={delay_ms}|{delay_ms},volume={wm_volume}[wm{i}_out]")
                wm_out_labels.append(f"[wm{i}_out]")
            # combine
            all_inputs = "[main]" + "".join(wm_out_labels)
            amix_inputs = 1 + n_wms
            filter_complex_parts.append(f"{all_inputs}amix=inputs={amix_inputs}:duration=first[aud_out]")
            audio_map_label = "[aud_out]"

        # If forcing re-encode due to copy selection, override fmt
        if force_reencode and fmt.get("codec") == "copy":
            fmt = {"codec": "aac", "channels": 2, "bitrate": "192k"}

    elif use_watermark and watermark_local_file and force_reencode:
        # fallback - similar approach (shouldn't reach due to above block)
        args.extend(["-i", watermark_local_file])
        input_duration = await get_media_duration(input_file)
        try:
            wm_dur = await get_media_duration(watermark_local_file)
        except:
            wm_dur = 0
        # default start+end
        max_within = 3600
        end_target = input_duration if input_duration <= max_within else max_within
        delay_ms = max(0, int((end_target - wm_dur) * 1000))
        filter_complex_parts.append(f"[1:a]asplit=2[wm1][wm2]")
        filter_complex_parts.append(f"[0:a:{track_index}]volume=1.0[main]")
        filter_complex_parts.append(f"[wm1]volume={wm_volume}[wm_start]")
        filter_complex_parts.append(f"[wm2]adelay={delay_ms}|{delay_ms},volume={wm_volume}[wm_end]")
        filter_complex_parts.append(f"[main][wm_start][wm_end]amix=inputs=3:duration=first[aud_out]")
        audio_map_label = "[aud_out]"
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

    if output_type == "remux":
        # include video and subtitles if present
        args.extend(["-map", "0:v:0?", "-map", "0:s?"])
        args.extend(["-c:v", "copy", "-c:s", "copy"])

    args.append(final_output_file)
    return args, final_output_file

# -------------------------------------------------------------------------------- #
# Conversion process: download watermark, build args, run ffmpeg with progress, upload and save job metadata
# Supports output_type 'audio', 'remux', 'all'
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
            wm_info = {"applied": False, "which": None, "positions": None, "volume": None}

            # download watermark if needed
            if use_watermark:
                if watermark_choice == "owner":
                    owner_wm = await get_owner_watermark()
                    wm_file_id = owner_wm.get("file_id")
                    wm_vol = owner_wm.get("volume", 0.2)
                    wm_pos = conv.get("wm_positions") or owner_wm.get("positions", ["start","end"])
                    wm_custom = conv.get("wm_custom_seconds") or owner_wm.get("custom_seconds", 0)
                    wm_max_within = owner_wm.get("max_within_seconds", 3600)
                    wm_info.update({"which": "owner", "volume": wm_vol, "positions": wm_pos, "custom": wm_custom, "max_within_seconds": wm_max_within})
                else:
                    admin_wm = await get_admin_watermark(user_id)
                    if admin_wm:
                        wm_file_id = admin_wm.get("file_id")
                        wm_vol = admin_wm.get("volume", 0.2)
                        wm_pos = conv.get("wm_positions") or admin_wm.get("positions", ["start","end"])
                        wm_custom = conv.get("wm_custom_seconds") or admin_wm.get("custom_seconds", 0)
                        wm_info.update({"which": "admin", "volume": wm_vol, "positions": wm_pos, "custom": wm_custom})
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

            # Process logic for output types:
            uploaded_files = []
            # helper to run a single conversion (build args, run ffmpeg, upload)
            async def run_single_conversion_and_upload(local_conv, out_type, out_fmt=None):
                # build ffmpeg args
                ffmpeg_args, final_output_file = await build_ffmpeg_args(local_conv, input_file, watermark_local, override_format=out_fmt, override_output_type=out_type)
                # run ffmpeg with progress
                # update status_msg to "Converting..."
                try:
                    await status_msg.edit_text("🔁 **Converting...**")
                except:
                    pass
                await run_ffmpeg_with_progress(ffmpeg_args, total_duration, status_msg, job_id, update_every=Config.PROGRESS_UPDATE_INTERVAL)
                # check output
                if not os.path.exists(final_output_file):
                    raise Exception("Conversion completed, but output file not found.")
                output_file_size = os.path.getsize(final_output_file)
                if output_file_size > Config.TELEGRAM_MAX_FILE_SIZE:
                    size_gb = output_file_size / (1024**3)
                    await status_msg.edit_text(f"❌ **Upload failed**\n\nConverted file is {size_gb:.2f} GB, which is over Telegram's limit.")
                    return None, None
                # prepare caption info
                info_block = "✅ **Conversion Complete!**\n\n"
                ffcodec = out_fmt.get('codec') if out_fmt else (local_conv.get("format") or {}).get('codec')
                info_block += f"Format: `{ffcodec}`\n"
                info_block += f"Watermark Applied: `{'Yes' if local_conv.get('watermark') else 'No'}`\n"
                if local_conv.get('watermark') and wm_info.get("applied"):
                    info_block += f"Watermark: `{wm_info.get('which')}`\n"
                    info_block += f"Positions: `{', '.join(wm_info.get('positions') or [])}`\n"
                    info_block += f"Volume: `{int(wm_info.get('volume',0.2)*100)}%`\n"
                info_block += f"Output Size: `{human_size(output_file_size)}`\n"
                info_block += f"Job ID: `{job_id}`\n"
                await status_msg.edit_text("✅ **Conversion Complete!**\n\nUploading result...")
                # upload file
                await bot.send_document(
                    chat_id,
                    document=final_output_file,
                    caption=info_block,
                    progress=progress_callback,
                    progress_args=(status_msg, "Uploading")
                )
                uploaded_files.append(final_output_file)
                return final_output_file, output_file_size

            if output_type == "audio":
                # single audio conversion
                final_file, size = await run_single_conversion_and_upload(conv, "audio", out_fmt=fmt)
            elif output_type == "remux":
                # remux video/keep video
                final_file, size = await run_single_conversion_and_upload(conv, "remux", out_fmt=fmt)
            elif output_type == "all":
                # first: audio (convert as audio)
                # create a shallow copy of conv for audio run (so we can reuse conv for video)
                conv_audio = dict(conv)
                conv_audio["output_type"] = "audio"
                final_audio_file, audio_size = await run_single_conversion_and_upload(conv_audio, "audio", out_fmt=fmt)
                # second: remux video and upload
                conv_video = dict(conv)
                conv_video["output_type"] = "remux"
                final_video_file, video_size = await run_single_conversion_and_upload(conv_video, "remux", out_fmt=fmt)
                # after both uploaded, set final_file to audio (primary)
                final_file = final_audio_file or final_video_file
                size = (audio_size or 0) + (video_size or 0)
            else:
                raise Exception("Unknown output type requested.")

            # Save job metadata to DB
            job_doc = {
                "_id": job_id,
                "chat_id": chat_id,
                "user_id": user_id,
                "input_file": input_file,
                "output_files": uploaded_files,
                "output_size": sum([os.path.getsize(f) for f in uploaded_files]) if uploaded_files else 0,
                "format": fmt,
                "watermark": wm_info,
                "timestamp": datetime.utcnow()
            }
            await jobs_collection.insert_one(job_doc)

            # cleanup UI
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
            # cleanup job_dir (must remove after BOTH uploaded in 'all' case)
            try:
                conv_latest = await get_user_conversation(chat_id)
                job_dir = conv_latest.get("job_dir") if conv_latest else conv.get("job_dir")
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
    web_app = web.Application(client_max_size=300_000_000)
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
