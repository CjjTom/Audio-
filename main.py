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

# ---------------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------------

load_dotenv()

logging.basicConfig(level=logging.DEBUG, format='[%(asctime)s - %(levelname)s] - %(message)s')
LOGGER = logging.getLogger(__name__)
logging.getLogger("pyrogram").setLevel(logging.WARNING)

class Config:
    API_ID              = int(os.environ.get("API_ID", 0))
    API_HASH            = os.environ.get("API_HASH", "")
    BOT_TOKEN           = os.environ.get("BOT_TOKEN", "")
    OWNER_ID            = int(os.environ.get("OWNER_ID", 0))

    MONGO_URI           = os.environ.get("MONGO_URI", "")
    PORT                = int(os.environ.get("PORT", 8080))

    # Directory for temporary file downloads – unique per process start
    DOWNLOAD_DIR        = os.environ.get("DOWNLOAD_DIR", f"/tmp/audio_bot_downloads_{uuid.uuid4()}/")

    # Keep-Alive Pinger (Heroku)
    STREAM_URL          = os.environ.get("STREAM_URL", "").rstrip('/')
    PING_INTERVAL       = int(os.environ.get("PING_INTERVAL", 1200))
    ON_HEROKU           = 'DYNO' in os.environ

    MAX_CONCURRENT_JOBS             = int(os.environ.get("MAX_CONCURRENT_JOBS", 1))
    TELEGRAM_MAX_FILE_SIZE          = int(os.environ.get("TELEGRAM_MAX_FILE_SIZE", 2 * 1024 * 1024 * 1024))
    CONVERSATION_CLEAR_DELAY        = int(os.environ.get("CONVERSATION_CLEAR_DELAY", 300))
    PROGRESS_UPDATE_INTERVAL        = float(os.environ.get("PROGRESS_UPDATE_INTERVAL", 2.0))
    # When True the owner watermark is always applied; admins cannot skip it.
    OWNER_WATERMARK_MANDATORY       = os.environ.get("OWNER_WATERMARK_MANDATORY", "1") == "1"

# ---------------------------------------------------------------------------
# STARTUP CHECKS
# ---------------------------------------------------------------------------

def check_ffmpeg_available():
    ff = shutil.which("ffmpeg")
    fp = shutil.which("ffprobe")
    if not ff or not fp:
        LOGGER.critical("ffmpeg or ffprobe not found in PATH. Conversions will fail.")
        return False
    LOGGER.info(f"ffmpeg: {ff}  ffprobe: {fp}")
    return True

check_ffmpeg_available()

required_vars = [Config.API_ID, Config.API_HASH, Config.BOT_TOKEN, Config.OWNER_ID,
                 Config.MONGO_URI, Config.PORT]
if not all(required_vars):
    LOGGER.critical("FATAL: One or more required env-vars are missing.")
    exit(1)

os.makedirs(Config.DOWNLOAD_DIR, exist_ok=True)

# ---------------------------------------------------------------------------
# IN-MEMORY GLOBALS
# ---------------------------------------------------------------------------

ffmpeg_semaphore   = asyncio.Semaphore(Config.MAX_CONCURRENT_JOBS)
DOWNLOAD_PROGRESS  = {}          # msg_id -> {ts, bytes}
JOB_TRACKERS       = {}          # job_id -> tracker dict (rebuilt each run; not critical to persist)
CANCEL_DOWNLOADS   = set()       # chat_ids requesting download cancellation

# ---------------------------------------------------------------------------
# DATABASE
# ---------------------------------------------------------------------------

db_client               = AsyncIOMotorClient(Config.MONGO_URI)
db                      = db_client['AudioBotDB']
user_conversations_col  = db['conversations']
bot_settings_collection = db['settings']       # owner watermark + global settings + default metadata
admin_collection        = db['admins']         # per-admin records (watermark, limits, stats, permanent)
jobs_collection         = db['jobs']           # job metadata (never deleted)
users_collection        = db['users']          # permanent user registry (never deleted)

# ---------------------------------------------------------------------------
# PERMANENT USER REGISTRY  – IDs are never removed
# ---------------------------------------------------------------------------

async def register_user(user_id: int, username: str = None, first_name: str = None):
    """Upsert user into permanent registry. Fields are only added, never cleared."""
    update_fields = {"last_seen": datetime.utcnow()}
    if username:
        update_fields["username"] = username
    if first_name:
        update_fields["first_name"] = first_name
    await users_collection.update_one(
        {"_id": user_id},
        {"$set": update_fields, "$setOnInsert": {"registered_at": datetime.utcnow()}},
        upsert=True
    )

# ---------------------------------------------------------------------------
# CONVERSATION HELPERS
# ---------------------------------------------------------------------------

async def get_user_conversation(chat_id):
    return await user_conversations_col.find_one({"_id": chat_id})

async def update_user_conversation(chat_id, data):
    """If data is None/empty, delete the conversation doc. Otherwise upsert."""
    if data:
        await user_conversations_col.update_one(
            {"_id": chat_id}, {"$set": data}, upsert=True
        )
    else:
        await user_conversations_col.delete_one({"_id": chat_id})

async def clear_conversation_after_delay(chat_id, delay=Config.CONVERSATION_CLEAR_DELAY):
    await asyncio.sleep(delay)
    await update_user_conversation(chat_id, None)
    LOGGER.info(f"Auto-cleared conversation state for chat_id={chat_id}")

# ---------------------------------------------------------------------------
# OWNER WATERMARK  (bot_settings_collection)
# ---------------------------------------------------------------------------

async def get_owner_watermark():
    doc = await bot_settings_collection.find_one({"_id": "owner_watermark"})
    if not doc:
        return {
            "file_id": None,
            "volume": 0.2,
            "positions": ["start", "end"],
            "custom_seconds": 0,
            "max_within_seconds": 3600,
            "fade_duration": 1.0,        # seconds for afade in/out
            "use_silence_detection": False,
            "normalize": True,
        }
    return {
        "file_id":              doc.get("file_id"),
        "volume":               float(doc.get("volume", 0.2)),
        "positions":            doc.get("positions", ["start", "end"]),
        "custom_seconds":       int(doc.get("custom_seconds", 0)),
        "max_within_seconds":   int(doc.get("max_within_seconds", 3600)),
        "fade_duration":        float(doc.get("fade_duration", 1.0)),
        "use_silence_detection":bool(doc.get("use_silence_detection", False)),
        "normalize":            bool(doc.get("normalize", True)),
    }

async def set_owner_watermark_file(file_id):
    await bot_settings_collection.update_one(
        {"_id": "owner_watermark"}, {"$set": {"file_id": file_id}}, upsert=True
    )

async def set_owner_watermark_volume(volume: float):
    await bot_settings_collection.update_one(
        {"_id": "owner_watermark"}, {"$set": {"volume": float(volume)}}, upsert=True
    )

async def set_owner_watermark_positions(positions: list, custom_seconds: int = 0):
    await bot_settings_collection.update_one(
        {"_id": "owner_watermark"},
        {"$set": {"positions": positions, "custom_seconds": int(custom_seconds)}},
        upsert=True
    )

async def set_owner_watermark_extra(fade_duration: float = None,
                                    use_silence_detection: bool = None,
                                    normalize: bool = None):
    fields = {}
    if fade_duration is not None:
        fields["fade_duration"] = float(fade_duration)
    if use_silence_detection is not None:
        fields["use_silence_detection"] = bool(use_silence_detection)
    if normalize is not None:
        fields["normalize"] = bool(normalize)
    if fields:
        await bot_settings_collection.update_one(
            {"_id": "owner_watermark"}, {"$set": fields}, upsert=True
        )

async def delete_owner_watermark():
    await bot_settings_collection.update_one(
        {"_id": "owner_watermark"}, {"$unset": {"file_id": ""}}
    )

# ---------------------------------------------------------------------------
# DEFAULT METADATA  (stored in bot_settings_collection)
# ---------------------------------------------------------------------------

async def get_default_metadata():
    doc = await bot_settings_collection.find_one({"_id": "default_metadata"})
    if not doc:
        return {"title": "", "artist": "", "album": "", "language": "", "comment": ""}
    return {
        "title":    doc.get("title", ""),
        "artist":   doc.get("artist", ""),
        "album":    doc.get("album", ""),
        "language": doc.get("language", ""),
        "comment":  doc.get("comment", ""),
    }

async def set_default_metadata(fields: dict):
    await bot_settings_collection.update_one(
        {"_id": "default_metadata"}, {"$set": fields}, upsert=True
    )

# ---------------------------------------------------------------------------
# PER-ADMIN WATERMARK  (admin_collection)
# ---------------------------------------------------------------------------

async def set_admin_watermark(user_id: int, file_id: str, volume: float = 0.2,
                               positions: list = None, custom_seconds: int = 0,
                               fade_duration: float = 1.0,
                               use_silence_detection: bool = False,
                               normalize: bool = True):
    if positions is None:
        positions = ["start", "end"]
    await admin_collection.update_one(
        {"_id": user_id},
        {"$set": {
            "watermark": {
                "file_id":               file_id,
                "volume":                float(volume),
                "positions":             positions,
                "custom_seconds":        int(custom_seconds),
                "date_added":            datetime.utcnow(),
                "fade_duration":         float(fade_duration),
                "use_silence_detection": bool(use_silence_detection),
                "normalize":             bool(normalize),
            }
        }},
        upsert=True
    )

async def update_admin_watermark_positions(user_id: int, positions: list, custom_seconds: int = 0):
    await admin_collection.update_one(
        {"_id": user_id},
        {"$set": {"watermark.positions": positions, "watermark.custom_seconds": int(custom_seconds)}},
        upsert=False
    )

async def update_admin_watermark_extra(user_id: int, fade_duration: float = None,
                                        use_silence_detection: bool = None,
                                        normalize: bool = None):
    fields = {}
    if fade_duration is not None:
        fields["watermark.fade_duration"] = float(fade_duration)
    if use_silence_detection is not None:
        fields["watermark.use_silence_detection"] = bool(use_silence_detection)
    if normalize is not None:
        fields["watermark.normalize"] = bool(normalize)
    if fields:
        await admin_collection.update_one({"_id": user_id}, {"$set": fields}, upsert=False)

async def get_admin_watermark(user_id: int):
    doc = await admin_collection.find_one({"_id": user_id})
    if not doc:
        return None
    return doc.get("watermark")

async def set_admin_watermark_volume(user_id: int, volume: float):
    await admin_collection.update_one(
        {"_id": user_id},
        {"$set": {"watermark.volume": float(volume)}},
        upsert=True
    )

# ---------------------------------------------------------------------------
# ADMIN LIMITS  (stored inside admin_collection)
# ---------------------------------------------------------------------------

async def get_admin_limits(user_id: int) -> dict:
    doc = await admin_collection.find_one({"_id": user_id})
    if not doc:
        return {"max_jobs_per_day": 0, "max_file_size_mb": 0}  # 0 = unlimited
    return doc.get("limits", {"max_jobs_per_day": 0, "max_file_size_mb": 0})

async def set_admin_limits(user_id: int, max_jobs_per_day: int = 0, max_file_size_mb: int = 0):
    await admin_collection.update_one(
        {"_id": user_id},
        {"$set": {"limits": {"max_jobs_per_day": int(max_jobs_per_day),
                              "max_file_size_mb": int(max_file_size_mb)}}},
        upsert=True
    )

async def get_admin_jobs_today(user_id: int) -> int:
    today_start = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    return await jobs_collection.count_documents({"user_id": user_id, "timestamp": {"$gte": today_start}})

# ---------------------------------------------------------------------------
# ADMIN LIST MANAGEMENT  (owner controls)
# ---------------------------------------------------------------------------

async def get_admin_list():
    """Return list of admin user_ids excluding the owner."""
    cursor = admin_collection.find({"_id": {"$ne": Config.OWNER_ID}})
    return [doc["_id"] async for doc in cursor]

async def add_admin(user_id: int):
    if user_id == Config.OWNER_ID:
        return
    await admin_collection.update_one(
        {"_id": user_id},
        {"$set": {"date_added": datetime.utcnow()}},
        upsert=True
    )

async def remove_admin(user_id: int):
    """
    Removes admin privileges by deleting the admin_collection record.
    The user's permanent record in users_collection is never touched.
    """
    await admin_collection.delete_one({"_id": user_id})

# ---------------------------------------------------------------------------
# ADMIN STATS
# ---------------------------------------------------------------------------

async def get_admin_stats(user_id: int) -> dict:
    """Return job statistics for a given admin."""
    total_jobs     = await jobs_collection.count_documents({"user_id": user_id})
    today_jobs     = await get_admin_jobs_today(user_id)
    today_start    = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    pipeline = [
        {"$match": {"user_id": user_id}},
        {"$group": {"_id": None, "total_size": {"$sum": "$output_size"}}}
    ]
    agg    = await jobs_collection.aggregate(pipeline).to_list(1)
    total_bytes = agg[0]["total_size"] if agg else 0
    last_job = await jobs_collection.find_one(
        {"user_id": user_id}, sort=[("timestamp", -1)]
    )
    return {
        "total_jobs":  total_jobs,
        "today_jobs":  today_jobs,
        "total_bytes": total_bytes,
        "last_job_ts": last_job.get("timestamp") if last_job else None,
    }

# ---------------------------------------------------------------------------
# FILTERS
# ---------------------------------------------------------------------------

async def admin_filter_func(_, __, message_or_query):
    try:
        if isinstance(message_or_query, (CallbackQuery, Message)):
            user_id = message_or_query.from_user.id
        else:
            user = getattr(message_or_query, 'from_user', None)
            user_id = user.id if user else None

        if user_id is None:
            return False
        if user_id == Config.OWNER_ID:
            return True

        admins = await get_admin_list()
        return user_id in admins
    except Exception as e:
        LOGGER.error(f"admin_filter_func error: {e}", exc_info=True)
        return False

admin_filter = filters.create(admin_filter_func)

# ---------------------------------------------------------------------------
# UTILITIES
# ---------------------------------------------------------------------------

async def run_shell_command(command: str) -> str:
    process = await asyncio.create_subprocess_shell(
        command,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await process.communicate()
    if process.returncode != 0:
        raise Exception(f"Command failed: {stderr.decode(errors='ignore').strip()}")
    return stdout.decode(errors='ignore').strip()

async def probe_media(file_path: str):
    cmd = f"ffprobe -v error -show_streams -of json {shlex.quote(file_path)}"
    try:
        result = await run_shell_command(cmd)
        return json.loads(result).get("streams", [])
    except Exception as e:
        LOGGER.error(f"probe_media failed for {file_path}: {e}")
        return []

async def get_media_duration(file_path: str) -> float:
    cmd = (
        f"ffprobe -v error -show_entries format=duration "
        f"-of default=noprint_wrappers=1:nokey=1 {shlex.quote(file_path)}"
    )
    try:
        return float(await run_shell_command(cmd))
    except Exception as e:
        LOGGER.warning(f"get_media_duration failed for {file_path}: {e}")
        return 0.0

def safe_ext_from_filename(filename: str) -> str:
    if not filename:
        return ""
    _, ext = os.path.splitext(filename)
    return ext.lower().lstrip('.')

def sanitize_filename(fn: str) -> str:
    if not fn:
        return f"file_{uuid.uuid4()}"
    fn = str(fn)
    fn = re.sub(r'[/\\<>:"|?*\x00-\x1F]', '_', fn)
    return fn

# ---------------------------------------------------------------------------
# PROGRESS UI HELPERS
# ---------------------------------------------------------------------------

def human_size(num_bytes: int) -> str:
    step = 1024.0
    if num_bytes < step:
        return f"{num_bytes} B"
    for unit in ["KB", "MB", "GB", "TB"]:
        num_bytes /= step
        if num_bytes < step:
            return f"{num_bytes:.2f} {unit}"
    return f"{num_bytes:.2f} PB"

def progress_bar(percent: float, length: int = 20) -> str:
    percent = max(0.0, min(100.0, percent))
    filled  = int(math.floor((percent / 100.0) * length))
    bar     = "█" * filled + "░" * (length - filled)
    return f"[{bar}]"

def format_time(seconds) -> str:
    if seconds is None or math.isinf(float(seconds)) or float(seconds) < 0:
        return "--:--:--"
    return str(timedelta(seconds=int(max(0, seconds))))

async def safe_edit(message, text, reply_markup=None):
    try:
        await message.edit_text(text, reply_markup=reply_markup, parse_mode=enums.ParseMode.MARKDOWN)
    except MessageNotModified:
        pass
    except Exception as e:
        LOGGER.warning(f"safe_edit failed: {e}")

# ---------------------------------------------------------------------------
# SILENCE DETECTION  – returns list of silent-gap midpoints (seconds)
# ---------------------------------------------------------------------------

async def detect_silence_midpoints(file_path: str, track_index: int,
                                    noise_db: float = -40.0,
                                    min_duration: float = 2.0) -> list:
    """
    Run ffmpeg silencedetect on a single audio stream and return midpoints
    of silent gaps (in seconds). Falls back to empty list on any error.
    """
    cmd = (
        f"ffmpeg -y -hide_banner -i {shlex.quote(file_path)} "
        f"-map 0:{track_index} "
        f"-af silencedetect=noise={noise_db}dB:d={min_duration} "
        f"-f null -"
    )
    try:
        process = await asyncio.create_subprocess_shell(
            cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        _, stderr_data = await process.communicate()
        output = stderr_data.decode(errors='ignore')
    except Exception as e:
        LOGGER.warning(f"silence_detection subprocess error: {e}")
        return []

    starts = []
    ends   = []
    for line in output.splitlines():
        if 'silence_start' in line:
            m = re.search(r'silence_start:\s*([\d.]+)', line)
            if m:
                starts.append(float(m.group(1)))
        elif 'silence_end' in line:
            m = re.search(r'silence_end:\s*([\d.]+)', line)
            if m:
                ends.append(float(m.group(1)))

    midpoints = []
    for s, e in zip(starts, ends):
        midpoints.append((s + e) / 2.0)

    LOGGER.info(f"silence detection found {len(midpoints)} silent gaps")
    return midpoints

# ---------------------------------------------------------------------------
# FFMPEG WITH PROGRESS + CANCEL SUPPORT
# ---------------------------------------------------------------------------

async def run_ffmpeg_with_progress(
    args_list: list,
    total_duration_seconds: float,
    status_msg,
    job_id: str,
    update_every: float = Config.PROGRESS_UPDATE_INTERVAL,
):
    """
    Runs ffmpeg with -progress pipe:1.  Parses out_time_ms / total_size lines.
    Reads stderr into a separate background task to avoid blocking and to
    capture the full error text without race conditions.
    Supports cancellation via JOB_TRACKERS[job_id]['cancelled'].
    """
    # Hard-reset tracker for this run
    JOB_TRACKERS[job_id] = {
        "cancelled":       False,
        "last_update":     0.0,
        "bytes_processed": 0,
        "start_ts":        time.time(),
        "last_time":       time.time(),
        "last_percent":    0.0,
    }

    # Inject -progress as a global option (right after 'ffmpeg')
    if "-progress" not in args_list:
        if args_list and args_list[0].lower().endswith("ffmpeg"):
            args_list[1:1] = ["-progress", "pipe:1", "-nostats"]
        else:
            args_list = ["ffmpeg", "-progress", "pipe:1", "-nostats"] + list(args_list)

    LOGGER.info(f"[job {job_id}] Running: {' '.join(args_list)}")

    process = await asyncio.create_subprocess_exec(
        *args_list,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )

    # Drain stderr in background so it never blocks stdout reads
    stderr_lines: list = []

    async def _drain_stderr():
        try:
            async for raw in process.stderr:
                stderr_lines.append(raw.decode(errors='ignore').rstrip())
        except Exception:
            pass

    drain_task = asyncio.create_task(_drain_stderr())

    cancel_kb = InlineKeyboardMarkup(
        [[InlineKeyboardButton("⛔ Cancel", callback_data=f"cancel_job|{job_id}")]]
    )

    out_time_ms  = 0
    total_size   = 0
    percent      = 0.0

    try:
        while True:
            # Check cancellation
            if JOB_TRACKERS.get(job_id, {}).get("cancelled"):
                LOGGER.info(f"[job {job_id}] Cancel requested – terminating ffmpeg")
                try:
                    process.send_signal(signal.SIGINT)
                    await asyncio.wait_for(process.wait(), timeout=10.0)
                except Exception:
                    process.terminate()
                raise asyncio.CancelledError("Conversion cancelled by user.")

            line = await process.stdout.readline()
            if not line:
                break

            text = line.decode(errors='ignore').strip()
            if not text or '=' not in text:
                continue

            k, _, v = text.partition('=')
            k, v = k.strip(), v.strip()

            if k == "out_time_ms":
                try:
                    out_time_ms = int(v)
                except Exception:
                    pass
            elif k == "total_size":
                try:
                    total_size = int(v)
                    JOB_TRACKERS[job_id]["bytes_processed"] = total_size
                except Exception:
                    pass
            elif k == "progress" and v == "end":
                percent = max(percent, 99.9)

            # Compute percent from duration
            if total_duration_seconds and out_time_ms:
                processed_s = out_time_ms / 1_000_000.0   # out_time_ms is µs
                percent = min((processed_s / total_duration_seconds) * 100.0, 99.99)
            else:
                percent = JOB_TRACKERS[job_id].get("last_percent", percent)

            now = time.time()
            if now - JOB_TRACKERS[job_id]["last_update"] >= update_every:
                elapsed = now - JOB_TRACKERS[job_id]["start_ts"]
                speed   = (total_size / elapsed) if elapsed > 0 and total_size else 0.0
                if speed > 0 and total_size and total_size > (out_time_ms / 1_000_000.0 * 1):
                    remaining = max(0, total_size - JOB_TRACKERS[job_id]["bytes_processed"])
                    eta_s = remaining / speed
                elif total_duration_seconds and percent > 0:
                    eta_s = total_duration_seconds * (100.0 - percent) / percent
                else:
                    eta_s = None

                txt = (
                    f"**Converting Progress:** {progress_bar(percent)}\n\n"
                    f"📊 **Percentage:** {percent:.2f}%\n\n"
                    f"⏳ **Elapsed:** {format_time(elapsed)}\n\n"
                    f"🚀 **Speed:** {human_size(int(speed))}/s\n\n"
                    f"⏳ **ETA:** {format_time(eta_s)}"
                )
                await safe_edit(status_msg, txt, reply_markup=cancel_kb)
                JOB_TRACKERS[job_id]["last_update"]  = now
                JOB_TRACKERS[job_id]["last_percent"] = percent

    except asyncio.CancelledError:
        try:
            if process.returncode is None:
                process.terminate()
        except Exception:
            pass
        drain_task.cancel()
        raise

    except Exception as e:
        try:
            if process.returncode is None:
                process.terminate()
        except Exception:
            pass
        drain_task.cancel()
        await asyncio.gather(drain_task, return_exceptions=True)
        err_text = "\n".join(stderr_lines[-40:])
        LOGGER.error(f"[job {job_id}] ffmpeg runtime error: {e}\nStderr tail:\n{err_text}")
        raise RuntimeError(f"FFmpeg runtime error: {e}\n{err_text}")

    finally:
        try:
            rc = await asyncio.wait_for(process.wait(), timeout=30.0)
        except Exception:
            rc = None

    await asyncio.gather(drain_task, return_exceptions=True)

    err_text = "\n".join(stderr_lines[-40:])

    if rc == 0:
        try:
            elapsed = time.time() - JOB_TRACKERS[job_id]["start_ts"]
            await safe_edit(
                status_msg,
                f"**Converting Progress:** {progress_bar(100.0)}\n\n"
                f"📊 **Percentage:** 100.00%\n\n"
                f"⏳ **Elapsed:** {format_time(elapsed)}\n\n"
                f"🚀 **Speed:** {human_size(0)}/s\n\n"
                f"⏳ **ETA:** 00:00:00",
                reply_markup=cancel_kb,
            )
        except Exception:
            pass
    elif rc is not None and rc != 0:
        LOGGER.error(f"[job {job_id}] ffmpeg failed rc={rc}:\n{err_text}")
        raise RuntimeError(f"FFmpeg failed (rc={rc}).\n{err_text}")

    LOGGER.info(f"[job {job_id}] ffmpeg finished successfully.")
    JOB_TRACKERS.pop(job_id, None)
    return True

# ---------------------------------------------------------------------------
# TELEGRAM BOT INSTANCE
# ---------------------------------------------------------------------------

bot = Client("AudioBot", api_id=Config.API_ID, api_hash=Config.API_HASH, bot_token=Config.BOT_TOKEN)

# ---------------------------------------------------------------------------
# DOWNLOAD / UPLOAD PROGRESS CALLBACK
# ---------------------------------------------------------------------------

async def progress_callback(current, total, message, action):
    global DOWNLOAD_PROGRESS, CANCEL_DOWNLOADS
    if message.chat.id in CANCEL_DOWNLOADS:
        CANCEL_DOWNLOADS.discard(message.chat.id)
        raise Exception("Download Cancelled by User")

    try:
        percent = (current / total) * 100 if total else 0.0
    except Exception:
        percent = 0.0

    now    = time.time()
    msg_id = getattr(message, "id", 0) or 0
    last   = DOWNLOAD_PROGRESS.get(msg_id, {"ts": 0, "bytes": 0})

    if now - last.get("ts", 0) > 2:
        bar       = progress_bar(percent)
        dt        = max(now - last.get("ts", now), 0.001)
        dbytes    = max(0, current - last.get("bytes", 0))
        speed     = dbytes / dt
        eta       = (total - current) / speed if speed > 0 and total and total > current else None
        header    = action if isinstance(action, str) else "Downloading"

        cancel_kb = InlineKeyboardMarkup(
            [[InlineKeyboardButton("❌ Cancel Download", callback_data="cancel_download_process")]]
        )
        txt = (
            f"**{header}** {bar}\n\n"
            f"📊 **Percentage:** {percent:.2f}%\n\n"
            f"✅ **Processed:** {human_size(int(current))} / {human_size(int(total))}\n\n"
            f"🚀 **Speed:** {human_size(int(speed))}/s\n\n"
            f"⏳ **ETA:** {format_time(eta)}"
        )
        try:
            await message.edit_text(txt, reply_markup=cancel_kb)
            DOWNLOAD_PROGRESS[msg_id] = {"ts": now, "bytes": current}
        except (MessageNotModified, FloodWait):
            pass
        except Exception as e:
            LOGGER.warning(f"progress_callback edit error: {e}")

# ---------------------------------------------------------------------------
# BOT HANDLERS — START / MAIN MENU
# ---------------------------------------------------------------------------

@bot.on_message(filters.command("start") & filters.private & admin_filter)
async def start_command(client, message: Message):
    user_id = message.from_user.id
    await register_user(user_id,
                        username=getattr(message.from_user, "username", None),
                        first_name=getattr(message.from_user, "first_name", None))
    buttons = [
        [InlineKeyboardButton("🎧 Audio Tools",        callback_data="audio_tools_menu")],
        [InlineKeyboardButton("➕ Send Audio/Video",    callback_data="quick_send")],
        [InlineKeyboardButton("📊 My Stats",            callback_data="my_stats")],
    ]
    if user_id == Config.OWNER_ID:
        buttons.append([InlineKeyboardButton("👨‍💼 Admin Management", callback_data="admin_menu")])
        buttons.append([InlineKeyboardButton("⚙️ Owner Settings",    callback_data="owner_settings_menu")])
    await message.reply_text(
        "**🎧 Audio Converter Bot**\n\n"
        "Accepts audio/video files, converts with optional watermark mixing, and returns the processed file.",
        reply_markup=InlineKeyboardMarkup(buttons)
    )
    await update_user_conversation(message.chat.id, None)

@bot.on_callback_query(filters.regex("^main_menu$") & admin_filter)
async def main_menu_cb(client, cb: CallbackQuery):
    await cb.answer()
    user_id = cb.from_user.id
    buttons = [
        [InlineKeyboardButton("🎧 Audio Tools",        callback_data="audio_tools_menu")],
        [InlineKeyboardButton("➕ Send Audio/Video",    callback_data="quick_send")],
        [InlineKeyboardButton("📊 My Stats",            callback_data="my_stats")],
    ]
    if user_id == Config.OWNER_ID:
        buttons.append([InlineKeyboardButton("👨‍💼 Admin Management", callback_data="admin_menu")])
        buttons.append([InlineKeyboardButton("⚙️ Owner Settings",    callback_data="owner_settings_menu")])
    try:
        await cb.message.edit_text(
            "**🎧 Audio Converter Bot**\n\nChoose an option:",
            reply_markup=InlineKeyboardMarkup(buttons)
        )
    except MessageNotModified:
        pass
    await update_user_conversation(cb.message.chat.id, None)

# ---------------------------------------------------------------------------
# AUDIO TOOLS MENU
# ---------------------------------------------------------------------------

@bot.on_callback_query(filters.regex("^audio_tools_menu$") & admin_filter)
async def audio_tools_menu_cb(client, cb: CallbackQuery):
    await cb.answer()
    await cb.message.edit_text(
        "**🎧 Audio Tools**\n\nChoose an option:",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🎵 Convert Audio/Video",  callback_data="convert_audio_start")],
            [InlineKeyboardButton("⚙️ My Watermark Settings", callback_data="admin_watermark_settings")],
            [InlineKeyboardButton("⬅️ Back to Main",         callback_data="main_menu")],
        ])
    )

# ---------------------------------------------------------------------------
# MY STATS  (available to all admins, showing their own data)
# ---------------------------------------------------------------------------

@bot.on_callback_query(filters.regex("^my_stats$") & admin_filter)
async def my_stats_cb(client, cb: CallbackQuery):
    await cb.answer()
    user_id = cb.from_user.id
    stats   = await get_admin_stats(user_id)
    limits  = await get_admin_limits(user_id)
    last_ts = stats["last_job_ts"].strftime("%Y-%m-%d %H:%M UTC") if stats["last_job_ts"] else "Never"
    text = (
        f"**📊 Your Statistics**\n\n"
        f"▪️ Total Jobs (all time): `{stats['total_jobs']}`\n"
        f"▪️ Jobs Today: `{stats['today_jobs']}`\n"
        f"▪️ Total Data Processed: `{human_size(stats['total_bytes'])}`\n"
        f"▪️ Last Job: `{last_ts}`\n\n"
        f"**Your Limits:**\n"
        f"▪️ Max Jobs/Day: `{'Unlimited' if not limits['max_jobs_per_day'] else limits['max_jobs_per_day']}`\n"
        f"▪️ Max File Size: `{'Unlimited' if not limits['max_file_size_mb'] else str(limits['max_file_size_mb']) + ' MB'}`"
    )
    await cb.message.edit_text(
        text,
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="main_menu")]])
    )

# ---------------------------------------------------------------------------
# QUICK SEND
# ---------------------------------------------------------------------------

@bot.on_callback_query(filters.regex("^quick_send$") & admin_filter)
async def quick_send_cb(client, cb: CallbackQuery):
    await cb.answer()
    job_id  = str(uuid.uuid4())
    job_dir = os.path.join(Config.DOWNLOAD_DIR, job_id)
    os.makedirs(job_dir, exist_ok=True)
    await update_user_conversation(cb.message.chat.id, {
        "stage":   "awaiting_media_file",
        "job_id":  job_id,
        "job_dir": job_dir,
    })
    try:
        await cb.message.edit_text(
            "🎵 **Send File**\n\nSend the audio/video file you want to process.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="cancel_conv")]])
        )
    except Exception:
        pass

@bot.on_callback_query(filters.regex("^cancel_conv$") & admin_filter)
async def cancel_conversation_handler(client, cb: CallbackQuery):
    await cb.answer("Operation cancelled.")
    chat_id = cb.message.chat.id
    conv    = await get_user_conversation(chat_id)
    if conv:
        job_dir = conv.get("job_dir")
        if job_dir and os.path.isdir(job_dir):
            try:
                shutil.rmtree(job_dir)
            except Exception as e:
                LOGGER.error(f"cleanup job_dir failed: {e}")
    CANCEL_DOWNLOADS.discard(chat_id)
    asyncio.create_task(clear_conversation_after_delay(chat_id))
    try:
        await cb.message.delete()
    except Exception:
        pass
    await start_command(client, cb.message)

@bot.on_callback_query(filters.regex("^cancel_download_process$") & admin_filter)
async def cancel_download_cb(client, cb: CallbackQuery):
    CANCEL_DOWNLOADS.add(cb.message.chat.id)
    await cb.answer("Stopping Download…", show_alert=True)

# ---------------------------------------------------------------------------
# OWNER SETTINGS MENU  (watermark + metadata – owner-only)
# ---------------------------------------------------------------------------

@bot.on_callback_query(filters.regex("^owner_settings_menu$") & filters.user(Config.OWNER_ID))
async def owner_settings_menu_cb(client, cb: CallbackQuery):
    await cb.answer()
    owner_wm = await get_owner_watermark()
    wm_status = "🟢 Set" if owner_wm.get("file_id") else "🔴 Not set"
    await cb.message.edit_text(
        f"**⚙️ Owner Settings**\n\n"
        f"Owner Watermark: {wm_status}\n"
        f"Volume: `{int(owner_wm.get('volume', 0.2) * 100)}%`\n"
        f"Positions: `{', '.join(owner_wm.get('positions', []))}`\n"
        f"Fade: `{owner_wm.get('fade_duration', 1.0)}s`\n"
        f"Silence Detection: `{'On' if owner_wm.get('use_silence_detection') else 'Off'}`\n"
        f"Normalize (EBU R128): `{'On' if owner_wm.get('normalize', True) else 'Off'}`",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("⬆️ Upload Owner Watermark",     callback_data="owner_wm_upload")],
            [InlineKeyboardButton("🔊 Set Volume",                 callback_data="owner_wm_volume")],
            [InlineKeyboardButton("📍 Set Positions",              callback_data="owner_wm_positions")],
            [InlineKeyboardButton("🌊 Fade Settings",              callback_data="owner_wm_fade")],
            [InlineKeyboardButton("🔇 Silence Detection Toggle",   callback_data="owner_wm_toggle_silence")],
            [InlineKeyboardButton("📻 Normalize Toggle",           callback_data="owner_wm_toggle_normalize")],
            [InlineKeyboardButton("🗑️ Delete Owner Watermark",    callback_data="owner_wm_delete")],
            [InlineKeyboardButton("✏️ Default Metadata",           callback_data="owner_metadata_menu")],
            [InlineKeyboardButton("⬅️ Back",                      callback_data="main_menu")],
        ])
    )

# --- owner watermark upload ---

@bot.on_callback_query(filters.regex("^owner_wm_upload$") & filters.user(Config.OWNER_ID))
async def owner_wm_upload_start(client, cb: CallbackQuery):
    await cb.answer()
    await update_user_conversation(cb.message.chat.id, {"stage": "awaiting_owner_wm"})
    await cb.message.edit_text(
        "📥 **Upload Owner Watermark**\n\nSend a short audio file (mp3/m4a).",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="owner_settings_menu")]])
    )

@bot.on_callback_query(filters.regex("^owner_wm_delete$") & filters.user(Config.OWNER_ID))
async def owner_wm_delete_cb(client, cb: CallbackQuery):
    await cb.answer("Watermark deleted.")
    await delete_owner_watermark()
    await owner_settings_menu_cb(client, cb)

# --- owner volume ---

@bot.on_callback_query(filters.regex("^owner_wm_volume$") & filters.user(Config.OWNER_ID))
async def owner_wm_volume_cb(client, cb: CallbackQuery):
    await cb.answer()
    await cb.message.edit_text(
        "🔊 Choose owner watermark volume:",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("10%",  callback_data="owner_wm_vol_0.10"),
             InlineKeyboardButton("20%",  callback_data="owner_wm_vol_0.20"),
             InlineKeyboardButton("30%",  callback_data="owner_wm_vol_0.30")],
            [InlineKeyboardButton("50%",  callback_data="owner_wm_vol_0.50"),
             InlineKeyboardButton("80%",  callback_data="owner_wm_vol_0.80"),
             InlineKeyboardButton("100%", callback_data="owner_wm_vol_1.00")],
            [InlineKeyboardButton("⬅️ Back", callback_data="owner_settings_menu")],
        ])
    )

@bot.on_callback_query(filters.regex(r"^owner_wm_vol_(\d+\.\d+)$") & filters.user(Config.OWNER_ID))
async def owner_wm_vol_save(client, cb: CallbackQuery):
    vol = float(re.search(r"(\d+\.\d+)$", cb.data).group(1))
    await set_owner_watermark_volume(vol)
    await cb.answer(f"Volume set to {int(vol * 100)}%.")
    await owner_settings_menu_cb(client, cb)

# --- owner positions ---

@bot.on_callback_query(filters.regex("^owner_wm_positions$") & filters.user(Config.OWNER_ID))
async def owner_wm_positions_cb(client, cb: CallbackQuery):
    await cb.answer()
    owner_wm = await get_owner_watermark()
    current  = set(owner_wm.get("positions", ["start", "end"]))
    all_opts = {"start", "middle", "end", "hourly"}
    sel_all_label = "❌ Deselect All" if all_opts.issubset(current) else "✅ Select All"

    def mk(name, label=None):
        mark  = "✅" if name in current else "❌"
        shown = label or name.capitalize()
        return InlineKeyboardButton(f"{mark} {shown}", callback_data=f"owner_togglepos_{name}")

    await cb.message.edit_text(
        "📍 Toggle positions for the owner watermark:",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton(sel_all_label, callback_data="owner_positions_select_all")],
            [mk("start", "Start (+3 min)"), mk("middle", "Middle")],
            [mk("end",   "End (-7 min)"),   mk("hourly", "Hourly (60 min)")],
            [InlineKeyboardButton("🔢 Custom Seconds", callback_data="owner_pos_custom_prompt")],
            [InlineKeyboardButton("➡️ Done",  callback_data="owner_pos_done"),
             InlineKeyboardButton("⬅️ Back", callback_data="owner_settings_menu")],
        ])
    )

@bot.on_callback_query(filters.regex("^owner_positions_select_all$") & filters.user(Config.OWNER_ID))
async def owner_positions_select_all_cb(client, cb: CallbackQuery):
    await cb.answer()
    owner_wm  = await get_owner_watermark()
    current   = set(owner_wm.get("positions", ["start", "end"]))
    all_set   = {"start", "middle", "end", "hourly"}
    new_pos   = ["start"] if all_set.issubset(current) else list(all_set)
    await set_owner_watermark_positions(new_pos, owner_wm.get("custom_seconds", 0))
    await owner_wm_positions_cb(client, cb)

@bot.on_callback_query(filters.regex(r"^owner_togglepos_(start|middle|end|hourly)$") & filters.user(Config.OWNER_ID))
async def owner_togglepos_cb(client, cb: CallbackQuery):
    await cb.answer()
    pos      = cb.data.split("_")[-1]
    owner_wm = await get_owner_watermark()
    positions = set(owner_wm.get("positions", ["start", "end"]))
    positions.discard(pos) if pos in positions else positions.add(pos)
    if not positions:
        positions = {"start"}
    await set_owner_watermark_positions(list(positions), owner_wm.get("custom_seconds", 0))
    await owner_wm_positions_cb(client, cb)

@bot.on_callback_query(filters.regex("^owner_pos_custom_prompt$") & filters.user(Config.OWNER_ID))
async def owner_pos_custom_prompt(client, cb: CallbackQuery):
    await cb.answer()
    await update_user_conversation(cb.message.chat.id, {"stage": "awaiting_owner_pos_custom"})
    await cb.message.edit_text(
        "🔢 Send custom time in seconds (e.g. 1800 = 30 min).",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="owner_settings_menu")]])
    )

@bot.on_callback_query(filters.regex("^owner_pos_done$") & filters.user(Config.OWNER_ID))
async def owner_pos_done_cb(client, cb: CallbackQuery):
    await cb.answer()
    await owner_settings_menu_cb(client, cb)

# --- owner fade ---

@bot.on_callback_query(filters.regex("^owner_wm_fade$") & filters.user(Config.OWNER_ID))
async def owner_wm_fade_cb(client, cb: CallbackQuery):
    await cb.answer()
    await cb.message.edit_text(
        "🌊 Choose watermark fade-in/out duration (seconds):",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("0s (Off)", callback_data="owner_wm_fade_0.0"),
             InlineKeyboardButton("0.5s",     callback_data="owner_wm_fade_0.5"),
             InlineKeyboardButton("1s",       callback_data="owner_wm_fade_1.0")],
            [InlineKeyboardButton("2s",       callback_data="owner_wm_fade_2.0"),
             InlineKeyboardButton("3s",       callback_data="owner_wm_fade_3.0")],
            [InlineKeyboardButton("⬅️ Back",  callback_data="owner_settings_menu")],
        ])
    )

@bot.on_callback_query(filters.regex(r"^owner_wm_fade_(\d+\.\d+)$") & filters.user(Config.OWNER_ID))
async def owner_wm_fade_save(client, cb: CallbackQuery):
    fd = float(re.search(r"(\d+\.\d+)$", cb.data).group(1))
    await set_owner_watermark_extra(fade_duration=fd)
    await cb.answer(f"Fade set to {fd}s.")
    await owner_settings_menu_cb(client, cb)

@bot.on_callback_query(filters.regex("^owner_wm_toggle_silence$") & filters.user(Config.OWNER_ID))
async def owner_wm_toggle_silence_cb(client, cb: CallbackQuery):
    await cb.answer()
    owner_wm = await get_owner_watermark()
    new_val  = not owner_wm.get("use_silence_detection", False)
    await set_owner_watermark_extra(use_silence_detection=new_val)
    await cb.answer(f"Silence Detection: {'On' if new_val else 'Off'}")
    await owner_settings_menu_cb(client, cb)

@bot.on_callback_query(filters.regex("^owner_wm_toggle_normalize$") & filters.user(Config.OWNER_ID))
async def owner_wm_toggle_normalize_cb(client, cb: CallbackQuery):
    await cb.answer()
    owner_wm = await get_owner_watermark()
    new_val  = not owner_wm.get("normalize", True)
    await set_owner_watermark_extra(normalize=new_val)
    await cb.answer(f"Normalize: {'On' if new_val else 'Off'}")
    await owner_settings_menu_cb(client, cb)

# ---------------------------------------------------------------------------
# DEFAULT METADATA (owner only)
# ---------------------------------------------------------------------------

@bot.on_callback_query(filters.regex("^owner_metadata_menu$") & filters.user(Config.OWNER_ID))
async def owner_metadata_menu_cb(client, cb: CallbackQuery):
    await cb.answer()
    meta = await get_default_metadata()
    await cb.message.edit_text(
        f"**✏️ Default Metadata**\n\n"
        f"Title:    `{meta['title'] or '(not set)'}`\n"
        f"Artist:   `{meta['artist'] or '(not set)'}`\n"
        f"Album:    `{meta['album'] or '(not set)'}`\n"
        f"Language: `{meta['language'] or '(not set)'}`\n"
        f"Comment:  `{meta['comment'] or '(not set)'}`\n\n"
        f"These are embedded in every output file automatically.",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("✏️ Set Title",    callback_data="meta_set_title")],
            [InlineKeyboardButton("✏️ Set Artist",   callback_data="meta_set_artist")],
            [InlineKeyboardButton("✏️ Set Album",    callback_data="meta_set_album")],
            [InlineKeyboardButton("✏️ Set Language", callback_data="meta_set_language")],
            [InlineKeyboardButton("✏️ Set Comment",  callback_data="meta_set_comment")],
            [InlineKeyboardButton("🗑️ Clear All",   callback_data="meta_clear_all")],
            [InlineKeyboardButton("⬅️ Back",         callback_data="owner_settings_menu")],
        ])
    )

@bot.on_callback_query(filters.regex(r"^meta_set_(title|artist|album|language|comment)$") & filters.user(Config.OWNER_ID))
async def meta_set_field_cb(client, cb: CallbackQuery):
    await cb.answer()
    field = cb.data.split("_")[-1]
    await update_user_conversation(cb.message.chat.id, {"stage": f"awaiting_meta_{field}"})
    await cb.message.edit_text(
        f"✏️ Send the new **{field}** value (or send `-` to clear it).",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="owner_metadata_menu")]])
    )

@bot.on_callback_query(filters.regex("^meta_clear_all$") & filters.user(Config.OWNER_ID))
async def meta_clear_all_cb(client, cb: CallbackQuery):
    await cb.answer("Metadata cleared.")
    await set_default_metadata({"title": "", "artist": "", "album": "", "language": "", "comment": ""})
    await owner_metadata_menu_cb(client, cb)

# ---------------------------------------------------------------------------
# ADMIN WATERMARK SETTINGS  (role-aware: admin sees only their own settings)
# ---------------------------------------------------------------------------

@bot.on_callback_query(filters.regex("^admin_watermark_settings$") & admin_filter)
async def admin_watermark_settings_cb(client, cb: CallbackQuery):
    await cb.answer()
    user_id  = cb.from_user.id
    admin_wm = await get_admin_watermark(user_id)

    wm_status = "🟢 Set" if admin_wm and admin_wm.get("file_id") else "🔴 Not set"
    text = (
        f"**⚙️ Your Watermark Settings**\n\n"
        f"Status: {wm_status}\n"
    )
    if admin_wm:
        text += (
            f"Volume: `{int(admin_wm.get('volume', 0.2) * 100)}%`\n"
            f"Positions: `{', '.join(admin_wm.get('positions', []))}`\n"
            f"Fade: `{admin_wm.get('fade_duration', 1.0)}s`\n"
            f"Silence Detection: `{'On' if admin_wm.get('use_silence_detection') else 'Off'}`\n"
            f"Normalize: `{'On' if admin_wm.get('normalize', True) else 'Off'}`\n"
        )

    await cb.message.edit_text(
        text,
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("⬆️ Upload My Watermark",      callback_data="admin_wm_upload")],
            [InlineKeyboardButton("🔊 Set My Volume",             callback_data="admin_wm_volume")],
            [InlineKeyboardButton("📍 Set My Positions",          callback_data="admin_wm_positions")],
            [InlineKeyboardButton("🌊 Fade Settings",             callback_data="admin_wm_fade")],
            [InlineKeyboardButton("🔇 Silence Detection Toggle",  callback_data="admin_wm_toggle_silence")],
            [InlineKeyboardButton("📻 Normalize Toggle",          callback_data="admin_wm_toggle_normalize")],
            [InlineKeyboardButton("⬅️ Back",                     callback_data="audio_tools_menu")],
        ])
    )

@bot.on_callback_query(filters.regex("^admin_wm_upload$") & admin_filter)
async def admin_wm_upload_start(client, cb: CallbackQuery):
    await cb.answer()
    await update_user_conversation(cb.message.chat.id, {"stage": "awaiting_admin_wm"})
    await cb.message.edit_text(
        "📥 **Upload Your Watermark**\n\nSend a short audio file (mp3/m4a).",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="admin_watermark_settings")]])
    )

@bot.on_callback_query(filters.regex("^admin_wm_volume$") & admin_filter)
async def admin_wm_volume_cb(client, cb: CallbackQuery):
    await cb.answer()
    admin_wm = await get_admin_watermark(cb.from_user.id)
    if not admin_wm or not admin_wm.get("file_id"):
        await cb.answer("Upload a watermark first.", show_alert=True)
        return
    await cb.message.edit_text(
        "🔊 Choose your watermark volume:",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("10%",  callback_data="admin_wm_vol_0.10"),
             InlineKeyboardButton("20%",  callback_data="admin_wm_vol_0.20"),
             InlineKeyboardButton("30%",  callback_data="admin_wm_vol_0.30")],
            [InlineKeyboardButton("50%",  callback_data="admin_wm_vol_0.50"),
             InlineKeyboardButton("80%",  callback_data="admin_wm_vol_0.80"),
             InlineKeyboardButton("100%", callback_data="admin_wm_vol_1.00")],
            [InlineKeyboardButton("⬅️ Back", callback_data="admin_watermark_settings")],
        ])
    )

@bot.on_callback_query(filters.regex(r"^admin_wm_vol_(\d+\.\d+)$") & admin_filter)
async def admin_wm_vol_save(client, cb: CallbackQuery):
    vol = float(re.search(r"(\d+\.\d+)$", cb.data).group(1))
    await set_admin_watermark_volume(cb.from_user.id, vol)
    await cb.answer(f"Volume set to {int(vol * 100)}%.")
    await admin_watermark_settings_cb(client, cb)

@bot.on_callback_query(filters.regex("^admin_wm_positions$") & admin_filter)
async def admin_wm_positions_cb(client, cb: CallbackQuery):
    await cb.answer()
    admin_wm = await get_admin_watermark(cb.from_user.id) or {"positions": ["start", "end"]}
    current  = set(admin_wm.get("positions", ["start", "end"]))
    all_opts = {"start", "middle", "end", "hourly"}
    sel_all_label = "❌ Deselect All" if all_opts.issubset(current) else "✅ Select All"

    def mk(name, label=None):
        mark  = "✅" if name in current else "❌"
        shown = label or name.capitalize()
        return InlineKeyboardButton(f"{mark} {shown}", callback_data=f"admin_togglepos_{name}")

    await cb.message.edit_text(
        "📍 Toggle positions for your watermark:",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton(sel_all_label, callback_data="admin_positions_select_all")],
            [mk("start", "Start (+3 min)"), mk("middle", "Middle")],
            [mk("end",   "End (-7 min)"),   mk("hourly", "Hourly (60 min)")],
            [InlineKeyboardButton("🔢 Custom Seconds", callback_data="admin_pos_custom_prompt")],
            [InlineKeyboardButton("➡️ Done",  callback_data="admin_pos_done"),
             InlineKeyboardButton("⬅️ Back", callback_data="admin_watermark_settings")],
        ])
    )

@bot.on_callback_query(filters.regex("^admin_positions_select_all$") & admin_filter)
async def admin_positions_select_all_cb(client, cb: CallbackQuery):
    await cb.answer()
    admin_wm  = await get_admin_watermark(cb.from_user.id) or {"positions": ["start", "end"], "custom_seconds": 0}
    current   = set(admin_wm.get("positions", ["start", "end"]))
    all_set   = {"start", "middle", "end", "hourly"}
    new_pos   = ["start"] if all_set.issubset(current) else list(all_set)
    await update_admin_watermark_positions(cb.from_user.id, new_pos, admin_wm.get("custom_seconds", 0))
    await admin_wm_positions_cb(client, cb)

@bot.on_callback_query(filters.regex(r"^admin_togglepos_(start|middle|end|hourly)$") & admin_filter)
async def admin_togglepos_cb(client, cb: CallbackQuery):
    await cb.answer()
    pos      = cb.data.split("_")[-1]
    admin_wm = await get_admin_watermark(cb.from_user.id) or {"positions": ["start", "end"], "custom_seconds": 0}
    positions = set(admin_wm.get("positions", ["start", "end"]))
    positions.discard(pos) if pos in positions else positions.add(pos)
    if not positions:
        positions = {"start"}
    await update_admin_watermark_positions(cb.from_user.id, list(positions), admin_wm.get("custom_seconds", 0))
    await admin_wm_positions_cb(client, cb)

@bot.on_callback_query(filters.regex("^admin_pos_custom_prompt$") & admin_filter)
async def admin_pos_custom_prompt(client, cb: CallbackQuery):
    await cb.answer()
    await update_user_conversation(cb.message.chat.id, {"stage": "awaiting_admin_pos_custom"})
    await cb.message.edit_text(
        "🔢 Send custom time in seconds (e.g. 1800 = 30 min).",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="admin_watermark_settings")]])
    )

@bot.on_callback_query(filters.regex("^admin_pos_done$") & admin_filter)
async def admin_pos_done_cb(client, cb: CallbackQuery):
    await cb.answer()
    await admin_watermark_settings_cb(client, cb)

@bot.on_callback_query(filters.regex("^admin_wm_fade$") & admin_filter)
async def admin_wm_fade_cb(client, cb: CallbackQuery):
    await cb.answer()
    await cb.message.edit_text(
        "🌊 Choose your watermark fade-in/out duration:",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("0s (Off)", callback_data="admin_wm_fade_0.0"),
             InlineKeyboardButton("0.5s",     callback_data="admin_wm_fade_0.5"),
             InlineKeyboardButton("1s",       callback_data="admin_wm_fade_1.0")],
            [InlineKeyboardButton("2s",       callback_data="admin_wm_fade_2.0"),
             InlineKeyboardButton("3s",       callback_data="admin_wm_fade_3.0")],
            [InlineKeyboardButton("⬅️ Back",  callback_data="admin_watermark_settings")],
        ])
    )

@bot.on_callback_query(filters.regex(r"^admin_wm_fade_(\d+\.\d+)$") & admin_filter)
async def admin_wm_fade_save(client, cb: CallbackQuery):
    fd = float(re.search(r"(\d+\.\d+)$", cb.data).group(1))
    await update_admin_watermark_extra(cb.from_user.id, fade_duration=fd)
    await cb.answer(f"Fade set to {fd}s.")
    await admin_watermark_settings_cb(client, cb)

@bot.on_callback_query(filters.regex("^admin_wm_toggle_silence$") & admin_filter)
async def admin_wm_toggle_silence_cb(client, cb: CallbackQuery):
    admin_wm = await get_admin_watermark(cb.from_user.id) or {}
    new_val  = not admin_wm.get("use_silence_detection", False)
    await update_admin_watermark_extra(cb.from_user.id, use_silence_detection=new_val)
    await cb.answer(f"Silence Detection: {'On' if new_val else 'Off'}")
    await admin_watermark_settings_cb(client, cb)

@bot.on_callback_query(filters.regex("^admin_wm_toggle_normalize$") & admin_filter)
async def admin_wm_toggle_normalize_cb(client, cb: CallbackQuery):
    admin_wm = await get_admin_watermark(cb.from_user.id) or {}
    new_val  = not admin_wm.get("normalize", True)
    await update_admin_watermark_extra(cb.from_user.id, normalize=new_val)
    await cb.answer(f"Normalize: {'On' if new_val else 'Off'}")
    await admin_watermark_settings_cb(client, cb)

# ---------------------------------------------------------------------------
# ADMIN MANAGEMENT  (owner only)
# ---------------------------------------------------------------------------

@bot.on_callback_query(filters.regex("^admin_menu$") & filters.user(Config.OWNER_ID))
async def admin_menu_cb(client, cb: CallbackQuery):
    await cb.answer()
    await cb.message.edit_text(
        "**👨‍💼 Admin Management**",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("➕ Add Admin",             callback_data="admin_add")],
            [InlineKeyboardButton("➖ Remove Admin",          callback_data="admin_remove_list")],
            [InlineKeyboardButton("📋 List Admins & Stats",   callback_data="admin_list_stats")],
            [InlineKeyboardButton("⚙️ Set Admin Limits",     callback_data="admin_set_limits_list")],
            [InlineKeyboardButton("⬅️ Back",                 callback_data="main_menu")],
        ])
    )

@bot.on_callback_query(filters.regex("^admin_add$") & filters.user(Config.OWNER_ID))
async def admin_add_start_cb(client, cb: CallbackQuery):
    await cb.answer()
    await update_user_conversation(cb.message.chat.id, {"stage": "awaiting_admin_id"})
    await cb.message.edit_text(
        "➕ Send the user ID to add as admin.",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="admin_menu")]])
    )

@bot.on_callback_query(filters.regex("^admin_remove_list$") & filters.user(Config.OWNER_ID))
async def admin_remove_list_cb(client, cb: CallbackQuery):
    await cb.answer()
    admins = await get_admin_list()
    if not admins:
        return await cb.message.edit_text(
            "No admins found.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")]])
        )
    buttons = []
    text    = "**➖ Remove Admin**\n\n"
    for admin_id in admins:
        try:
            user = await client.get_users(admin_id)
            name = user.first_name or f"User {admin_id}"
        except Exception:
            name = f"User {admin_id}"
        text += f"▪️ {name} (`{admin_id}`)\n"
        buttons.append([InlineKeyboardButton(f"❌ {name}", callback_data=f"admin_remove_{admin_id}")])
    buttons.append([InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")])
    await cb.message.edit_text(text, reply_markup=InlineKeyboardMarkup(buttons))

@bot.on_callback_query(filters.regex(r"^admin_remove_(\d+)$") & filters.user(Config.OWNER_ID))
async def admin_remove_confirm_cb(client, cb: CallbackQuery):
    user_id = int(re.search(r"(\d+)$", cb.data).group(1))
    await remove_admin(user_id)
    await cb.answer(f"Admin {user_id} removed.", show_alert=True)
    await admin_remove_list_cb(client, cb)

@bot.on_callback_query(filters.regex("^admin_list_stats$") & filters.user(Config.OWNER_ID))
async def admin_list_stats_cb(client, cb: CallbackQuery):
    await cb.answer()
    admins = await get_admin_list()
    if not admins:
        return await cb.message.edit_text(
            "No admins found.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")]])
        )
    text = "**📋 Admin Stats**\n\n"
    for admin_id in admins[:20]:  # cap at 20 to avoid message overflow
        try:
            user   = await client.get_users(admin_id)
            name   = user.first_name or f"User {admin_id}"
        except Exception:
            name   = f"User {admin_id}"
        stats  = await get_admin_stats(admin_id)
        limits = await get_admin_limits(admin_id)
        text  += (
            f"👤 **{name}** (`{admin_id}`)\n"
            f"   Jobs total/today: `{stats['total_jobs']} / {stats['today_jobs']}`\n"
            f"   Data: `{human_size(stats['total_bytes'])}`\n"
            f"   Limit/day: `{'∞' if not limits['max_jobs_per_day'] else limits['max_jobs_per_day']}`  "
            f"   Max size: `{'∞' if not limits['max_file_size_mb'] else str(limits['max_file_size_mb']) + ' MB'}`\n\n"
        )
    await cb.message.edit_text(
        text,
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")]])
    )

@bot.on_callback_query(filters.regex("^admin_set_limits_list$") & filters.user(Config.OWNER_ID))
async def admin_set_limits_list_cb(client, cb: CallbackQuery):
    await cb.answer()
    admins = await get_admin_list()
    if not admins:
        return await cb.message.edit_text(
            "No admins found.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")]])
        )
    buttons = []
    for admin_id in admins:
        try:
            user = await client.get_users(admin_id)
            name = user.first_name or f"User {admin_id}"
        except Exception:
            name = f"User {admin_id}"
        buttons.append([InlineKeyboardButton(f"⚙️ {name}", callback_data=f"admin_limits_{admin_id}")])
    buttons.append([InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")])
    await cb.message.edit_text("Choose an admin to set limits:", reply_markup=InlineKeyboardMarkup(buttons))

@bot.on_callback_query(filters.regex(r"^admin_limits_(\d+)$") & filters.user(Config.OWNER_ID))
async def admin_limits_cb(client, cb: CallbackQuery):
    await cb.answer()
    target_id = int(re.search(r"(\d+)$", cb.data).group(1))
    limits    = await get_admin_limits(target_id)
    await update_user_conversation(cb.message.chat.id, {
        "stage":            "awaiting_admin_limits",
        "limits_target_id": target_id,
    })
    await cb.message.edit_text(
        f"**⚙️ Limits for admin `{target_id}`**\n\n"
        f"Current: max_jobs_per_day=`{limits['max_jobs_per_day'] or '∞'}`, "
        f"max_file_size_mb=`{limits['max_file_size_mb'] or '∞'}`\n\n"
        f"Send two numbers separated by space:\n`<max_jobs_per_day> <max_file_size_mb>`\n"
        f"Use `0` for unlimited. Example: `10 500`",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="admin_set_limits_list")]])
    )

# ---------------------------------------------------------------------------
# CONVERSION FLOW START
# ---------------------------------------------------------------------------

@bot.on_callback_query(filters.regex("^convert_audio_start$") & admin_filter)
async def convert_audio_start_cb(client, cb: CallbackQuery):
    await cb.answer()
    job_id  = str(uuid.uuid4())
    job_dir = os.path.join(Config.DOWNLOAD_DIR, job_id)
    os.makedirs(job_dir, exist_ok=True)
    await update_user_conversation(cb.message.chat.id, {
        "stage":   "awaiting_media_file",
        "job_id":  job_id,
        "job_dir": job_dir,
    })
    await cb.message.edit_text(
        "🎵 **Send File**\n\nSend the audio/video file you want to process.",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="cancel_conv")]])
    )

# ---------------------------------------------------------------------------
# GENERIC MESSAGE HANDLER ROUTER
# ---------------------------------------------------------------------------

@bot.on_message(
    filters.private
    & (filters.audio | filters.video | filters.document | filters.text | filters.voice)
    & admin_filter
)
async def message_handler_router(client, message: Message):
    chat_id = message.chat.id
    user_id = message.from_user.id

    # Always keep user registry current
    await register_user(user_id,
                        username=getattr(message.from_user, "username", None),
                        first_name=getattr(message.from_user, "first_name", None))

    LOGGER.info(
        f"Msg from {user_id}: "
        f"audio={bool(message.audio)} video={bool(message.video)} "
        f"doc={bool(message.document)} text={bool(message.text)} voice={bool(getattr(message,'voice',None))}"
    )

    try:
        conv = await get_user_conversation(chat_id)
    except Exception as e:
        LOGGER.error(f"get_user_conversation error for {chat_id}: {e}", exc_info=True)
        conv = None

    # ---- helper: is doc a media file? ----
    async def doc_is_media(doc) -> bool:
        if not doc:
            return False
        mime = getattr(doc, "mime_type", "") or ""
        name = getattr(doc, "file_name", "") or ""
        if mime.startswith("audio") or mime.startswith("video"):
            return True
        ext = safe_ext_from_filename(name)
        return ext in {"mka", "mkv", "mp3", "m4a", "aac", "opus", "flac",
                       "wav", "ogg", "mp4", "mov", "webm", "m2ts", "ts"}

    # Detect media
    sent_media = None
    if   message.audio:                        sent_media = ("audio",    message.audio)
    elif message.video:                        sent_media = ("video",    message.video)
    elif message.document:                     sent_media = ("document", message.document)
    elif getattr(message, "voice", None):      sent_media = ("audio",    message.voice)

    # Auto-create session if media arrives without an active session
    if not conv and sent_media:
        if sent_media[0] == "document" and not await doc_is_media(sent_media[1]):
            await message.reply_text(
                "🔎 No active conversion session.\n\n"
                "Use **Audio Tools → Convert Audio** or press ➕ Send Audio/Video first.",
                quote=True
            )
            return
        job_id  = str(uuid.uuid4())
        job_dir = os.path.join(Config.DOWNLOAD_DIR, job_id)
        os.makedirs(job_dir, exist_ok=True)
        await update_user_conversation(chat_id, {
            "stage":   "awaiting_media_file",
            "job_id":  job_id,
            "job_dir": job_dir,
        })
        conv = await get_user_conversation(chat_id)
        LOGGER.info(f"Auto-created session for chat {chat_id}, job {job_id}")

    if not conv:
        await message.reply_text(
            "🔎 No active session.\n\nUse **Audio Tools → Convert Audio** to start.",
            quote=True
        )
        return

    stage = conv.get("stage", "")
    LOGGER.debug(f"chat={chat_id} stage={stage}")

    # ── Owner watermark upload ──────────────────────────────────────────────
    if stage == "awaiting_owner_wm":
        if user_id != Config.OWNER_ID:
            await message.reply_text("Permission denied.")
            return
        doc = message.audio or getattr(message, "voice", None) or message.document
        if not doc or not await doc_is_media(doc):
            await message.reply_text("Please send an audio file.")
            return
        await set_owner_watermark_file(doc.file_id)
        await update_user_conversation(chat_id, None)
        await message.reply_text(
            "✅ Owner watermark saved.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="owner_settings_menu")]])
        )
        return

    # ── Admin watermark upload ──────────────────────────────────────────────
    if stage == "awaiting_admin_wm":
        doc = message.audio or getattr(message, "voice", None) or message.document
        if not doc or not await doc_is_media(doc):
            await message.reply_text("Please send an audio file.")
            return
        await set_admin_watermark(user_id, doc.file_id)
        await update_user_conversation(chat_id, None)
        await message.reply_text(
            "✅ Your watermark saved.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_watermark_settings")]])
        )
        return

    # ── Owner custom position seconds ──────────────────────────────────────
    if stage == "awaiting_owner_pos_custom":
        if user_id != Config.OWNER_ID:
            await message.reply_text("Permission denied.")
            return
        try:
            secs     = int(message.text.strip())
            owner_wm = await get_owner_watermark()
            await set_owner_watermark_positions(owner_wm.get("positions", ["start", "end"]), custom_seconds=secs)
            await update_user_conversation(chat_id, None)
            await message.reply_text(
                f"✅ Owner custom seconds set to {secs}s.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="owner_settings_menu")]])
            )
        except ValueError:
            await message.reply_text("Send a valid integer.")
        return

    # ── Admin custom position seconds ──────────────────────────────────────
    if stage == "awaiting_admin_pos_custom":
        try:
            secs     = int(message.text.strip())
            admin_wm = await get_admin_watermark(user_id) or {"positions": ["start", "end"]}
            await update_admin_watermark_positions(user_id, admin_wm.get("positions", ["start", "end"]), custom_seconds=secs)
            await update_user_conversation(chat_id, None)
            await message.reply_text(
                f"✅ Custom seconds set to {secs}s.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_watermark_settings")]])
            )
        except ValueError:
            await message.reply_text("Send a valid integer.")
        return

    # ── Per-job watermark custom seconds ───────────────────────────────────
    if stage == "awaiting_wm_custom_seconds":
        try:
            secs      = int(message.text.strip())
            positions = list(set(conv.get("wm_positions", [])) | {"custom"})
            await update_user_conversation(chat_id, {
                "wm_positions":    positions,
                "wm_custom_seconds": secs,
                "stage":           "awaiting_wm_position_choice",
            })
            conv = await get_user_conversation(chat_id) or {}
            text, kb = build_wm_positions_text_and_keyboard(conv)
            await message.reply_text(text, reply_markup=kb)
        except ValueError:
            await message.reply_text("Send a valid integer.")
        return

    # ── Media file ─────────────────────────────────────────────────────────
    if stage == "awaiting_media_file":
        if not (message.audio or message.video or message.document or getattr(message, "voice", None)):
            await message.reply_text("Send a valid media file.")
            return
        await handle_media_file(client, message, conv)
        return

    # ── Add admin (owner only) ─────────────────────────────────────────────
    if stage == "awaiting_admin_id" and message.text:
        if user_id != Config.OWNER_ID:
            return
        try:
            new_id = int(message.text.strip())
            await add_admin(new_id)
            await update_user_conversation(chat_id, None)
            await message.reply_text(
                f"✅ Admin `{new_id}` added.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")]])
            )
        except ValueError:
            await message.reply_text("Send a valid integer user ID.")
        return

    # ── Admin limits (owner only) ──────────────────────────────────────────
    if stage == "awaiting_admin_limits" and message.text:
        if user_id != Config.OWNER_ID:
            return
        target_id = conv.get("limits_target_id")
        try:
            parts = message.text.strip().split()
            if len(parts) != 2:
                raise ValueError
            max_jobs, max_size = int(parts[0]), int(parts[1])
            await set_admin_limits(target_id, max_jobs_per_day=max_jobs, max_file_size_mb=max_size)
            await update_user_conversation(chat_id, None)
            await message.reply_text(
                f"✅ Limits updated for `{target_id}`:\n"
                f"max_jobs_per_day=`{max_jobs or '∞'}`, max_file_size_mb=`{max_size or '∞'}`",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")]])
            )
        except (ValueError, TypeError):
            await message.reply_text("Send two integers: `<max_jobs_per_day> <max_file_size_mb>`. Use 0 for unlimited.")
        return

    # ── Default metadata fields (owner only) ──────────────────────────────
    for field in ("title", "artist", "album", "language", "comment"):
        if stage == f"awaiting_meta_{field}":
            if user_id != Config.OWNER_ID:
                return
            val = message.text.strip() if message.text else ""
            if val == "-":
                val = ""
            await set_default_metadata({field: val})
            await update_user_conversation(chat_id, None)
            await message.reply_text(
                f"✅ {field.capitalize()} set to: `{val or '(cleared)'}`",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="owner_metadata_menu")]])
            )
            return

# ---------------------------------------------------------------------------
# HANDLE MEDIA FILE  (download + probe + track selection)
# ---------------------------------------------------------------------------

async def handle_media_file(client, message: Message, conv: dict):
    media   = message.audio or message.video or message.document or getattr(message, "voice", None)
    chat_id = message.chat.id
    user_id = message.from_user.id
    job_dir = conv.get("job_dir")

    if not job_dir or not os.path.isdir(job_dir):
        await message.reply_text("Error: Job directory not found. Please start over.", quote=True)
        asyncio.create_task(clear_conversation_after_delay(chat_id, delay=5))
        return

    # ── Limit checks ────────────────────────────────────────────────────────
    if user_id != Config.OWNER_ID:
        limits = await get_admin_limits(user_id)

        # Jobs-per-day limit
        if limits["max_jobs_per_day"] > 0:
            today_count = await get_admin_jobs_today(user_id)
            if today_count >= limits["max_jobs_per_day"]:
                await message.reply_text(
                    f"⛔ Daily job limit reached (`{limits['max_jobs_per_day']}` jobs/day). "
                    f"Please wait until tomorrow.",
                    quote=True
                )
                return

        # File size limit
        file_size_bytes = getattr(media, "file_size", 0) or 0
        if limits["max_file_size_mb"] > 0:
            max_bytes = limits["max_file_size_mb"] * 1024 * 1024
            if file_size_bytes > max_bytes:
                await message.reply_text(
                    f"⛔ File too large. Your limit is `{limits['max_file_size_mb']} MB`.",
                    quote=True
                )
                return

    # ── Determine original filename ─────────────────────────────────────────
    original_filename = (
        getattr(message.document, "file_name", None)
        or getattr(message.audio,    "file_name", None)
        or getattr(message.video,    "file_name", None)
        or getattr(message, "caption", None)
        or f"file_{uuid.uuid4()}"
    )
    original_filename = sanitize_filename(original_filename)

    ext = safe_ext_from_filename(original_filename)
    if not ext:
        if message.video:
            ext = "mkv"
        elif message.audio or getattr(message, "voice", None):
            ext = "m4a"
        else:
            ext = "dat"
        original_filename += f".{ext}"

    input_target_name = f"input_{uuid.uuid4()}_{os.path.basename(original_filename)}"
    input_file_path   = os.path.join(job_dir, input_target_name)

    status_msg = await message.reply_text("📥 **Downloading… 0%**", quote=True)

    # ── Download ────────────────────────────────────────────────────────────
    downloaded = None
    try:
        downloaded = await message.download(
            file_name=input_file_path,
            progress=progress_callback,
            progress_args=(status_msg, "Downloading"),
        )
        if downloaded:
            input_file_path = downloaded
    except Exception as e:
        if "Download Cancelled by User" in str(e):
            LOGGER.info(f"Download cancelled for chat {chat_id}")
            try:
                await status_msg.edit_text("❌ **Download Cancelled**")
            except Exception:
                pass
            asyncio.create_task(clear_conversation_after_delay(chat_id))
            try:
                shutil.rmtree(job_dir)
            except Exception:
                pass
            return
        LOGGER.error(f"Download failed: {e}", exc_info=True)
        try:
            await status_msg.edit_text(f"❌ **Download Failed**\n\n`{e}`")
        except Exception:
            pass
        asyncio.create_task(clear_conversation_after_delay(chat_id))
        return
    finally:
        DOWNLOAD_PROGRESS.pop(getattr(status_msg, "id", None), None)

    # ── Probe ────────────────────────────────────────────────────────────────
    await status_msg.edit_text("🔬 **Analysing file…**")
    all_streams   = await probe_media(input_file_path)
    audio_streams = [s for s in all_streams if s.get("codec_type") == "audio"]
    video_streams = [s for s in all_streams if s.get("codec_type") == "video"]

    if not audio_streams:
        await status_msg.edit_text(
            "❌ No audio streams found.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="cancel_conv")]])
        )
        return

    is_audio_only = not bool(video_streams)

    await update_user_conversation(chat_id, {
        "input_file_path":  input_file_path,
        "original_filename": original_filename,
        "audio_streams":    audio_streams,
        "is_audio_only":    is_audio_only,
        "job_dir":          job_dir,
        "stage":            "awaiting_track_selection",
    })

    # ── Show track selection ────────────────────────────────────────────────
    buttons = []
    text    = "**🔬 Analysis Complete!**\n\nAudio Tracks:\n\n"
    for stream in audio_streams:
        idx     = stream.get("index")
        codec   = stream.get("codec_name", "unknown").upper()
        lang    = (stream.get("tags") or {}).get("language", "und").upper()
        ch      = stream.get("channels", "?")
        layout  = stream.get("channel_layout", "?")
        label   = f"Track {idx}: {codec} ({layout} {ch}ch) [{lang}]"
        text   += f"▪️ {label}\n"
        buttons.append([InlineKeyboardButton(label, callback_data=f"track_{idx}")])
    buttons.append([InlineKeyboardButton("❌ Cancel", callback_data="cancel_conv")])
    await status_msg.edit_text(text, reply_markup=InlineKeyboardMarkup(buttons))

# ---------------------------------------------------------------------------
# TRACK SELECTION
# ---------------------------------------------------------------------------

@bot.on_callback_query(filters.regex(r"^track_(\d+)$") & admin_filter)
async def track_selection_cb(client, cb: CallbackQuery):
    await cb.answer()
    chat_id = cb.message.chat.id
    conv    = await get_user_conversation(chat_id)
    if not conv or conv.get("stage") != "awaiting_track_selection":
        return await cb.answer("Session expired. Start over.", show_alert=True)

    track_index = int(re.search(r"(\d+)$", cb.data).group(1))
    stream_obj  = next((s for s in conv.get("audio_streams", []) if s.get("index") == track_index), None)
    if not stream_obj:
        return await cb.answer("Track not found.", show_alert=True)

    await update_user_conversation(chat_id, {
        "selected_track_index": track_index,
        "selected_stream_obj":  stream_obj,
        "stage":                "awaiting_format_selection",
    })

    buttons = [
        [InlineKeyboardButton("🎵 AAC Stereo 192k",         callback_data="format_aac_stereo")],
        [InlineKeyboardButton("🎧 AAC 5.1 320k",            callback_data="format_aac_5_1")],
        [InlineKeyboardButton("🎵 MP3 Stereo 192k",         callback_data="format_mp3_stereo")],
    ]
    if stream_obj.get("codec_name") == "aac":
        buttons.insert(0, [InlineKeyboardButton("✨ Copy AAC Stream (Fastest)", callback_data="format_aac_copy")])
    buttons.extend([
        [InlineKeyboardButton("⬅️ Choose Different File", callback_data="convert_audio_start")],
        [InlineKeyboardButton("❌ Cancel",                 callback_data="cancel_conv")],
    ])
    await cb.message.edit_text(
        f"✅ Track {track_index} selected.\n\nChoose output format:",
        reply_markup=InlineKeyboardMarkup(buttons)
    )

# ---------------------------------------------------------------------------
# FORMAT SELECTION
# ---------------------------------------------------------------------------

@bot.on_callback_query(filters.regex(r"^format_(.+)$") & admin_filter)
async def format_selection_cb(client, cb: CallbackQuery):
    await cb.answer()
    chat_id    = cb.message.chat.id
    conv       = await get_user_conversation(chat_id)
    if not conv or conv.get("stage") != "awaiting_format_selection":
        return await cb.answer("Session expired.", show_alert=True)

    raw_choice = cb.data[len("format_"):]
    fmt_data   = {"codec": "aac", "channels": 2, "bitrate": "192k"}

    if "aac_5_1" in raw_choice:
        fmt_data = {"codec": "aac",         "channels": 6, "bitrate": "320k"}
    elif "mp3" in raw_choice:
        fmt_data = {"codec": "libmp3lame",  "channels": 2, "bitrate": "192k"}
    elif "copy" in raw_choice:
        fmt_data = {"codec": "copy",        "channels": "copy", "bitrate": "copy"}

    await update_user_conversation(chat_id, {"format": fmt_data})

    owner_wm = await get_owner_watermark()
    admin_wm = await get_admin_watermark(cb.from_user.id)
    wm_available = bool(owner_wm.get("file_id")) or bool(admin_wm and admin_wm.get("file_id"))

    if fmt_data["codec"] == "copy":
        # Copy mode: watermark not applicable; skip watermark step
        await update_user_conversation(chat_id, {"watermark": False})
        await ask_for_output_type(cb, conv)
    elif wm_available:
        await update_user_conversation(chat_id, {"stage": "awaiting_watermark_selection"})
        buttons = []
        if owner_wm.get("file_id"):
            buttons.append([InlineKeyboardButton("💧 Use Owner Watermark", callback_data="watermark_use_owner")])
        if admin_wm and admin_wm.get("file_id"):
            buttons.append([InlineKeyboardButton("🧑‍💼 Use My Watermark",   callback_data="watermark_use_admin")])
        buttons.append([InlineKeyboardButton("❌ No Watermark",            callback_data="watermark_use_no")])
        buttons.append([InlineKeyboardButton("❌ Cancel",                  callback_data="cancel_conv")])
        await cb.message.edit_text(
            "✅ Format selected.\n\nDo you want to mix a watermark?",
            reply_markup=InlineKeyboardMarkup(buttons)
        )
    else:
        await update_user_conversation(chat_id, {"watermark": False})
        await ask_for_output_type(cb, conv)

# ---------------------------------------------------------------------------
# WATERMARK SELECTION
# ---------------------------------------------------------------------------

@bot.on_callback_query(filters.regex(r"^watermark_use_(owner|admin|no)$") & admin_filter)
async def watermark_selection_cb(client, cb: CallbackQuery):
    await cb.answer()
    choice  = cb.data.split("_")[-1]
    chat_id = cb.message.chat.id
    conv    = await get_user_conversation(chat_id)
    if not conv or conv.get("stage") != "awaiting_watermark_selection":
        return await cb.answer("Session expired.", show_alert=True)

    if choice == "owner":
        owner_wm = await get_owner_watermark()
        if not owner_wm.get("file_id"):
            return await cb.answer("Owner watermark not set.", show_alert=True)
        await update_user_conversation(chat_id, {
            "watermark":        True,
            "watermark_choice": "owner",
            "wm_positions":     owner_wm.get("positions", ["start", "end"]),
            "wm_custom_seconds": owner_wm.get("custom_seconds", 0),
            "stage":            "awaiting_wm_volume_choice",
        })
        await ask_wm_volume(cb, chat_id)

    elif choice == "admin":
        admin_wm = await get_admin_watermark(cb.from_user.id)
        if not admin_wm or not admin_wm.get("file_id"):
            return await cb.answer("Your watermark is not set.", show_alert=True)
        await update_user_conversation(chat_id, {
            "watermark":        True,
            "watermark_choice": "admin",
            "wm_positions":     admin_wm.get("positions", ["start", "end"]),
            "wm_custom_seconds": admin_wm.get("custom_seconds", 0),
            "stage":            "awaiting_wm_volume_choice",
        })
        await ask_wm_volume(cb, chat_id)

    else:
        await update_user_conversation(chat_id, {"watermark": False})
        await ask_for_output_type(cb, conv)

# ── Per-job volume override ──────────────────────────────────────────────────

async def ask_wm_volume(cb: CallbackQuery, chat_id: int):
    """Ask the user if they want to override watermark volume for this job."""
    await cb.message.edit_text(
        "🔊 **Watermark Volume for this job:**\n\nChoose or keep default:",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("10%",      callback_data="job_wm_vol_0.10"),
             InlineKeyboardButton("20%",      callback_data="job_wm_vol_0.20"),
             InlineKeyboardButton("30%",      callback_data="job_wm_vol_0.30")],
            [InlineKeyboardButton("50%",      callback_data="job_wm_vol_0.50"),
             InlineKeyboardButton("80%",      callback_data="job_wm_vol_0.80"),
             InlineKeyboardButton("100%",     callback_data="job_wm_vol_1.00")],
            [InlineKeyboardButton("🔄 Keep Default Volume", callback_data="job_wm_vol_default")],
        ])
    )

@bot.on_callback_query(filters.regex(r"^job_wm_vol_(\d+\.\d+|default)$") & admin_filter)
async def job_wm_volume_cb(client, cb: CallbackQuery):
    await cb.answer()
    chat_id = cb.message.chat.id
    conv    = await get_user_conversation(chat_id)
    if not conv:
        return await cb.answer("Session expired.", show_alert=True)

    raw = cb.data[len("job_wm_vol_"):]
    if raw == "default":
        job_wm_volume = None   # None → use saved default at build time
    else:
        job_wm_volume = float(raw)

    await update_user_conversation(chat_id, {
        "job_wm_volume": job_wm_volume,
        "stage":         "awaiting_wm_position_choice",
    })
    conv = await get_user_conversation(chat_id) or {}
    text, kb = build_wm_positions_text_and_keyboard(conv)
    await cb.message.edit_text(text, reply_markup=kb)

# ---------------------------------------------------------------------------
# WM POSITION TOGGLE CALLBACKS  (per-job)
# ---------------------------------------------------------------------------

def build_wm_positions_text_and_keyboard(conv: dict):
    positions_ui  = set(conv.get("wm_positions", []))
    custom_secs   = int(conv.get("wm_custom_seconds", 0) or 0)
    all_options   = {"start", "middle", "end", "hourly"}
    sel_all_label = "❌ Deselect All" if all_options.issubset(positions_ui) else "✅ Select All"

    base_labels = [p for p in ["start", "middle", "end", "hourly"] if p in positions_ui]
    selected_text = ", ".join(base_labels)
    if "custom" in positions_ui and custom_secs:
        extra = f"custom({custom_secs}s)"
        selected_text = (selected_text + " + " + extra) if selected_text else extra
    if not selected_text:
        selected_text = "start"

    def mk(name, label):
        mark = "✅" if name in positions_ui else "❌"
        return InlineKeyboardButton(f"{mark} {label}", callback_data=f"wm_toggle_{name}")

    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton(sel_all_label, callback_data="wm_toggle_select_all")],
        [mk("start",  "Start (+3 min)"), mk("middle", "Middle")],
        [mk("end",    "End (-7 min)"),   mk("hourly", "Hourly (60 min)")],
        [InlineKeyboardButton("🔢 Custom Seconds", callback_data="wm_pos_custom_prompt")],
        [InlineKeyboardButton("➡️ Continue",       callback_data="wm_pos_done")],
    ])
    return f"📍 **Watermark Positions**\nSelected: `{selected_text}`\n\nToggle then press Continue.", kb

@bot.on_callback_query(filters.regex("^wm_toggle_select_all$") & admin_filter)
async def wm_toggle_select_all_cb(client, cb: CallbackQuery):
    await cb.answer()
    chat_id = cb.message.chat.id
    conv    = await get_user_conversation(chat_id)
    if not conv:
        return await cb.answer("Session expired.", show_alert=True)
    current     = set(conv.get("wm_positions", []))
    all_set     = {"start", "middle", "end", "hourly"}
    new_pos     = ["start"] if all_set.issubset(current) else list(all_set)
    await update_user_conversation(chat_id, {"wm_positions": new_pos})
    conv["wm_positions"] = new_pos
    text, kb = build_wm_positions_text_and_keyboard(conv)
    await cb.message.edit_text(text, reply_markup=kb)

@bot.on_callback_query(filters.regex(r"^wm_toggle_(start|middle|end|hourly)$") & admin_filter)
async def wm_toggle_generic_cb(client, cb: CallbackQuery):
    await cb.answer()
    choice  = cb.data.split("_")[-1]
    chat_id = cb.message.chat.id
    conv    = await get_user_conversation(chat_id)
    if not conv:
        return await cb.answer("Session expired.", show_alert=True)
    current = set(conv.get("wm_positions", []))
    current.discard(choice) if choice in current else current.add(choice)
    if not current:
        current = {"start"}
    await update_user_conversation(chat_id, {"wm_positions": list(current)})
    conv["wm_positions"] = list(current)
    text, kb = build_wm_positions_text_and_keyboard(conv)
    await cb.message.edit_text(text, reply_markup=kb)

@bot.on_callback_query(filters.regex("^wm_pos_custom_prompt$") & admin_filter)
async def wm_pos_custom_prompt(client, cb: CallbackQuery):
    await cb.answer()
    await update_user_conversation(cb.message.chat.id, {"stage": "awaiting_wm_custom_seconds"})
    await cb.message.edit_text(
        "🔢 Send custom time in seconds (e.g. 1800 = 30 min).",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="cancel_conv")]])
    )

@bot.on_callback_query(filters.regex("^wm_pos_done$") & admin_filter)
async def wm_pos_done_cb(client, cb: CallbackQuery):
    await cb.answer()
    chat_id = cb.message.chat.id
    conv    = await get_user_conversation(chat_id)
    if not conv:
        return await cb.answer("Session expired.", show_alert=True)
    await ask_for_output_type(cb, conv)

# ---------------------------------------------------------------------------
# OUTPUT TYPE SELECTION
# ---------------------------------------------------------------------------

async def ask_for_output_type(cb: CallbackQuery, conv: dict):
    chat_id = cb.message.chat.id
    conv    = await get_user_conversation(chat_id) or conv
    if conv.get("is_audio_only"):
        await update_user_conversation(chat_id, {"output_type": "audio", "stage": "processing"})
        try:
            await cb.message.edit_text("✅ **Ready!**\n\nProcessing audio…")
        except Exception:
            pass
        await start_conversion_process(cb)
    else:
        await update_user_conversation(chat_id, {"stage": "awaiting_output_selection"})
        await cb.message.edit_text(
            "✅ Settings complete.\n\nChoose output type:",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🎵 Audio Only (.m4a/.mp3)", callback_data="output_audio"),
                 InlineKeyboardButton("🎬 Remux Video (.mkv)",     callback_data="output_remux")],
                [InlineKeyboardButton("📦 All (Audio + Video)",    callback_data="output_all")],
                [InlineKeyboardButton("❌ Cancel",                 callback_data="cancel_conv")],
            ])
        )

@bot.on_callback_query(filters.regex(r"^output_(audio|remux|all)$") & admin_filter)
async def output_selection_cb(client, cb: CallbackQuery):
    await cb.answer()
    chat_id     = cb.message.chat.id
    conv        = await get_user_conversation(chat_id)
    if not conv or conv.get("stage") not in (
        "awaiting_output_selection", "awaiting_wm_position_choice", "processing"
    ):
        return await cb.answer("Session expired.", show_alert=True)
    output_type = cb.data.split("_", 1)[1]
    await update_user_conversation(chat_id, {"output_type": output_type, "stage": "processing"})
    await cb.message.edit_text(f"✅ **Ready!**\n\nStarting conversion (output: {output_type})…")
    await start_conversion_process(cb)

# ---------------------------------------------------------------------------
# FFMPEG ARGS BUILDER
#
# Lip-sync / timing preservation strategy
# ────────────────────────────────────────
# The most common cause of lip-sync drift when audio is re-muxed into video:
#   1. AAC encoder priming / padding  → `initial_padding` metadata written to
#      the container differs between encoders. We use `-af aresample=async=1000`
#      + explicit `-ar 48000` so the sample-rate is always predictable.
#   2. amix `duration=first` clips output to the shortest input instead of the
#      main track. Fixed: we use `duration=longest` + `dropout_transition=0`.
#      The watermark is padded with `apad` so amix never shortens the main.
#   3. adelay introduces integer-ms rounding errors at 1000-ms resolution.
#      We convert delay to sample-accurate form: `adelay=<ms>:all=1`.
#   4. For the main audio track we pass it through `aresample=resampler=swr`
#      which guarantees SWR is used (consistent latency vs. soxr).
#   5. We always write `-af aresample=async=1000` on the main track when no
#      watermark is used, to keep the clock corrected.
#   6. `-avoid_negative_ts make_zero` prevents PTS < 0 issues in containers.
#   7. When remuxing we always copy video with `-vsync passthrough` to avoid
#      any frame-timing changes.
# ---------------------------------------------------------------------------

async def build_ffmpeg_args(
    conv: dict,
    input_file: str,
    watermark_local_file: str | None,
    override_format=None,
    override_output_type=None,
    silence_midpoints: list | None = None,
):
    track_index     = conv["selected_track_index"]
    stream_obj      = conv["selected_stream_obj"]
    fmt             = override_format if override_format else conv.get("format")
    use_watermark   = conv.get("watermark", False)
    watermark_choice = conv.get("watermark_choice", "owner")
    job_dir         = conv["job_dir"]
    wm_positions    = conv.get("wm_positions") or []
    wm_custom_secs  = int(conv.get("wm_custom_seconds", 0) or 0)
    output_type     = override_output_type if override_output_type else conv.get("output_type")
    job_wm_volume   = conv.get("job_wm_volume")   # None = use saved default

    # ── Output extension ───────────────────────────────────────────────────
    if output_type == "remux":
        output_ext = "mkv"
    elif fmt.get("codec") == "libmp3lame":
        output_ext = "mp3"
    else:
        output_ext = "m4a"

    original_filename = conv.get("original_filename") or f"output_{uuid.uuid4()}"
    base_name, _      = os.path.splitext(original_filename)
    suffix            = f"_{output_type}" if output_type == "remux" else ""
    final_output_file = os.path.join(job_dir, f"{base_name}{suffix}.{output_ext}")

    # ── Watermark metadata fetch ───────────────────────────────────────────
    wm_volume       = 0.2
    fade_duration   = 1.0
    use_silence_det = False
    do_normalize    = True

    if use_watermark:
        if watermark_choice == "owner":
            owner_wm       = await get_owner_watermark()
            wm_volume      = owner_wm.get("volume", 0.2)
            fade_duration  = owner_wm.get("fade_duration", 1.0)
            use_silence_det= owner_wm.get("use_silence_detection", False)
            do_normalize   = owner_wm.get("normalize", True)
        else:
            admin_wm = await get_admin_watermark(conv.get("user_id") or 0)
            if admin_wm:
                wm_volume      = admin_wm.get("volume", 0.2)
                fade_duration  = admin_wm.get("fade_duration", 1.0)
                use_silence_det= admin_wm.get("use_silence_detection", False)
                do_normalize   = admin_wm.get("normalize", True)

    # Per-job volume override
    if job_wm_volume is not None:
        wm_volume = job_wm_volume

    # Force re-encode if copy+watermark
    force_reencode = (fmt.get("codec") == "copy" and use_watermark)
    if force_reencode:
        fmt = {"codec": "aac", "channels": 2, "bitrate": "192k"}

    # ── Base args ─────────────────────────────────────────────────────────
    # -avoid_negative_ts make_zero  → prevents PTS < 0 in the container
    args = [
        "ffmpeg", "-y", "-hide_banner",
        "-avoid_negative_ts", "make_zero",
        "-i", input_file,
    ]

    filter_complex_parts = []
    audio_map_label      = f"0:{track_index}"

    # ── Watermark filter chain ─────────────────────────────────────────────
    if use_watermark and watermark_local_file:
        try:
            wm_dur = await get_media_duration(watermark_local_file)
        except Exception:
            wm_dur = 0.0

        try:
            input_duration = await get_media_duration(input_file)
        except Exception:
            input_duration = 0.0

        args.extend(["-i", watermark_local_file])

        WM_START_OFFSET = 180.0   # 3 min
        WM_END_OFFSET   = 420.0   # 7 min before end

        # Build raw position list
        raw_positions: list[float] = []

        if use_silence_det and silence_midpoints:
            # Place watermark at each silent gap midpoint
            for mp in silence_midpoints:
                if mp + wm_dur <= input_duration:
                    raw_positions.append(mp)
        else:
            for p in wm_positions:
                if p == "start":
                    t = min(WM_START_OFFSET, max(0.0, input_duration - wm_dur))
                    raw_positions.append(t)
                elif p == "middle":
                    raw_positions.append(max(0.0, (input_duration / 2.0) - (wm_dur / 2.0)))
                elif p == "end":
                    t = input_duration - (WM_END_OFFSET + wm_dur)
                    if t < 0:
                        t = max(0.0, input_duration - wm_dur)
                    raw_positions.append(max(0.0, t))
                elif p == "custom":
                    raw_positions.append(max(0.0, float(wm_custom_secs)))
                elif p == "hourly":
                    t = 3600.0
                    upper = max(0.0, input_duration - wm_dur)
                    while t <= upper:
                        raw_positions.append(t)
                        t += 3600.0

        # Deduplicate, sort, convert to integer seconds
        unique_positions = sorted(set(int(max(0, p)) for p in raw_positions))

        n_wms = len(unique_positions)
        if n_wms > 0:
            # Build afade chain for the watermark (if fade_duration > 0)
            def wm_fade_chain(idx: int) -> str:
                """Return the audio filter chain applied to each watermark copy."""
                parts = []
                if fade_duration > 0:
                    fd_str = f"{fade_duration:.3f}"
                    # Pad watermark to full duration first so afade works on the right segment
                    parts.append(f"afade=t=in:st=0:d={fd_str}")
                    parts.append(f"afade=t=out:st={max(0.0, wm_dur - fade_duration):.3f}:d={fd_str}")
                if do_normalize:
                    # loudnorm two-pass is best but requires two ffmpeg runs;
                    # for single-pass we use dynaudnorm which approximates EBU R128
                    parts.append("dynaudnorm=p=0.9:m=100:s=5")
                parts.append(f"volume={wm_volume:.4f}")
                return ",".join(parts) if parts else "anull"

            # Split watermark into n copies
            filter_complex_parts.append(
                f"[1:a]asplit={n_wms}" + "".join(f"[wm{i}]" for i in range(n_wms))
            )

            # Main track: resample for clock consistency
            main_chain = "aresample=resampler=swr,aresample=async=1000"
            if do_normalize:
                main_chain += ",dynaudnorm=p=0.9:m=100:s=5"
            filter_complex_parts.append(f"[0:{track_index}]{main_chain}[main]")

            wm_out_labels = []
            for i, pos_sec in enumerate(unique_positions):
                delay_ms   = pos_sec * 1000          # exact integer ms → sample-accurate
                fade_chain = wm_fade_chain(i)
                # apad ensures amix never shortens output; adelay all=1 applies to all channels
                filter_complex_parts.append(
                    f"[wm{i}]{fade_chain},adelay={delay_ms}:all=1,apad[wm{i}_out]"
                )
                wm_out_labels.append(f"[wm{i}_out]")

            # amix: duration=longest so main track length is preserved exactly
            all_inputs  = "[main]" + "".join(wm_out_labels)
            amix_inputs = 1 + n_wms
            filter_complex_parts.append(
                f"{all_inputs}amix=inputs={amix_inputs}:duration=longest:dropout_transition=0[aud_out]"
            )
            audio_map_label = "[aud_out]"
        else:
            # No valid positions; still resample main for consistency
            filter_complex_parts.append(
                f"[0:{track_index}]aresample=resampler=swr,aresample=async=1000[aud_out]"
            )
            audio_map_label = "[aud_out]"

    else:
        # No watermark: single aresample pass for clock correction
        filter_complex_parts.append(
            f"[0:{track_index}]aresample=resampler=swr,aresample=async=1000[aud_out]"
        )
        audio_map_label = "[aud_out]"

    # Apply filter_complex
    if filter_complex_parts:
        args.extend(["-filter_complex", ";".join(filter_complex_parts)])

    # Map audio
    args.extend(["-map", audio_map_label])

    # Codec settings
    if fmt.get("codec") == "copy":
        args.extend(["-c:a", "copy"])
    else:
        ar = 48000
        args.extend([
            "-c:a", fmt["codec"],
            "-b:a", fmt["bitrate"],
            "-ac",  str(fmt["channels"]),
            "-ar",  str(ar),
        ])

    # ── Default metadata injection ─────────────────────────────────────────
    meta = await get_default_metadata()
    for key, val in meta.items():
        if val:
            args.extend([f"-metadata", f"{key}={val}"])

    # Track language metadata
    lang = (stream_obj or {}).get("tags", {}).get("language", "und")
    if meta.get("language"):
        lang = meta["language"]   # owner default overrides
    args.extend(["-metadata:s:a:0", f"language={lang}"])

    # ── Remux-specific: copy video + subtitles ─────────────────────────────
    if output_type == "remux":
        args.extend([
            "-map", "0:v:0?",
            "-map", "0:s?",
            "-c:v", "copy",
            "-c:s", "copy",
            "-vsync", "passthrough",   # never alter video frame timestamps
        ])

    args.append(final_output_file)
    return args, final_output_file

# ---------------------------------------------------------------------------
# CANCEL JOB CALLBACK
# ---------------------------------------------------------------------------

@bot.on_callback_query(filters.regex(r"^cancel_job\|(.+)$") & admin_filter)
async def cancel_job_cb(client, cb: CallbackQuery):
    await cb.answer("Cancel request received.")
    job_id  = cb.data.split("|", 1)[1]
    tracker = JOB_TRACKERS.get(job_id)
    if tracker is None:
        await cb.message.edit_text("Job not found or already completed.")
        return
    tracker["cancelled"] = True
    await cb.message.edit_text("⛔ Cancelling job… please wait.")

# ---------------------------------------------------------------------------
# MAIN CONVERSION PROCESS
# ---------------------------------------------------------------------------

async def start_conversion_process(cb: CallbackQuery):
    chat_id    = cb.message.chat.id
    status_msg = cb.message

    async with ffmpeg_semaphore:
        conv = await get_user_conversation(chat_id)
        if not conv or conv.get("stage") != "processing":
            return

        job_id  = conv.get("job_id") or str(uuid.uuid4())
        user_id = cb.from_user.id
        conv["user_id"] = user_id

        JOB_TRACKERS.setdefault(job_id, {
            "cancelled": False, "last_update": 0.0, "start_ts": time.time()
        })

        uploaded_files: list[str] = []

        try:
            job_dir     = conv["job_dir"]
            input_file  = conv["input_file_path"]
            fmt         = conv["format"]
            use_wm      = conv.get("watermark", False)
            wm_choice   = conv.get("watermark_choice", "owner")
            output_type = conv.get("output_type", "audio")

            watermark_local = None
            wm_info = {"applied": False, "which": None, "positions": None, "volume": None}

            # ── Silence detection (if requested) ──────────────────────────
            silence_midpoints: list = []
            if use_wm:
                wm_cfg = await get_owner_watermark() if wm_choice == "owner" \
                    else (await get_admin_watermark(user_id) or {})
                if wm_cfg.get("use_silence_detection"):
                    await status_msg.edit_text("🔇 **Detecting silent gaps…**")
                    silence_midpoints = await detect_silence_midpoints(
                        input_file, conv["selected_track_index"]
                    )

            # ── Download watermark ────────────────────────────────────────
            if use_wm:
                wm_file_id  = None
                wm_vol      = 0.2
                wm_pos      = conv.get("wm_positions", ["start", "end"])
                wm_custom   = conv.get("wm_custom_seconds", 0)

                if wm_choice == "owner":
                    owner_wm    = await get_owner_watermark()
                    wm_file_id  = owner_wm.get("file_id")
                    wm_vol      = owner_wm.get("volume", 0.2)
                    # Per-job override wins
                    if conv.get("job_wm_volume") is not None:
                        wm_vol = conv["job_wm_volume"]
                    wm_info.update({"which": "owner", "volume": wm_vol,
                                     "positions": wm_pos, "custom": wm_custom})
                else:
                    admin_wm = await get_admin_watermark(user_id)
                    if admin_wm:
                        wm_file_id = admin_wm.get("file_id")
                        wm_vol     = admin_wm.get("volume", 0.2)
                        if conv.get("job_wm_volume") is not None:
                            wm_vol = conv["job_wm_volume"]
                        wm_info.update({"which": "admin", "volume": wm_vol,
                                         "positions": wm_pos, "custom": wm_custom})

                conv["wm_positions"]     = wm_pos
                conv["wm_custom_seconds"] = wm_custom

                if wm_file_id:
                    try:
                        await status_msg.edit_text("📥 **Downloading watermark…**")
                        watermark_local = await bot.download_media(
                            wm_file_id,
                            file_name=os.path.join(job_dir, "watermark_audio")
                        )
                        wm_info["applied"] = True
                    except Exception as e:
                        LOGGER.error(f"Watermark download failed: {e}", exc_info=True)
                        raise Exception("Watermark download failed.") from e
                else:
                    use_wm = False
                    conv["watermark"] = False

            # ── Prepare ────────────────────────────────────────────────────
            await status_msg.edit_text("🔧 **Preparing conversion…**")
            total_duration = await get_media_duration(input_file)

            # ── Helper: build + run + upload for a single output ───────────
            async def run_single(local_conv: dict, out_type: str, out_fmt=None):
                ffmpeg_args, final_output = await build_ffmpeg_args(
                    local_conv, input_file, watermark_local,
                    override_format=out_fmt,
                    override_output_type=out_type,
                    silence_midpoints=silence_midpoints,
                )
                try:
                    await status_msg.edit_text("🔁 **Converting…**")
                except Exception:
                    pass

                await run_ffmpeg_with_progress(
                    ffmpeg_args, total_duration, status_msg, job_id,
                    update_every=Config.PROGRESS_UPDATE_INTERVAL
                )

                if not os.path.exists(final_output):
                    raise Exception("Conversion finished but output file not found.")

                out_size = os.path.getsize(final_output)
                if out_size > Config.TELEGRAM_MAX_FILE_SIZE:
                    await status_msg.edit_text(
                        f"❌ Output is {human_size(out_size)}, exceeding Telegram's limit."
                    )
                    return None, None

                codec_lbl = (out_fmt or local_conv.get("format", {})).get("codec", "?")
                wm_flag   = local_conv.get("watermark") and wm_info.get("applied")
                caption   = (
                    f"✅ **Conversion Complete!**\n\n"
                    f"Format: `{codec_lbl}`\n"
                    f"Watermark: `{'Yes – ' + wm_info.get('which','') if wm_flag else 'No'}`\n"
                )
                if wm_flag:
                    caption += (
                        f"Positions: `{', '.join(wm_info.get('positions') or [])}`\n"
                        f"Volume: `{int(wm_info.get('volume', 0.2) * 100)}%`\n"
                    )
                caption += (
                    f"Output Size: `{human_size(out_size)}`\n"
                    f"Job ID: `{job_id}`"
                )

                await status_msg.edit_text("✅ **Converting Complete!**\n\nUploading…")
                await bot.send_document(
                    chat_id,
                    document=final_output,
                    caption=caption,
                    progress=progress_callback,
                    progress_args=(status_msg, "Uploading"),
                )
                uploaded_files.append(final_output)
                return final_output, out_size

            # ── Dispatch by output_type ────────────────────────────────────
            if output_type == "audio":
                await run_single(conv, "audio", out_fmt=fmt)
            elif output_type == "remux":
                await run_single(conv, "remux", out_fmt=fmt)
            elif output_type == "all":
                await run_single(dict(conv, output_type="audio"),  "audio", out_fmt=fmt)
                await run_single(dict(conv, output_type="remux"),  "remux", out_fmt=fmt)
            else:
                raise Exception(f"Unknown output type: {output_type}")

            # ── Save job metadata (permanent) ──────────────────────────────
            job_doc = {
                "_id":          job_id,
                "chat_id":      chat_id,
                "user_id":      user_id,
                "input_file":   input_file,
                "output_files": uploaded_files,
                "output_size":  sum(os.path.getsize(f) for f in uploaded_files if os.path.exists(f)),
                "format":       fmt,
                "watermark":    wm_info,
                "timestamp":    datetime.utcnow(),
            }
            await jobs_collection.insert_one(job_doc)

            try:
                await status_msg.delete()
            except Exception:
                pass

        except asyncio.CancelledError:
            try:
                await status_msg.edit_text("⚠️ **Conversion Cancelled**\n\nCleaning up…")
            except Exception:
                pass

        except Exception as e:
            LOGGER.error(f"Conversion failed: {e}", exc_info=True)
            try:
                await status_msg.edit_text(f"❌ **Error!**\n\nProcess failed: `{e}`")
            except Exception:
                pass

        finally:
            try:
                conv_latest = await get_user_conversation(chat_id)
                job_dir     = (conv_latest or conv).get("job_dir")
                if job_dir and os.path.isdir(job_dir):
                    shutil.rmtree(job_dir)
                    LOGGER.info(f"Cleaned up job directory: {job_dir}")
            except Exception as e:
                LOGGER.error(f"Cleanup failed: {e}")
            DOWNLOAD_PROGRESS.pop(getattr(status_msg, "id", None), None)
            asyncio.create_task(clear_conversation_after_delay(chat_id))

# ---------------------------------------------------------------------------
# WEB SERVER & PING
# ---------------------------------------------------------------------------

routes = web.RouteTableDef()

@routes.get("/", allow_head=True)
async def root_route_handler(request):
    return web.Response(text="Audio Bot is alive!", content_type="text/html")

async def web_server():
    web_app = web.Application(client_max_size=300_000_000)
    web_app.add_routes(routes)
    return web_app

async def ping_server():
    if not Config.ON_HEROKU or not Config.STREAM_URL:
        LOGGER.info("Pinger disabled (not on Heroku or STREAM_URL not set).")
        return
    LOGGER.info(f"Pinger started for {Config.STREAM_URL}.")
    while True:
        await asyncio.sleep(Config.PING_INTERVAL)
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
                async with session.get(Config.STREAM_URL) as resp:
                    LOGGER.info(f"Pinged {Config.STREAM_URL}: HTTP {resp.status}")
        except Exception as e:
            LOGGER.warning(f"Pinger error: {e}")

# ---------------------------------------------------------------------------
# APP LIFECYCLE
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    async def main_startup_shutdown_logic():
        LOGGER.info("Application starting up…")
        await bot.start()
        bot_info = await bot.get_me()
        LOGGER.info(f"Bot @{bot_info.username} started.")
        asyncio.create_task(ping_server())

        web_app = await web_server()
        runner  = web.AppRunner(web_app)
        await runner.setup()
        site = web.TCPSite(runner, "0.0.0.0", Config.PORT)
        await site.start()
        LOGGER.info(f"Web server on port {Config.PORT}.")

        try:
            await bot.send_message(Config.OWNER_ID, "**✅ Audio Bot restarted — all services online!**")
        except Exception as e:
            LOGGER.warning(f"Could not send startup message: {e}")

        await asyncio.Event().wait()

    loop = asyncio.get_event_loop()

    async def shutdown_handler(sig):
        LOGGER.info(f"Received {sig.name} — shutting down…")
        if bot.is_connected:
            await bot.stop()
        tasks = [t for t in asyncio.all_tasks(loop) if t is not asyncio.current_task()]
        if tasks:
            LOGGER.info(f"Cancelling {len(tasks)} tasks…")
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
        loop.stop()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(
                sig, lambda s=sig: asyncio.create_task(shutdown_handler(s))
            )
        except NotImplementedError:
            pass

    try:
        LOGGER.info("Starting event loop…")
        loop.run_until_complete(main_startup_shutdown_logic())
        loop.run_forever()
    except Exception as e:
        LOGGER.critical(f"Critical error: {e}", exc_info=True)
    finally:
        LOGGER.info("Event loop stopped. Final cleanup.")
        try:
            if os.path.isdir(Config.DOWNLOAD_DIR):
                shutil.rmtree(Config.DOWNLOAD_DIR)
                LOGGER.info(f"Cleaned up DOWNLOAD_DIR: {Config.DOWNLOAD_DIR}")
        except Exception as e:
            LOGGER.error(f"Failed to clean DOWNLOAD_DIR: {e}")
        if loop.is_running():
            loop.stop()
        if not loop.is_closed():
            loop.close()
        LOGGER.info("Shutdown complete.")
