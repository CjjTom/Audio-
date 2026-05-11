"""
Audio Converter Bot — Production build
Lean rewrite: no dynaudnorm, no heavy aresample, no vsync passthrough.
New: URL/direct-link download support.
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
from urllib.parse import urlparse, unquote
from aiohttp import web
from datetime import datetime, timedelta
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv
from pyrogram import Client, filters, enums
from pyrogram.errors import FloodWait, MessageNotModified
from pyrogram.types import (
    Message,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    CallbackQuery,
)

# ═══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════

load_dotenv()

logging.basicConfig(
    level=logging.DEBUG,
    format="[%(asctime)s - %(levelname)s] - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
LOGGER = logging.getLogger(__name__)
logging.getLogger("pyrogram").setLevel(logging.WARNING)
logging.getLogger("motor").setLevel(logging.WARNING)


class Config:
    API_ID                  = int(os.environ.get("API_ID", 0))
    API_HASH                = os.environ.get("API_HASH", "")
    BOT_TOKEN               = os.environ.get("BOT_TOKEN", "")
    OWNER_ID                = int(os.environ.get("OWNER_ID", 0))
    MONGO_URI               = os.environ.get("MONGO_URI", "")
    PORT                    = int(os.environ.get("PORT", 8080))
    DOWNLOAD_DIR            = os.environ.get("DOWNLOAD_DIR",
                                              f"/tmp/audiobot_{uuid.uuid4().hex[:8]}/")
    STREAM_URL              = os.environ.get("STREAM_URL", "").rstrip("/")
    PING_INTERVAL           = int(os.environ.get("PING_INTERVAL", 1200))
    ON_HEROKU               = "DYNO" in os.environ
    MAX_CONCURRENT_JOBS     = int(os.environ.get("MAX_CONCURRENT_JOBS", 1))
    # Telegram hard cap is 2 GB for bots
    TELEGRAM_MAX_FILE_SIZE  = int(os.environ.get("TELEGRAM_MAX_FILE_SIZE",
                                                   2 * 1024 * 1024 * 1024))
    CONVERSATION_CLEAR_DELAY = int(os.environ.get("CONVERSATION_CLEAR_DELAY", 300))
    PROGRESS_UPDATE_INTERVAL = float(os.environ.get("PROGRESS_UPDATE_INTERVAL", 3.0))
    # Chunk size for URL downloads (256 KB — easy on RAM)
    URL_DOWNLOAD_CHUNK_SIZE  = int(os.environ.get("URL_DOWNLOAD_CHUNK_SIZE", 262144))
    # Connection timeout for URL downloads
    URL_DOWNLOAD_TIMEOUT     = int(os.environ.get("URL_DOWNLOAD_TIMEOUT", 60))


# ═══════════════════════════════════════════════════════════════════════════════
# STARTUP VALIDATION
# ═══════════════════════════════════════════════════════════════════════════════

def _check_ffmpeg():
    ff  = shutil.which("ffmpeg")
    ffp = shutil.which("ffprobe")
    if not ff or not ffp:
        LOGGER.critical("ffmpeg / ffprobe not found in PATH. Conversions will fail.")
        return False
    LOGGER.info(f"ffmpeg: {ff}  ffprobe: {ffp}")
    return True


_check_ffmpeg()

if not all([Config.API_ID, Config.API_HASH, Config.BOT_TOKEN,
            Config.OWNER_ID, Config.MONGO_URI]):
    LOGGER.critical("FATAL: Required env-vars missing.")
    raise SystemExit(1)

os.makedirs(Config.DOWNLOAD_DIR, exist_ok=True)

# ═══════════════════════════════════════════════════════════════════════════════
# GLOBALS
# ═══════════════════════════════════════════════════════════════════════════════

ffmpeg_semaphore  = asyncio.Semaphore(Config.MAX_CONCURRENT_JOBS)
DOWNLOAD_PROGRESS = {}          # msg_id -> {"ts": float, "bytes": int}
JOB_TRACKERS      = {}          # job_id -> tracker dict
CANCEL_DOWNLOADS  = set()       # chat_ids that requested download cancellation

# ═══════════════════════════════════════════════════════════════════════════════
# DATABASE
# ═══════════════════════════════════════════════════════════════════════════════

_db_client            = AsyncIOMotorClient(Config.MONGO_URI)
_db                   = _db_client["AudioBotDB"]
user_conversations_col = _db["conversations"]
bot_settings_col       = _db["settings"]     # owner watermark, global metadata
admin_col              = _db["admins"]        # per-admin records (permanent)
jobs_col               = _db["jobs"]          # job history (permanent)
users_col              = _db["users"]         # permanent user registry (never deleted)


# ─── Conversation ──────────────────────────────────────────────────────────────

async def get_user_conversation(chat_id: int):
    return await user_conversations_col.find_one({"_id": chat_id})


async def update_user_conversation(chat_id: int, data: dict | None):
    if data:
        await user_conversations_col.update_one(
            {"_id": chat_id}, {"$set": data}, upsert=True
        )
    else:
        await user_conversations_col.delete_one({"_id": chat_id})


async def clear_conversation_after_delay(chat_id: int,
                                         delay: int = Config.CONVERSATION_CLEAR_DELAY):
    await asyncio.sleep(delay)
    await update_user_conversation(chat_id, None)


# ─── Permanent user registry ───────────────────────────────────────────────────

async def register_user(user_id: int, username: str = None, first_name: str = None):
    """Upsert user — IDs are NEVER removed from this collection."""
    upd: dict = {"last_seen": datetime.utcnow()}
    if username:
        upd["username"]   = username
    if first_name:
        upd["first_name"] = first_name
    await users_col.update_one(
        {"_id": user_id},
        {"$set": upd, "$setOnInsert": {"registered_at": datetime.utcnow()}},
        upsert=True,
    )


# ─── Owner watermark ──────────────────────────────────────────────────────────

async def get_owner_watermark() -> dict:
    doc = await bot_settings_col.find_one({"_id": "owner_watermark"})
    if not doc:
        return {
            "file_id":        None,
            "volume":         0.2,
            "positions":      ["start", "end"],
            "custom_seconds": 0,
            "fade_duration":  1.0,
        }
    return {
        "file_id":        doc.get("file_id"),
        "volume":         float(doc.get("volume", 0.2)),
        "positions":      doc.get("positions", ["start", "end"]),
        "custom_seconds": int(doc.get("custom_seconds", 0)),
        "fade_duration":  float(doc.get("fade_duration", 1.0)),
    }


async def set_owner_watermark_file(file_id: str):
    await bot_settings_col.update_one(
        {"_id": "owner_watermark"}, {"$set": {"file_id": file_id}}, upsert=True
    )


async def set_owner_watermark_volume(volume: float):
    await bot_settings_col.update_one(
        {"_id": "owner_watermark"}, {"$set": {"volume": float(volume)}}, upsert=True
    )


async def set_owner_watermark_positions(positions: list, custom_seconds: int = 0):
    await bot_settings_col.update_one(
        {"_id": "owner_watermark"},
        {"$set": {"positions": positions, "custom_seconds": int(custom_seconds)}},
        upsert=True,
    )


async def set_owner_watermark_fade(fade_duration: float):
    await bot_settings_col.update_one(
        {"_id": "owner_watermark"},
        {"$set": {"fade_duration": float(fade_duration)}},
        upsert=True,
    )


async def delete_owner_watermark():
    await bot_settings_col.update_one(
        {"_id": "owner_watermark"}, {"$unset": {"file_id": ""}}
    )


# ─── Default metadata ─────────────────────────────────────────────────────────

async def get_default_metadata() -> dict:
    doc = await bot_settings_col.find_one({"_id": "default_metadata"})
    if not doc:
        return {"title": "", "artist": "", "album": "", "language": "", "comment": ""}
    return {k: doc.get(k, "") for k in ("title", "artist", "album", "language", "comment")}


async def set_default_metadata(fields: dict):
    await bot_settings_col.update_one(
        {"_id": "default_metadata"}, {"$set": fields}, upsert=True
    )


# ─── Per-admin watermark ──────────────────────────────────────────────────────

async def get_admin_watermark(user_id: int) -> dict | None:
    doc = await admin_col.find_one({"_id": user_id})
    return doc.get("watermark") if doc else None


async def set_admin_watermark(user_id: int, file_id: str,
                               volume: float = 0.2, positions: list = None,
                               custom_seconds: int = 0, fade_duration: float = 1.0):
    if positions is None:
        positions = ["start", "end"]
    await admin_col.update_one(
        {"_id": user_id},
        {"$set": {"watermark": {
            "file_id":        file_id,
            "volume":         float(volume),
            "positions":      positions,
            "custom_seconds": int(custom_seconds),
            "fade_duration":  float(fade_duration),
            "date_added":     datetime.utcnow(),
        }}},
        upsert=True,
    )


async def set_admin_watermark_volume(user_id: int, volume: float):
    await admin_col.update_one(
        {"_id": user_id},
        {"$set": {"watermark.volume": float(volume)}},
        upsert=True,
    )


async def update_admin_watermark_positions(user_id: int, positions: list,
                                            custom_seconds: int = 0):
    await admin_col.update_one(
        {"_id": user_id},
        {"$set": {
            "watermark.positions":      positions,
            "watermark.custom_seconds": int(custom_seconds),
        }},
        upsert=False,
    )


async def set_admin_watermark_fade(user_id: int, fade_duration: float):
    await admin_col.update_one(
        {"_id": user_id},
        {"$set": {"watermark.fade_duration": float(fade_duration)}},
        upsert=False,
    )


# ─── Admin list + limits + stats ──────────────────────────────────────────────

async def get_admin_list() -> list[int]:
    cursor = admin_col.find({"_id": {"$ne": Config.OWNER_ID}})
    return [doc["_id"] async for doc in cursor]


async def add_admin(user_id: int):
    if user_id == Config.OWNER_ID:
        return
    await admin_col.update_one(
        {"_id": user_id},
        {"$setOnInsert": {"date_added": datetime.utcnow()}},
        upsert=True,
    )


async def remove_admin(user_id: int):
    """Delete admin record — users_col is NEVER touched."""
    await admin_col.delete_one({"_id": user_id})


async def get_admin_limits(user_id: int) -> dict:
    doc = await admin_col.find_one({"_id": user_id})
    if not doc:
        return {"max_jobs_per_day": 0, "max_file_size_mb": 0}
    return doc.get("limits", {"max_jobs_per_day": 0, "max_file_size_mb": 0})


async def set_admin_limits(user_id: int, max_jobs_per_day: int = 0,
                            max_file_size_mb: int = 0):
    await admin_col.update_one(
        {"_id": user_id},
        {"$set": {"limits": {
            "max_jobs_per_day": int(max_jobs_per_day),
            "max_file_size_mb": int(max_file_size_mb),
        }}},
        upsert=True,
    )


async def get_admin_jobs_today(user_id: int) -> int:
    today = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    return await jobs_col.count_documents(
        {"user_id": user_id, "timestamp": {"$gte": today}}
    )


async def get_admin_stats(user_id: int) -> dict:
    total_jobs = await jobs_col.count_documents({"user_id": user_id})
    today_jobs = await get_admin_jobs_today(user_id)
    agg = await jobs_col.aggregate([
        {"$match": {"user_id": user_id}},
        {"$group": {"_id": None, "total": {"$sum": "$output_size"}}},
    ]).to_list(1)
    total_bytes = agg[0]["total"] if agg else 0
    last = await jobs_col.find_one({"user_id": user_id}, sort=[("timestamp", -1)])
    return {
        "total_jobs":  total_jobs,
        "today_jobs":  today_jobs,
        "total_bytes": total_bytes,
        "last_job_ts": last.get("timestamp") if last else None,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# FILTERS
# ═══════════════════════════════════════════════════════════════════════════════

async def _is_admin(_, __, update) -> bool:
    try:
        uid = update.from_user.id if hasattr(update, "from_user") else None
        if uid is None:
            return False
        if uid == Config.OWNER_ID:
            return True
        admins = await get_admin_list()
        return uid in admins
    except Exception as e:
        LOGGER.error(f"admin filter error: {e}")
        return False


admin_filter = filters.create(_is_admin)

# ═══════════════════════════════════════════════════════════════════════════════
# UTILITY FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════════

def human_size(n: int) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(n) < 1024.0:
            return f"{n:.2f} {unit}"
        n /= 1024.0
    return f"{n:.2f} PB"


def progress_bar(pct: float, length: int = 20) -> str:
    pct    = max(0.0, min(100.0, pct))
    filled = int((pct / 100.0) * length)
    return f"[{'█' * filled}{'░' * (length - filled)}]"


def format_time(secs) -> str:
    if secs is None or math.isinf(float(secs)) or float(secs) < 0:
        return "--:--:--"
    return str(timedelta(seconds=int(max(0, secs))))


def sanitize_filename(fn: str) -> str:
    fn = str(fn or f"file_{uuid.uuid4().hex[:8]}")
    return re.sub(r'[/\\<>:"|?*\x00-\x1F]', "_", fn)


def safe_ext(filename: str) -> str:
    _, ext = os.path.splitext(filename or "")
    return ext.lower().lstrip(".")


MEDIA_EXTS = {
    "mka", "mkv", "mp3", "m4a", "aac", "opus", "flac",
    "wav", "ogg", "mp4", "mov", "webm", "m2ts", "ts", "ac3", "eac3",
}


async def safe_edit(msg, text: str, reply_markup=None):
    try:
        await msg.edit_text(
            text, reply_markup=reply_markup, parse_mode=enums.ParseMode.MARKDOWN
        )
    except MessageNotModified:
        pass
    except Exception as e:
        LOGGER.warning(f"safe_edit: {e}")


async def run_shell(cmd: str) -> str:
    proc = await asyncio.create_subprocess_shell(
        cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    out, err = await proc.communicate()
    if proc.returncode != 0:
        raise RuntimeError(err.decode(errors="ignore").strip())
    return out.decode(errors="ignore").strip()


async def probe_streams(path: str) -> list:
    cmd = f"ffprobe -v error -show_streams -of json {shlex.quote(path)}"
    try:
        return json.loads(await run_shell(cmd)).get("streams", [])
    except Exception as e:
        LOGGER.error(f"probe_streams: {e}")
        return []


async def get_duration(path: str) -> float:
    cmd = (
        f"ffprobe -v error -show_entries format=duration "
        f"-of default=noprint_wrappers=1:nokey=1 {shlex.quote(path)}"
    )
    try:
        return float(await run_shell(cmd))
    except Exception:
        return 0.0


# ═══════════════════════════════════════════════════════════════════════════════
# DOWNLOAD PROGRESS CALLBACK  (Telegram downloads / uploads)
# ═══════════════════════════════════════════════════════════════════════════════

async def progress_callback(current: int, total: int, msg, action: str = "Downloading"):
    if msg.chat.id in CANCEL_DOWNLOADS:
        CANCEL_DOWNLOADS.discard(msg.chat.id)
        raise Exception("Cancelled by user")

    pct  = (current / total * 100) if total else 0.0
    now  = time.time()
    mid  = getattr(msg, "id", 0) or 0
    last = DOWNLOAD_PROGRESS.get(mid, {"ts": 0, "bytes": 0})

    if now - last["ts"] < 2.5:
        return

    dt      = max(now - last["ts"], 0.001)
    speed   = max(0, current - last["bytes"]) / dt
    eta     = (total - current) / speed if speed > 0 and total and total > current else None

    cancel_kb = InlineKeyboardMarkup(
        [[InlineKeyboardButton("❌ Cancel", callback_data="cancel_download_process")]]
    )
    txt = (
        f"**{action}** {progress_bar(pct)}\n\n"
        f"📊 **{pct:.2f}%**\n\n"
        f"✅ {human_size(current)} / {human_size(total)}\n\n"
        f"🚀 {human_size(int(speed))}/s\n\n"
        f"⏳ {format_time(eta)}"
    )
    try:
        await msg.edit_text(txt, reply_markup=cancel_kb)
        DOWNLOAD_PROGRESS[mid] = {"ts": now, "bytes": current}
    except (MessageNotModified, FloodWait):
        pass
    except Exception as e:
        LOGGER.warning(f"progress_callback: {e}")


# ═══════════════════════════════════════════════════════════════════════════════
# URL DOWNLOAD  (new feature)
# ═══════════════════════════════════════════════════════════════════════════════

def _filename_from_url(url: str, content_disposition: str = None,
                        content_type: str = None) -> str:
    """
    Best-effort filename extraction from:
    1. Content-Disposition header (attachment; filename="...")
    2. URL path basename
    3. Fallback with extension guessed from content-type
    """
    # 1. Content-Disposition
    if content_disposition:
        m = re.search(r'filename\*?=(?:UTF-8\'\')?["\']?([^"\';\r\n]+)["\']?',
                      content_disposition, re.IGNORECASE)
        if m:
            fn = unquote(m.group(1).strip().strip('"\''))
            if fn:
                return sanitize_filename(fn)

    # 2. URL path
    parsed = urlparse(url)
    path   = unquote(parsed.path)
    base   = os.path.basename(path.rstrip("/"))
    if base and "." in base:
        return sanitize_filename(base)

    # 3. Guess from content-type
    ct = (content_type or "").lower().split(";")[0].strip()
    ext_map = {
        "audio/mpeg":       "mp3",
        "audio/mp4":        "m4a",
        "audio/aac":        "aac",
        "audio/ogg":        "ogg",
        "audio/flac":       "flac",
        "audio/wav":        "wav",
        "audio/x-matroska": "mka",
        "video/mp4":        "mp4",
        "video/x-matroska": "mkv",
        "video/webm":       "webm",
    }
    ext = ext_map.get(ct, "dat")
    return sanitize_filename(f"download_{uuid.uuid4().hex[:8]}.{ext}")


async def download_url(url: str, dest_path: str, status_msg,
                        chat_id: int) -> str:
    """
    Download a URL to dest_path, showing progress. Returns the final file path
    (may differ if filename was determined from headers).
    Raises on error or user cancellation.
    """
    timeout = aiohttp.ClientTimeout(
        total=None,          # no total timeout — large files
        connect=Config.URL_DOWNLOAD_TIMEOUT,
        sock_read=Config.URL_DOWNLOAD_TIMEOUT,
    )
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (compatible; AudioBot/2.0; +https://t.me)"
        )
    }

    async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
        try:
            async with session.get(url, allow_redirects=True) as resp:
                if resp.status not in (200, 206):
                    raise RuntimeError(f"HTTP {resp.status} from server")

                content_disposition = resp.headers.get("Content-Disposition", "")
                content_type        = resp.headers.get("Content-Type", "")
                total               = int(resp.headers.get("Content-Length", 0))

                # Determine final filename
                fn       = _filename_from_url(url, content_disposition, content_type)
                final    = os.path.join(os.path.dirname(dest_path), fn)

                downloaded = 0
                last_ts    = 0.0
                last_bytes = 0
                mid        = getattr(status_msg, "id", 0) or 0

                cancel_kb = InlineKeyboardMarkup(
                    [[InlineKeyboardButton("❌ Cancel Download",
                                           callback_data="cancel_download_process")]]
                )

                with open(final, "wb") as f:
                    async for chunk in resp.content.iter_chunked(
                        Config.URL_DOWNLOAD_CHUNK_SIZE
                    ):
                        if chat_id in CANCEL_DOWNLOADS:
                            CANCEL_DOWNLOADS.discard(chat_id)
                            f.close()
                            try:
                                os.remove(final)
                            except Exception:
                                pass
                            raise asyncio.CancelledError("Download cancelled by user")

                        f.write(chunk)
                        downloaded += len(chunk)

                        now = time.time()
                        if now - last_ts >= 2.5:
                            pct   = (downloaded / total * 100) if total else 0.0
                            dt    = max(now - last_ts, 0.001)
                            speed = max(0, downloaded - last_bytes) / dt
                            eta   = (
                                (total - downloaded) / speed
                                if speed > 0 and total and total > downloaded
                                else None
                            )
                            bar = progress_bar(pct if total else 0.0)
                            txt = (
                                f"**🌐 Downloading from URL** {bar}\n\n"
                                f"📊 **{pct:.2f}%** ({human_size(downloaded)}"
                                f"{' / ' + human_size(total) if total else ''})\n\n"
                                f"🚀 {human_size(int(speed))}/s\n\n"
                                f"⏳ {format_time(eta)}"
                            )
                            try:
                                await status_msg.edit_text(txt, reply_markup=cancel_kb)
                                DOWNLOAD_PROGRESS[mid] = {"ts": now, "bytes": downloaded}
                            except (MessageNotModified, FloodWait):
                                pass
                            except Exception:
                                pass
                            last_ts    = now
                            last_bytes = downloaded

                LOGGER.info(f"URL download complete: {final} ({human_size(downloaded)})")
                return final

        except asyncio.CancelledError:
            raise
        except aiohttp.ClientError as e:
            raise RuntimeError(f"Network error: {e}") from e


# ═══════════════════════════════════════════════════════════════════════════════
# FFMPEG  — PROGRESS RUNNER
# ═══════════════════════════════════════════════════════════════════════════════

async def run_ffmpeg(args: list, total_secs: float, status_msg, job_id: str) -> None:
    """
    Run ffmpeg with -progress pipe:1.
    Stderr is drained asynchronously (no blocking, no race condition).
    Cancellable via JOB_TRACKERS[job_id]['cancelled'] = True.
    """
    JOB_TRACKERS[job_id] = {
        "cancelled":   False,
        "last_update": 0.0,
        "start_ts":    time.time(),
        "last_pct":    0.0,
    }

    # Inject progress flags right after "ffmpeg"
    if args and args[0].lower().endswith("ffmpeg") and "-progress" not in args:
        args = [args[0], "-progress", "pipe:1", "-nostats"] + list(args[1:])

    LOGGER.info(f"[{job_id}] CMD: {' '.join(args)}")

    proc = await asyncio.create_subprocess_exec(
        *args,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )

    stderr_lines: list[str] = []

    async def _drain_stderr():
        try:
            async for raw in proc.stderr:
                line = raw.decode(errors="ignore").rstrip()
                stderr_lines.append(line)
                # Only log lines that look like errors
                if any(kw in line.lower() for kw in ("error", "invalid", "unable")):
                    LOGGER.debug(f"[{job_id}] ffmpeg: {line}")
        except Exception:
            pass

    drain_task = asyncio.create_task(_drain_stderr())

    cancel_kb = InlineKeyboardMarkup(
        [[InlineKeyboardButton("⛔ Cancel", callback_data=f"cancel_job|{job_id}")]]
    )

    out_time_ms = 0
    total_size  = 0
    percent     = 0.0
    upd_every   = Config.PROGRESS_UPDATE_INTERVAL

    try:
        while True:
            tracker = JOB_TRACKERS.get(job_id, {})
            if tracker.get("cancelled"):
                LOGGER.info(f"[{job_id}] Cancel requested")
                try:
                    proc.send_signal(signal.SIGINT)
                    await asyncio.wait_for(proc.wait(), timeout=10.0)
                except Exception:
                    proc.terminate()
                raise asyncio.CancelledError("Job cancelled by user")

            line = await proc.stdout.readline()
            if not line:
                break

            text = line.decode(errors="ignore").strip()
            if not text or "=" not in text:
                continue

            k, _, v = text.partition("=")
            k, v    = k.strip(), v.strip()

            if k == "out_time_ms":
                try:
                    out_time_ms = int(v)
                except Exception:
                    pass
            elif k == "total_size":
                try:
                    total_size = int(v)
                except Exception:
                    pass
            elif k == "progress" and v == "end":
                percent = max(percent, 99.9)

            if total_secs and out_time_ms:
                pct     = (out_time_ms / 1_000_000.0) / total_secs * 100.0
                percent = min(pct, 99.99)
            else:
                percent = tracker.get("last_pct", percent)

            now = time.time()
            if now - tracker.get("last_update", 0) >= upd_every:
                elapsed = now - tracker["start_ts"]
                speed   = (total_size / elapsed) if elapsed > 0 and total_size else 0.0
                if total_secs and percent > 0:
                    eta_s = total_secs * (100.0 - percent) / percent
                elif speed > 0 and total_size:
                    eta_s = max(0, total_size - total_size * percent / 100) / speed
                else:
                    eta_s = None

                txt = (
                    f"**Converting** {progress_bar(percent)}\n\n"
                    f"📊 **{percent:.2f}%**\n\n"
                    f"⏳ Elapsed: {format_time(elapsed)}\n\n"
                    f"🚀 Speed: {human_size(int(speed))}/s\n\n"
                    f"⏳ ETA: {format_time(eta_s)}"
                )
                await safe_edit(status_msg, txt, reply_markup=cancel_kb)
                JOB_TRACKERS[job_id]["last_update"] = now
                JOB_TRACKERS[job_id]["last_pct"]    = percent

    except asyncio.CancelledError:
        try:
            if proc.returncode is None:
                proc.terminate()
        except Exception:
            pass
        drain_task.cancel()
        raise

    except Exception as e:
        try:
            if proc.returncode is None:
                proc.terminate()
        except Exception:
            pass
        drain_task.cancel()
        await asyncio.gather(drain_task, return_exceptions=True)
        raise RuntimeError(f"FFmpeg runtime error: {e}") from e

    # Wait for process to finish cleanly
    try:
        rc = await asyncio.wait_for(proc.wait(), timeout=60.0)
    except asyncio.TimeoutError:
        proc.terminate()
        rc = None

    await asyncio.gather(drain_task, return_exceptions=True)

    if rc != 0:
        tail = "\n".join(stderr_lines[-30:])
        LOGGER.error(f"[{job_id}] ffmpeg rc={rc}\n{tail}")
        raise RuntimeError(f"FFmpeg failed (rc={rc}).\n{tail}")

    # Final 100%
    try:
        elapsed = time.time() - JOB_TRACKERS.get(job_id, {}).get("start_ts", time.time())
        await safe_edit(
            status_msg,
            f"**Converting** {progress_bar(100.0)}\n\n"
            f"📊 **100.00%**\n\n"
            f"⏳ Elapsed: {format_time(elapsed)}\n\n"
            f"✅ Done!",
        )
    except Exception:
        pass

    JOB_TRACKERS.pop(job_id, None)
    LOGGER.info(f"[{job_id}] ffmpeg finished OK")


# ═══════════════════════════════════════════════════════════════════════════════
# FFMPEG ARGS BUILDER
#
# Lip-sync guarantee — lightweight approach:
# ──────────────────────────────────────────
# • -ar 48000 on the encoded stream ensures a fixed, known sample rate.
# • adelay=<ms>:all=1 uses sample-accurate integer-ms delay applied to ALL
#   channels, avoiding channel-layout mismatch.
# • amix=duration=longest:dropout_transition=0 keeps the main track's full
#   length; watermark copies are padded with `apad` so amix never clips.
# • afade is applied with sample-accurate st= timing relative to each
#   watermark instance.
# • For remux: -c:v copy -c:s copy  (no -vsync, no -avoid_negative_ts).
#   These global flags caused massive buffering in muxers on low-RAM servers.
# • No dynaudnorm, no heavy aresample chains.
# ═══════════════════════════════════════════════════════════════════════════════

WM_START_OFFSET = 180.0   # place "start" watermark 3 min in
WM_END_OFFSET   = 420.0   # place "end" watermark 7 min before finish


async def build_ffmpeg_args(
    conv:                dict,
    input_file:          str,
    watermark_local:     str | None,
    override_format:     dict  | None = None,
    override_output_type: str  | None = None,
) -> tuple[list, str]:
    """Return (ffmpeg_args_list, output_file_path)."""

    track_index  = conv["selected_track_index"]
    stream_obj   = conv.get("selected_stream_obj", {})
    fmt          = override_format or conv["format"]
    use_wm       = conv.get("watermark", False)
    wm_choice    = conv.get("watermark_choice", "owner")
    job_dir      = conv["job_dir"]
    wm_positions = conv.get("wm_positions") or []
    wm_custom_s  = int(conv.get("wm_custom_seconds", 0) or 0)
    output_type  = override_output_type or conv.get("output_type", "audio")
    job_wm_vol   = conv.get("job_wm_volume")   # None → use saved default

    # ── Output path ───────────────────────────────────────────────────────────
    base, _    = os.path.splitext(conv.get("original_filename") or f"out_{uuid.uuid4().hex[:8]}")
    codec_name = fmt.get("codec", "aac")

    if output_type == "remux":
        ext = "mkv"
    elif codec_name == "libmp3lame":
        ext = "mp3"
    else:
        ext = "m4a"

    suffix      = "_remux" if output_type == "remux" else ""
    output_file = os.path.join(job_dir, f"{base}{suffix}.{ext}")

    # ── Watermark config ──────────────────────────────────────────────────────
    wm_volume      = 0.2
    fade_duration  = 1.0

    if use_wm:
        if wm_choice == "owner":
            cfg           = await get_owner_watermark()
        else:
            cfg           = await get_admin_watermark(conv.get("user_id") or 0) or {}
        wm_volume     = float(cfg.get("volume", 0.2))
        fade_duration = float(cfg.get("fade_duration", 1.0))

    if job_wm_vol is not None:
        wm_volume = float(job_wm_vol)

    # Force re-encode if copy+watermark requested
    if fmt.get("codec") == "copy" and use_wm:
        fmt = {"codec": "aac", "channels": 2, "bitrate": "192k"}
        codec_name = "aac"

    # ── Base ffmpeg command ───────────────────────────────────────────────────
    args = ["ffmpeg", "-y", "-hide_banner", "-i", input_file]

    filter_parts   = []
    audio_out_lbl  = f"0:{track_index}"   # updated below if filtering

    # ── Watermark filter complex ──────────────────────────────────────────────
    if use_wm and watermark_local:
        try:
            wm_dur = await get_duration(watermark_local)
        except Exception:
            wm_dur = 0.0

        try:
            in_dur = await get_duration(input_file)
        except Exception:
            in_dur = 0.0

        args.extend(["-i", watermark_local])

        # Build position list (seconds)
        raw: list[float] = []
        for p in wm_positions:
            if p == "start":
                t = min(WM_START_OFFSET, max(0.0, in_dur - wm_dur))
                raw.append(t)
            elif p == "middle":
                raw.append(max(0.0, in_dur / 2.0 - wm_dur / 2.0))
            elif p == "end":
                t = in_dur - (WM_END_OFFSET + wm_dur)
                raw.append(max(0.0, t if t >= 0 else max(0.0, in_dur - wm_dur)))
            elif p == "custom":
                raw.append(max(0.0, float(wm_custom_s)))
            elif p == "hourly":
                t     = 3600.0
                upper = max(0.0, in_dur - wm_dur)
                while t <= upper:
                    raw.append(t)
                    t += 3600.0

        positions = sorted(set(int(max(0, p)) for p in raw))
        n = len(positions)

        if n > 0:
            # Split watermark into N copies
            filter_parts.append(
                f"[1:a]asplit={n}" + "".join(f"[wm{i}]" for i in range(n))
            )
            # Main audio passthrough (no heavy processing)
            filter_parts.append(f"[0:{track_index}]anull[main]")

            for i, pos_s in enumerate(positions):
                delay_ms = pos_s * 1000
                # Build per-instance chain
                chain = f"volume={wm_volume:.4f}"
                if fade_duration > 0:
                    fd  = fade_duration
                    # fade in at start of this instance
                    chain = f"afade=t=in:st=0:d={fd:.3f},{chain}"
                    # fade out at end of wm clip
                    out_t = max(0.0, wm_dur - fd)
                    chain = f"{chain},afade=t=out:st={out_t:.3f}:d={fd:.3f}"
                # adelay:all=1 → sample-accurate, all channels
                # apad → ensure amix never truncates main
                filter_parts.append(
                    f"[wm{i}]{chain},adelay={int(delay_ms)}:all=1,apad[wm{i}_out]"
                )

            inputs    = "[main]" + "".join(f"[wm{i}_out]" for i in range(n))
            n_inputs  = 1 + n
            # duration=longest → main track length is always preserved exactly
            filter_parts.append(
                f"{inputs}amix=inputs={n_inputs}:duration=longest:dropout_transition=0[aud_out]"
            )
            audio_out_lbl = "[aud_out]"
        else:
            # No valid positions — route main as-is
            filter_parts.append(f"[0:{track_index}]anull[aud_out]")
            audio_out_lbl = "[aud_out]"

    else:
        # No watermark — pass main audio through trivially so we can still map it
        filter_parts.append(f"[0:{track_index}]anull[aud_out]")
        audio_out_lbl = "[aud_out]"

    # Apply filter_complex
    args.extend(["-filter_complex", ";".join(filter_parts)])

    # Map processed audio
    args.extend(["-map", audio_out_lbl])

    # ── Audio codec settings ──────────────────────────────────────────────────
    if fmt.get("codec") == "copy":
        # Copy mode: remove filter_complex and remap directly
        # Rebuild for copy — filter_complex is incompatible with -c:a copy
        args = ["ffmpeg", "-y", "-hide_banner", "-i", input_file]
        args.extend(["-map", f"0:{track_index}", "-c:a", "copy"])
    else:
        args.extend([
            "-c:a",  fmt["codec"],
            "-b:a",  fmt.get("bitrate", "192k"),
            "-ac",   str(fmt.get("channels", 2)),
            "-ar",   "48000",   # fixed sample rate — key for lip-sync
        ])

    # ── Default metadata ──────────────────────────────────────────────────────
    meta = await get_default_metadata()
    for key, val in meta.items():
        if val:
            args.extend(["-metadata", f"{key}={val}"])

    # Language tag on the output audio stream
    lang = (stream_obj.get("tags") or {}).get("language", "und")
    if meta.get("language"):
        lang = meta["language"]
    args.extend(["-metadata:s:a:0", f"language={lang}"])

    # ── Remux: copy video + subtitles ────────────────────────────────────────
    if output_type == "remux":
        args.extend([
            "-map",  "0:v:0?",   # video (optional — may not exist)
            "-map",  "0:s?",     # subtitles (optional)
            "-c:v",  "copy",
            "-c:s",  "copy",
        ])

    args.append(output_file)
    return args, output_file


# ═══════════════════════════════════════════════════════════════════════════════
# BOT INSTANCE
# ═══════════════════════════════════════════════════════════════════════════════

bot = Client(
    "AudioBot",
    api_id=Config.API_ID,
    api_hash=Config.API_HASH,
    bot_token=Config.BOT_TOKEN,
)

# ═══════════════════════════════════════════════════════════════════════════════
# /start  AND  MAIN MENU
# ═══════════════════════════════════════════════════════════════════════════════

def _main_kb(user_id: int) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton("🎧 Audio Tools",       callback_data="audio_tools_menu")],
        [InlineKeyboardButton("➕ Send Audio/Video",   callback_data="quick_send")],
        [InlineKeyboardButton("📊 My Stats",           callback_data="my_stats")],
    ]
    if user_id == Config.OWNER_ID:
        rows.append([InlineKeyboardButton("⚙️ Owner Settings",    callback_data="owner_settings_menu")])
        rows.append([InlineKeyboardButton("👨‍💼 Admin Management", callback_data="admin_menu")])
    return InlineKeyboardMarkup(rows)


@bot.on_message(filters.command("start") & filters.private & admin_filter)
async def cmd_start(client, msg: Message):
    uid = msg.from_user.id
    await register_user(uid,
                        username=getattr(msg.from_user, "username", None),
                        first_name=getattr(msg.from_user, "first_name", None))
    await update_user_conversation(msg.chat.id, None)
    await msg.reply_text(
        "**🎧 Audio Converter Bot**\n\n"
        "Convert audio tracks, mix watermarks, remux video — all in Telegram.",
        reply_markup=_main_kb(uid),
    )


@bot.on_callback_query(filters.regex("^main_menu$") & admin_filter)
async def cb_main_menu(client, cb: CallbackQuery):
    await cb.answer()
    await update_user_conversation(cb.message.chat.id, None)
    try:
        await cb.message.edit_text(
            "**🎧 Audio Converter Bot**\n\nChoose an option:",
            reply_markup=_main_kb(cb.from_user.id),
        )
    except MessageNotModified:
        pass


# ═══════════════════════════════════════════════════════════════════════════════
# AUDIO TOOLS MENU
# ═══════════════════════════════════════════════════════════════════════════════

@bot.on_callback_query(filters.regex("^audio_tools_menu$") & admin_filter)
async def cb_audio_tools(client, cb: CallbackQuery):
    await cb.answer()
    await cb.message.edit_text(
        "**🎧 Audio Tools**",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🎵 Convert Audio/Video",   callback_data="convert_audio_start")],
            [InlineKeyboardButton("⚙️ My Watermark Settings", callback_data="admin_watermark_settings")],
            [InlineKeyboardButton("⬅️ Back",                  callback_data="main_menu")],
        ]),
    )


# ═══════════════════════════════════════════════════════════════════════════════
# MY STATS
# ═══════════════════════════════════════════════════════════════════════════════

@bot.on_callback_query(filters.regex("^my_stats$") & admin_filter)
async def cb_my_stats(client, cb: CallbackQuery):
    await cb.answer()
    uid    = cb.from_user.id
    stats  = await get_admin_stats(uid)
    limits = await get_admin_limits(uid)
    last   = stats["last_job_ts"].strftime("%Y-%m-%d %H:%M UTC") if stats["last_job_ts"] else "Never"
    await cb.message.edit_text(
        f"**📊 Your Statistics**\n\n"
        f"Total jobs: `{stats['total_jobs']}`\n"
        f"Jobs today: `{stats['today_jobs']}`\n"
        f"Total data: `{human_size(stats['total_bytes'])}`\n"
        f"Last job:   `{last}`\n\n"
        f"**Limits:**\n"
        f"Max jobs/day: `{'Unlimited' if not limits['max_jobs_per_day'] else limits['max_jobs_per_day']}`\n"
        f"Max file size: `{'Unlimited' if not limits['max_file_size_mb'] else str(limits['max_file_size_mb']) + ' MB'}`",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="main_menu")]]),
    )


# ═══════════════════════════════════════════════════════════════════════════════
# QUICK SEND  /  CONVERT START
# ═══════════════════════════════════════════════════════════════════════════════

async def _init_new_job(chat_id: int) -> dict:
    job_id  = str(uuid.uuid4())
    job_dir = os.path.join(Config.DOWNLOAD_DIR, job_id)
    os.makedirs(job_dir, exist_ok=True)
    data = {"stage": "awaiting_media_file", "job_id": job_id, "job_dir": job_dir}
    await update_user_conversation(chat_id, data)
    return data


@bot.on_callback_query(filters.regex("^quick_send$") & admin_filter)
async def cb_quick_send(client, cb: CallbackQuery):
    await cb.answer()
    await _init_new_job(cb.message.chat.id)
    await cb.message.edit_text(
        "🎵 **Send File or URL**\n\n"
        "• Send an audio/video file directly, **or**\n"
        "• Send a direct HTTP/HTTPS download link.",
        reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton("❌ Cancel", callback_data="cancel_conv")]]
        ),
    )


@bot.on_callback_query(filters.regex("^convert_audio_start$") & admin_filter)
async def cb_convert_start(client, cb: CallbackQuery):
    await cb.answer()
    await _init_new_job(cb.message.chat.id)
    await cb.message.edit_text(
        "🎵 **Send File or URL**\n\n"
        "• Send an audio/video file directly, **or**\n"
        "• Send a direct HTTP/HTTPS download link.",
        reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton("❌ Cancel", callback_data="cancel_conv")]]
        ),
    )


# ═══════════════════════════════════════════════════════════════════════════════
# CANCEL HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

@bot.on_callback_query(filters.regex("^cancel_conv$") & admin_filter)
async def cb_cancel_conv(client, cb: CallbackQuery):
    await cb.answer("Cancelled.")
    chat_id = cb.message.chat.id
    conv    = await get_user_conversation(chat_id)
    if conv:
        jd = conv.get("job_dir")
        if jd and os.path.isdir(jd):
            shutil.rmtree(jd, ignore_errors=True)
    CANCEL_DOWNLOADS.discard(chat_id)
    asyncio.create_task(clear_conversation_after_delay(chat_id, delay=1))
    try:
        await cb.message.delete()
    except Exception:
        pass
    await cmd_start(client, cb.message)


@bot.on_callback_query(filters.regex("^cancel_download_process$") & admin_filter)
async def cb_cancel_download(client, cb: CallbackQuery):
    CANCEL_DOWNLOADS.add(cb.message.chat.id)
    await cb.answer("Stopping download…", show_alert=True)


@bot.on_callback_query(filters.regex(r"^cancel_job\|(.+)$") & admin_filter)
async def cb_cancel_job(client, cb: CallbackQuery):
    job_id  = cb.data.split("|", 1)[1]
    tracker = JOB_TRACKERS.get(job_id)
    if tracker is None:
        await cb.answer("Job not found or already done.", show_alert=True)
        return
    tracker["cancelled"] = True
    await cb.answer("Cancel requested.")
    await cb.message.edit_text("⛔ Cancelling… please wait.")


# ═══════════════════════════════════════════════════════════════════════════════
# OWNER SETTINGS MENU
# ═══════════════════════════════════════════════════════════════════════════════

@bot.on_callback_query(filters.regex("^owner_settings_menu$") & filters.user(Config.OWNER_ID))
async def cb_owner_settings(client, cb: CallbackQuery):
    await cb.answer()
    wm = await get_owner_watermark()
    st = "🟢 Set" if wm.get("file_id") else "🔴 Not set"
    await cb.message.edit_text(
        f"**⚙️ Owner Settings**\n\n"
        f"Watermark: {st}\n"
        f"Volume:    `{int(wm.get('volume', 0.2) * 100)}%`\n"
        f"Positions: `{', '.join(wm.get('positions', []))}`\n"
        f"Fade:      `{wm.get('fade_duration', 1.0)}s`",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("⬆️ Upload Watermark",  callback_data="owner_wm_upload")],
            [InlineKeyboardButton("🔊 Volume",             callback_data="owner_wm_volume")],
            [InlineKeyboardButton("📍 Positions",          callback_data="owner_wm_positions")],
            [InlineKeyboardButton("🌊 Fade",               callback_data="owner_wm_fade")],
            [InlineKeyboardButton("🗑️ Delete Watermark",  callback_data="owner_wm_delete")],
            [InlineKeyboardButton("✏️ Default Metadata",   callback_data="owner_metadata_menu")],
            [InlineKeyboardButton("⬅️ Back",              callback_data="main_menu")],
        ]),
    )


# ── Owner WM upload ────────────────────────────────────────────────────────────

@bot.on_callback_query(filters.regex("^owner_wm_upload$") & filters.user(Config.OWNER_ID))
async def cb_owner_wm_upload(client, cb: CallbackQuery):
    await cb.answer()
    await update_user_conversation(cb.message.chat.id, {"stage": "awaiting_owner_wm"})
    await cb.message.edit_text(
        "📥 Send a short audio file as the owner watermark.",
        reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton("❌ Cancel", callback_data="owner_settings_menu")]]
        ),
    )


@bot.on_callback_query(filters.regex("^owner_wm_delete$") & filters.user(Config.OWNER_ID))
async def cb_owner_wm_delete(client, cb: CallbackQuery):
    await delete_owner_watermark()
    await cb.answer("Watermark deleted.")
    await cb_owner_settings(client, cb)


# ── Owner volume ───────────────────────────────────────────────────────────────

@bot.on_callback_query(filters.regex("^owner_wm_volume$") & filters.user(Config.OWNER_ID))
async def cb_owner_wm_vol(client, cb: CallbackQuery):
    await cb.answer()
    await cb.message.edit_text(
        "🔊 Owner watermark volume:",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("10%",  callback_data="owner_vol_0.10"),
             InlineKeyboardButton("20%",  callback_data="owner_vol_0.20"),
             InlineKeyboardButton("30%",  callback_data="owner_vol_0.30")],
            [InlineKeyboardButton("50%",  callback_data="owner_vol_0.50"),
             InlineKeyboardButton("80%",  callback_data="owner_vol_0.80"),
             InlineKeyboardButton("100%", callback_data="owner_vol_1.00")],
            [InlineKeyboardButton("⬅️ Back", callback_data="owner_settings_menu")],
        ]),
    )


@bot.on_callback_query(filters.regex(r"^owner_vol_(\d+\.\d+)$") & filters.user(Config.OWNER_ID))
async def cb_owner_vol_set(client, cb: CallbackQuery):
    vol = float(re.search(r"(\d+\.\d+)$", cb.data).group(1))
    await set_owner_watermark_volume(vol)
    await cb.answer(f"Volume → {int(vol * 100)}%")
    await cb_owner_settings(client, cb)


# ── Owner positions ────────────────────────────────────────────────────────────

@bot.on_callback_query(filters.regex("^owner_wm_positions$") & filters.user(Config.OWNER_ID))
async def cb_owner_wm_pos(client, cb: CallbackQuery):
    await cb.answer()
    wm      = await get_owner_watermark()
    current = set(wm.get("positions", ["start", "end"]))
    all_o   = {"start", "middle", "end", "hourly"}
    sel_lbl = "❌ Deselect All" if all_o.issubset(current) else "✅ Select All"

    def mk(n, lbl=None):
        mark = "✅" if n in current else "❌"
        return InlineKeyboardButton(f"{mark} {lbl or n.capitalize()}",
                                    callback_data=f"owner_tpos_{n}")

    await cb.message.edit_text(
        "📍 Owner watermark positions:",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton(sel_lbl, callback_data="owner_pos_selall")],
            [mk("start", "Start (+3 min)"), mk("middle", "Middle")],
            [mk("end",   "End (−7 min)"),   mk("hourly", "Hourly (60 min)")],
            [InlineKeyboardButton("🔢 Custom Seconds", callback_data="owner_pos_custom_prompt")],
            [InlineKeyboardButton("➡️ Done",  callback_data="owner_settings_menu"),
             InlineKeyboardButton("⬅️ Back", callback_data="owner_settings_menu")],
        ]),
    )


@bot.on_callback_query(filters.regex("^owner_pos_selall$") & filters.user(Config.OWNER_ID))
async def cb_owner_pos_selall(client, cb: CallbackQuery):
    await cb.answer()
    wm      = await get_owner_watermark()
    current = set(wm.get("positions", []))
    all_o   = {"start", "middle", "end", "hourly"}
    new_pos = ["start"] if all_o.issubset(current) else list(all_o)
    await set_owner_watermark_positions(new_pos, wm.get("custom_seconds", 0))
    await cb_owner_wm_pos(client, cb)


@bot.on_callback_query(
    filters.regex(r"^owner_tpos_(start|middle|end|hourly)$") & filters.user(Config.OWNER_ID)
)
async def cb_owner_tpos(client, cb: CallbackQuery):
    await cb.answer()
    pos     = cb.data.split("_")[-1]
    wm      = await get_owner_watermark()
    current = set(wm.get("positions", ["start", "end"]))
    current.discard(pos) if pos in current else current.add(pos)
    if not current:
        current = {"start"}
    await set_owner_watermark_positions(list(current), wm.get("custom_seconds", 0))
    await cb_owner_wm_pos(client, cb)


@bot.on_callback_query(filters.regex("^owner_pos_custom_prompt$") & filters.user(Config.OWNER_ID))
async def cb_owner_pos_custom(client, cb: CallbackQuery):
    await cb.answer()
    await update_user_conversation(cb.message.chat.id, {"stage": "awaiting_owner_pos_custom"})
    await cb.message.edit_text(
        "🔢 Send custom seconds (e.g. 1800 = 30 min).",
        reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton("❌ Cancel", callback_data="owner_settings_menu")]]
        ),
    )


# ── Owner fade ─────────────────────────────────────────────────────────────────

@bot.on_callback_query(filters.regex("^owner_wm_fade$") & filters.user(Config.OWNER_ID))
async def cb_owner_wm_fade(client, cb: CallbackQuery):
    await cb.answer()
    await cb.message.edit_text(
        "🌊 Watermark fade in/out duration:",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("0s (off)", callback_data="owner_fade_0.0"),
             InlineKeyboardButton("0.5s",     callback_data="owner_fade_0.5"),
             InlineKeyboardButton("1s",       callback_data="owner_fade_1.0")],
            [InlineKeyboardButton("2s",       callback_data="owner_fade_2.0"),
             InlineKeyboardButton("3s",       callback_data="owner_fade_3.0")],
            [InlineKeyboardButton("⬅️ Back",  callback_data="owner_settings_menu")],
        ]),
    )


@bot.on_callback_query(filters.regex(r"^owner_fade_(\d+\.\d+)$") & filters.user(Config.OWNER_ID))
async def cb_owner_fade_set(client, cb: CallbackQuery):
    fd = float(re.search(r"(\d+\.\d+)$", cb.data).group(1))
    await set_owner_watermark_fade(fd)
    await cb.answer(f"Fade → {fd}s")
    await cb_owner_settings(client, cb)


# ═══════════════════════════════════════════════════════════════════════════════
# DEFAULT METADATA  (owner only)
# ═══════════════════════════════════════════════════════════════════════════════

@bot.on_callback_query(filters.regex("^owner_metadata_menu$") & filters.user(Config.OWNER_ID))
async def cb_metadata_menu(client, cb: CallbackQuery):
    await cb.answer()
    meta = await get_default_metadata()
    await cb.message.edit_text(
        f"**✏️ Default Metadata**\n\n"
        f"Title:    `{meta['title']    or '(not set)'}`\n"
        f"Artist:   `{meta['artist']   or '(not set)'}`\n"
        f"Album:    `{meta['album']    or '(not set)'}`\n"
        f"Language: `{meta['language'] or '(not set)'}`\n"
        f"Comment:  `{meta['comment']  or '(not set)'}`",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("✏️ Title",    callback_data="meta_set_title"),
             InlineKeyboardButton("✏️ Artist",   callback_data="meta_set_artist")],
            [InlineKeyboardButton("✏️ Album",    callback_data="meta_set_album"),
             InlineKeyboardButton("✏️ Language", callback_data="meta_set_language")],
            [InlineKeyboardButton("✏️ Comment",  callback_data="meta_set_comment")],
            [InlineKeyboardButton("🗑️ Clear All", callback_data="meta_clear_all")],
            [InlineKeyboardButton("⬅️ Back",      callback_data="owner_settings_menu")],
        ]),
    )


@bot.on_callback_query(
    filters.regex(r"^meta_set_(title|artist|album|language|comment)$")
    & filters.user(Config.OWNER_ID)
)
async def cb_meta_set_field(client, cb: CallbackQuery):
    await cb.answer()
    field = cb.data.split("_")[-1]
    await update_user_conversation(cb.message.chat.id, {"stage": f"awaiting_meta_{field}"})
    await cb.message.edit_text(
        f"✏️ Send new **{field}** (or `-` to clear).",
        reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton("❌ Cancel", callback_data="owner_metadata_menu")]]
        ),
    )


@bot.on_callback_query(filters.regex("^meta_clear_all$") & filters.user(Config.OWNER_ID))
async def cb_meta_clear(client, cb: CallbackQuery):
    await set_default_metadata({"title": "", "artist": "", "album": "", "language": "", "comment": ""})
    await cb.answer("Metadata cleared.")
    await cb_metadata_menu(client, cb)


# ═══════════════════════════════════════════════════════════════════════════════
# ADMIN WATERMARK SETTINGS  (role-aware: admins see ONLY their own settings)
# ═══════════════════════════════════════════════════════════════════════════════

@bot.on_callback_query(filters.regex("^admin_watermark_settings$") & admin_filter)
async def cb_admin_wm_settings(client, cb: CallbackQuery):
    await cb.answer()
    uid  = cb.from_user.id
    wm   = await get_admin_watermark(uid)
    st   = "🟢 Set" if wm and wm.get("file_id") else "🔴 Not set"
    body = f"**⚙️ Your Watermark**\n\nStatus: {st}\n"
    if wm:
        body += (
            f"Volume:    `{int(wm.get('volume', 0.2) * 100)}%`\n"
            f"Positions: `{', '.join(wm.get('positions', []))}`\n"
            f"Fade:      `{wm.get('fade_duration', 1.0)}s`\n"
        )
    await cb.message.edit_text(
        body,
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("⬆️ Upload Watermark", callback_data="admin_wm_upload")],
            [InlineKeyboardButton("🔊 Volume",            callback_data="admin_wm_volume")],
            [InlineKeyboardButton("📍 Positions",         callback_data="admin_wm_positions")],
            [InlineKeyboardButton("🌊 Fade",              callback_data="admin_wm_fade")],
            [InlineKeyboardButton("⬅️ Back",             callback_data="audio_tools_menu")],
        ]),
    )


@bot.on_callback_query(filters.regex("^admin_wm_upload$") & admin_filter)
async def cb_admin_wm_upload(client, cb: CallbackQuery):
    await cb.answer()
    await update_user_conversation(cb.message.chat.id, {"stage": "awaiting_admin_wm"})
    await cb.message.edit_text(
        "📥 Send a short audio file as your watermark.",
        reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton("❌ Cancel", callback_data="admin_watermark_settings")]]
        ),
    )


@bot.on_callback_query(filters.regex("^admin_wm_volume$") & admin_filter)
async def cb_admin_wm_vol(client, cb: CallbackQuery):
    await cb.answer()
    wm = await get_admin_watermark(cb.from_user.id)
    if not wm or not wm.get("file_id"):
        await cb.answer("Upload a watermark first.", show_alert=True)
        return
    await cb.message.edit_text(
        "🔊 Your watermark volume:",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("10%",  callback_data="admin_vol_0.10"),
             InlineKeyboardButton("20%",  callback_data="admin_vol_0.20"),
             InlineKeyboardButton("30%",  callback_data="admin_vol_0.30")],
            [InlineKeyboardButton("50%",  callback_data="admin_vol_0.50"),
             InlineKeyboardButton("80%",  callback_data="admin_vol_0.80"),
             InlineKeyboardButton("100%", callback_data="admin_vol_1.00")],
            [InlineKeyboardButton("⬅️ Back", callback_data="admin_watermark_settings")],
        ]),
    )


@bot.on_callback_query(filters.regex(r"^admin_vol_(\d+\.\d+)$") & admin_filter)
async def cb_admin_vol_set(client, cb: CallbackQuery):
    vol = float(re.search(r"(\d+\.\d+)$", cb.data).group(1))
    await set_admin_watermark_volume(cb.from_user.id, vol)
    await cb.answer(f"Volume → {int(vol * 100)}%")
    await cb_admin_wm_settings(client, cb)


@bot.on_callback_query(filters.regex("^admin_wm_positions$") & admin_filter)
async def cb_admin_wm_pos(client, cb: CallbackQuery):
    await cb.answer()
    uid     = cb.from_user.id
    wm      = await get_admin_watermark(uid) or {"positions": ["start", "end"]}
    current = set(wm.get("positions", ["start", "end"]))
    all_o   = {"start", "middle", "end", "hourly"}
    sel_lbl = "❌ Deselect All" if all_o.issubset(current) else "✅ Select All"

    def mk(n, lbl=None):
        mark = "✅" if n in current else "❌"
        return InlineKeyboardButton(f"{mark} {lbl or n.capitalize()}",
                                    callback_data=f"admin_tpos_{n}")

    await cb.message.edit_text(
        "📍 Your watermark positions:",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton(sel_lbl, callback_data="admin_pos_selall")],
            [mk("start", "Start (+3 min)"), mk("middle", "Middle")],
            [mk("end",   "End (−7 min)"),   mk("hourly", "Hourly (60 min)")],
            [InlineKeyboardButton("🔢 Custom Seconds", callback_data="admin_pos_custom_prompt")],
            [InlineKeyboardButton("➡️ Done",  callback_data="admin_watermark_settings"),
             InlineKeyboardButton("⬅️ Back", callback_data="admin_watermark_settings")],
        ]),
    )


@bot.on_callback_query(filters.regex("^admin_pos_selall$") & admin_filter)
async def cb_admin_pos_selall(client, cb: CallbackQuery):
    await cb.answer()
    uid     = cb.from_user.id
    wm      = await get_admin_watermark(uid) or {"positions": ["start", "end"], "custom_seconds": 0}
    current = set(wm.get("positions", []))
    all_o   = {"start", "middle", "end", "hourly"}
    new_pos = ["start"] if all_o.issubset(current) else list(all_o)
    await update_admin_watermark_positions(uid, new_pos, wm.get("custom_seconds", 0))
    await cb_admin_wm_pos(client, cb)


@bot.on_callback_query(
    filters.regex(r"^admin_tpos_(start|middle|end|hourly)$") & admin_filter
)
async def cb_admin_tpos(client, cb: CallbackQuery):
    await cb.answer()
    uid     = cb.from_user.id
    pos     = cb.data.split("_")[-1]
    wm      = await get_admin_watermark(uid) or {"positions": ["start", "end"], "custom_seconds": 0}
    current = set(wm.get("positions", ["start", "end"]))
    current.discard(pos) if pos in current else current.add(pos)
    if not current:
        current = {"start"}
    await update_admin_watermark_positions(uid, list(current), wm.get("custom_seconds", 0))
    await cb_admin_wm_pos(client, cb)


@bot.on_callback_query(filters.regex("^admin_pos_custom_prompt$") & admin_filter)
async def cb_admin_pos_custom(client, cb: CallbackQuery):
    await cb.answer()
    await update_user_conversation(cb.message.chat.id, {"stage": "awaiting_admin_pos_custom"})
    await cb.message.edit_text(
        "🔢 Send custom seconds (e.g. 1800 = 30 min).",
        reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton("❌ Cancel", callback_data="admin_watermark_settings")]]
        ),
    )


@bot.on_callback_query(filters.regex("^admin_wm_fade$") & admin_filter)
async def cb_admin_wm_fade(client, cb: CallbackQuery):
    await cb.answer()
    await cb.message.edit_text(
        "🌊 Your watermark fade duration:",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("0s (off)", callback_data="admin_fade_0.0"),
             InlineKeyboardButton("0.5s",     callback_data="admin_fade_0.5"),
             InlineKeyboardButton("1s",       callback_data="admin_fade_1.0")],
            [InlineKeyboardButton("2s",       callback_data="admin_fade_2.0"),
             InlineKeyboardButton("3s",       callback_data="admin_fade_3.0")],
            [InlineKeyboardButton("⬅️ Back",  callback_data="admin_watermark_settings")],
        ]),
    )


@bot.on_callback_query(filters.regex(r"^admin_fade_(\d+\.\d+)$") & admin_filter)
async def cb_admin_fade_set(client, cb: CallbackQuery):
    fd = float(re.search(r"(\d+\.\d+)$", cb.data).group(1))
    await set_admin_watermark_fade(cb.from_user.id, fd)
    await cb.answer(f"Fade → {fd}s")
    await cb_admin_wm_settings(client, cb)


# ═══════════════════════════════════════════════════════════════════════════════
# ADMIN MANAGEMENT  (owner only)
# ═══════════════════════════════════════════════════════════════════════════════

@bot.on_callback_query(filters.regex("^admin_menu$") & filters.user(Config.OWNER_ID))
async def cb_admin_menu(client, cb: CallbackQuery):
    await cb.answer()
    await cb.message.edit_text(
        "**👨‍💼 Admin Management**",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("➕ Add Admin",           callback_data="admin_add")],
            [InlineKeyboardButton("➖ Remove Admin",        callback_data="admin_remove_list")],
            [InlineKeyboardButton("📋 Stats Leaderboard",  callback_data="admin_list_stats")],
            [InlineKeyboardButton("⚙️ Set Limits",         callback_data="admin_set_limits_list")],
            [InlineKeyboardButton("⬅️ Back",               callback_data="main_menu")],
        ]),
    )


@bot.on_callback_query(filters.regex("^admin_add$") & filters.user(Config.OWNER_ID))
async def cb_admin_add(client, cb: CallbackQuery):
    await cb.answer()
    await update_user_conversation(cb.message.chat.id, {"stage": "awaiting_admin_id"})
    await cb.message.edit_text(
        "➕ Send the user ID to add as admin.",
        reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton("❌ Cancel", callback_data="admin_menu")]]
        ),
    )


@bot.on_callback_query(filters.regex("^admin_remove_list$") & filters.user(Config.OWNER_ID))
async def cb_admin_remove_list(client, cb: CallbackQuery):
    await cb.answer()
    admins = await get_admin_list()
    if not admins:
        return await cb.message.edit_text(
            "No admins found.",
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")]]
            ),
        )
    rows = []
    for aid in admins:
        try:
            u    = await client.get_users(aid)
            name = u.first_name or f"User {aid}"
        except Exception:
            name = f"User {aid}"
        rows.append([InlineKeyboardButton(f"❌ {name} ({aid})",
                                           callback_data=f"admin_rm_{aid}")])
    rows.append([InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")])
    await cb.message.edit_text("➖ Tap to remove:", reply_markup=InlineKeyboardMarkup(rows))


@bot.on_callback_query(filters.regex(r"^admin_rm_(\d+)$") & filters.user(Config.OWNER_ID))
async def cb_admin_rm(client, cb: CallbackQuery):
    uid = int(re.search(r"(\d+)$", cb.data).group(1))
    await remove_admin(uid)
    await cb.answer(f"Admin {uid} removed.", show_alert=True)
    await cb_admin_remove_list(client, cb)


@bot.on_callback_query(filters.regex("^admin_list_stats$") & filters.user(Config.OWNER_ID))
async def cb_admin_list_stats(client, cb: CallbackQuery):
    await cb.answer()
    admins = await get_admin_list()
    if not admins:
        return await cb.message.edit_text(
            "No admins.",
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")]]
            ),
        )
    text = "**📋 Admin Stats**\n\n"
    for aid in admins[:15]:
        try:
            u    = await client.get_users(aid)
            name = u.first_name or f"User {aid}"
        except Exception:
            name = f"User {aid}"
        s  = await get_admin_stats(aid)
        lm = await get_admin_limits(aid)
        text += (
            f"👤 **{name}** (`{aid}`)\n"
            f"   Jobs: `{s['total_jobs']}` total / `{s['today_jobs']}` today\n"
            f"   Data: `{human_size(s['total_bytes'])}`\n"
            f"   Limits: `{lm['max_jobs_per_day'] or '∞'}` jobs/day, "
            f"`{lm['max_file_size_mb'] or '∞'} MB`\n\n"
        )
    await cb.message.edit_text(
        text,
        reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")]]
        ),
    )


@bot.on_callback_query(filters.regex("^admin_set_limits_list$") & filters.user(Config.OWNER_ID))
async def cb_admin_limits_list(client, cb: CallbackQuery):
    await cb.answer()
    admins = await get_admin_list()
    if not admins:
        return await cb.message.edit_text(
            "No admins.",
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")]]
            ),
        )
    rows = []
    for aid in admins:
        try:
            u    = await client.get_users(aid)
            name = u.first_name or f"User {aid}"
        except Exception:
            name = f"User {aid}"
        rows.append([InlineKeyboardButton(f"⚙️ {name}", callback_data=f"admin_lim_{aid}")])
    rows.append([InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")])
    await cb.message.edit_text("Choose admin:", reply_markup=InlineKeyboardMarkup(rows))


@bot.on_callback_query(filters.regex(r"^admin_lim_(\d+)$") & filters.user(Config.OWNER_ID))
async def cb_admin_lim_pick(client, cb: CallbackQuery):
    await cb.answer()
    tid    = int(re.search(r"(\d+)$", cb.data).group(1))
    limits = await get_admin_limits(tid)
    await update_user_conversation(cb.message.chat.id, {
        "stage":            "awaiting_admin_limits",
        "limits_target_id": tid,
    })
    await cb.message.edit_text(
        f"**⚙️ Limits for `{tid}`**\n\n"
        f"Current: `{limits['max_jobs_per_day'] or '∞'}` jobs/day, "
        f"`{limits['max_file_size_mb'] or '∞'} MB`\n\n"
        f"Send: `<max_jobs_per_day> <max_file_size_mb>`\n"
        f"Use 0 for unlimited. Example: `10 500`",
        reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton("❌ Cancel", callback_data="admin_set_limits_list")]]
        ),
    )


# ═══════════════════════════════════════════════════════════════════════════════
# GENERIC MESSAGE HANDLER ROUTER
# ═══════════════════════════════════════════════════════════════════════════════

async def _doc_is_media(doc) -> bool:
    if not doc:
        return False
    mime = getattr(doc, "mime_type", "") or ""
    name = getattr(doc, "file_name", "") or ""
    if mime.startswith("audio") or mime.startswith("video"):
        return True
    return safe_ext(name) in MEDIA_EXTS


def _is_url(text: str) -> bool:
    """Return True if the text looks like a direct HTTP(S) download URL."""
    if not text:
        return False
    text = text.strip()
    return bool(re.match(r"^https?://\S+$", text, re.IGNORECASE))


@bot.on_message(
    filters.private
    & (filters.audio | filters.video | filters.document | filters.text | filters.voice)
    & admin_filter
)
async def msg_router(client, msg: Message):
    chat_id = msg.chat.id
    uid     = msg.from_user.id

    # Keep user registry current (never deletes)
    await register_user(uid,
                        username=getattr(msg.from_user, "username", None),
                        first_name=getattr(msg.from_user, "first_name", None))

    try:
        conv = await get_user_conversation(chat_id)
    except Exception as e:
        LOGGER.error(f"get_user_conversation: {e}")
        conv = None

    # Detect what was sent
    sent_media = None
    if   msg.audio:                       sent_media = ("audio",    msg.audio)
    elif msg.video:                       sent_media = ("video",    msg.video)
    elif msg.document:                    sent_media = ("document", msg.document)
    elif getattr(msg, "voice", None):     sent_media = ("audio",    msg.voice)

    is_url_msg = (not sent_media and msg.text and _is_url(msg.text))

    # Auto-create session if media/URL arrives with no active session
    if not conv and (sent_media or is_url_msg):
        if sent_media and sent_media[0] == "document" and not await _doc_is_media(sent_media[1]):
            await msg.reply_text(
                "No active session. Use **Audio Tools → Convert Audio** first.",
                quote=True,
            )
            return
        conv = await _init_new_job(chat_id)
        LOGGER.info(f"Auto-created session for {chat_id}")

    if not conv:
        await msg.reply_text(
            "No active session. Use **Audio Tools → Convert Audio** to start.",
            quote=True,
        )
        return

    stage = conv.get("stage", "")
    LOGGER.debug(f"chat={chat_id} stage={stage} uid={uid}")

    # ── Owner WM upload ────────────────────────────────────────────────────
    if stage == "awaiting_owner_wm":
        if uid != Config.OWNER_ID:
            await msg.reply_text("Permission denied.")
            return
        doc = msg.audio or getattr(msg, "voice", None) or msg.document
        if not doc or not await _doc_is_media(doc):
            await msg.reply_text("Please send an audio file.")
            return
        await set_owner_watermark_file(doc.file_id)
        await update_user_conversation(chat_id, None)
        await msg.reply_text(
            "✅ Owner watermark saved.",
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("⬅️ Back", callback_data="owner_settings_menu")]]
            ),
        )
        return

    # ── Admin WM upload ────────────────────────────────────────────────────
    if stage == "awaiting_admin_wm":
        doc = msg.audio or getattr(msg, "voice", None) or msg.document
        if not doc or not await _doc_is_media(doc):
            await msg.reply_text("Please send an audio file.")
            return
        await set_admin_watermark(uid, doc.file_id)
        await update_user_conversation(chat_id, None)
        await msg.reply_text(
            "✅ Your watermark saved.",
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("⬅️ Back", callback_data="admin_watermark_settings")]]
            ),
        )
        return

    # ── Owner custom position seconds ──────────────────────────────────────
    if stage == "awaiting_owner_pos_custom":
        if uid != Config.OWNER_ID:
            return
        if not msg.text:
            await msg.reply_text("Send a number.")
            return
        try:
            secs = int(msg.text.strip())
            wm   = await get_owner_watermark()
            await set_owner_watermark_positions(wm.get("positions", ["start", "end"]), secs)
            await update_user_conversation(chat_id, None)
            await msg.reply_text(
                f"✅ Custom position set to {secs}s.",
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("⬅️ Back", callback_data="owner_settings_menu")]]
                ),
            )
        except ValueError:
            await msg.reply_text("Send a valid integer.")
        return

    # ── Admin custom position seconds ──────────────────────────────────────
    if stage == "awaiting_admin_pos_custom":
        if not msg.text:
            await msg.reply_text("Send a number.")
            return
        try:
            secs = int(msg.text.strip())
            wm   = await get_admin_watermark(uid) or {"positions": ["start", "end"]}
            await update_admin_watermark_positions(uid, wm.get("positions", ["start", "end"]), secs)
            await update_user_conversation(chat_id, None)
            await msg.reply_text(
                f"✅ Custom position set to {secs}s.",
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("⬅️ Back", callback_data="admin_watermark_settings")]]
                ),
            )
        except ValueError:
            await msg.reply_text("Send a valid integer.")
        return

    # ── Per-job watermark custom seconds ───────────────────────────────────
    if stage == "awaiting_wm_custom_seconds":
        if not msg.text:
            await msg.reply_text("Send a number.")
            return
        try:
            secs      = int(msg.text.strip())
            positions = list(set(conv.get("wm_positions", [])) | {"custom"})
            await update_user_conversation(chat_id, {
                "wm_positions":     positions,
                "wm_custom_seconds": secs,
                "stage":            "awaiting_wm_position_choice",
            })
            conv = await get_user_conversation(chat_id) or {}
            txt, kb = _build_wm_pos_ui(conv)
            await msg.reply_text(txt, reply_markup=kb)
        except ValueError:
            await msg.reply_text("Send a valid integer.")
        return

    # ── Media file ─────────────────────────────────────────────────────────
    if stage == "awaiting_media_file":
        if is_url_msg:
            await _handle_url(client, msg, conv)
        elif sent_media:
            await _handle_media(client, msg, conv)
        else:
            await msg.reply_text("Please send a file or a direct URL.")
        return

    # ── Add admin (owner only) ─────────────────────────────────────────────
    if stage == "awaiting_admin_id":
        if uid != Config.OWNER_ID:
            return
        if not msg.text:
            await msg.reply_text("Send a user ID.")
            return
        try:
            new_id = int(msg.text.strip())
            await add_admin(new_id)
            await update_user_conversation(chat_id, None)
            await msg.reply_text(
                f"✅ Admin `{new_id}` added.",
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")]]
                ),
            )
        except ValueError:
            await msg.reply_text("Send a valid integer user ID.")
        return

    # ── Admin limits (owner only) ──────────────────────────────────────────
    if stage == "awaiting_admin_limits":
        if uid != Config.OWNER_ID:
            return
        tid = conv.get("limits_target_id")
        if not msg.text:
            await msg.reply_text("Send two numbers.")
            return
        try:
            parts = msg.text.strip().split()
            if len(parts) != 2:
                raise ValueError
            max_jobs, max_mb = int(parts[0]), int(parts[1])
            await set_admin_limits(tid, max_jobs_per_day=max_jobs, max_file_size_mb=max_mb)
            await update_user_conversation(chat_id, None)
            await msg.reply_text(
                f"✅ Limits updated for `{tid}`:\n"
                f"max_jobs_per_day=`{max_jobs or '∞'}`, max_file_size_mb=`{max_mb or '∞'}`",
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")]]
                ),
            )
        except (ValueError, TypeError):
            await msg.reply_text(
                "Send exactly two integers: `<max_jobs_per_day> <max_file_size_mb>`"
            )
        return

    # ── Default metadata fields (owner only) ──────────────────────────────
    for field in ("title", "artist", "album", "language", "comment"):
        if stage == f"awaiting_meta_{field}":
            if uid != Config.OWNER_ID:
                return
            val = (msg.text or "").strip()
            if val == "-":
                val = ""
            await set_default_metadata({field: val})
            await update_user_conversation(chat_id, None)
            await msg.reply_text(
                f"✅ {field.capitalize()}: `{val or '(cleared)'}`",
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("⬅️ Back", callback_data="owner_metadata_menu")]]
                ),
            )
            return


# ═══════════════════════════════════════════════════════════════════════════════
# URL DOWNLOAD HANDLER
# ═══════════════════════════════════════════════════════════════════════════════

async def _handle_url(client, msg: Message, conv: dict):
    chat_id = msg.chat.id
    uid     = msg.from_user.id
    url     = msg.text.strip()
    job_dir = conv.get("job_dir")

    if not job_dir or not os.path.isdir(job_dir):
        await msg.reply_text("Session error. Please start over.", quote=True)
        asyncio.create_task(clear_conversation_after_delay(chat_id, delay=5))
        return

    # Limit check (file size unknown pre-download; we check after)
    if uid != Config.OWNER_ID:
        limits = await get_admin_limits(uid)
        if limits["max_jobs_per_day"] > 0:
            today_count = await get_admin_jobs_today(uid)
            if today_count >= limits["max_jobs_per_day"]:
                await msg.reply_text(
                    f"⛔ Daily job limit (`{limits['max_jobs_per_day']}`) reached.",
                    quote=True,
                )
                return

    status_msg = await msg.reply_text(
        f"🌐 **Downloading from URL…**\n\n`{url[:80]}{'...' if len(url) > 80 else ''}`",
        quote=True,
    )

    dest_placeholder = os.path.join(job_dir, f"url_dl_{uuid.uuid4().hex[:8]}.dat")
    input_file_path  = None

    try:
        input_file_path = await download_url(url, dest_placeholder, status_msg, chat_id)
    except asyncio.CancelledError:
        try:
            await status_msg.edit_text("❌ **Download Cancelled**")
        except Exception:
            pass
        asyncio.create_task(clear_conversation_after_delay(chat_id))
        shutil.rmtree(job_dir, ignore_errors=True)
        return
    except Exception as e:
        LOGGER.error(f"URL download error: {e}", exc_info=True)
        try:
            await status_msg.edit_text(f"❌ **Download Failed**\n\n`{e}`")
        except Exception:
            pass
        asyncio.create_task(clear_conversation_after_delay(chat_id))
        return
    finally:
        DOWNLOAD_PROGRESS.pop(getattr(status_msg, "id", None), None)

    if not input_file_path or not os.path.exists(input_file_path):
        await status_msg.edit_text("❌ Downloaded file not found.")
        asyncio.create_task(clear_conversation_after_delay(chat_id))
        return

    # File size limit check (post-download)
    if uid != Config.OWNER_ID:
        limits     = await get_admin_limits(uid)
        file_bytes = os.path.getsize(input_file_path)
        if limits["max_file_size_mb"] > 0:
            max_b = limits["max_file_size_mb"] * 1024 * 1024
            if file_bytes > max_b:
                await status_msg.edit_text(
                    f"⛔ File too large (`{human_size(file_bytes)}`). "
                    f"Your limit is `{limits['max_file_size_mb']} MB`."
                )
                shutil.rmtree(job_dir, ignore_errors=True)
                asyncio.create_task(clear_conversation_after_delay(chat_id))
                return

    original_filename = os.path.basename(input_file_path)
    await _probe_and_show_tracks(
        status_msg, chat_id, uid, input_file_path, original_filename, conv
    )


# ═══════════════════════════════════════════════════════════════════════════════
# TELEGRAM MEDIA DOWNLOAD HANDLER
# ═══════════════════════════════════════════════════════════════════════════════

async def _handle_media(client, msg: Message, conv: dict):
    chat_id = msg.chat.id
    uid     = msg.from_user.id
    job_dir = conv.get("job_dir")

    if not job_dir or not os.path.isdir(job_dir):
        await msg.reply_text("Session error. Please start over.", quote=True)
        asyncio.create_task(clear_conversation_after_delay(chat_id, delay=5))
        return

    media = (msg.audio or msg.video or msg.document or getattr(msg, "voice", None))

    # ── Limit checks ──────────────────────────────────────────────────────
    if uid != Config.OWNER_ID:
        limits     = await get_admin_limits(uid)
        file_bytes = getattr(media, "file_size", 0) or 0

        if limits["max_jobs_per_day"] > 0:
            today_count = await get_admin_jobs_today(uid)
            if today_count >= limits["max_jobs_per_day"]:
                await msg.reply_text(
                    f"⛔ Daily job limit (`{limits['max_jobs_per_day']}`) reached.",
                    quote=True,
                )
                return

        if limits["max_file_size_mb"] > 0 and file_bytes > 0:
            max_b = limits["max_file_size_mb"] * 1024 * 1024
            if file_bytes > max_b:
                await msg.reply_text(
                    f"⛔ File too large (`{human_size(file_bytes)}`). "
                    f"Your limit is `{limits['max_file_size_mb']} MB`.",
                    quote=True,
                )
                return

    # ── Determine filename ─────────────────────────────────────────────────
    original_filename = sanitize_filename(
        getattr(msg.document, "file_name", None)
        or getattr(msg.audio,    "file_name", None)
        or getattr(msg.video,    "file_name", None)
        or getattr(msg, "caption", None)
        or f"file_{uuid.uuid4().hex[:8]}"
    )
    ext = safe_ext(original_filename)
    if not ext:
        if msg.video:
            ext = "mkv"
        elif msg.audio or getattr(msg, "voice", None):
            ext = "m4a"
        else:
            ext = "dat"
        original_filename += f".{ext}"

    input_path = os.path.join(job_dir, f"input_{uuid.uuid4().hex[:8]}_{original_filename}")
    status_msg = await msg.reply_text("📥 **Downloading… 0%**", quote=True)

    # ── Download ───────────────────────────────────────────────────────────
    downloaded = None
    try:
        downloaded = await msg.download(
            file_name=input_path,
            progress=progress_callback,
            progress_args=(status_msg, "Downloading"),
        )
        if downloaded:
            input_path = downloaded
    except Exception as e:
        if "Cancelled by user" in str(e):
            try:
                await status_msg.edit_text("❌ **Download Cancelled**")
            except Exception:
                pass
            shutil.rmtree(job_dir, ignore_errors=True)
            asyncio.create_task(clear_conversation_after_delay(chat_id))
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

    if not input_path or not os.path.exists(input_path):
        await status_msg.edit_text("❌ Download produced no file.")
        asyncio.create_task(clear_conversation_after_delay(chat_id))
        return

    await _probe_and_show_tracks(status_msg, chat_id, uid, input_path, original_filename, conv)


# ═══════════════════════════════════════════════════════════════════════════════
# PROBE + SHOW TRACK SELECTION  (shared between Telegram download and URL download)
# ═══════════════════════════════════════════════════════════════════════════════

async def _probe_and_show_tracks(status_msg, chat_id: int, uid: int,
                                  input_path: str, original_filename: str,
                                  conv: dict):
    await status_msg.edit_text("🔬 **Analysing file…**")
    all_streams   = await probe_streams(input_path)
    audio_streams = [s for s in all_streams if s.get("codec_type") == "audio"]
    video_streams = [s for s in all_streams if s.get("codec_type") == "video"]

    if not audio_streams:
        await status_msg.edit_text(
            "❌ No audio streams found in this file.",
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("❌ Cancel", callback_data="cancel_conv")]]
            ),
        )
        return

    is_audio_only = not bool(video_streams)

    await update_user_conversation(chat_id, {
        "input_file_path":  input_path,
        "original_filename": original_filename,
        "audio_streams":    audio_streams,
        "is_audio_only":    is_audio_only,
        "stage":            "awaiting_track_selection",
    })

    text    = "**🔬 Analysis Complete!**\n\nAudio Tracks:\n\n"
    buttons = []
    for s in audio_streams:
        idx    = s.get("index")
        codec  = s.get("codec_name", "?").upper()
        lang   = (s.get("tags") or {}).get("language", "und").upper()
        ch     = s.get("channels", "?")
        layout = s.get("channel_layout", "?")
        lbl    = f"Track {idx}: {codec} ({layout} {ch}ch) [{lang}]"
        text  += f"▪️ {lbl}\n"
        buttons.append([InlineKeyboardButton(lbl, callback_data=f"track_{idx}")])
    buttons.append([InlineKeyboardButton("❌ Cancel", callback_data="cancel_conv")])
    await status_msg.edit_text(text, reply_markup=InlineKeyboardMarkup(buttons))


# ═══════════════════════════════════════════════════════════════════════════════
# TRACK SELECTION
# ═══════════════════════════════════════════════════════════════════════════════

@bot.on_callback_query(filters.regex(r"^track_(\d+)$") & admin_filter)
async def cb_track_select(client, cb: CallbackQuery):
    await cb.answer()
    chat_id = cb.message.chat.id
    conv    = await get_user_conversation(chat_id)
    if not conv or conv.get("stage") != "awaiting_track_selection":
        return await cb.answer("Session expired.", show_alert=True)

    idx    = int(re.search(r"(\d+)$", cb.data).group(1))
    stream = next((s for s in conv.get("audio_streams", []) if s.get("index") == idx), None)
    if not stream:
        return await cb.answer("Track not found.", show_alert=True)

    await update_user_conversation(chat_id, {
        "selected_track_index": idx,
        "selected_stream_obj":  stream,
        "stage":                "awaiting_format_selection",
    })

    rows = [
        [InlineKeyboardButton("🎵 AAC Stereo 192k",   callback_data="format_aac_stereo")],
        [InlineKeyboardButton("🎧 AAC 5.1 Surround",  callback_data="format_aac_5_1")],
        [InlineKeyboardButton("🎵 MP3 Stereo 192k",   callback_data="format_mp3_stereo")],
    ]
    if stream.get("codec_name") == "aac":
        rows.insert(0, [InlineKeyboardButton(
            "✨ Copy AAC (fastest, no re-encode)", callback_data="format_aac_copy"
        )])
    rows.extend([
        [InlineKeyboardButton("⬅️ Choose Different File", callback_data="convert_audio_start")],
        [InlineKeyboardButton("❌ Cancel",                 callback_data="cancel_conv")],
    ])
    await cb.message.edit_text(
        f"✅ Track {idx} selected.\n\nChoose output format:",
        reply_markup=InlineKeyboardMarkup(rows),
    )


# ═══════════════════════════════════════════════════════════════════════════════
# FORMAT SELECTION
# ═══════════════════════════════════════════════════════════════════════════════

@bot.on_callback_query(filters.regex(r"^format_(.+)$") & admin_filter)
async def cb_format_select(client, cb: CallbackQuery):
    await cb.answer()
    chat_id = cb.message.chat.id
    conv    = await get_user_conversation(chat_id)
    if not conv or conv.get("stage") != "awaiting_format_selection":
        return await cb.answer("Session expired.", show_alert=True)

    raw    = cb.data[len("format_"):]
    fmt    = {"codec": "aac", "channels": 2, "bitrate": "192k"}   # default

    if "aac_5_1" in raw:
        fmt = {"codec": "aac",         "channels": 6, "bitrate": "320k"}
    elif "mp3" in raw:
        fmt = {"codec": "libmp3lame",  "channels": 2, "bitrate": "192k"}
    elif "copy" in raw:
        fmt = {"codec": "copy",        "channels": "copy", "bitrate": "copy"}

    await update_user_conversation(chat_id, {"format": fmt})

    owner_wm = await get_owner_watermark()
    admin_wm = await get_admin_watermark(cb.from_user.id)
    wm_ok    = bool(owner_wm.get("file_id")) or bool(admin_wm and admin_wm.get("file_id"))

    if fmt["codec"] == "copy":
        # Copy mode: watermark impossible without re-encode — skip
        await update_user_conversation(chat_id, {"watermark": False})
        await _ask_output_type(cb, conv)
    elif wm_ok:
        await update_user_conversation(chat_id, {"stage": "awaiting_watermark_selection"})
        rows = []
        if owner_wm.get("file_id"):
            rows.append([InlineKeyboardButton("💧 Owner Watermark",  callback_data="wm_use_owner")])
        if admin_wm and admin_wm.get("file_id"):
            rows.append([InlineKeyboardButton("🧑‍💼 My Watermark",    callback_data="wm_use_admin")])
        rows.append([InlineKeyboardButton("❌ No Watermark",          callback_data="wm_use_no")])
        rows.append([InlineKeyboardButton("❌ Cancel",                callback_data="cancel_conv")])
        await cb.message.edit_text(
            "✅ Format selected.\n\nMix a watermark?",
            reply_markup=InlineKeyboardMarkup(rows),
        )
    else:
        await update_user_conversation(chat_id, {"watermark": False})
        await _ask_output_type(cb, conv)


# ═══════════════════════════════════════════════════════════════════════════════
# WATERMARK SELECTION
# ═══════════════════════════════════════════════════════════════════════════════

@bot.on_callback_query(filters.regex(r"^wm_use_(owner|admin|no)$") & admin_filter)
async def cb_wm_use(client, cb: CallbackQuery):
    await cb.answer()
    choice  = cb.data.split("_")[-1]
    chat_id = cb.message.chat.id
    conv    = await get_user_conversation(chat_id)
    if not conv or conv.get("stage") != "awaiting_watermark_selection":
        return await cb.answer("Session expired.", show_alert=True)

    if choice == "owner":
        wm = await get_owner_watermark()
        if not wm.get("file_id"):
            return await cb.answer("Owner watermark not set.", show_alert=True)
        await update_user_conversation(chat_id, {
            "watermark":        True,
            "watermark_choice": "owner",
            "wm_positions":     wm.get("positions", ["start", "end"]),
            "wm_custom_seconds": wm.get("custom_seconds", 0),
            "stage":            "awaiting_wm_volume_choice",
        })
        await _ask_job_wm_volume(cb)

    elif choice == "admin":
        wm = await get_admin_watermark(cb.from_user.id)
        if not wm or not wm.get("file_id"):
            return await cb.answer("Your watermark is not set.", show_alert=True)
        await update_user_conversation(chat_id, {
            "watermark":        True,
            "watermark_choice": "admin",
            "wm_positions":     wm.get("positions", ["start", "end"]),
            "wm_custom_seconds": wm.get("custom_seconds", 0),
            "stage":            "awaiting_wm_volume_choice",
        })
        await _ask_job_wm_volume(cb)

    else:
        await update_user_conversation(chat_id, {"watermark": False})
        await _ask_output_type(cb, conv)


# ── Per-job volume override ────────────────────────────────────────────────────

async def _ask_job_wm_volume(cb: CallbackQuery):
    await cb.message.edit_text(
        "🔊 **Watermark volume for this job:**",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("10%",  callback_data="jwv_0.10"),
             InlineKeyboardButton("20%",  callback_data="jwv_0.20"),
             InlineKeyboardButton("30%",  callback_data="jwv_0.30")],
            [InlineKeyboardButton("50%",  callback_data="jwv_0.50"),
             InlineKeyboardButton("80%",  callback_data="jwv_0.80"),
             InlineKeyboardButton("100%", callback_data="jwv_1.00")],
            [InlineKeyboardButton("🔄 Use default volume", callback_data="jwv_default")],
        ]),
    )


@bot.on_callback_query(filters.regex(r"^jwv_(\d+\.\d+|default)$") & admin_filter)
async def cb_job_wm_vol(client, cb: CallbackQuery):
    await cb.answer()
    chat_id = cb.message.chat.id
    conv    = await get_user_conversation(chat_id)
    if not conv:
        return await cb.answer("Session expired.", show_alert=True)

    raw = cb.data[len("jwv_"):]
    vol = None if raw == "default" else float(raw)

    await update_user_conversation(chat_id, {
        "job_wm_volume": vol,
        "stage":         "awaiting_wm_position_choice",
    })
    conv = await get_user_conversation(chat_id) or {}
    txt, kb = _build_wm_pos_ui(conv)
    await cb.message.edit_text(txt, reply_markup=kb)


# ═══════════════════════════════════════════════════════════════════════════════
# WATERMARK POSITION TOGGLE UI  (per-job)
# ═══════════════════════════════════════════════════════════════════════════════

def _build_wm_pos_ui(conv: dict) -> tuple[str, InlineKeyboardMarkup]:
    pos_set     = set(conv.get("wm_positions", []))
    custom_secs = int(conv.get("wm_custom_seconds", 0) or 0)
    all_o       = {"start", "middle", "end", "hourly"}
    sel_lbl     = "❌ Deselect All" if all_o.issubset(pos_set) else "✅ Select All"

    base   = [p for p in ["start", "middle", "end", "hourly"] if p in pos_set]
    sel_tx = ", ".join(base)
    if "custom" in pos_set and custom_secs:
        sel_tx = (sel_tx + f" + custom({custom_secs}s)") if sel_tx else f"custom({custom_secs}s)"
    if not sel_tx:
        sel_tx = "start"

    def mk(n, lbl):
        mark = "✅" if n in pos_set else "❌"
        return InlineKeyboardButton(f"{mark} {lbl}", callback_data=f"wmt_{n}")

    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton(sel_lbl, callback_data="wmt_all")],
        [mk("start", "Start (+3 min)"), mk("middle", "Middle")],
        [mk("end",   "End (−7 min)"),   mk("hourly", "Hourly (60 min)")],
        [InlineKeyboardButton("🔢 Custom Seconds", callback_data="wmt_custom_prompt")],
        [InlineKeyboardButton("➡️ Continue",       callback_data="wmt_done")],
    ])
    return f"📍 **Positions** — Selected: `{sel_tx}`\n\nToggle then Continue.", kb


@bot.on_callback_query(filters.regex("^wmt_all$") & admin_filter)
async def cb_wmt_all(client, cb: CallbackQuery):
    await cb.answer()
    chat_id = cb.message.chat.id
    conv    = await get_user_conversation(chat_id)
    if not conv:
        return await cb.answer("Session expired.", show_alert=True)
    cur     = set(conv.get("wm_positions", []))
    all_o   = {"start", "middle", "end", "hourly"}
    new_pos = ["start"] if all_o.issubset(cur) else list(all_o)
    await update_user_conversation(chat_id, {"wm_positions": new_pos})
    conv["wm_positions"] = new_pos
    txt, kb = _build_wm_pos_ui(conv)
    await cb.message.edit_text(txt, reply_markup=kb)


@bot.on_callback_query(filters.regex(r"^wmt_(start|middle|end|hourly)$") & admin_filter)
async def cb_wmt_toggle(client, cb: CallbackQuery):
    await cb.answer()
    n       = cb.data.split("_", 1)[1]
    chat_id = cb.message.chat.id
    conv    = await get_user_conversation(chat_id)
    if not conv:
        return await cb.answer("Session expired.", show_alert=True)
    cur = set(conv.get("wm_positions", []))
    cur.discard(n) if n in cur else cur.add(n)
    if not cur:
        cur = {"start"}
    await update_user_conversation(chat_id, {"wm_positions": list(cur)})
    conv["wm_positions"] = list(cur)
    txt, kb = _build_wm_pos_ui(conv)
    await cb.message.edit_text(txt, reply_markup=kb)


@bot.on_callback_query(filters.regex("^wmt_custom_prompt$") & admin_filter)
async def cb_wmt_custom_prompt(client, cb: CallbackQuery):
    await cb.answer()
    await update_user_conversation(cb.message.chat.id, {"stage": "awaiting_wm_custom_seconds"})
    await cb.message.edit_text(
        "🔢 Send seconds (e.g. 1800 = 30 min).",
        reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton("❌ Cancel", callback_data="cancel_conv")]]
        ),
    )


@bot.on_callback_query(filters.regex("^wmt_done$") & admin_filter)
async def cb_wmt_done(client, cb: CallbackQuery):
    await cb.answer()
    chat_id = cb.message.chat.id
    conv    = await get_user_conversation(chat_id)
    if not conv:
        return await cb.answer("Session expired.", show_alert=True)
    await _ask_output_type(cb, conv)


# ═══════════════════════════════════════════════════════════════════════════════
# OUTPUT TYPE
# ═══════════════════════════════════════════════════════════════════════════════

async def _ask_output_type(cb: CallbackQuery, conv: dict):
    chat_id = cb.message.chat.id
    conv    = await get_user_conversation(chat_id) or conv
    if conv.get("is_audio_only"):
        await update_user_conversation(chat_id, {"output_type": "audio", "stage": "processing"})
        try:
            await cb.message.edit_text("✅ **Ready!** Starting audio conversion…")
        except Exception:
            pass
        await _start_conversion(cb)
    else:
        await update_user_conversation(chat_id, {"stage": "awaiting_output_selection"})
        await cb.message.edit_text(
            "✅ Settings complete.\n\nChoose output type:",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🎵 Audio Only (.m4a/.mp3)", callback_data="out_audio"),
                 InlineKeyboardButton("🎬 Remux Video (.mkv)",     callback_data="out_remux")],
                [InlineKeyboardButton("📦 All (Audio + Video)",    callback_data="out_all")],
                [InlineKeyboardButton("❌ Cancel",                 callback_data="cancel_conv")],
            ]),
        )


@bot.on_callback_query(filters.regex(r"^out_(audio|remux|all)$") & admin_filter)
async def cb_output_select(client, cb: CallbackQuery):
    await cb.answer()
    chat_id = cb.message.chat.id
    conv    = await get_user_conversation(chat_id)
    if not conv or conv.get("stage") not in (
        "awaiting_output_selection", "awaiting_wm_position_choice", "processing"
    ):
        return await cb.answer("Session expired.", show_alert=True)
    otype = cb.data.split("_", 1)[1]
    await update_user_conversation(chat_id, {"output_type": otype, "stage": "processing"})
    await cb.message.edit_text(f"✅ **Ready!** Output: `{otype}` — Starting…")
    await _start_conversion(cb)


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN CONVERSION ORCHESTRATOR
# ═══════════════════════════════════════════════════════════════════════════════

async def _start_conversion(cb: CallbackQuery):
    chat_id    = cb.message.chat.id
    status_msg = cb.message
    uid        = cb.from_user.id

    async with ffmpeg_semaphore:
        conv = await get_user_conversation(chat_id)
        if not conv or conv.get("stage") != "processing":
            return

        job_id   = conv.get("job_id") or str(uuid.uuid4())
        job_dir  = conv["job_dir"]
        conv["user_id"] = uid

        # Pre-set tracker
        JOB_TRACKERS.setdefault(job_id, {
            "cancelled": False, "last_update": 0.0, "start_ts": time.time()
        })

        uploaded: list[str] = []

        try:
            input_file  = conv["input_file_path"]
            fmt         = conv["format"]
            use_wm      = conv.get("watermark", False)
            wm_choice   = conv.get("watermark_choice", "owner")
            output_type = conv.get("output_type", "audio")

            wm_local    = None
            wm_info     = {"applied": False, "which": None, "positions": [], "volume": 0.2}

            # ── Download watermark file ────────────────────────────────────
            if use_wm:
                wm_file_id = None
                wm_vol     = 0.2
                wm_pos     = conv.get("wm_positions") or ["start", "end"]
                wm_cust    = conv.get("wm_custom_seconds", 0)

                if wm_choice == "owner":
                    cfg        = await get_owner_watermark()
                    wm_file_id = cfg.get("file_id")
                    wm_vol     = float(cfg.get("volume", 0.2))
                else:
                    cfg = await get_admin_watermark(uid) or {}
                    wm_file_id = cfg.get("file_id")
                    wm_vol     = float(cfg.get("volume", 0.2))

                if conv.get("job_wm_volume") is not None:
                    wm_vol = float(conv["job_wm_volume"])

                # Merge positions back into conv for build_ffmpeg_args
                conv["wm_positions"]     = wm_pos
                conv["wm_custom_seconds"] = wm_cust
                conv["job_wm_volume"]    = wm_vol   # already resolved

                if wm_file_id:
                    try:
                        await status_msg.edit_text("📥 **Downloading watermark…**")
                        wm_local = await bot.download_media(
                            wm_file_id,
                            file_name=os.path.join(job_dir, f"wm_{uuid.uuid4().hex[:8]}"),
                        )
                        wm_info.update({
                            "applied":   True,
                            "which":     wm_choice,
                            "positions": wm_pos,
                            "volume":    wm_vol,
                        })
                    except Exception as e:
                        LOGGER.error(f"WM download: {e}", exc_info=True)
                        raise RuntimeError("Watermark download failed.") from e
                else:
                    use_wm = False
                    conv["watermark"] = False

            total_dur = await get_duration(input_file)

            # ── Helper: one ffmpeg pass + upload ──────────────────────────
            async def _do_one(c: dict, otype: str, ofmt: dict | None = None) -> str | None:
                run_conv = dict(c)
                if ofmt:
                    run_conv = dict(run_conv, format=ofmt)
                run_conv["output_type"] = otype

                ffargs, out_path = await build_ffmpeg_args(
                    run_conv, input_file, wm_local,
                    override_output_type=otype,
                )

                await status_msg.edit_text(
                    f"🔁 **Converting** (`{otype}`)…"
                )
                await run_ffmpeg(ffargs, total_dur, status_msg, job_id)

                if not os.path.exists(out_path):
                    raise RuntimeError("Output file missing after ffmpeg run.")

                out_sz = os.path.getsize(out_path)
                if out_sz > Config.TELEGRAM_MAX_FILE_SIZE:
                    await status_msg.edit_text(
                        f"❌ Output `{human_size(out_sz)}` exceeds Telegram limit."
                    )
                    return None

                wm_flag   = wm_info["applied"]
                wm_lbl    = f"Yes – {wm_info['which']}" if wm_flag else "No"
                pos_lbl   = ", ".join(wm_info["positions"]) if wm_flag else "-"
                vol_lbl   = f"{int(wm_info['volume'] * 100)}%" if wm_flag else "-"
                caption   = (
                    f"✅ **Done!**\n\n"
                    f"Format:    `{(ofmt or run_conv.get('format', {})).get('codec', '?')}`\n"
                    f"Output:    `{otype}`\n"
                    f"Watermark: `{wm_lbl}`\n"
                    f"Positions: `{pos_lbl}`\n"
                    f"Volume:    `{vol_lbl}`\n"
                    f"Size:      `{human_size(out_sz)}`\n"
                    f"Job ID:    `{job_id}`"
                )

                await status_msg.edit_text("✅ **Done! Uploading…**")
                await bot.send_document(
                    chat_id,
                    document=out_path,
                    caption=caption,
                    progress=progress_callback,
                    progress_args=(status_msg, "Uploading"),
                )
                uploaded.append(out_path)
                DOWNLOAD_PROGRESS.pop(getattr(status_msg, "id", None), None)
                return out_path

            # ── Dispatch ──────────────────────────────────────────────────
            if output_type == "audio":
                await _do_one(conv, "audio")
            elif output_type == "remux":
                await _do_one(conv, "remux")
            elif output_type == "all":
                await _do_one(conv, "audio")
                await _do_one(conv, "remux")
            else:
                raise ValueError(f"Unknown output type: {output_type}")

            # ── Persist job record (permanent) ────────────────────────────
            job_doc = {
                "_id":          job_id,
                "chat_id":      chat_id,
                "user_id":      uid,
                "input_file":   input_file,
                "output_files": uploaded,
                "output_size":  sum(
                    os.path.getsize(f) for f in uploaded if os.path.exists(f)
                ),
                "format":       fmt,
                "watermark":    wm_info,
                "timestamp":    datetime.utcnow(),
            }
            try:
                await jobs_col.insert_one(job_doc)
            except Exception as e:
                LOGGER.error(f"Failed to save job record: {e}")

            try:
                await status_msg.delete()
            except Exception:
                pass

        except asyncio.CancelledError:
            try:
                await status_msg.edit_text("⚠️ **Conversion Cancelled.**")
            except Exception:
                pass

        except Exception as e:
            LOGGER.error(f"Conversion error: {e}", exc_info=True)
            try:
                await status_msg.edit_text(f"❌ **Error:** `{e}`")
            except Exception:
                pass

        finally:
            # Always clean up job directory
            try:
                conv_latest = await get_user_conversation(chat_id)
                jd = (conv_latest or conv).get("job_dir")
                if jd and os.path.isdir(jd):
                    shutil.rmtree(jd, ignore_errors=True)
            except Exception as e:
                LOGGER.error(f"Cleanup error: {e}")
            DOWNLOAD_PROGRESS.pop(getattr(status_msg, "id", None), None)
            JOB_TRACKERS.pop(job_id, None)
            asyncio.create_task(clear_conversation_after_delay(chat_id))


# ═══════════════════════════════════════════════════════════════════════════════
# WEB SERVER  (keep-alive + health check)
# ═══════════════════════════════════════════════════════════════════════════════

_routes = web.RouteTableDef()


@_routes.get("/", allow_head=True)
async def _root(request):
    return web.Response(text="Audio Bot — OK", content_type="text/plain")


async def _web_server():
    app = web.Application(client_max_size=300_000_000)
    app.add_routes(_routes)
    return app


async def _pinger():
    if not Config.ON_HEROKU or not Config.STREAM_URL:
        LOGGER.info("Pinger disabled.")
        return
    LOGGER.info(f"Pinger → {Config.STREAM_URL} every {Config.PING_INTERVAL}s")
    async with aiohttp.ClientSession() as session:
        while True:
            await asyncio.sleep(Config.PING_INTERVAL)
            try:
                async with session.get(
                    Config.STREAM_URL, timeout=aiohttp.ClientTimeout(total=10)
                ) as r:
                    LOGGER.info(f"Ping: HTTP {r.status}")
            except Exception as e:
                LOGGER.warning(f"Pinger error: {e}")


# ═══════════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════════

async def _main():
    LOGGER.info("Starting bot…")
    await bot.start()
    me = await bot.get_me()
    LOGGER.info(f"Logged in as @{me.username}")

    asyncio.create_task(_pinger())

    app     = await _web_server()
    runner  = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", Config.PORT)
    await site.start()
    LOGGER.info(f"Web server on :{Config.PORT}")

    try:
        await bot.send_message(Config.OWNER_ID, "✅ **Bot restarted — all systems online!**")
    except Exception as e:
        LOGGER.warning(f"Startup message: {e}")

    await asyncio.Event().wait()   # block forever


if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    async def _shutdown(sig_name: str):
        LOGGER.info(f"Signal {sig_name} — shutting down…")
        if bot.is_connected:
            await bot.stop()
        tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        loop.stop()

    for _sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(
                _sig,
                lambda s=_sig: asyncio.create_task(_shutdown(s.name)),
            )
        except NotImplementedError:
            pass

    try:
        loop.run_until_complete(_main())
        loop.run_forever()
    except (KeyboardInterrupt, SystemExit):
        pass
    finally:
        LOGGER.info("Cleaning up…")
        try:
            if os.path.isdir(Config.DOWNLOAD_DIR):
                shutil.rmtree(Config.DOWNLOAD_DIR, ignore_errors=True)
        except Exception:
            pass
        if not loop.is_closed():
            loop.close()
        LOGGER.info("Shutdown complete.")
