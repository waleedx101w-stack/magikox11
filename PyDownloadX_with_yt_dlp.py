# -*- coding: utf-8 -*-
"""
PyDownloadX — Open Source Download Manager (with yt-dlp integration)
Merged: multi-thread downloader + yt-dlp YouTube handling + GUI
Requirements:
    pip install requests urllib3 customtkinter pyperclip yt-dlp
    (ffmpeg recommended for merging video+audio when using yt-dlp)
"""

import os
import sys
import time
import json
import threading
import requests
import shutil
import hashlib
import base64
import socket
import platform
import logging
from datetime import datetime
from http.server import SimpleHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse
from pathlib import Path
from contextlib import contextmanager
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Optional GUI imports
try:
    import customtkinter as ctk
    from tkinter import filedialog, messagebox
    HAS_GUI = True
except Exception:
    HAS_GUI = False

try:
    import pyperclip
    HAS_CLIPBOARD = True
except Exception:
    HAS_CLIPBOARD = False

# Attempt to import yt-dlp
try:
    from yt_dlp import YoutubeDL
    HAS_YTDLP = True
except Exception:
    YoutubeDL = None
    HAS_YTDLP = False

# Logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler('PyDownloadX.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Paths & defaults
APP_NAME = "PyDownloadX"
APP_VERSION = "9.0-yt"
DOWNLOAD_DIR = Path("downloads").resolve()
SHARED_DIR = Path("shared").resolve()
DATA_DIR = Path("data").resolve()
CONFIG_FILE = "config.json"
HISTORY_FILE = "history.json"

os.makedirs(DOWNLOAD_DIR, exist_ok=True)
os.makedirs(SHARED_DIR, exist_ok=True)
os.makedirs(DATA_DIR, exist_ok=True)

DEFAULT_CONFIG = {
    "download_dir": str(DOWNLOAD_DIR),
    "max_threads": 12,
    "chunk_size": 2 * 1024 * 1024,  # 2MB
    "timeout": 180,
    "retries": 6,
    "server_port": 8080,
    "server_password": "PyDownloadX2025",
    "enable_clipboard_monitor": True,
    "save_history": True,
    "max_simultaneous_downloads": 2,
    "theme": "dark",
    "min_file_size_for_multithread": 100 * 1024 * 1024,
    "buffer_size": 8192,
    "checkpoint_interval_seconds": 30,
    "global_speed_limit_kbps": 0  # 0 = unlimited
}

def load_config():
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                cfg = json.load(f)
            merged = {**DEFAULT_CONFIG, **cfg}
            for k in ("max_threads", "chunk_size", "timeout", "retries", "max_simultaneous_downloads",
                      "min_file_size_for_multithread", "buffer_size", "checkpoint_interval_seconds"):
                merged[k] = int(merged.get(k, DEFAULT_CONFIG[k]))
            merged["global_speed_limit_kbps"] = int(merged.get("global_speed_limit_kbps", 0))
            return merged
        except Exception as e:
            logger.warning(f"Config load error: {e}. Using defaults.")
            return DEFAULT_CONFIG
    else:
        with open(CONFIG_FILE, "w", encoding="utf-8") as f:
            json.dump(DEFAULT_CONFIG, f, ensure_ascii=False, indent=2)
        return DEFAULT_CONFIG

config = load_config()

# ---------------- Token bucket rate limiter ----------------
class TokenBucket:
    def __init__(self, rate_bytes_per_sec=0, capacity_bytes=None):
        self.rate = rate_bytes_per_sec
        self.capacity = capacity_bytes if capacity_bytes is not None else max(rate_bytes_per_sec, 65536)
        self.tokens = self.capacity
        self.timestamp = time.time()
        self.lock = threading.Lock()

    def set_rate(self, rate_bps):
        with self.lock:
            self.rate = rate_bps
            if self.capacity < rate_bps:
                self.capacity = rate_bps
            if self.tokens > self.capacity:
                self.tokens = self.capacity

    def acquire(self, num_bytes):
        if self.rate <= 0:
            return
        while True:
            with self.lock:
                now = time.time()
                elapsed = now - self.timestamp
                added = elapsed * self.rate
                if added > 0:
                    self.tokens = min(self.capacity, self.tokens + added)
                    self.timestamp = now
                if self.tokens >= num_bytes:
                    self.tokens -= num_bytes
                    return
                else:
                    needed = num_bytes - self.tokens
                    sleep_time = needed / max(self.rate, 1)
            time.sleep(min(sleep_time, 0.5))

global_limiter = TokenBucket(rate_bytes_per_sec=config.get("global_speed_limit_kbps", 0) * 1024)

def set_global_limit_kbps(kbps):
    bps = int(kbps) * 1024
    global_limiter.set_rate(bps)

# ---------------- Networking session ----------------
def create_optimized_session():
    session = requests.Session()
    retry_strategy = Retry(
        total=config.get("retries", 6),
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset(["HEAD", "GET", "OPTIONS"])
    )
    adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=50, pool_maxsize=50)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update({
        'User-Agent': f'{APP_NAME}/{APP_VERSION} (Python Requests)',
        'Connection': 'keep-alive'
    })
    return session

# ---------------- Utilities ----------------
def save_history(entry):
    if not config.get("save_history", True):
        return
    try:
        hist = []
        if os.path.exists(HISTORY_FILE):
            with open(HISTORY_FILE, 'r', encoding='utf-8') as f:
                hist = json.load(f)
        hist.append(entry)
        with open(HISTORY_FILE, 'w', encoding='utf-8') as f:
            json.dump(hist, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.warning(f"History save error: {e}")


def safe_path_join(base, *paths):
    base_path = Path(base).resolve()
    try:
        full_path = base_path.joinpath(*paths).resolve()
        if not str(full_path).startswith(str(base_path)):
            raise ValueError("Path traversal detected!")
        return full_path
    except Exception as e:
        logger.error(f"Path error: {e}")
        return base_path


def compute_sha256(path):
    try:
        h = hashlib.sha256()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(1024*1024), b""):
                h.update(chunk)
        return h.hexdigest()
    except Exception as e:
        logger.warning(f"Hash compute error: {e}")
        return "N/A"

# ---------------- yt-dlp helper (integrated) ----------------
def ensure_yt_dlp_available():
    if not HAS_YTDLP:
        raise RuntimeError("yt-dlp is not installed. Install with: pip install yt-dlp")


def download_youtube_with_yt_dlp(url: str, out_path: Path, on_progress=None, per_limit_bps: int = 0, global_limit_bps: int = 0, prefer_audio=False):
    ensure_yt_dlp_available()
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    yopts = {
        'outtmpl': str(out_path),
        'noplaylist': True,
        'continuedl': True,
        'quiet': True,
        'no_warnings': True,
        'ignoreerrors': False,
    }
    # ratelimit
    if per_limit_bps and per_limit_bps > 0:
        yopts['ratelimit'] = float(per_limit_bps)
    elif global_limit_bps and global_limit_bps > 0:
        yopts['ratelimit'] = float(global_limit_bps)
    if prefer_audio:
        yopts['format'] = 'bestaudio/best'
    else:
        yopts['format'] = 'bestvideo+bestaudio/best'
    # progress hook
    def hook(d):
        try:
            status = d.get('status')
            if status == 'downloading':
                downloaded = d.get('downloaded_bytes') or 0
                total = d.get('total_bytes') or d.get('total_bytes_estimate') or 0
                speed = d.get('speed') or 0.0
                filename = Path(d.get('filename') or out_path).name
                if on_progress:
                    try:
                        on_progress(downloaded, total, filename, speed, 'Downloading')
                    except Exception:
                        pass
            elif status == 'finished':
                filename = Path(d.get('filename') or out_path).name
                if on_progress:
                    try:
                        on_progress(d.get('downloaded_bytes') or 0, d.get('total_bytes') or 0, filename, 0.0, 'Finished')
                    except Exception:
                        pass
            elif status == 'error':
                if on_progress:
                    try:
                        on_progress(0, 0, str(out_path.name), 0.0, 'Error')
                    except Exception:
                        pass
        except Exception as e:
            logger.debug(f"yt-dlp hook exception: {e}")
    yopts['progress_hooks'] = [hook]
    logger.info(f"yt-dlp starting: {url} -> {out_path} (ratelimit={yopts.get('ratelimit','unlimited')})")
    ydl = YoutubeDL(yopts)
    info = None
    try:
        info = ydl.extract_info(url, download=True)
        return {
            'id': info.get('id') if isinstance(info, dict) else None,
            'title': info.get('title') if isinstance(info, dict) else None,
            'filename': info.get('_filename') if isinstance(info, dict) and info.get('_filename') else str(out_path)
        }
    except Exception as e:
        logger.error(f"yt-dlp download error for {url}: {e}")
        raise

# ---------------- Local server ----------------
class LocalServer:
    def __init__(self):
        self.password = str(config.get("server_password", "PyDownloadX2025"))
        self.port = int(config.get("server_port", 8080))
        self.ip = self.get_ip()
        self.url = f"http://{self.ip}:{self.port}"
        self.running = False

    def get_ip(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            s.connect(("8.8.8.8", 80))
            ip = s.getsockname()[0]
        except Exception:
            ip = "127.0.0.1"
        finally:
            s.close()
        return ip

    def start(self):
        if self.running:
            return
        self.running = True
        t = threading.Thread(target=self.run, daemon=True, name="LocalServer")
        t.start()

    def run(self):
        try:
            os.chdir(SHARED_DIR)
            class Handler(SimpleHTTPRequestHandler):
                def do_GET(self_inner):
                    auth_header = self_inner.headers.get('Authorization')
                    if not auth_header or not auth_header.startswith('Basic '):
                        self_inner.send_response(401)
                        self_inner.send_header('WWW-Authenticate', 'Basic realm="PyDownloadX"')
                        self_inner.end_headers()
                        self_inner.wfile.write(b"Unauthorized")
                        return
                    try:
                        b64 = auth_header.split(' ', 1)[1]
                        decoded = base64.b64decode(b64).decode(errors='ignore')
                        if ':' in decoded:
                            _, pwd = decoded.split(':', 1)
                        else:
                            pwd = decoded
                        if pwd != getattr(self_inner.server, "password",""):
                            self_inner.send_response(401)
                            self_inner.send_header('WWW-Authenticate', 'Basic realm="PyDownloadX"')
                            self_inner.end_headers()
                            self_inner.wfile.write(b"Unauthorized")
                            return
                    except Exception:
                        self_inner.send_response(401)
                        self_inner.send_header('WWW-Authenticate', 'Basic realm="PyDownloadX"')
                        self_inner.end_headers()
                        self_inner.wfile.write(b"Unauthorized")
                        return
                    return super().do_GET()
            server = HTTPServer(("0.0.0.0", self.port), Handler)
            server.password = self.password
            logger.info(f"Local HTTP server running at {self.url}, serving {SHARED_DIR}")
            server.serve_forever()
        except Exception as e:
            logger.error(f"Server error: {e}")

# ---------------- Downloader (with yt-dlp integration) ----------------
class Downloader:
    def __init__(self, url, app=None, on_progress=None, on_complete=None, per_limit_kbps=0):
        self.url = url
        self.app = app
        self.on_progress = on_progress
        self.on_complete = on_complete
        self.per_limit = int(per_limit_kbps) * 1024
        self.per_limiter = TokenBucket(rate_bytes_per_sec=self.per_limit) if self.per_limit > 0 else TokenBucket(rate_bytes_per_sec=0)
        self.filename = self._extract_filename(url)
        self.path = safe_path_join(config.get("download_dir", str(DOWNLOAD_DIR)), self.filename)
        if isinstance(self.path, Path):
            self.path = self.path
        else:
            self.path = Path(self.path)
        self.total_size = 0
        self.downloaded = 0
        self.speed = 0.0
        self.speeds = []
        self.state = "Queued"
        self.etag = ""
        self.last_modified = ""
        self.resume_path = f"{self.path}.resume"
        self.lock = threading.RLock()
        self.cancelled = False
        self.paused = False
        self.session = create_optimized_session()
        self.part_info = {}
        self.start_time = time.time()

    def _extract_filename(self, url):
        try:
            clean = url.split('?')[0].split('#')[0]
            parsed = urlparse(clean)
            name = os.path.basename(parsed.path)
            if not name or '.' not in name:
                name = hashlib.md5(url.encode()).hexdigest()[:12] + ".bin"
            invalid_chars = '<>:"|?*'
            for c in invalid_chars:
                name = name.replace(c, '_')
            return name
        except Exception:
            return "download_" + str(int(time.time())) + ".bin"

    def set_state(self, new_state):
        self.state = new_state
        if self.app:
            try:
                self.app.after(0, lambda: self.app.on_state_change(self))
            except Exception:
                pass

    def start(self):
        t = threading.Thread(target=self._run, daemon=True, name=f"DL-{self.filename}")
        t.start()

    def _on_yt_progress(self, downloaded, total, filename, speed, state):
        # map yt-dlp callbacks to regular on_progress
        self.downloaded = downloaded
        self.total_size = total or self.total_size
        self.speed = speed
        self.set_state(state)
        self._progress_callback()

    def _run(self):
        try:
            # If it's a YouTube (or similar) URL and yt-dlp available => use it
            lower = self.url.lower()
            is_youtube_like = any(x in lower for x in ("youtube.com", "youtu.be", "vimeo.com", "twitch.tv", "dailymotion.com"))
            if is_youtube_like and HAS_YTDLP:
                self.set_state("Starting")
                try:
                    # choose output path; yt-dlp may add extension
                    out = Path(self.path)
                    per = self.per_limit
                    glob = global_limiter.rate if hasattr(global_limiter, 'rate') else 0
                    download_youtube_with_yt_dlp(self.url, out, on_progress=self._on_yt_progress, per_limit_bps=per, global_limit_bps=glob, prefer_audio=False)
                    # success
                    self.set_state("Completed")
                    self._on_finished()
                    return
                except Exception as e:
                    logger.warning(f"yt-dlp handling failed for {self.url}: {e} - falling back to HTTP downloader")

            # fallback to HTTP/requests based flow
            self.set_state("Starting")
            head = None
            try:
                head = self.session.head(self.url, allow_redirects=True, timeout=config.get("timeout", 180))
                head.raise_for_status()
                cl = head.headers.get('content-length')
                self.total_size = int(cl) if cl and cl.isdigit() else 0
                self.etag = head.headers.get('etag', '') or ''
                self.last_modified = head.headers.get('last-modified', '') or ''
            except Exception as e:
                logger.debug(f"HEAD failed for {self.url}: {e}")
                self.total_size = 0

            logger.info(f"Start: {self.filename} size={self.total_size} ETag={self.etag}")
            resume_offset = 0
            if os.path.exists(self.resume_path) and os.path.exists(self.path):
                try:
                    with open(self.resume_path, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    stor_etag = data.get('etag', '')
                    if stor_etag and self.etag and stor_etag != self.etag:
                        try:
                            os.remove(self.path)
                        except Exception:
                            pass
                    else:
                        resume_offset = os.path.getsize(self.path)
                except Exception as e:
                    logger.debug(f"Resume load error: {e}")
            self.downloaded = resume_offset

            supports_range = False
            try:
                supports_range = head.headers.get('accept-ranges', '').lower() == 'bytes' if head is not None else False
            except Exception:
                supports_range = False

            if self.total_size >= config.get("min_file_size_for_multithread", 100*1024*1024) and supports_range:
                try:
                    if not self.path.exists() or os.path.getsize(self.path) != self.total_size:
                        fd = os.open(str(self.path), os.O_RDWR | os.O_CREAT)
                        try:
                            os.ftruncate(fd, self.total_size)
                        finally:
                            os.close(fd)
                except Exception as e:
                    logger.warning(f"Preallocation failed: {e}")
                self._multi_thread_download()
            else:
                self._single_thread_download()
        except Exception as e:
            logger.error(f"Downloader run exception: {e}", exc_info=True)
            self.set_state("Failed")
            if self.on_complete:
                try:
                    self.on_complete()
                except Exception:
                    pass

    @contextmanager
    def safe_response(self, *args, **kwargs):
        r = None
        try:
            r = self.session.get(*args, **kwargs)
            r.raise_for_status()
            yield r
        finally:
            if r is not None:
                r.close()

    def _single_thread_download(self):
        self.set_state("Downloading")
        resume_offset = self.downloaded
        for attempt in range(config.get("retries", 6)):
            if self.cancelled:
                self.save_checkpoint()
                self.set_state("Failed")
                return
            try:
                headers = {}
                if resume_offset > 0:
                    headers['Range'] = f"bytes={resume_offset}-"
                with self.safe_response(self.url, stream=True, timeout=config.get("timeout", 180), headers=headers, allow_redirects=True) as r:
                    mode = 'ab' if resume_offset > 0 else 'wb'
                    with open(self.path, mode) as f:
                        last_checkpoint = time.time()
                        for chunk in r.iter_content(chunk_size=config.get("chunk_size", 2*1024*1024)):
                            if self.cancelled:
                                self.save_checkpoint()
                                self.set_state("Failed")
                                return
                            while self.paused:
                                self.set_state("Paused")
                                time.sleep(0.1)
                            if self.per_limit > 0:
                                self.per_limiter.acquire(len(chunk))
                            if global_limiter.rate > 0:
                                global_limiter.acquire(len(chunk))
                            f.write(chunk)
                            with self.lock:
                                self.downloaded += len(chunk)
                                self._update_speed(len(chunk))
                                self._progress_callback()
                            if time.time() - last_checkpoint >= config.get("checkpoint_interval_seconds", 30):
                                self.save_checkpoint()
                                last_checkpoint = time.time()
                self.clear_checkpoint()
                self.set_state("Completed")
                self._on_finished()
                return
            except Exception as e:
                logger.warning(f"Single attempt {attempt+1} failed for {self.filename}: {e}")
                time.sleep(min(2 ** attempt, 60))
        self.set_state("Failed")
        if self.on_complete:
            try:
                self.on_complete()
            except Exception:
                pass

    def _multi_thread_download(self):
        self.set_state("Downloading")
        total = self.total_size
        max_threads = max(1, config.get("max_threads", 8))
        if total > 500 * 1024 * 1024 * 1024:
            num_parts = min(max_threads, 32)
        elif total > 50 * 1024 * 1024 * 1024:
            num_parts = min(max_threads, 16)
        else:
            num_parts = min(max_threads, 8)
        part_size = total // num_parts
        threads = []
        self.part_info = {}
        for i in range(num_parts):
            start = i * part_size
            end = start + part_size - 1 if i < num_parts - 1 else total - 1
            done = False
            filesize = os.path.getsize(self.path) if self.path.exists() else 0
            if filesize >= end + 1:
                done = True
            self.part_info[i] = {"start": start, "end": end, "done": done}
        for i, info in self.part_info.items():
            if info["done"]:
                continue
            t = threading.Thread(target=self._download_part, args=(info["start"], info["end"], i), daemon=True, name=f"{self.filename}-part-{i}")
            t.start()
            threads.append(t)
        for t in threads:
            t.join(timeout=config.get("timeout", 180) * 20)
        if all(info.get("done", False) for info in self.part_info.values()):
            self.clear_checkpoint()
            self.set_state("Completed")
            self._on_finished()
        else:
            logger.warning("Some parts incomplete, switching to single-thread fallback")
            self._single_thread_download()

    def _download_part(self, start, end, idx):
        headers = {'Range': f"bytes={start}-{end}"}
        part_len = end - start + 1
        for attempt in range(config.get("retries", 6)):
            if self.cancelled:
                return
            try:
                with self.safe_response(self.url, stream=True, timeout=config.get("timeout", 180), headers=headers, allow_redirects=True) as r:
                    with open(self.path, "r+b") as f:
                        f.seek(start)
                        written = 0
                        last_checkpoint = time.time()
                        for chunk in r.iter_content(chunk_size=config.get("chunk_size", 2*1024*1024)):
                            if self.cancelled:
                                return
                            while self.paused:
                                time.sleep(0.1)
                            if self.per_limit > 0:
                                self.per_limiter.acquire(len(chunk))
                            if global_limiter.rate > 0:
                                global_limiter.acquire(len(chunk))
                            f.write(chunk)
                            written += len(chunk)
                            with self.lock:
                                self.downloaded += len(chunk)
                                self._update_speed(len(chunk))
                                self._progress_callback()
                            if time.time() - last_checkpoint >= config.get("checkpoint_interval_seconds", 30):
                                self.save_checkpoint()
                                last_checkpoint = time.time()
                        if written >= part_len:
                            self.part_info[idx]["done"] = True
                            self.save_checkpoint()
                            logger.info(f"Part {idx} done for {self.filename}")
                            return
                        else:
                            logger.warning(f"Part {idx} incomplete: {written}/{part_len}")
            except Exception as e:
                logger.warning(f"Part {idx} attempt {attempt+1} failed: {e}")
                time.sleep(min(2 ** attempt, 30))
        logger.error(f"Part {idx} failed after attempts")

    def _update_speed(self, bytes_count):
        now = time.time()
        elapsed = now - getattr(self, '_last_speed_time', self.start_time)
        self._last_speed_time = now
        self.speeds.append(bytes_count / (elapsed if elapsed > 0 else 1e-6))
        if len(self.speeds) > 10:
            self.speeds.pop(0)
        self.speed = sum(self.speeds) / len(self.speeds) if self.speeds else 0.0

    def _progress_callback(self):
        if self.on_progress:
            try:
                self.on_progress(self.downloaded, self.total_size, self.filename, self.speed, self.state)
            except Exception:
                pass

    def save_checkpoint(self):
        try:
            actual = os.path.getsize(self.path) if os.path.exists(self.path) else 0
            data = {
                'url': self.url,
                'filename': self.filename,
                'total_size': self.total_size,
                'downloaded': actual,
                'etag': self.etag,
                'last_modified': self.last_modified,
                'part_info': self.part_info,
                'state': self.state,
                'timestamp': datetime.now().isoformat()
            }
            with open(self.resume_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            logger.debug(f"Checkpoint saved for {self.filename}")
        except Exception as e:
            logger.warning(f"Checkpoint save error: {e}")

    def clear_checkpoint(self):
        try:
            if os.path.exists(self.resume_path):
                os.remove(self.resume_path)
                logger.debug(f"Checkpoint cleared for {self.filename}")
        except Exception as e:
            logger.warning(f"Clear checkpoint error: {e}")

    def _on_finished(self):
        try:
            sha = compute_sha256(self.path) if os.path.exists(self.path) else "N/A"
            save_history({
                "name": self.filename,
                "size": os.path.getsize(self.path) if os.path.exists(self.path) else 0,
                "date": datetime.now().strftime("%Y-%m-%d %H:%M"),
                "status": "Completed",
                "url": self.url,
                "sha256": sha
            })
            logger.info(f"Completed {self.filename} SHA256={sha}")
            if self.on_complete:
                try:
                    self.on_complete()
                except Exception:
                    pass
        except Exception as e:
            logger.warning(f"On finished error: {e}")

    def pause(self):
        self.paused = True
        self.set_state("Paused")
        logger.info(f"Paused {self.filename}")

    def resume(self):
        if self.paused:
            self.paused = False
            self.set_state("Downloading")
            logger.info(f"Resumed {self.filename}")

    def cancel(self):
        self.cancelled = True
        logger.info(f"Cancelled {self.filename}")
        self.set_state("Failed")

# ---------------- GUI simplified (if available) ----------------
if HAS_GUI:
    ctk.set_appearance_mode(config.get("theme", "dark"))
    ctk.set_default_color_theme("blue")

    class ModernApp(ctk.CTk):
        def __init__(self):
            super().__init__()
            self.title(f"{APP_NAME} v{APP_VERSION}")
            self.geometry("1100x750")
            self.server = LocalServer()
            self.server.start()
            self.downloads = {}
            self.download_queue = []
            self.global_speed_kbps = config.get("global_speed_limit_kbps", 0)
            set_global_limit_kbps(self.global_speed_kbps)
            self._build_ui()
            if config.get("enable_clipboard_monitor", True) and HAS_CLIPBOARD:
                self._start_clipboard_monitor()

        def _build_ui(self):
            self.sidebar = ctk.CTkFrame(self, width=240)
            self.sidebar.pack(side="left", fill="y")
            ctk.CTkLabel(self.sidebar, text=APP_NAME, font=("Arial", 18, "bold")).pack(pady=12)
            ctk.CTkLabel(self.sidebar, text=f"v{APP_VERSION}", font=("Arial", 10)).pack(pady=(0,12))
            ctk.CTkButton(self.sidebar, text="Settings", command=self.open_settings, width=200).pack(pady=8)
            ctk.CTkButton(self.sidebar, text="Pause All", command=self.pause_all, width=200).pack(pady=8)
            ctk.CTkButton(self.sidebar, text="Resume All", command=self.resume_all, width=200).pack(pady=8)
            self.main_area = ctk.CTkFrame(self)
            self.main_area.pack(side="right", fill="both", expand=True, padx=12, pady=12)
            input_frame = ctk.CTkFrame(self.main_area, height=60)
            input_frame.pack(fill="x", pady=(0,8))
            input_frame.pack_propagate(False)
            self.url_entry = ctk.CTkEntry(input_frame, placeholder_text="Paste download URL here", height=36)
            self.url_entry.pack(side="left", fill="x", expand=True, padx=(12,8), pady=12)
            ctk.CTkButton(input_frame, text="Add", width=100, command=self._on_add).pack(side="left", padx=(0,12))
            self.scroll = ctk.CTkScrollableFrame(self.main_area)
            self.scroll.pack(fill="both", expand=True)

        def _on_add(self):
            url = self.url_entry.get().strip()
            if not url:
                messagebox.showwarning("Input", "Please provide URL")
                return
            self.add_download(url)
            self.url_entry.delete(0, "end")

        def add_download(self, url, per_limit_kbps=0):
            if url in self.downloads:
                messagebox.showinfo("Info", "URL already in list")
                return
            active = sum(1 for v in self.downloads.values() if not v.get("finished", False))
            if active >= config.get("max_simultaneous_downloads", 2):
                self.download_queue.append((url, per_limit_kbps))
                self._create_card_placeholder(url, "Queued")
                return
            dl = Downloader(url, app=self, on_progress=self._on_progress, on_complete=lambda u=url: self._on_complete(u), per_limit_kbps=per_limit_kbps)
            card = self._create_card(dl)
            self.downloads[url] = {"downloader": dl, "card": card, "finished": False}
            dl.start()

        def _start_next_in_queue(self):
            active = sum(1 for v in self.downloads.values() if not v.get("finished", False))
            while self.download_queue and active < config.get("max_simultaneous_downloads", 2):
                url, per = self.download_queue.pop(0)
                dl = Downloader(url, app=self, on_progress=self._on_progress, on_complete=lambda u=url: self._on_complete(u), per_limit_kbps=per)
                card = self._create_card(dl)
                self.downloads[url] = {"downloader": dl, "card": card, "finished": False}
                dl.start()
                active += 1
