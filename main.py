import os
import re
import json
import glob
import time
import random
import logging
from datetime import timedelta

from functions_framework import http
from google.cloud import storage

# —— 优先方案：YouTube Transcript API（文本+可翻译）——
from youtube_transcript_api import (
    YouTubeTranscriptApi,
    TranscriptsDisabled,
    NoTranscriptFound,
    VideoUnavailable,
)

# —— 备用方案：yt-dlp（只下载字幕文件）——
import yt_dlp
from yt_dlp.utils import DownloadError
from yt_dlp.networking.exceptions import HTTPError as YTDLPHTTPError

logging.basicConfig(level=logging.INFO)

# ===== 环境变量 =====
BUCKET = os.environ.get("VIDEO_BUCKET")            # 必填：目标 GCS 桶名
DEFAULT_FORMAT = (os.environ.get("DEFAULT_FORMAT") or "vtt").lower()  # vtt/srt
# 可选：cookies 路径；若文件存在则自动用于 yt-dlp（提高成功率）
YT_COOKIES_PATH = os.environ.get("YT_COOKIES_PATH", "/var/secrets/cookies.txt")

# Cloud Run/Functions 允许写 /tmp
OUT_DIR = "/tmp"

# 解析 YT ID
_YT_ID_RE = re.compile(r"(?:youtu\.be/|v=)([A-Za-z0-9_-]{11})")

def _parse_video_id(url_or_id: str) -> str:
    m = _YT_ID_RE.search(url_or_id or "")
    return m.group(1) if m else (url_or_id or "").strip()

# ===== 时间格式化（SRT/VTT）=====
def _fmt_srt_time(t: float) -> str:
    h = int(t // 3600); m = int((t % 3600) // 60); s = int(t % 60)
    ms = int(round((t - int(t)) * 1000))
    return f"{h:02}:{m:02}:{s:02},{ms:03}"

def _fmt_vtt_time(t: float) -> str:
    h = int(t // 3600); m = int((t % 3600) // 60); s = int(t % 60)
    ms = int(round((t - int(t)) * 1000))
    return f"{h:02}:{m:02}:{s:02}.{ms:03}"

def _to_srt(snippets) -> str:
    lines = []
    for i, seg in enumerate(snippets, 1):
        start = seg["start"]
        end = start + seg.get("duration", 0)
        lines.append(str(i))
        lines.append(f"{_fmt_srt_time(start)} --> {_fmt_srt_time(end)}")
        lines.append(seg.get("text", "").replace("\n", " ").strip())
        lines.append("")
    return "\n".join(lines)

def _to_vtt(snippets) -> str:
    out = ["WEBVTT", ""]
    for seg in snippets:
        start = seg["start"]
        end = start + seg.get("duration", 0)
        out.append(f"{_fmt_vtt_time(start)} --> {_fmt_vtt_time(end)}")
        out.append(seg.get("text", "").replace("\n", " ").strip())
        out.append("")
    return "\n".join(out)

def _write_file(path: str, content: str):
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)

def _upload_and_sign(local_path: str, bucket: str, ttl: int, content_type: str) -> str:
    client = storage.Client()
    bkt = client.bucket(bucket)
    blob_name = f"subs/{os.path.basename(local_path)}"
    blob = bkt.blob(blob_name)
    blob.upload_from_filename(local_path, content_type=content_type)
    return blob.generate_signed_url(expiration=timedelta(seconds=ttl), version="v4")

# ===== 方案 A：youtube-transcript-api（优先）=====
def fetch_with_transcript_api(video_id: str, target_langs, fmt: str, translate_missing: bool):
    """
    返回 {"ok": True, "files": [{"lang","path"}], "languages_meta":[...]}
    """
    tlist = YouTubeTranscriptApi.list_transcripts(video_id)
    langs_meta = sorted({t.language_code for t in tlist})

    results = []
    want_langs = target_langs or list(langs_meta)  # 未指定则抓所有已有语言

    for lang in want_langs:
        transcript = None
        try:
            transcript = tlist.find_transcript([lang])
        except Exception:
            if translate_missing:
                for t in tlist:
                    if getattr(t, "is_translatable", False):
                        try:
                            transcript = t.translate(lang)
                            break
                        except Exception:
                            continue
        if not transcript:
            logging.info(f"[TranscriptAPI] No transcript for {lang}")
            continue

        data = transcript.fetch()  # [{text,start,duration}, ...]
        raw = [dict(text=x["text"], start=float(x["start"]), duration=float(x.get("duration", 0.0))) for x in data]

        ext = "srt" if fmt == "srt" else "vtt"
        fname = f"{video_id}.{lang}.{ext}"
        fpath = f"{OUT_DIR}/{fname}"
        text = _to_srt(raw) if ext == "srt" else _to_vtt(raw)
        _write_file(fpath, text)
        results.append({"lang": lang, "path": fpath})

    return {"ok": True, "files": results, "languages_meta": sorted(langs_meta)}

# ===== 方案 B：yt-dlp（降级，抗 429 强化版）=====
_ANDROID_UA = "com.google.android.youtube/19.15.38 (Linux; U; Android 13) gzip"
def _yt_dlp_opts_base(ext: str, accept_lang_header: str | None, cookiefile: str | None):
    headers = {
        "User-Agent": _ANDROID_UA,
    }
    if accept_lang_header:
        headers["Accept-Language"] = accept_lang_header

    opts = {
        "skip_download": True,
        "writesubtitles": True,
        "writeautomaticsub": True,
        "subtitlesformat": ext,                         # vtt/srt
        "outtmpl": f"{OUT_DIR}/%(title).80s-%(id)s.%(ext)s",
        "quiet": True,

        # —— 抗 429：退避 + 降速 + Android 客户端画像 ——
        "retries": 10,
        "retry_sleep": "exponential",
        "sleep_interval_requests": 2.0,
        "max_sleep_interval_requests": 6.0,
        "throttledratelimit": 1024 * 256,              # 256 KiB/s（谨慎：部分网络会更稳）
        "http_headers": headers,
        "socket_timeout": 30,
        "extractor_args": {
            "youtube": {
                "player_client": ["android"],          # “移动端”画像
            }
        },
    }
    if cookiefile and os.path.exists(cookiefile):
        opts["cookiefile"] = cookiefile
        logging.info("[yt-dlp] Using cookiefile")

    return opts

_YT_CLIENT_PROFILES = [
    # 多画像轮换：都属“更常见客户端”，帮助规避某些路径限流
    {"extractor_args": {"youtube": {"player_client": ["android"]}}},
    {"extractor_args": {"youtube": {"player_client": ["android_embedded"]}}},
    {"extractor_args": {"youtube": {"player_client": ["web_embedded", "android"]}}},
    {"extractor_args": {"youtube": {"player_client": ["mweb"]}}},
    # TV/leanback 有时也行，但某些视频不列字幕；可按需开启：
    # {"extractor_args": {"youtube": {"player_client": ["tv_embedded"]}}},
]

def fetch_with_ytdlp_smart(url_or_id: str, target_langs, fmt: str):
    """
    多画像轮换 + 退避重试；只下载字幕
    """
    ext = "srt" if fmt == "srt" else "vtt"

    # 根据请求语言拼 Accept-Language（提高匹配机会）
    accept_lang = None
    if target_langs and isinstance(target_langs, list) and len(target_langs) > 0:
        accept_lang = ",".join([f"{l};q=1.0" for l in target_langs[:3]])

    base_opts = _yt_dlp_opts_base(ext, accept_lang, YT_COOKIES_PATH)
    # 语言：未指定则抓所有
    if target_langs and len(target_langs) > 0:
        base_opts["subtitleslangs"] = target_langs
    else:
        base_opts["allsubtitles"] = True

    last_err = None
    for idx, profile in enumerate(_YT_CLIENT_PROFILES, 1):
        opts = dict(base_opts)
        # 合并画像配置
        ea = dict(base_opts.get("extractor_args", {}))
        for k, v in profile.get("extractor_args", {}).items():
            ea[k] = v
        opts["extractor_args"] = ea

        # 额外的随机抖动，进一步“降速”
        jitter = random.uniform(0.5, 1.5)
        time.sleep(jitter)

        logging.info(f"[yt-dlp] Try profile #{idx}: {opts['extractor_args']}")

        try:
            with yt_dlp.YoutubeDL(opts) as ydl:
                info = ydl.extract_info(url_or_id, download=True)
                vid = info.get("id")
            paths = glob.glob(f"{OUT_DIR}/*-{vid}.*.{ext}")
            files = []
            for p in paths:
                base = os.path.basename(p)
                parts = base.split(".")
                lang = parts[-2] if len(parts) >= 3 else "unknown"
                files.append({"lang": lang, "path": p})
            if files:
                return {"ok": True, "files": files}
            # 没拿到文件，当作失败进入下一画像
            last_err = RuntimeError("no subtitle files written")
        except (DownloadError, YTDLPHTTPError) as e:
            last_err = e
            # 429/403 场景：再退避一下再换画像
            msg = str(e)
            if "429" in msg or "Too Many Requests" in msg or "403" in msg:
                sleep_s = random.uniform(3.0, 7.0)
                logging.warning(f"[yt-dlp] {msg} -> backoff {sleep_s:.1f}s")
                time.sleep(sleep_s)
            else:
                logging.warning(f"[yt-dlp] {msg}")
        except Exception as e:
            last_err = e
            logging.exception(f"[yt-dlp] unexpected: {e}")
            # 小退避
            time.sleep(random.uniform(1.0, 3.0))

    # 全部失败
    if last_err:
        raise last_err
    raise RuntimeError("yt-dlp fallback failed without specific error")

# ===== HTTP 入口 =====
@http
def fetch_subtitles(request):
    if request.method != "POST":
        return ("Use POST with JSON body.", 405)

    if not BUCKET:
        return ("Missing env VIDEO_BUCKET", 500)

    body = request.get_json(silent=True) or {}
    url = body.get("url") or body.get("id")
    if not url:
        return ("Missing 'url' or 'id'", 400)

    fmt = (body.get("format") or DEFAULT_FORMAT).lower()
    langs = body.get("langs")
    translate_missing = body.get("translate_missing", True)
    ttl = int(body.get("ttl_seconds", 3600))

    video_id = _parse_video_id(url)
    logging.info(f"[REQ] video_id={video_id} fmt={fmt} langs={langs} translate_missing={translate_missing}")

    files = []
    meta_langs = []

    # —— 方案 A：Transcript API ——（最不容易 429）
    try:
        r = fetch_with_transcript_api(video_id, langs, fmt, translate_missing)
        files = r["files"]
        meta_langs = r.get("languages_meta", [])
        logging.info(f"[TranscriptAPI] got files={len(files)} langs_meta={meta_langs}")
    except (TranscriptsDisabled, NoTranscriptFound, VideoUnavailable) as e:
        logging.warning(f"[TranscriptAPI] known: {e}")
    except Exception as e:
        logging.exception(f"[TranscriptAPI] unexpected: {e}")

    # —— 方案 B：yt-dlp ——（加固版）
    if not files:
        try:
            y = fetch_with_ytdlp_smart(url, langs, fmt)
            files = y["files"]
            logging.info(f"[yt-dlp] got files={len(files)}")
        except Exception as e:
            logging.exception(f"[yt-dlp] failed: {e}")
            return (json.dumps({"ok": False, "video_id": video_id, "error": str(e)}),
                    502, {"Content-Type": "application/json"})

    if not files:
        return (json.dumps({
            "ok": False,
            "video_id": video_id,
            "error": "No subtitles found by either transcript API or yt-dlp."
        }, ensure_ascii=False), 404, {"Content-Type": "application/json"})

    # 上传 + 签名
    content_type = "text/vtt" if fmt == "vtt" else "text/plain"
    out = []
    for f in files:
        try:
            signed = _upload_and_sign(f["path"], BUCKET, ttl, content_type=content_type)
        finally:
            try:
                os.remove(f["path"])
            except Exception:
                pass
        out.append({"lang": f["lang"], "signed_url": signed})

    payload = {"ok": True, "video_id": video_id, "format": fmt, "files": out,
               "languages_detected": meta_langs}
    return (json.dumps(payload, ensure_ascii=False), 200, {"Content-Type": "application/json"})
