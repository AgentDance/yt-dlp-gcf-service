# main.py
import os
import re
import json
import glob
import time
import random
import logging
from datetime import timedelta
from typing import List, Dict, Any, Optional

from functions_framework import http
from google.cloud import storage

# ===== 优先方案：文本字幕 API =====
from youtube_transcript_api import (
    YouTubeTranscriptApi,
    TranscriptsDisabled,
    NoTranscriptFound,
    VideoUnavailable,
)

# ===== 备用方案：yt-dlp 仅拉取字幕文件 =====
import yt_dlp
from yt_dlp.utils import DownloadError
from yt_dlp.networking.exceptions import HTTPError as YTDLPHTTPError

logging.basicConfig(level=logging.INFO)

# =============================================================================
# 环境变量
# =============================================================================
BUCKET = os.environ.get("VIDEO_BUCKET")  # 可选：有就上传备份；没有也能返回正文
DEFAULT_FORMAT = (os.environ.get("DEFAULT_FORMAT") or "vtt").lower()
ENABLE_SIGNED_URL = os.environ.get("ENABLE_SIGNED_URL", "false").lower() == "true"
SERVICE_ACCOUNT_EMAIL = os.environ.get("SERVICE_ACCOUNT_EMAIL")  # 仅当 ENABLE_SIGNED_URL=true 时需要

# Cookies 两种来源：
# 1) 冷启动从 env 注入：YT_COOKIES_TEXT（Secret 注入），写到 /tmp/cookies.txt
# 2) 单次请求的 cookie_header（优先级更高）
YT_COOKIES_PATH = os.environ.get("YT_COOKIES_PATH", "/tmp/cookies.txt")
YT_COOKIES_TEXT = os.environ.get("YT_COOKIES_TEXT")  # 通过 --set-secrets 注入

OUT_DIR = "/tmp"

# YouTube ID 解析（兼容常见形式）
_YT_ID_RE = re.compile(
    r"(?:youtu\.be/|v=|shorts/|live/|embed/)([A-Za-z0-9_-]{11})|^([A-Za-z0-9_-]{11})$"
)

def _parse_video_id(url_or_id: str) -> str:
    s = (url_or_id or "").strip()
    m = _YT_ID_RE.search(s)
    if not m:
        return s
    return next(g for g in m.groups() if g)  # 第一个非 None 的捕获组

# =============================================================================
# Cookies 处理
# =============================================================================
def _write_cookiefile_from_header(cookie_header: str, out_path: str):
    """
    将 "NAME=VALUE; NAME2=VALUE2" 的 Header 串转换为最小 Netscape cookies.txt，
    生成两个域：.youtube.com 与 .www.youtube.com
    """
    expires = 2147483647  # 远期
    lines = ["# Netscape HTTP Cookie File"]
    # 粗切分，保留含 '=' 的键值
    parts = [p.strip() for p in cookie_header.split(";") if p.strip()]
    kvs = []
    for p in parts:
        if "=" in p:
            name, val = p.split("=", 1)
            kvs.append((name.strip(), val.strip()))
    for domain in [".youtube.com", ".www.youtube.com"]:
        for name, val in kvs:
            # Netscape 列：domain, include_subdomains, path, secure, expires, name, value
            lines.append("\t".join([domain, "TRUE", "/", "TRUE", str(expires), name, val]))
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")

def _hydrate_cookies_from_env():
    """
    若存在 YT_COOKIES_TEXT（通过 --set-secrets 注入），在冷启动时将其写入 YT_COOKIES_PATH。
    兼容两种格式：
      1) Netscape cookies.txt（以 '# Netscape HTTP Cookie File' 开头）
      2) 'NAME=VALUE; NAME2=VALUE2; ...' 的 Header 串
    """
    try:
        if not YT_COOKIES_TEXT:
            return
        text = YT_COOKIES_TEXT.strip()
        os.makedirs(os.path.dirname(YT_COOKIES_PATH), exist_ok=True)
        if text.startswith("# Netscape HTTP Cookie File"):
            with open(YT_COOKIES_PATH, "w", encoding="utf-8") as f:
                f.write(text if text.endswith("\n") else text + "\n")
            logging.info(f"[cookies] hydrated Netscape to {YT_COOKIES_PATH}")
        else:
            _write_cookiefile_from_header(text, YT_COOKIES_PATH)
            logging.info(f"[cookies] hydrated from header to {YT_COOKIES_PATH}")
    except Exception as e:
        logging.warning(f"[cookies] hydrate failed: {e}")

# 冷启动做一次
_hydrate_cookies_from_env()

# =============================================================================
# SRT/VTT 格式化
# =============================================================================
def _fmt_srt_time(t: float) -> str:
    h = int(t // 3600); m = int((t % 3600) // 60); s = int(t % 60)
    ms = int(round((t - int(t)) * 1000))
    return f"{h:02}:{m:02}:{s:02},{ms:03}"

def _fmt_vtt_time(t: float) -> str:
    h = int(t // 3600); m = int((t % 3600) // 60); s = int(t % 60)
    ms = int(round((t - int(t)) * 1000))
    return f"{h:02}:{m:02}:{s:02}.{ms:03}"

def _to_srt(snippets: List[Dict[str, Any]]) -> str:
    lines = []
    for i, seg in enumerate(snippets, 1):
        start = seg["start"]; end = start + seg.get("duration", 0.0)
        lines.append(str(i))
        lines.append(f"{_fmt_srt_time(start)} --> {_fmt_srt_time(end)}")
        lines.append(seg.get("text", "").replace("\n", " ").strip())
        lines.append("")
    return "\n".join(lines)

def _to_vtt(snippets: List[Dict[str, Any]]) -> str:
    out = ["WEBVTT", ""]
    for seg in snippets:
        start = seg["start"]; end = start + seg.get("duration", 0.0)
        out.append(f"{_fmt_vtt_time(start)} --> {_fmt_vtt_time(end)}")
        out.append(seg.get("text", "").replace("\n", " ").strip())
        out.append("")
    return "\n".join(out)

# =============================================================================
# GCS 上传 +（可选）签名 URL
# =============================================================================
def _upload_to_gcs(local_path: str, bucket: str, content_type: str) -> Optional[str]:
    if not bucket:
        return None
    try:
        client = storage.Client()
        bkt = client.bucket(bucket)
        blob_name = f"subs/{os.path.basename(local_path)}"
        blob = bkt.blob(blob_name)
        blob.upload_from_filename(local_path, content_type=content_type)
        return f"gs://{bucket}/{blob_name}"
    except Exception as e:
        logging.warning(f"[GCS] upload failed: {e}")
        return None

def _maybe_signed_url(gs_uri: str, ttl: int) -> Optional[str]:
    """
    可选：尝试生成签名 URL；失败则返回 None，不抛错。
    需要 ENABLE_SIGNED_URL=true 且 SERVICE_ACCOUNT_EMAIL 已设置并有权限。
    """
    if not (gs_uri and gs_uri.startswith("gs://") and ENABLE_SIGNED_URL and SERVICE_ACCOUNT_EMAIL):
        return None
    try:
        _, path = gs_uri.split("gs://", 1)
        bucket, blob_name = path.split("/", 1)
        client = storage.Client()
        blob = client.bucket(bucket).blob(blob_name)

        # 使用 IAM Signer（无需本地私钥）
        from google.auth.transport.requests import Request
        from google.auth import compute_engine
        from google.auth.iam import Signer

        source_credentials = compute_engine.Credentials()
        request = Request()
        signer = Signer(request, source_credentials, SERVICE_ACCOUNT_EMAIL)

        class _SignCred:
            def __init__(self, signer, email):
                self.signer = signer
                self.signer_email = email

        signed_credentials = _SignCred(signer, SERVICE_ACCOUNT_EMAIL)

        url = blob.generate_signed_url(
            expiration=timedelta(seconds=ttl),
            version="v4",
            credentials=signed_credentials,
        )
        return url
    except Exception as e:
        logging.warning(f"[GCS] signed URL failed: {e}")
        return None

# =============================================================================
# 方案 A：youtube-transcript-api
# =============================================================================
def _write_snippets_to_file(snips: List[Dict[str, Any]], video_id: str, lang: str, fmt: str) -> Dict[str, str]:
    ext = "srt" if fmt == "srt" else "vtt"
    text = _to_srt(snips) if ext == "srt" else _to_vtt(snips)
    fname = f"{video_id}.{lang}.{ext}"
    fpath = f"{OUT_DIR}/{fname}"
    with open(fpath, "w", encoding="utf-8") as f:
        f.write(text)
    return {"lang": lang, "path": fpath, "text": text}

def _fetch_with_transcript_api_list(video_id: str, target_langs: Optional[List[str]], fmt: str, translate_missing: bool):
    tlist = YouTubeTranscriptApi.list_transcripts(video_id)
    langs_meta = sorted({t.language_code for t in tlist})
    want_langs = target_langs or list(langs_meta)
    results = []
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
            logging.info(f"[TranscriptAPI:list] No transcript for {lang}")
            continue
        data = transcript.fetch()
        raw = [dict(text=x["text"], start=float(x["start"]), duration=float(x.get("duration", 0.0))) for x in data]
        results.append(_write_snippets_to_file(raw, video_id, lang, fmt))
    return {"ok": True, "files": results, "languages_meta": sorted(langs_meta)}

def _fetch_with_transcript_api_get(video_id: str, target_langs: Optional[List[str]], fmt: str, translate_missing: bool):
    probe_langs = target_langs or ["en", "en-US", "en-GB", "zh-Hans", "zh-Hant", "ja", "es"]
    results = []
    meta = set()
    for lang in probe_langs:
        try:
            data = YouTubeTranscriptApi.get_transcript(video_id, languages=[lang])
        except (NoTranscriptFound, TranscriptsDisabled, VideoUnavailable):
            continue
        except Exception as e:
            logging.warning(f"[TranscriptAPI:get] {lang} -> {e}")
            continue
        raw = [dict(text=x["text"], start=float(x["start"]), duration=float(x.get("duration", 0.0))) for x in data]
        results.append(_write_snippets_to_file(raw, video_id, lang, fmt))
        meta.add(lang)
    return {"ok": True, "files": results, "languages_meta": sorted(meta)}

def fetch_with_transcript_api(video_id: str, target_langs: Optional[List[str]], fmt: str, translate_missing: bool):
    if hasattr(YouTubeTranscriptApi, "list_transcripts"):
        return _fetch_with_transcript_api_list(video_id, target_langs, fmt, translate_missing)
    else:
        return _fetch_with_transcript_api_get(video_id, target_langs, fmt, translate_missing)

# =============================================================================
# 方案 B：yt-dlp（降级，抗 429）
# =============================================================================
_ANDROID_UA = "com.google.android.youtube/19.15.38 (Linux; U; Android 13) gzip"

def _yt_dlp_opts_base(ext: str, accept_lang_header: Optional[str], cookiefile: Optional[str]):
    headers = {"User-Agent": _ANDROID_UA}
    if accept_lang_header:
        headers["Accept-Language"] = accept_lang_header
    opts = {
        "skip_download": True,
        "writesubtitles": True,
        "writeautomaticsub": True,
        "subtitlesformat": ext,  # "vtt" or "srt"
        "outtmpl": f"{OUT_DIR}/%(title).80s-%(id)s.%(ext)s",
        "quiet": True,
        "retries": 10,
        "retry_sleep": "exponential",
        "sleep_interval_requests": 2.0,
        "max_sleep_interval_requests": 6.0,
        "throttledratelimit": 1024 * 256,
        "http_headers": headers,
        "socket_timeout": 30,
        "extractor_args": {"youtube": {"player_client": ["android"]}},
    }
    if cookiefile and os.path.exists(cookiefile):
        opts["cookiefile"] = cookiefile
        logging.info("[yt-dlp] Using cookiefile")
    return opts

_YT_CLIENT_PROFILES = [
    {"extractor_args": {"youtube": {"player_client": ["android"]}}},
    {"extractor_args": {"youtube": {"player_client": ["android_embedded"]}}},
    {"extractor_args": {"youtube": {"player_client": ["web_embedded", "android"]}}},
    {"extractor_args": {"youtube": {"player_client": ["mweb"]}}},
]

def fetch_with_ytdlp_smart(url_or_id: str, target_langs: Optional[List[str]], fmt: str):
    ext = "srt" if fmt == "srt" else "vtt"
    accept_lang = None
    if target_langs:
        accept_lang = ",".join([f"{l};q=1.0" for l in target_langs[:3]])
    base_opts = _yt_dlp_opts_base(ext, accept_lang, YT_COOKIES_PATH)
    if target_langs and len(target_langs) > 0:
        base_opts["subtitleslangs"] = target_langs
    else:
        base_opts["allsubtitles"] = True

    last_err = None
    for idx, profile in enumerate(_YT_CLIENT_PROFILES, 1):
        opts = dict(base_opts)
        ea = dict(base_opts.get("extractor_args", {}))
        for k, v in profile.get("extractor_args", {}).items():
            ea[k] = v
        opts["extractor_args"] = ea

        time.sleep(random.uniform(0.5, 1.5))
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
                with open(p, "r", encoding="utf-8") as f:
                    text = f.read()
                files.append({"lang": lang, "path": p, "text": text})
            if files:
                return {"ok": True, "files": files}
            last_err = RuntimeError("no subtitle files written")
        except (DownloadError, YTDLPHTTPError) as e:
            last_err = e
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
            time.sleep(random.uniform(1.0, 3.0))
    if last_err:
        raise last_err
    raise RuntimeError("yt-dlp fallback failed without specific error")

# =============================================================================
# HTTP 入口
# =============================================================================
@http
def fetch_subtitles(request):
    # 对任意 GET 请求返回 JSON 健康信息，便于 jq 校验
    if request.method == "GET":
        ok = os.path.exists(YT_COOKIES_PATH)
        body = {"ok": True, "cookies_file": ok, "cookies_path": YT_COOKIES_PATH}
        return (json.dumps(body), 200, {"Content-Type": "application/json"})

    if request.method != "POST":
        return ("Use POST with JSON body.", 405)

    body = request.get_json(silent=True) or {}
    url = body.get("url") or body.get("id")
    if not url:
        return ("Missing 'url' or 'id'", 400)

    fmt = (body.get("format") or DEFAULT_FORMAT).lower()
    langs = body.get("langs")
    translate_missing = bool(body.get("translate_missing", True))
    ttl = int(body.get("ttl_seconds", 3600))

    # 单次请求临时 cookies（优先于全局）
    cookie_header = body.get("cookie_header")
    temp_cookie_path = None
    if isinstance(cookie_header, str) and cookie_header.strip():
        temp_cookie_path = "/tmp/req_cookies.txt"
        _write_cookiefile_from_header(cookie_header.strip(), temp_cookie_path)
        os.environ["YT_COOKIES_PATH"] = temp_cookie_path  # 仅本次请求使用

    video_id = _parse_video_id(url)
    logging.info(f"[REQ] video_id={video_id} fmt={fmt} langs={langs} translate_missing={translate_missing}")

    files: List[Dict[str, Any]] = []
    meta_langs: List[str] = []

    # 方案 A：transcript api
    try:
        r = fetch_with_transcript_api(video_id, langs, fmt, translate_missing)
        files = r["files"]
        meta_langs = r.get("languages_meta", [])
        logging.info(f"[TranscriptAPI] got files={len(files)} langs_meta={meta_langs}")
    except (TranscriptsDisabled, NoTranscriptFound, VideoUnavailable) as e:
        logging.warning(f"[TranscriptAPI] known: {e}")
    except Exception as e:
        logging.exception(f"[TranscriptAPI] unexpected: {e}")

    # 方案 B：yt-dlp
    if not files:
        try:
            y = fetch_with_ytdlp_smart(url, langs, fmt)
            files = y["files"]
            logging.info(f"[yt-dlp] got files={len(files)}")
        except Exception as e:
            # 清理临时 cookies
            if temp_cookie_path:
                try:
                    os.remove(temp_cookie_path)
                except Exception:
                    pass
                os.environ.pop("YT_COOKIES_PATH", None)

            logging.exception(f"[yt-dlp] failed: {e}")
            return (json.dumps({"ok": False, "video_id": video_id, "error": str(e)}, ensure_ascii=False),
                    502, {"Content-Type": "application/json"})

    if not files:
        # 清理临时 cookies
        if temp_cookie_path:
            try:
                os.remove(temp_cookie_path)
            except Exception:
                pass
            os.environ.pop("YT_COOKIES_PATH", None)

        return (json.dumps({
            "ok": False, "video_id": video_id,
            "error": "No subtitles found by either transcript API or yt-dlp."
        }, ensure_ascii=False), 404, {"Content-Type": "application/json"})

    # 上传（可选）+ 返回正文（一定有）
    content_type = "text/vtt" if fmt == "vtt" else "text/plain"
    out = []
    for f in files:
        gs_uri = None
        try:
            gs_uri = _upload_to_gcs(f["path"], BUCKET, content_type=content_type) if BUCKET else None
        except Exception as e:
            logging.warning(f"[GCS] upload err: {e}")
        signed_url = _maybe_signed_url(gs_uri, ttl) if gs_uri else None
        out.append({
            "lang": f["lang"],
            "filename": os.path.basename(f["path"]),
            "gcs_uri": gs_uri,
            "signed_url": signed_url,
            "content": f["text"],  # 直接返回字幕文本，保证可用
        })
        try:
            os.remove(f["path"])
        except Exception:
            pass

    # 清理临时 cookies
    if temp_cookie_path:
        try:
            os.remove(temp_cookie_path)
        except Exception:
            pass
        os.environ.pop("YT_COOKIES_PATH", None)

    payload = {
        "ok": True,
        "video_id": video_id,
        "format": fmt,
        "files": out,
        "languages_detected": meta_langs
    }
    return (json.dumps(payload, ensure_ascii=False), 200, {"Content-Type": "application/json"})
