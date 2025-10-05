import os
import re
import json
import glob
from datetime import timedelta

from functions_framework import http
from google.cloud import storage

# 优先库
from youtube_transcript_api import (
    YouTubeTranscriptApi,
    TranscriptsDisabled,
    NoTranscriptFound,
    VideoUnavailable,
)

# 备用库
import yt_dlp

# ====== 配置 ======
BUCKET = os.environ.get("VIDEO_BUCKET")  # 必填：目标 GCS 桶名
DEFAULT_FORMAT = "vtt"  # vtt 或 srt

# Cloud Run/Functions 的可写目录：/tmp（实例级临时盘）
# 注意：/tmp 在函数实例生命周期内有效，且受内存/磁盘配额限制。用后建议删除。 
# （Cloud Run/Functions 写文件推荐使用 /tmp） 
# =================

# ---------- 小工具 ----------
_YT_ID_RE = re.compile(
    r"(?:youtu\.be/|v=)([A-Za-z0-9_-]{11})"
)

def _parse_video_id(url_or_id: str) -> str:
    m = _YT_ID_RE.search(url_or_id)
    return m.group(1) if m else url_or_id.strip()

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
        lines.append("")  # blank
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
    url = blob.generate_signed_url(expiration=timedelta(seconds=ttl), version="v4")
    return url

# ---------- 方案 A：youtube-transcript-api ----------
def fetch_with_transcript_api(video_id: str, target_langs: list[str] | None, fmt: str, translate_missing: bool):
    """
    返回 {"ok": True, "files": [{"lang","path"}], "languages_meta":[...]} 或抛异常
    """
    api = YouTubeTranscriptApi()
    # 列出可用字幕（手动/自动）与可翻译目标
    tlist = api.list(video_id)  # TranscriptList
    langs_meta = sorted({t.language_code for t in tlist})

    results = []
    want_langs = target_langs or list(langs_meta)  # 未指定则抓所有已提供语言

    for lang in want_langs:
        transcript = None
        # 优先找该语言（手动优先于自动，库本身就有优先级）
        try:
            transcript = tlist.find_transcript([lang])
        except Exception:
            # 若没该语言，且允许翻译，则从任意可翻译字幕翻到目标语言
            if translate_missing:
                for t in tlist:
                    if t.is_translatable:
                        try:
                            transcript = t.translate(lang)
                            break
                        except Exception:
                            continue
        if not transcript:
            continue

        data = transcript.fetch()  # 列表：{text,start,duration} ...
        raw = [dict(text=x["text"], start=float(x["start"]), duration=float(x.get("duration", 0.0))) for x in data]

        ext = "srt" if fmt == "srt" else "vtt"
        fname = f"{video_id}.{lang}.{ext}"
        fpath = f"/tmp/{fname}"

        text = _to_srt(raw) if ext == "srt" else _to_vtt(raw)
        _write_file(fpath, text)
        results.append({"lang": lang, "path": fpath})

    return {
        "ok": True,
        "files": results,
        "languages_meta": sorted(langs_meta),
    }

# ---------- 方案 B：yt-dlp（仅下载字幕） ----------
def fetch_with_ytdlp(url_or_id: str, target_langs: list[str] | None, fmt: str):
    """
    使用 yt-dlp 只下载字幕（含自动字幕），返回 {"ok":True,"files":[{"lang","path"}]}
    """
    ext = "srt" if fmt == "srt" else "vtt"
    ydl_opts = {
        "skip_download": True,
        "writesubtitles": True,
        "writeautomaticsub": True,    # 需要时可抓自动字幕
        "subtitlesformat": ext,       # vtt/srt
        "outtmpl": "/tmp/%(title).80s-%(id)s.%(ext)s",
        "quiet": True,
        # 未指定语言 -> 抓全部
        **({"subtitleslangs": target_langs} if (target_langs and len(target_langs) > 0) else {"allsubtitles": True}),
    }
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(url_or_id, download=True)
        vid = info.get("id")

    # yt-dlp 命名形如：Title-<id>.<lang>.<ext>
    paths = glob.glob(f"/tmp/*-{vid}.*.{ext}")
    files = []
    for p in paths:
        base = os.path.basename(p)
        parts = base.split(".")
        lang = parts[-2] if len(parts) >= 3 else "unknown"
        files.append({"lang": lang, "path": p})
    return {"ok": True, "files": files}

# ---------- HTTP 入口 ----------
@http
def fetch_subtitles(request):
    """
    POST JSON:
    {
      "url": "...",                        # 或 "id": "VIDEO_ID"
      "langs": ["en","zh-Hans"],           # 可选；不传则抓所有可用语言
      "format": "vtt" | "srt",             # 可选，默认 vtt
      "translate_missing": true,           # 可选，默认 true：缺某语言时尝试翻译
      "ttl_seconds": 3600                  # 可选，签名 URL 有效期
    }
    """
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

    # 先走 Transcript API
    files = []
    meta_langs = []
    try:
        r = fetch_with_transcript_api(video_id, langs, fmt, translate_missing)
        files = r["files"]
        meta_langs = r.get("languages_meta", [])
    except (TranscriptsDisabled, NoTranscriptFound, VideoUnavailable) as e:
        # 这几类是常见的“拿不到文本字幕”的场景，转用 yt-dlp
        pass
    except Exception as e:
        # 其他异常（例如云端 IP 被屏蔽），也尝试降级
        pass

    # 若方案 A 没取到任何文件，降级 yt-dlp（仅下载字幕）
    if not files:
        y = fetch_with_ytdlp(url, langs, fmt)
        files = y["files"]

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
        signed = _upload_and_sign(f["path"], BUCKET, ttl, content_type=content_type)
        try: os.remove(f["path"])
        except Exception: pass
        out.append({"lang": f["lang"], "signed_url": signed})

    payload = {
        "ok": True,
        "video_id": video_id,
        "format": fmt,
        "files": out,
        "languages_detected": meta_langs   # 仅供参考
    }
    return (json.dumps(payload, ensure_ascii=False), 200, {"Content-Type": "application/json"})
