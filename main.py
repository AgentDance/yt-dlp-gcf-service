import json
import os
import subprocess
import tempfile
import youtube_dl # yt-dlp 实际安装的包名是 youtube-dl

# --- 配置 ffmpeg/ffprobe 路径 ---
# 假设 ffmpeg 和 ffprobe 位于函数根目录下的 'bin' 文件夹中
BIN_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'bin')
FFMPEG_PATH = os.path.join(BIN_DIR, 'ffmpeg')
FFPROBE_PATH = os.path.join(BIN_DIR, 'ffprobe')

# 在函数启动时确保二进制文件可执行
# Cloud Functions 会在部署时解压文件，所以这里我们确保在每次冷启动时设置权限
if os.path.exists(FFMPEG_PATH):
    os.chmod(FFMPEG_PATH, 0o755)
if os.path.exists(FFPROBE_PATH):
    os.chmod(FFPROBE_PATH, 0o755)

def get_srt_captions(video_id):
    """
    使用 yt-dlp 获取视频的英文字幕（SRT格式）。
    """
    # 临时目录，用于存储 yt-dlp 下载的字幕文件。
    # 在 Cloud Functions 中，只有 /tmp 目录是可写的。
    temp_output_dir = tempfile.gettempdir()

    ydl_opts = {
        'writesubtitles': True,
        'subtitleslangs': ['en'], # 优先获取英文字幕
        'subtitlesformat': 'srt',
        'skip_download': True, # 只获取字幕，不下载视频
        'outtmpl': os.path.join(temp_output_dir, '%(id)s.%(ext)s'), # 输出到临时目录
        'restrictfilenames': True,
        'quiet': True, # 安静模式，减少日志输出
        'no_warnings': True,
        'format': 'bestaudio/best', # 即使跳过下载，yt-dlp也可能需要这个参数来解析信息
        'postprocessors': [{
            'key': 'FFmpegExtractAudio',
            'preferredcodec': 'mp3',
            'preferredquality': '192',
        }, {
            'key': 'FFmpegMetadata'
        }],
        # 显式指定 ffmpeg/ffprobe 所在目录
        'ffmpeg_location': BIN_DIR,
    }

    try:
        with youtube_dl.YoutubeDL(ydl_opts) as ydl:
            # download=False 表示不下载视频本身，只提取信息
            info_dict = ydl.extract_info(video_id, download=False)
            
            # 检查是否有可用的字幕
            if 'requested_subtitles' in info_dict and info_dict['requested_subtitles']:
                subtitle_path = None
                for lang, sub_info in info_dict['requested_subtitles'].items():
                    if sub_info.get('ext') == 'srt':
                        subtitle_path = sub_info.get('filepath')
                        break

                if subtitle_path and os.path.exists(subtitle_path):
                    with open(subtitle_path, 'r', encoding='utf-8') as f:
                        captions = f.read()
                    os.remove(subtitle_path) # 清理临时文件
                    return captions
                else:
                    return "No SRT subtitles found for the requested language."
            else:
                return "No subtitles found for the video or requested language."

    except youtube_dl.DownloadError as e:
        print(f"yt-dlp Download Error: {e}")
        return f"Failed to get captions: {e}"
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return f"An unexpected error occurred: {e}"

def get_captions_http(request):
    """
    Google Cloud Functions 的 HTTP 触发器入口点。
    这个函数接收一个 flask.Request 对象。
    """
    try:
        # 尝试从 JSON 请求体中获取 videoId
        request_json = request.get_json(silent=True)
        # 尝试从 URL 查询参数中获取 videoId
        request_args = request.args

        video_id = None
        if request_json and 'videoId' in request_json:
            video_id = request_json['videoId']
        elif request_args and 'videoId' in request_args:
            video_id = request_args['videoId']
        
        if not video_id:
            # 返回 400 Bad Request
            return json.dumps({'error': 'Missing videoId parameter'}), 400, {'Content-Type': 'application/json'}

        captions = get_srt_captions(video_id)

        # 检查是否是错误消息
        if captions.startswith("Failed to get captions") or \
           captions.startswith("An unexpected error occurred") or \
           captions.startswith("No "):
            status_code = 500
            # 返回 JSON 错误信息
            return json.dumps({'error': captions}), status_code, {'Content-Type': 'application/json'}
        else:
            status_code = 200
            # 返回纯文本 SRT
            return captions, status_code, {'Content-Type': 'text/plain; charset=utf-8'}

    except Exception as e:
        print(f"GCF handler error: {e}")
        # 返回 500 Internal Server Error
        return json.dumps({'error': f'Internal server error: {e}'}), 500, {'Content-Type': 'application/json'}
