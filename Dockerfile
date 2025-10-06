# 用稳定的 Python 3.12
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# 先装依赖，提升缓存命中
COPY requirements.txt .
RUN pip install -r requirements.txt

# 拷贝源代码（确保 main.py 在仓库根或把路径调一下）
COPY . .

# Cloud Run 监听 8080
ENV PORT=8080

# 用 functions-framework 暴露你的函数 fetch_subtitles
CMD ["functions-framework", "--target=fetch_subtitles", "--host=0.0.0.0", "--port=8080"]
