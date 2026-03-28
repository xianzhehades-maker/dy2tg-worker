# Dockerfile for Hugging Face Space Deployment
# 使用 Python 3.12 基础镜像

FROM python:3.12-slim

# 设置工作目录
WORKDIR /app

# 强制打破缓存 - 每次构建都会变化
ARG CACHE_BUST=1
ENV REBUILD_VERSION=20260321

# 安装系统依赖 (包括 FFmpeg 和编译工具)
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    make \
    git \
    curl \
    wget \
    ffmpeg \
    supervisor \
    python3-dev \
    libssl-dev \
    libxml2-dev \
    libxslt-dev \
    zlib1g-dev \
    libffi-dev \
    && rm -rf /var/lib/apt/lists/*

# 复制 requirements.txt
COPY requirements.txt .

# 安装 Python 依赖
RUN pip install --no-cache-dir -r requirements.txt

# 复制项目文件
COPY . .

# 验证文件是否正确拷贝
RUN echo "=== Verifying copied files ===" && \
    if [ -f core/group_bot.py ]; then \
        echo "group_bot.py exists with $(wc -l < core/group_bot.py) lines"; \
    else \
        echo "ERROR: group_bot.py NOT FOUND!"; \
        exit 1; \
    fi

# 创建必要的目录
RUN mkdir -p /app/data /app/downloads/raw /app/downloads/processed /app/downloads/upload /app/logs

# 暴露端口
EXPOSE 7860

# 设置环境变量
ENV PYTHONUNBUFFERED=1
ENV DEBIAN_FRONTEND=noninteractive

# 初始化数据库
RUN python init_db.py

# 创建 Supervisor 配置文件
RUN echo '[supervisord]\n\
nodaemon=true\n\
logfile=/var/log/supervisord.log\n\
pidfile=/var/run/supervisord.pid\n\
\n\
[program:full_telethon_bot]\n\
command=python core/full_telethon_bot.py\n\
directory=/app\n\
autostart=true\n\
autorestart=true\n\
stdout_logfile=/dev/stdout\n\
stdout_logfile_maxbytes=0\n\
stderr_logfile=/dev/stderr\n\
stderr_logfile_maxbytes=0\n\
\n\
[program:group_workflow]\n\
command=python core/group_workflow.py\n\
directory=/app\n\
autostart=true\n\
autorestart=true\n\
stdout_logfile=/dev/stdout\n\
stdout_logfile_maxbytes=0\n\
stderr_logfile=/dev/stderr\n\
stderr_logfile_maxbytes=0\n\
\n\
[program:persistence]\n\
command=python persistence_sync.py\n\
directory=/app\n\
autostart=true\n\
autorestart=true\n\
stdout_logfile=/dev/stdout\n\
stdout_logfile_maxbytes=0\n\
stderr_logfile=/dev/stderr\n\
stderr_logfile_maxbytes=0\n\
' > /etc/supervisor/conf.d/supervisord.conf

# 启动命令
CMD ["/usr/bin/supervisord", "-c", "/etc/supervisor/conf.d/supervisord.conf"]
