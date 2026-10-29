# ====================================================================
# Dockerfile for notion-2api
# ====================================================================

FROM python:3.11-slim

# 设置环境变量
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
WORKDIR /app

# 安装 Python 依赖
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# 复制应用代码
COPY . .

# 创建并切换到非 root 用户
RUN useradd --create-home appuser && \
    chown -R appuser:appuser /app
USER appuser

# 暴露端口并启动
# 注意：本应用把账号池/注册状态保存在进程内存 + JSON/SQLite 中，
# 多 worker 会导致各进程状态不一致（加号后别的 worker 看不到），因此只能单 worker。
EXPOSE 8000
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
