# Build with Docker instead of Railpack/Nixpacks. Railpack mounts every service
# variable as a build-time "secret"; a single malformed/empty-named variable makes it
# fail with `secret ID missing for "" environment variable`. Docker injects env vars at
# RUNTIME only, so the build no longer depends on the variable set at all.
FROM python:3.13-slim

# Faster, quieter Python in containers
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# Install deps first for layer caching
COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

# App code
COPY . .

# The pipeline is a batch job (runs once and exits); Railway's cron triggers it.
CMD ["python", "main.py"]
