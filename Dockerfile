FROM python:3.12-slim-bookworm
WORKDIR /stream_writer

COPY requirements.txt .
RUN apt-get update && apt-get install -y --no-install-recommends \
    && pip install --no-cache-dir -r requirements.txt \
    && rm -rf /var/lib/apt/lists/*

COPY src ./src
# CMD ["python", "-u", "-m", "src.server"]
CMD ["python", "-m", "src.server"]
