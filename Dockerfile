FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    cron \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY src/ ./src/
COPY sql/ ./sql/

# Copy entrypoint script
COPY entrypoint.sh .
RUN chmod +x entrypoint.sh

# Create log directory
RUN mkdir -p /var/log/etl

# Default environment variables
ENV LOG_LEVEL=INFO
ENV SYNC_SCHEDULE="0 2 * * *"
ENV PYTHONUNBUFFERED=1

ENTRYPOINT ["./entrypoint.sh"]
