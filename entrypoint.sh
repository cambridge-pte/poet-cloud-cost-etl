#!/bin/bash
set -e

# Export environment variables for cron
printenv | grep -v "no_proxy" >> /etc/environment

# Create cron job from SYNC_SCHEDULE
echo "${SYNC_SCHEDULE:-0 2 * * *} root cd /app && /usr/local/bin/python src/main.py >> /var/log/etl/sync.log 2>&1" > /etc/cron.d/etl-sync
chmod 0644 /etc/cron.d/etl-sync
crontab /etc/cron.d/etl-sync

echo "$(date) - POET Cloud Cost ETL starting..."
echo "Sync schedule: ${SYNC_SCHEDULE:-0 2 * * *}"

# Run initial sync if RUN_ON_START is set
if [ "${RUN_ON_START:-true}" = "true" ]; then
    echo "$(date) - Running initial sync..."
    python src/main.py
fi

# Start cron in foreground
echo "$(date) - Starting cron scheduler..."
cron -f
