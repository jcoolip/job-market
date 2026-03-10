#!/bin/bash

# Config
LOG_DIR="/home/justin/dev/job_market/ingest/logs"
LOG_FILE="$LOG_DIR/job_market_$(date +'%Y%m%d_%H%M%S').log"
EMAIL="job_market@jcullop.com"

# Switch: 0 = error only, 1 = email every run
EMAIL_ALL=1

# Activate virtualenv
source /home/justin/dev/job_market/.venv/bin/activate

cd /home/justin/dev/job_market/ingest

# Run pipeline
echo "Starting pipeline at $(date)" | tee -a "$LOG_FILE"
ROWS_ADDED=$(/home/justin/dev/job_market/.venv/bin/python3 -m pipeline 2>&1 | tee -a "$LOG_FILE")

EXIT_CODE=${PIPESTATUS[0]}  # capture pipeline exit code

# Compose message
SUBJECT=""
BODY=""

if [ $EXIT_CODE -ne 0 ]; then
    SUBJECT="Job Market Pipeline FAILED"
    BODY="Pipeline failed at $(date)\n\nLast 50 lines of log:\n$(tail -n 50 $LOG_FILE)"
    echo -e "$BODY" | mail -s "$SUBJECT" "$EMAIL"
elif [ $EMAIL_ALL -eq 1 ]; then
    SUBJECT="Job Market Pipeline SUCCESS"
    BODY="Pipeline completed successfully at $(date)\nRows added/updated:\n$ROWS_ADDED"
    echo -e "$BODY" | mail -s "$SUBJECT" "$EMAIL"
fi
