#!/bin/bash

# MRE Enhancement Server Refresh Script
# This script should be run hourly via cron job
# Usage: ./refresh_servers.sh

# Configuration - UPDATE THIS FOR YOUR ENVIRONMENT
API_BASE_URL="${API_BASE_URL:-http://localhost:5001}"
LOG_FILE="/var/log/mre_refresh.log"
LOCK_FILE="/tmp/mre_refresh.lock"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Logging function
log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') - $1" | tee -a "$LOG_FILE"
}

# Error handling
set -e

# Check if refresh is already running
if [ -f "$LOCK_FILE" ]; then
    PID=$(cat "$LOCK_FILE")
    if ps -p "$PID" > /dev/null 2>&1; then
        log "${YELLOW}Refresh already running (PID: $PID). Exiting.${NC}"
        exit 1
    else
        log "${YELLOW}Stale lock file found. Removing.${NC}"
        rm -f "$LOCK_FILE"
    fi
fi

# Create lock file
echo $$ > "$LOCK_FILE"

# Function to cleanup on exit
cleanup() {
    log "Cleaning up..."
    rm -f "$LOCK_FILE"
}

# Set trap to cleanup on script exit
trap cleanup EXIT

log "${GREEN}Starting MRE server refresh process${NC}"

# Check if API is accessible
if ! curl -s --connect-timeout 10 "$API_BASE_URL/api/health" > /dev/null; then
    log "${RED}Error: API server is not accessible at $API_BASE_URL${NC}"
    exit 1
fi

# Check current refresh status
log "Checking current refresh status..."
STATUS_RESPONSE=$(curl -s "$API_BASE_URL/api/refresh_status")
IS_REFRESHING=$(echo "$STATUS_RESPONSE" | grep -o '"is_refreshing":[^,]*' | cut -d':' -f2 | tr -d ' ')

if [ "$IS_REFRESHING" = "true" ]; then
    log "${YELLOW}Refresh already in progress. Exiting.${NC}"
    exit 0
fi

# Perform bulk refresh
log "Initiating bulk refresh of all server tables..."
REFRESH_RESPONSE=$(curl -s -X POST "$API_BASE_URL/api/bulk_refresh_all_servers")

# Check if refresh was successful
if echo "$REFRESH_RESPONSE" | grep -q '"all_successful":true'; then
    log "${GREEN}Bulk refresh completed successfully${NC}"
    
    # Log detailed results
    echo "$REFRESH_RESPONSE" | jq -r '.results | to_entries[] | "  \(.key): \(.value)"' | while read line; do
        log "  $line"
    done
    
    # Get updated status
    log "Final refresh status:"
    FINAL_STATUS=$(curl -s "$API_BASE_URL/api/refresh_status")
    LAST_REFRESH_TIME=$(echo "$FINAL_STATUS" | grep -o '"last_refresh_time":"[^"]*"' | cut -d'"' -f4)
    log "  Last refresh time: $LAST_REFRESH_TIME"
    
else
    log "${RED}Bulk refresh completed with errors${NC}"
    echo "$REFRESH_RESPONSE" | jq -r '.results | to_entries[] | "  \(.key): \(.value)"' | while read line; do
        log "  $line"
    done
    exit 1
fi

log "${GREEN}MRE server refresh process completed${NC}"

# Optional: Send notification (uncomment and configure as needed)
# if command -v mail > /dev/null 2>&1; then
#     echo "MRE server refresh completed at $(date)" | mail -s "MRE Refresh Complete" admin@example.com
# fi 