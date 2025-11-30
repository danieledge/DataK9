#!/bin/bash
# Memory monitoring script for DataK9 profiler
# Monitors Python process memory usage in real-time
# Author: Daniel Edge

if [ -z "$1" ]; then
    echo "Usage: $0 <process_name_pattern>"
    echo "Example: $0 'validation_framework.cli profile'"
    exit 1
fi

PATTERN="$1"
INTERVAL=2  # Check every 2 seconds
LOG_FILE="memory_monitor_$(date +%Y%m%d_%H%M%S).log"

echo "=========================================="
echo "DataK9 Memory Monitor"
echo "=========================================="
echo "Pattern: $PATTERN"
echo "Interval: ${INTERVAL}s"
echo "Log: $LOG_FILE"
echo "=========================================="
echo ""

# Function to get memory in MB
get_memory() {
    local pid=$1
    # Get RSS (Resident Set Size) in KB, convert to MB
    ps -p $pid -o rss= | awk '{print $1/1024}'
}

# Function to format memory with color
format_memory() {
    local mem=$1
    if (( $(echo "$mem > 500" | bc -l) )); then
        echo -e "\033[0;31m${mem} MB\033[0m"  # Red if > 500MB
    elif (( $(echo "$mem > 300" | bc -l) )); then
        echo -e "\033[0;33m${mem} MB\033[0m"  # Yellow if > 300MB
    else
        echo -e "\033[0;32m${mem} MB\033[0m"  # Green otherwise
    fi
}

# Header
echo "Time            PID     Memory (RSS)    Status"
echo "------------------------------------------------"

# Log header
echo "Time,PID,Memory_MB" > "$LOG_FILE"

max_memory=0
pid=""

while true; do
    # Find the process
    new_pid=$(pgrep -f "$PATTERN" | head -1)

    if [ -z "$new_pid" ]; then
        if [ -n "$pid" ]; then
            # Process ended
            echo "$(date '+%H:%M:%S')  ----    ----            Process ended"
            echo ""
            echo "=========================================="
            echo "Summary:"
            echo "  Max Memory: $(format_memory $max_memory)"
            echo "  Log saved: $LOG_FILE"
            echo "=========================================="
            break
        else
            # Process not started yet
            echo "$(date '+%H:%M:%S')  ----    ----            Waiting for process..."
        fi
    else
        pid=$new_pid
        memory=$(get_memory $pid)

        # Update max memory
        if (( $(echo "$memory > $max_memory" | bc -l) )); then
            max_memory=$memory
        fi

        # Display
        timestamp=$(date '+%H:%M:%S')
        formatted_mem=$(format_memory $memory)
        echo "$timestamp  $pid     $formatted_mem      Running (max: ${max_memory%.??} MB)"

        # Log to file
        echo "$timestamp,$pid,$memory" >> "$LOG_FILE"
    fi

    sleep $INTERVAL
done
