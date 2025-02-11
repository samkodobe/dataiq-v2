#!/bin/bash

# Check if the correct number of arguments is provided
if [ "$#" -ne 3 ]; then
    echo "Usage: $0 <start_date YYYYMMDD> <number_of_previous_dates k> <end_date YYYYMMDD>"
    exit 1
fi

start_date="$1"
count="$2"
end_date="$3"
max_parallel_jobs=2  # Adjust based on available server resources

# Function to convert YYYYMMDD to YYYY-MM-DD for `date` command compatibility
convert_to_dash_format() {
    echo "$1" | sed -E 's/^([0-9]{4})([0-9]{2})([0-9]{2})$/\1-\2-\3/'
}

# Function to get previous date in `YYYYMMDD` format
get_previous_date() {
    local input_date_dash=$(convert_to_dash_format "$1")
    date -d "$input_date_dash -1 day" +"%Y%m%d"
}

# Function to check active jobs and limit parallel execution
wait_for_available_resources() {
    local cpu_cores
    cpu_cores=$(nproc)
    local max_safe_load
    max_safe_load=$(echo "$cpu_cores * 0.8" | bc)

    while true; do
        active_jobs=$(jobs -rp | wc -l)
        system_load=$(awk '{print $1}' < /proc/loadavg)

        if [ "$active_jobs" -lt "$max_parallel_jobs" ] && (( $(echo "$system_load < $max_safe_load" | bc -l) )); then
            break  # Exit loop if system is ready
        fi

        echo "System under high load ($system_load), waiting for jobs to finish..."
        sleep 60  # Wait and recheck
    done
}

# Ensure valid date order
if [[ "$start_date" -lt "$end_date" ]]; then
    echo "Error: Start date must be later than or equal to the end date."
    exit 1
fi

# Process dates in batches
current_date="$start_date"

while [[ "$current_date" -ge "$end_date" ]]; do
    batch_dates=()
    for ((i = 0; i <= count; i++)); do
        batch_dates+=("$current_date")
        current_date=$(get_previous_date "$current_date")

        # Stop if we reached the end date
        if [[ "$current_date" -lt "$end_date" ]]; then
            break
        fi
    done

    # Wait if system is overloaded before launching a new batch
    wait_for_available_resources

    # Run ingestion in the background
    echo "Starting ingestion: ./ingest-to-clickhouse.sh ${batch_dates[*]}"
    ./ingest-to-clickhouse.sh "${batch_dates[@]}" &

    # Allow background jobs but limit resource consumption
    sleep 300
done

# Wait for all background jobs to finish
wait

echo "All ingestion processes completed."
