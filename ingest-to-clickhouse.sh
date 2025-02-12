#!/bin/bash

# Check if at least one date is provided
if [ "$#" -lt 1 ]; then
    echo "Usage: $0 <date1> [date2] [date3] ..."
    exit 1
fi

remote_host="san-storage"
remote_directory="/data1/backup/JMG"
local_data_dir="data"

# Function to check if a folder is empty
is_folder_empty() {
    local directory="$1"
    if [ -d "$directory" ] && [ -z "$(ls -A "$directory")" ]; then
        return 0  # Folder is empty (assumed processed)
    else
        return 1  # Folder contains files or does not exist
    fi
}

# Function to fetch missing files from the remote server
spool_from_remote() {
    local spool_date="$1"
    local directory="$2"

    # Check if folder is empty (assume processed)
    if is_folder_empty "$directory"; then
        echo "Skipping $spool_date - Folder exists but is empty (already processed)."
        return 0
    fi

    # Ensure local directory exists
    mkdir -p "$directory"

    # Get the list of files from the remote directory
    remote_file_list=$(ssh "$remote_host" "cd $remote_directory && ls *$spool_date*.txt.gz 2>/dev/null" | tr '\n' ' ')
    remote_file_count=$(echo "$remote_file_list" | wc -w)

    # Check if files exist on the remote server
    if [ -z "$remote_file_list" ]; then
        echo "No files found on remote server for date $spool_date."
        return 1
    fi

    echo "Remote files found for $spool_date: $remote_file_list ($remote_file_count files)"

    # Identify missing files
    missing_files=()
    for file in $remote_file_list; do
        if [ ! -f "$directory/$file" ]; then
            missing_files+=("$file")
        fi
    done

    # Fetch only missing files
    if [ ${#missing_files[@]} -gt 0 ]; then
        echo "Fetching missing files for $spool_date: ${missing_files[@]}"
        for file in "${missing_files[@]}"; do
            (
                echo "Fetching $file..."
                scp -q "$remote_host:$remote_directory/$file" "$directory/"
            ) &
        done
        wait  # Wait for all parallel SCP processes to finish
    fi

    # Verify file count after fetching
    local_file_count=$(ls -1 "$directory"/*.txt.gz 2>/dev/null | wc -l)

    if [ "$local_file_count" -ne "$remote_file_count" ]; then
        echo "Error: File count mismatch for $spool_date! Expected $remote_file_count, but got $local_file_count"
        return 1
    fi

    echo "All files for $spool_date successfully spooled!"
    return 0
}

# Function to process a directory
process_directory() {
    local directory="$1"
    echo "Processing directory: $directory"

    # Unzip all files before processing
    gunzip "$directory"/*.gz

    # Run Spark Script
    spark-submit --jars ./jars/clickhouse-jdbc-0.7.1-patch1-all.jar process-dump.py "$directory"

    # Remove files after successful processing
    echo "Deleting processed files in $directory"
    rm -rf "$directory"/*

    echo "Processing completed for $directory!"
}

# Process multiple dates
for date_to_spool in "$@"; do
    parent_directory="$local_data_dir/$date_to_spool"

    echo "Processing date: $date_to_spool"
    echo "Local directory: $parent_directory"

    # Spool files first, then process if successful
    spool_from_remote "$date_to_spool" "$parent_directory" && process_directory "$parent_directory" &
done

wait  # Ensure all background processes complete

echo "All ingestion processes completed."
