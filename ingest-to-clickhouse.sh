#!/bin/bash

# Check if the correct number of arguments is provided
if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <date in format yyyymmdd>"
    exit 1
fi

remote_host="san-storage"
remote_directory="/data1/backup/JMG"

spool_from_remote() {
    local spool_date="$1"
    local directory="$2"

    # Check if the directory exists
    if [ ! -d "$directory" ]; then
        echo "Error: Parent directory not found: $directory"
        exit 1
    fi

    remote_file_list=$(ssh "$remote_host" "cd $remote_directory && ls *$spool_date*.txt.gz 2>/dev/null" | tr '\n' ' ')
    # Check if we got any files
    if [ -z "$remote_file_list" ]; then
        echo "No files found on remote server for date $spool_date."
        exit 0
    fi

    echo "Remote files found: $remote_file_list"
    missing_files=()
    for file in $remote_file_list; do
        if [ ! -f "$directory/$file" ]; then
            missing_files+=("$file")
        fi
    done

    if [ ${#missing_files[@]} -eq 0 ]; then
        echo "All files already exist locally. No need to fetch."
        exit 0
    fi

    echo "Fetching missing files: ${missing_files[@]}"

    # Convert array to space-separated string for SCP
    scp_files=$(printf "%s " "${missing_files[@]}")

    # Fetch only missing files
    scp -v "$remote_host:$remote_directory/{$scp_files}" "$directory"

    echo "Files successfully fetched!"

    # check files for date from Remote directory
    # ssh "san-storage" "cd $remote_directory && for file in *$spool_date*.txt.gz; do basename \$file; done 2>/dev/null" > "$remote_file_list"

    # spool files that matches date from remote to local directory
    # scp -v "san-storage:/data1/backup/JMG/jartlr$spool_date*" "$directory"
}

# Function to gzip files in a directory
gzip_files() {
    local directory="$1"
    echo "Gzipping files in directory: $directory"
    gzip "$directory"/*
}

# Function to process a directory
process_directory() {
    local directory="$1"
    echo "Processing directory: $directory"

    # Unzip files
    for file in "$directory"/*.gz; do
        if [ -e "$file" ]; then
            gunzip "$file"
            echo "Unzipped $file"
        fi
    done

    # Run Spark Script
    spark-submit --jars ./jars/clickhouse-jdbc-0.7.1-patch1-all.jar process-dump.py "$directory"

    # Remove files at the end of the processing
    gzip_files "$directory"
    # rm -rf "$directory"
}

# Specify the parent directory containing subdirectories
date_to_spool="$1"
parent_directory="data/$date_to_spool"

echo "parent_directory: $parent_directory"

mkdir -p $parent_directory

if [[ "$1" != "20210101" ]]; then
    spool_from_remote "$date_to_spool" "$parent_directory"
fi

# Process the parent directory
if [ -d "$parent_directory" ]; then
    process_directory "$parent_directory"
fi

echo "Script completed."
