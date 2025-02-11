#!/bin/bash

# Check if the correct number of arguments is provided
if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <date in format yyyymmdd>"
    exit 1
fi

spool_from_remote() {
    local spool_date="$1"
    local directory="$2"

    # Check if the directory exists
    if [ ! -d "$directory" ]; then
        echo "Error: Parent directory not found: $directory"
        exit 1
    fi

    # spool files that matches date from remote to local directory
    scp -v "san-storage:/data1/backup/JMG/jartlr{$spool_date}*" "$directory"
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

    # # Unzip files
    # for file in "$directory"/*.gz; do
    #     if [ -e "$file" ]; then
    #         gunzip "$file"
    #         echo "Unzipped $file"
    #     fi
    # done

    # Run Spark Script
    spark-submit --jars ./jars/clickhouse-jdbc-0.7.1-patch1-all.jar process-dump.py "$directory"

    # Gzip files at the end of the processing
    # gzip_files "$directory"
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
