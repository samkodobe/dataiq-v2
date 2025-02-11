What to Build 

- Determine next N records to fetch from remote source 
    - Input strategy 
        - SFTP 
            - records are files 
            - Fetch specs - all file names in remove server
            - extract information (date, fileName, fileId) 
            - save to DB (this will be invoked by a CRON job)
    - Join records in all_files with processed_file_log table, check outstanding and use strategy to determine next 
    - Specify number of records to retrieve as input 
    - Specify input and fetch strategy - not present in processed_file_log
        - latest - most recent by date limit by size
        - backlog - 
        - specify date to start from 

- Fetch specific file(s) from remote 
    - Takes an array of file names to retrieve from server 
    - Specify directory to save files to

- Unzip gz files 
    - Specify output file / directory 
    - Support single file or directory  

- Preprocess dump data 
    - Enable output strategy 
        - File - specify filename, directory 
        - Kafka topic - topicName, kafkaConnection
    - extract relevant columns from file
        - specify delimiter, columns
        - Enable transform strategy on each column e.g. plain, base64decode, maskNumbers (extensible to use an ML model)

- Load data 
    - Enable input strategy 
        - File - specify filename, directory 
        - Kafka topic - topicName, kafkaConnection
    - Load into click house 

- Clean up (invoked by a cron job)
    - Remove processed gz files
    - Remove processed uncompressed files 
    - Remove processed pre-process buffer files 
