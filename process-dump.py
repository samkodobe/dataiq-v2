from pyspark.sql import SparkSession
from pyspark.sql.functions import split, input_file_name, regexp_replace, length, when, unhex
from dotenv import load_dotenv
import os
import sys

load_dotenv()

# Check if the correct number of command line arguments is provided
if len(sys.argv) != 2:
    print("Usage: python script.py <log_directory>")
    sys.exit(1)

# Get the log directory from the command line argument
log_directory = sys.argv[1]

# Initialize Spark Session
# spark = SparkSession.builder \
#     .appName("DataIQSpooler") \
#     .master("spark://localhost:7077") \
#     .config("spark.driver.extraClassPath", "/opt/spark/jars/clickhouse-jdbc-0.3.2.jar") \
#     .config("spark.executor.extraClassPath", "/opt/spark/jars/clickhouse-jdbc-0.3.2.jar") \
#     .getOrCreate()

spark = SparkSession.builder \
    .appName("DataIQSpooler") \
    .config("spark.driver.extraClassPath", "/opt/spark/jars/clickhouse-jdbc-0.7.1-patch1-all.jar") \
    .getOrCreate()

# Read all files in the directory as a text file
log_data = spark.read.text(log_directory)

# Split each line by the '|' delimiter and select the first three columns
log_data = log_data.withColumn("split_data", split(log_data.value, "\\|"))
log_data = log_data.withColumn("batchName", input_file_name())
log_data = log_data.filter(log_data.split_data[47] == "SUBMIT_SM")
# log_data.show

filtered_log_data = log_data.selectExpr("split_data[1] as bankcode", "split_data[2] as phonenumber", "split_data[3] as message", "split_data[4] as receivedat", "split_data[11] as network", "split_data[12] as msgid", "batchName")
                   
filtered_log_data = filtered_log_data.withColumn("receivedat", (filtered_log_data["receivedat"].cast("timestamp")))

# Define a function to mask numbers in a column
def mask_numbers(column):
    return regexp_replace(column, r'\d', 'X')

# filtered_log_data = filtered_log_data.withColumn("safemessage", mask_numbers(filtered_log_data["message"]))

# # Define a condition to check if the column contains hexadecimal-encoded strings
is_hex_condition = (
    (length(filtered_log_data["message"]) % 2 == 0) & 
    (filtered_log_data["message"].rlike("^[0-9A-Fa-f]+$"))
)

# # Apply the decoding using when and unhex
filtered_log_data = filtered_log_data.withColumn("message", when(is_hex_condition, unhex(filtered_log_data["message"]).cast("string")).otherwise(filtered_log_data["message"]))

# 
filtered_log_data.show()

# Configure ClickHouse connection properties
clickhouse_url = os.getenv('CLICKHOUSE_URL') + "?database=" + os.getenv('CLICKHOUSE_DATABASE')
properties = {
    "user": os.getenv('CLICKHOUSE_USER'),
    "password": os.getenv('CLICKHOUSE_PASSWORD'),
    "driver": "com.clickhouse.jdbc.ClickHouseDriver"  # ✅ Updated driver class
}

# # Write the DataFrame to Clickhouse
filtered_log_data.write.jdbc(url=clickhouse_url, table="smstransactions_v3", mode="append", properties=properties)

# Stop the Spark session
spark.stop()