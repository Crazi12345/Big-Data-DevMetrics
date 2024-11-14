# Import spark session
from pyspark.sql import SparkSession # type: ignore #Entry point for spark application

import subprocess
import os

# Base HDFS URI
HDFS_URI = "hdfs://namenode:9000/"

# Define paths for input and output on HDFS
input_file_path = f"{HDFS_URI}/file.csv"
output_path = f"{HDFS_URI}/output_folder"

# Get the IP address of the host machine
SPARK_DRIVER_HOST = (
    subprocess.check_output(["hostname", "-i"]).decode(encoding="utf-8").strip()
)
# Remove any occurrence of localhost address from SPARK_DRIVER_HOST
SPARK_DRIVER_HOST = SPARK_DRIVER_HOST.replace("127.0.0.1", "").strip()

os.environ["SPARK_LOCAL_IP"] = SPARK_DRIVER_HOST

#Create spark session with HDFS config
spark = SparkSession.builder.appName("name") \
 .config("spark.master", "spark://spark-master-svc:7077")\
 .config("spark.driver.bindAddress", "0.0.0.0")\
 .config("spark.driver.host", SPARK_DRIVER_HOST)\
 .config("spark.driver.port", "7077")\
 .config("spark.submit.deployMode", "client")\
 .getOrCreate() #create app to start import of data (arg may not be necessary)

# Load CSV data into Spark DataFrame
dataFrame = spark.read.format("csv").option("header", True).option("separator", ",").option("inferSchema",True).load(input_file_path)
#Alt: dataFrame = spark.read.csv(csv_file_path, header=True, inferSchema=True, separator=",")

#Show first 10 rows and do not trim columns
dataFrame.show(10, False)

# Register DataFrame as a SQL temporary view
dataFrame.createOrReplaceTempView("table_name")

#Use spark.sql(SQL COMMAND) to run directly on dataframe. Example: 
result_df = spark.sql("SELECT location, username FROM users WHERE column3 > 100")

## SAVE TO HDFS ##

# Save the result DataFrame to HDFS in Parquet format
result_df.write.mode("overwrite").parquet(output_path)

# Stop the Spark session
spark.stop()