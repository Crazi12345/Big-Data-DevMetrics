from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("KafkaSparkStreaming") \
    .getOrCreate()

# Read from Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka-broker:9092") \
    .option("subscribe", "input-topic") \
    .load()

# Transform Data
value_df = kafka_df.selectExpr("CAST(value AS STRING)")
transformed_df = value_df.withColumn("processed_value", col("value"))

# Write Back to Kafka
query = transformed_df.selectExpr("CAST(processed_value AS STRING) as value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka-broker:9092") \
    .option("topic", "output-topic") \
    .option("checkpointLocation", "/tmp/spark-checkpoints") \
    .start()

query.awaitTermination()
