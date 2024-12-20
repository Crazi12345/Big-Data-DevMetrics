from pyspark.sql import functions as F
from pyspark.sql import types as T
from src.utils import SPARK_ENV, get_spark_context

if __name__ == "__main__":
    additional_conf = {
        "spark.jars.packages": "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2"
    }
    spark = get_spark_context(
        app_name="Kafka Streamning", 
        config=SPARK_ENV.K8S, 
        additional_conf=additional_conf
    )
    sc = spark.sparkContext
    sc.setLocalProperty("spark.scheduler.pool","pool1")

    kafka_options = {
        "kafka.bootstrap.servers": "kafka:9092",
        "startingOffsets": "earliest",  
        "subscribe": "JOINED_STREAM",  
    }

    df = spark.readStream.format("kafka").options(**kafka_options).load()
    deserialized_df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    
    data_schema = T.StructType([
        T.StructField("USERID", T.IntegerType(), True),
        T.StructField("QUESTIONID", T.IntegerType(), True),
        T.StructField("POSTTYPEID", T.IntegerType(), True),
        T.StructField("QUESTIONCREATIONDATE", T.StringType(), True),
        T.StructField("QUESTIONVIEWCOUNT", T.IntegerType(), True),
        T.StructField("QUESTIONTAGS", T.StringType(), True),
        T.StructField("USERREPUTATION", T.IntegerType(), True),
        T.StructField("USERLASTACCESSDATE", T.StringType(), True),
        T.StructField("USERLOCATION", T.StringType(), True)
    ])

    #Parse JSON messages
    parsed_df = deserialized_df.withColumn("parsed_value",F.from_json(F.col("value"),data_schema))\
    .select(F.col("parsed_value.*"))

    #Get location count
    location_count_df = parsed_df.groupBy("USERLOCATION").agg(F.count("USERLOCATION").alias("LocationCount"))

    #Split tags on | and remove empty
    tags_exploded_df = parsed_df.withColumn("Tag",F.explode(F.split(F.col("QUESTIONTAGS"),"\\|")))\
    .filter(F.col("Tag") != "") 

    #Count tag occurrence by location
    tag_count_location_df = tags_exploded_df.groupBy("USERLOCATION","Tag").agg(F.count("Tag").alias("TagCountByLocation"))

    #Serializing as JSON (only if we want to send to kafka as json)
    
    location_count_df = location_count_df.withColumn("value",F.to_json(F.struct(
        "USERLOCATION",
        "LocationCount"
    )))

    tag_count_location_df = tag_count_location_df.withColumn("value",F.to_json(F.struct(
        "USERLOCATION",
        "Tag",
        "TagCountByLocation"
    )))

    #Send back to kafka 
    #JSON
    locationQuery = location_count_df.selectExpr("CAST(USERLOCATION as STRING) as key","value")\
    .writeStream\
    .outputMode("update")\
    .format("kafka")\
    .option("kafka.bootstrap.servers","kafka:9092")\
    .option("topic","UserLocation_Counts")\
    .option("checkpointLocation","/tmp/location_checkpoint")\
    .start()

    tagLocationQuery = tag_count_location_df.selectExpr("CAST(Tag as STRING) as key","value")\
    .writeStream\
    .outputMode("update")\
    .format("kafka")\
    .option("kafka.bootstrap.servers","kafka:9092")\
    .option("topic","TagLocation_Counts")\
    .option("checkpointLocation","/tmp/tagLocation_checkpoint")\
    .start()

    spark.streams.awaitAnyTermination()