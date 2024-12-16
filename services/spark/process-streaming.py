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

    kafka_options = {
        "kafka.bootstrap.servers": "kafka:9092",
        "startingOffsets": "earliest",  # Start from the beginning when we consume from kafka
        "subscribe": "INGESTION",  # Our topic name
    }

    df = spark.readStream.format("kafka").options(**kafka_options).load()
    deserialized_df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

    deserialized_df.printSchema()

    avro_schema = """
    {
        "type": "record",
        "name": "Question",
        "fields": [
        { "name": "Id", "type": "int" },
        { "name": "PostTypeId", "type": "int" },
        { "name": "AcceptedAnswerId", "type": "int" },
        { "name": "CreationDate", "type": "string" },
        { "name": "Score", "type": "int" },
        { "name": "ViewCount", "type": "int" },
        { "name": "OwnerUserId", "type": "int" },
        { "name": "LastEditorUserId", "type": "int" },
        { "name": "LastEditDate", "type": "string" },
        { "name": "LastActivityDate", "type": "string" },
        { "name": "Title", "type": "string" },
        { "name": "Tags", "type": "string" },
        { "name": "AnswerCount", "type": "int" },
        { "name": "CommentCount", "type": "int" },
        { "name": "FavoriteCount", "type": "int" },
        { "name": "ContentLicense", "type": "string" }
        ]
    }
    """

    post_schema = T.StructType([
        T.StructField("Id", T.IntegerType(), True),
        T.StructField("PostTypeId", T.IntegerType(), True),
        T.StructField("ParentId", T.IntegerType(), True),
        T.StructField("CreationDate", T.StringType(), True),
        T.StructField("Score", T.IntegerType(), True),
        T.StructField("OwnerUserId", T.IntegerType(), True),
        T.StructField("LastEditorUserId", T.IntegerType(), True),
        T.StructField("LastEditorDisplayName", T.StringType(), True),
        T.StructField("LastEditDate", T.StringType(), True),
        T.StructField("LastActivityDate", T.StringType(), True),
        T.StructField("CommentCount", T.IntegerType(), True),
        T.StructField("CommunityOwnedDate", T.StringType(), True),
        T.StructField("ContentLicense", T.StringType(), True)
    ])

    question_schema = T.StructType([
        T.StructField("Id", T.IntegerType(), True),
        T.StructField("PostTypeId", T.IntegerType(), True),
        T.StructField("AcceptedAnswerId", T.IntegerType(), True),
        T.StructField("CreationDate", T.StringType(), True),
        T.StructField("Score", T.IntegerType(), True),
        T.StructField("ViewCount", T.IntegerType(), True),
        T.StructField("OwnerUserId", T.IntegerType(), True),
        T.StructField("LastEditorUserId", T.IntegerType(), True),
        T.StructField("LastEditDate", T.StringType(), True),
        T.StructField("LastActivityDate", T.StringType(), True),
        T.StructField("Title", T.StringType(), True),
        T.StructField("Tags", T.StringType(), True),
        T.StructField("AnswerCount", T.IntegerType(), True),
        T.StructField("CommentCount", T.IntegerType(), True),
        T.StructField("FavoriteCount", T.IntegerType(), True),
        T.StructField("ContentLicense", T.StringType(), True)
    ])
    
    #Parse JSON messages
    parsed_df = deserialized_df.withColumn("parsed_value",F.from_json(F.col("value"),post_schema))\
    .select(F.col("parsed_value.*"))

    #Example filtering
    questions_df = parsed_df.filter(F.col("PostTypeId")==1).select("Id","Score","OwnerUserId")

    posts_df = parsed_df.filter(F.col("PostTypeId") != 1)

    
    #Serializing as avro (only if we want to send to kafka as avro)
    """questions_df = questions_df.withColumn("value",to_avro(F.struct(
        "Id",
        "PostTypeId",
        "AcceptedAnswerId",
        "CreationDate",
        "Score",
        "ViewCount",
        "OwnerUserId",
        "LastEditorUserId",
        "LastEditDate",
        "LastActivityDate",
        "Title",
        "Tags",
        "AnswerCount",
        "CommentCount",
        "FavoriteCount",
        "ContentLicense"
    ),avro_schema))
    """
    #Serializing as JSON (only if we want to send to kafka as json)
    questions_df = questions_df.withColumn("value",F.to_json(F.struct(
        "Id",
        "Score",
        "OwnerUserId"
    )))

    #Send back to kafka (to topic called "PROCESSED_QUESTIONS")
    #JSON
    
    query = questions_df.selectExpr("CAST(Id as STRING) as key","value")\
    .writeStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers","kafka:9092")\
    .option("topic","PROCESSED_DATA")\
    .option("checkpointLocation","/tmp")\
    .start()
    

    #As AVRO
    """
    questions_df.selectExpr("CAST(Id AS STRING) as key", "value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("topic", "PROCESSED_DATA") \
    .start()
    """

    query.awaitTermination()