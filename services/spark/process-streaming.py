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
        "subscribe": "Question,Users",  # Our topic name
        "failOnDataLoss": "false"
    }

    #Read data from kafka
    df = spark.readStream.format("kafka").options(**kafka_options).load()
    
    #Extract key, value, and topic
    df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)","topic")

    df.printSchema()

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
    
    user_schema = T.StructType([
        T.StructField("Id", T.IntegerType(), True),
        T.StructField("Reputation", T.IntegerType(), True),
        T.StructField("CreationDate", T.StringType(), True),
        T.StructField("DisplayName", T.StringType(), True),
        T.StructField("LastAccessDate", T.StringType(), True),
        T.StructField("AboutMe", T.StringType(), True),
        T.StructField("Views", T.IntegerType(), True),
        T.StructField("UpVotes", T.IntegerType(), True),
        T.StructField("DownVotes", T.IntegerType(), True),
        T.StructField("AccountId", T.IntegerType(), True),
        T.StructField("WebsiteUrl", T.StringType(), True),
        T.StructField("Location", T.StringType(), True)
    ])


    questions_df = df.filter(F.col("topic") == "Question") \
        .withColumn("parsed_value", F.from_json(F.col("value"), question_schema)) \
        .select(F.col("parsed_value.*"))
    
    users_df = df.filter(F.col("topic") == "Users") \
        .withColumn("parsed_value", F.from_json(F.col("value"), user_schema)) \
        .select(F.col("parsed_value.*"))
    
    # Filter questions and join with users based on OwnerUserId
    question_user_df = questions_df.filter(F.col("PostTypeId") == 1).alias("q") \
        .join(users_df.alias("u"), F.col("q.OwnerUserId") == F.col("u.Id"), "inner") \
        .select(
            F.col("q.Id").alias("QuestionId"),
            F.col("q.Tags"),
            F.col("u.Location").alias("UserLocation")
        )
    
    result_df = question_user_df.withColumn("value", F.to_json(F.struct(
        "QuestionId",
        "Tags",
        "UserLocation"
    )))


    """
    #Parse and deserialize post data
    parsed_df = deserialized_df.withColumn("parsed_value",F.from_json(F.col("value"),post_schema))\
    .select(F.col("parsed_value.*"))

    # Parse and deserialize user data
    users_df = deserialized_df.withColumn("parsed_user_value", F.from_json(F.col("value"), user_schema))\
        .select(F.col("parsed_user_value.*"))

    # Parse and deserialize question data
    questions_df = deserialized_df.withColumn("parsed_question_value", F.from_json(F.col("value"), question_schema))\
        .select(F.col("parsed_question_value.*"))

    #Example filtering
    #questions_df = parsed_df.filter(F.col("PostTypeId")==1).select("Id","Score","OwnerUserId")
    # Filter questions (PostTypeId == 1 for questions) and join with users
    question_user_df = questions_df.filter(F.col("PostTypeId") == 1).alias("q") \
    .join(users_df.alias("u"), F.col("q.OwnerUserId") == F.col("u.Id"), "inner") \
    .select(
        F.col("q.Id").alias("QuestionId"),
        F.col("q.Tags"),
        F.col("u.Location").alias("UserLocation")
    )

    
    #Serializing as JSON (only if we want to send to kafka as json)
    result_df = question_user_df.withColumn("value", F.to_json(F.struct(
    "QuestionId",
    "Tags",
    "UserLocation"
    )))
    """
    #Send back to kafka (to topic called "PROCESSED_DATA_USERS")
    #JSON
    query = result_df.selectExpr("CAST(QuestionId as STRING) as key","value")\
    .writeStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers","kafka:9092")\
    .option("topic","PROCESSED_DATA_USERS")\
    .option("checkpointLocation","/tmp/checkpoint1")\
    .start()

    query.awaitTermination()