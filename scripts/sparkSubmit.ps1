#!/bin/bash

spark-submit --master spark://spark-master-svc:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2 --conf spark.jars.ivy=/tmp/.ivy2 ../services/spark/process-streaming.py
