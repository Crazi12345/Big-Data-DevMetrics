#!/bin/bash

spark-submit --master spark://spark-master-svc:7077 ../services/spark/process-streaming.py
