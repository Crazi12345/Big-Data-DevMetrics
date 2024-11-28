kubectl exec -it spark-master-0 -- bash -c 'export SPARK_HOME=/opt/bitnami/spark && $SPARK_HOME/bin/spark-submit \
    --master spark://spark-master-svc:7077 \
    --deploy-mode client \
    --jars /opt/bitnami/spark/custom-jars/spark-sql-kafka-0-10_2.12-3.5.2.jar \
    /tmp/spark-streaming.py'
