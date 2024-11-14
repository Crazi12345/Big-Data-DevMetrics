spark-submit sparkSessionSetup.py\
    --master k8s://https://10.123.3.156:16443 \
    --name process_github_data \
    --conf spark.executor.instances=3 \ #Optional