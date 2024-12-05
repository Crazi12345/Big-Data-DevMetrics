#!/bin/bash

#Install Spark
helm install --values ../services/spark/spark-values.yaml spark oci://registry-1.docker.io/bitnamicharts/spark --version 9.2.9
#Upgrade spark
#helm upgrade --values ../services/spark/spark-values.yaml spark oci://registry-1.docker.io/bitnamicharts/spark --version 9.2.9
#Manually delete if forced restart needed: 
#kubectl delete pods -l app.kubernetes.io/name=spark

#Setup port-forward
kubectl port-forward svc/spark-master-svc 8080:80

#Mount config map with kafka-spark connect jar
kubectl create configmap spark-jars --from-file=spark-sql-kafka-0-10_2.12-3.5.2.jar



