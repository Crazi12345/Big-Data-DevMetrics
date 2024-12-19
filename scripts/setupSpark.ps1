#!/bin/bash

#Install Spark
helm install --values ../services/spark/spark-values.yaml spark oci://registry-1.docker.io/bitnamicharts/spark --version 9.2.9
#Upgrade spark
#helm upgrade --values ../services/spark/spark-values.yaml spark oci://registry-1.docker.io/bitnamicharts/spark --version 9.2.9
#Manually delete if forced restart needed: 
#kubectl delete pods -l app.kubernetes.io/name=spark

#kubectl port-forward svc/spark-master-svc 9090:80 &




