#!/bin/bash

#Install Spark
helm install --values ../services/spark/spark-values.yaml spark oci://registry-1.docker.io/bitnamicharts/spark --version 9.2.9
#Setup port-forward
kubectl port-forward svc/spark-master-svc 8080:80


