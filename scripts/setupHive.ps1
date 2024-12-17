#!/bin/bash

#Set up PostgreSQL for metastore
helm install hive-postgresql \
  --version=12.1.5 \
  --set auth.username=root \
  --set auth.password=pwd1234 \
  --set auth.database=hive \
  --set primary.extendedConfiguration="password_encryption=md5" \
  --repo https://charts.bitnami.com/bitnami \
  postgresql

#Apply yaml files
kubectl apply -f ../services/hive-metastore.yaml
kubectl apply -f ../services/hive/hive.yaml

#Portforward the UI
# kubectl port-forward svc/hiveserver2 10002:10002