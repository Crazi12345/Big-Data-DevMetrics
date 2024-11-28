#!/bin/bash

#Install Kafka
helm install --values ../services/kafka/kafka-values.yaml kafka oci://registry-1.docker.io/bitnamicharts/kafka --version 30.0.4
kubectl run kafka-client --restart='Never' --image docker.io/bitnami/kafka:3.8.0-debian-12-r3  --command -- sleep infinity

# registry
kubectl apply -f ../services/kafka/kafka-schema-registry.yaml
# BIT CONNECT
kubectl apply -f ../services/kafka/kafka-connect.yaml
# db
kubectl apply -f ../services/kafka/kafka-ksqldb.yaml




# RED PANDA
kubectl apply -f ../services/kafka/redpanda.yaml
kubectl port-forward svc/redpanda 8080 &
