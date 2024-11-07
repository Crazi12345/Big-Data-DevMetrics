#!/bin/bash

#deploy the stolen code
kubectl apply -f services/hdfs/namenode.yaml
kubectl apply -f services/hdfs/hdfs-cli.yaml
kubectl apply -f services/hdfs/datanodes.yaml
kubectl apply -f services/hdfs/configmap.yaml

