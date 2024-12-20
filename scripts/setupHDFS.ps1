#!/bin/bash


#deploy the stolen code
kubectl apply -f ../services/hdfs/configmap.yaml
kubectl apply -f ../services/hdfs/namenode.yaml
kubectl apply -f ../services/hdfs/hdfs-cli.yaml
kubectl apply -f ../services/hdfs/datanodes.yaml

#Port forward
kubectl port-forward service/namenode 9870:9870 &
