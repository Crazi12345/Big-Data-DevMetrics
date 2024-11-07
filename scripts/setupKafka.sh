#!/bin/bash

#Install Kafka
helm install --values ../services/kafka/kafka-values.yaml kafka oci://registry-1.docker.io/bitnamicharts/kafka --version 30.0.4

