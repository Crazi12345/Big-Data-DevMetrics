#!/bin/bash

kubectl apply -f ../services/client/client.yaml

kubectl port-forward svc/client 3000 &