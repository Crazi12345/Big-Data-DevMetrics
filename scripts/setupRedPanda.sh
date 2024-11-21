#Setup UI for kafka cluster
kubectl apply -f ../services/kafka/redpanda.yaml

kubectl port-forward svc/redpanda 8081 &