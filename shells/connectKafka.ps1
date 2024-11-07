# This script connects to the kafka-client pod and opens an interactive Bash shell

# Execute kubectl command with interactive terminal
kubectl exec -it kafka-client -- bash
