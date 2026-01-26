#!/bin/bash
set -e

# Configuration
NAMESPACE="kafka"
KAFKA_CONNECT_URL="http://localhost:8083"

echo "Creating namespace..."
kubectl create namespace ${NAMESPACE} || echo "Namespace already exists"

echo "Deploying Kafka Connect..."
kubectl apply -f k8s/kafka-connect-deployment.yaml

echo "Waiting for Kafka Connect to be ready..."
kubectl wait --for=condition=available --timeout=300s \
  deployment/kafka-connect -n ${NAMESPACE}

echo "Port forwarding to Kafka Connect..."
kubectl port-forward -n ${NAMESPACE} svc/kafka-connect 8083:8083 &
PORT_FORWARD_PID=$!
sleep 5

echo "Deploying PostgreSQL source connector..."
curl -X POST ${KAFKA_CONNECT_URL}/connectors \
  -H "Content-Type: application/json" \
  -d @connectors/postgres-source-aws.json

echo "Deploying MongoDB sink connector..."
curl -X POST ${KAFKA_CONNECT_URL}/connectors \
  -H "Content-Type: application/json" \
  -d @connectors/mongodb-sink-aws.json

echo "Checking connector status..."
curl ${KAFKA_CONNECT_URL}/connectors

echo "Stopping port forward..."
kill $PORT_FORWARD_PID

echo "✅ Deployment complete!"
