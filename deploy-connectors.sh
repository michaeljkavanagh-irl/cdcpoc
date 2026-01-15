#!/bin/bash

# deploy-connectors.sh
# Helper script to deploy Kafka Connect connectors

set -e

echo "🚀 Deploying Kafka Connect Connectors..."
echo ""

# Check if Kafka Connect is ready
echo "⏳ Checking if Kafka Connect is ready..."
until curl -s http://localhost:8083/ > /dev/null; do
    echo "   Waiting for Kafka Connect to be ready..."
    sleep 5
done
echo "✅ Kafka Connect is ready!"
echo ""

# Deploy PostgreSQL Source Connector
echo "📊 Deploying PostgreSQL Source Connector..."
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @connectors/postgres-source.json \
  -w "\n" \
  -s | jq '.'

if [ $? -eq 0 ]; then
    echo "✅ PostgreSQL Source Connector deployed successfully!"
else
    echo "❌ Failed to deploy PostgreSQL Source Connector"
    exit 1
fi
echo ""

# Deploy MongoDB Sink Connector
echo "🍃 Deploying MongoDB Atlas Sink Connector..."
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @connectors/mongodb-sink.json \
  -w "\n" \
  -s | jq '.'

if [ $? -eq 0 ]; then
    echo "✅ MongoDB Sink Connector deployed successfully!"
else
    echo "❌ Failed to deploy MongoDB Sink Connector"
    echo "   Make sure you've configured your Atlas connection URI in connectors/mongodb-sink.json"
    exit 1
fi
echo ""

# Check connector status
echo "📋 Connector Status:"
echo ""
echo "PostgreSQL Source Connector:"
curl -s http://localhost:8083/connectors/postgres-source-connector/status | jq '.'
echo ""
echo "MongoDB Sink Connector:"
curl -s http://localhost:8083/connectors/mongodb-sink-connector/status | jq '.'
echo ""

echo "✨ All connectors deployed successfully!"
echo ""
echo "You can check connector status anytime with:"
echo "  curl http://localhost:8083/connectors/postgres-source-connector/status | jq '.'"
echo "  curl http://localhost:8083/connectors/mongodb-sink-connector/status | jq '.'"
