#!/bin/bash
set -e

# Configuration
AWS_REGION="us-east-1"
NAMESPACE="kafka"
CLUSTER_NAME="your-eks-cluster"

echo "Setting up AWS Secrets..."
echo "Creating MongoDB credentials secret..."
aws secretsmanager create-secret \
  --name prod/mongodb/credentials \
  --description "MongoDB Atlas credentials for Kafka Connect" \
  --secret-string '{
    "username": "your-username",
    "password": "your-password",
    "cluster": "cluster0.xxxxx.mongodb.net",
    "database": "production"
  }' \
  --region ${AWS_REGION} || echo "Secret already exists, skipping..."

echo "Creating PostgreSQL credentials secret..."
aws secretsmanager create-secret \
  --name prod/postgres/credentials \
  --description "PostgreSQL RDS credentials for Kafka Connect" \
  --secret-string '{
    "hostname": "postgres.rds.amazonaws.com",
    "port": "5432",
    "username": "postgres",
    "password": "your-password",
    "database": "production"
  }' \
  --region ${AWS_REGION} || echo "Secret already exists, skipping..."

echo "Creating IAM policy..."
aws iam create-policy \
  --policy-name KafkaConnectSecretsPolicy \
  --policy-document file://k8s/iam-policy.json || echo "Policy already exists, skipping..."

echo "Creating IAM role for IRSA..."
eksctl create iamserviceaccount \
  --name kafka-connect \
  --namespace ${NAMESPACE} \
  --cluster ${CLUSTER_NAME} \
  --attach-policy-arn arn:aws:iam::$(aws sts get-caller-identity --query Account --output text):policy/KafkaConnectSecretsPolicy \
  --approve \
  --region ${AWS_REGION} || echo "Service account already exists, skipping..."

echo "✅ AWS setup complete!"
