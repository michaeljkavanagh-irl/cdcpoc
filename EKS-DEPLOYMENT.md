# EKS Deployment Guide

## Prerequisites

- AWS CLI configured
- kubectl configured for your EKS cluster
- eksctl installed
- Docker installed
- Maven installed (for building custom SMT)

## Deployment Steps

### 1. Update Configuration

Edit the following files with your values:

**build-and-push.sh:**
- `AWS_ACCOUNT_ID`: Your AWS account ID
- `AWS_REGION`: Your AWS region

**setup-aws-secrets.sh:**
- `CLUSTER_NAME`: Your EKS cluster name
- Update secret values in the script with actual credentials

**k8s/kafka-connect-deployment.yaml:**
- `YOUR_ECR_REPO`: Your ECR repository URL
- `CONNECT_BOOTSTRAP_SERVERS`: Your Kafka broker endpoints
- Update IAM role ARN in ServiceAccount annotation

**k8s/iam-policy.json:**
- Replace `ACCOUNT_ID` with your AWS account ID

### 2. Setup AWS Secrets Manager

```bash
chmod +x setup-aws-secrets.sh
./setup-aws-secrets.sh
```

This creates:
- Secrets in AWS Secrets Manager for MongoDB and PostgreSQL
- IAM policy for Secrets Manager access
- IAM role with IRSA for the kafka-connect ServiceAccount

### 3. Build and Push Docker Image

```bash
chmod +x build-and-push.sh
./build-and-push.sh
```

This:
- Builds the custom SMT JAR
- Creates Docker image with all connectors and ConfigProvider
- Pushes to ECR

### 4. Deploy to EKS

```bash
chmod +x deploy-to-eks.sh
./deploy-to-eks.sh
```

This:
- Deploys Kafka Connect to EKS
- Creates and registers both connectors

### 5. Verify Deployment

```bash
# Check pods
kubectl get pods -n kafka

# Check logs
kubectl logs -n kafka deployment/kafka-connect -f

# Check connectors
kubectl port-forward -n kafka svc/kafka-connect 8083:8083
curl http://localhost:8083/connectors
curl http://localhost:8083/connectors/postgres-source-connector-v2/status
curl http://localhost:8083/connectors/mongodb-sink-connector/status
```

## Architecture

```
┌────────────────────────────────────────┐
│  EKS Cluster                           │
│  ┌──────────────────────────────────┐ │
│  │ Kafka Connect Pod                │ │
│  │ ┌──────────────────────────────┐ │ │
│  │ │ - Debezium PostgreSQL        │ │ │
│  │ │ - MongoDB Sink               │ │ │
│  │ │ - Custom SMT                 │ │ │
│  │ │ - AWS Secrets ConfigProvider│ │ │
│  │ └──────────────────────────────┘ │ │
│  │                                  │ │
│  │ ServiceAccount (IRSA)            │ │
│  └─────────┬────────────────────────┘ │
└────────────┼─────────────────────────┘
             │
             │ IAM Role
             ▼
┌────────────────────────────────────────┐
│  AWS Secrets Manager                   │
│  - prod/mongodb/credentials            │
│  - prod/postgres/credentials           │
└────────────────────────────────────────┘
```

## Secrets Format

### MongoDB Secret (prod/mongodb/credentials)
```json
{
  "username": "your-mongodb-username",
  "password": "your-mongodb-password",
  "cluster": "cluster0.xxxxx.mongodb.net",
  "database": "your-database"
}
```

### PostgreSQL Secret (prod/postgres/credentials)
```json
{
  "hostname": "your-postgres-host",
  "port": "5432",
  "username": "your-postgres-username",
  "password": "your-postgres-password",
  "database": "your-database"
}
```

## Updating Secrets

```bash
# Update MongoDB password
aws secretsmanager update-secret \
  --secret-id prod/mongodb/credentials \
  --secret-string '{
    "username": "mk",
    "password": "new-password",
    "cluster": "cluster0.xxxxx.mongodb.net",
    "database": "production"
  }'

# Restart Kafka Connect to pick up changes
kubectl rollout restart deployment/kafka-connect -n kafka
```

## Troubleshooting

### Check ConfigProvider is loaded
```bash
kubectl logs -n kafka deployment/kafka-connect | grep -i "config.provider"
```

### Check IAM permissions
```bash
# Verify pod can access secrets
kubectl exec -n kafka deployment/kafka-connect -- \
  aws secretsmanager get-secret-value --secret-id prod/mongodb/credentials
```

### Check connector errors
```bash
curl http://localhost:8083/connectors/mongodb-sink-connector/status | jq
```
