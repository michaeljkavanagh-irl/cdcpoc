#!/bin/bash
set -e

# Configuration
AWS_REGION="us-east-1"
AWS_ACCOUNT_ID="YOUR_ACCOUNT_ID"
ECR_REPO="kafka-connect"
IMAGE_TAG="latest"

echo "Building custom SMT..."
cd custom-smt
mvn clean package
cd ..

echo "Building Docker image..."
docker build -f Dockerfile.kafka-connect -t ${ECR_REPO}:${IMAGE_TAG} .

echo "Logging into ECR..."
aws ecr get-login-password --region ${AWS_REGION} | \
  docker login --username AWS --password-stdin ${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com

echo "Creating ECR repository if it doesn't exist..."
aws ecr describe-repositories --repository-names ${ECR_REPO} --region ${AWS_REGION} || \
  aws ecr create-repository --repository-name ${ECR_REPO} --region ${AWS_REGION}

echo "Tagging image..."
docker tag ${ECR_REPO}:${IMAGE_TAG} \
  ${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${ECR_REPO}:${IMAGE_TAG}

echo "Pushing to ECR..."
docker push ${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${ECR_REPO}:${IMAGE_TAG}

echo "✅ Build and push complete!"
echo "Image: ${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${ECR_REPO}:${IMAGE_TAG}"
