# AWS EKS Deployment Guide (Backend)

This document explains how to run the backend in AWS using Amazon EKS.

## Interview-friendly quick start

Use this if you want the shortest possible deploy story:

```powershell
aws configure
eksctl create cluster --name item-kafka-eks --region af-south-1 --version 1.29 --nodegroup-name workers --node-type t3.large --nodes 2 --nodes-min 2 --nodes-max 6
aws ecr create-repository --repository-name webflux-kafka-backend --region af-south-1
docker build -t webflux-kafka-backend:latest .
docker tag webflux-kafka-backend:latest 123456789012.dkr.ecr.af-south-1.amazonaws.com/webflux-kafka-backend:latest
docker push 123456789012.dkr.ecr.af-south-1.amazonaws.com/webflux-kafka-backend:latest
kubectl apply -f k8s/namespace.yaml
kubectl apply -f k8s/backend-configmap.yaml
kubectl apply -f k8s/backend-secret.example.yaml
kubectl apply -f k8s/backend-deployment.yaml
kubectl apply -f k8s/backend-service.yaml
kubectl apply -f k8s/backend-hpa.yaml
kubectl get pods -n webflux-kafka-poc
```

The rest of this file explains the same flow in a developer-friendly, step-by-step way.

## What this mode is for

Use EKS when you need:

- Kubernetes-native deployment patterns (Deployments, Services, Ingress, HPA)
- Rolling updates and self-healing pods
- Horizontal pod scaling under load
- A cloud runtime closer to production than local Docker

For this project, a typical AWS architecture is:

- Backend app on EKS
- Kafka on Amazon MSK
- MySQL on Amazon RDS
- ALB Ingress for external HTTP entry

## Prerequisites

- AWS account with permissions for EKS/ECR/EC2/IAM
- `aws` CLI configured (`aws configure`)
- `kubectl`
- `eksctl`
- Docker

## 1) Create ECR repository and push image

```powershell
aws ecr create-repository --repository-name webflux-kafka-backend --region af-south-1
aws ecr get-login-password --region af-south-1 | docker login --username AWS --password-stdin 123456789012.dkr.ecr.af-south-1.amazonaws.com

docker build -t webflux-kafka-backend:latest .
docker tag webflux-kafka-backend:latest 123456789012.dkr.ecr.af-south-1.amazonaws.com/webflux-kafka-backend:latest
docker push 123456789012.dkr.ecr.af-south-1.amazonaws.com/webflux-kafka-backend:latest
```

Update the image in `k8s/backend-deployment.yaml`.

## 2) Create EKS cluster

```powershell
eksctl create cluster --name item-kafka-eks --region af-south-1 --version 1.29 --nodegroup-name workers --node-type t3.large --nodes 2 --nodes-min 2 --nodes-max 6
aws eks update-kubeconfig --region af-south-1 --name item-kafka-eks
kubectl get nodes
```

## 3) Provision external dependencies

Create and note endpoints for:

- Amazon RDS MySQL (source + sink schemas)
- Amazon MSK (bootstrap servers)

Seed DB with this repo's scripts:

- `sql-scripts/02_mysql_item_source_seed_200.sql`
- `sql-scripts/03_mysql_item_sink_and_consumed_tables.sql`

## 4) Configure manifests

Edit:

- `k8s/backend-configmap.yaml`
  - `ITEM_MSSQL_URL` (source DB)
  - `ITEM_MYSQL_URL` (sink DB)
  - `ITEM_KAFKA_BOOTSTRAP_SERVERS` (MSK brokers)
  - `ITEM_CORS_ALLOWED_ORIGINS` (frontend domain)
- `k8s/backend-secret.example.yaml`
  - copy to a real secret file (for example `backend-secret.yaml`)
  - set DB credentials/JWT values

Do not commit real secrets.

## 5) Deploy to EKS

```powershell
kubectl apply -f k8s/namespace.yaml
kubectl apply -f k8s/backend-configmap.yaml
kubectl apply -f k8s/backend-secret.example.yaml
kubectl apply -f k8s/backend-deployment.yaml
kubectl apply -f k8s/backend-service.yaml
kubectl apply -f k8s/backend-hpa.yaml
```

Optional ALB ingress:

```powershell
kubectl apply -f k8s/backend-ingress-alb.yaml
```

## 6) Validate deployment

```powershell
kubectl get pods -n webflux-kafka-poc
kubectl get svc -n webflux-kafka-poc
kubectl get hpa -n webflux-kafka-poc
kubectl logs -n webflux-kafka-poc deploy/webflux-kafka-backend --tail=200
```

If using ingress:

```powershell
kubectl get ingress -n webflux-kafka-poc
```

## 7) Autoscaling behavior

- `k8s/backend-hpa.yaml` scales pods from 2 to 10 at ~70% CPU target.
- For node autoscaling, install Cluster Autoscaler or Karpenter.
- HPA scales pods; Cluster Autoscaler/Karpenter scales worker nodes.

## ECS/Fargate alternative

You can also run this backend on ECS/Fargate instead of EKS.

- Use ECS if you want managed containers without Kubernetes.
- Use EKS if you want Kubernetes APIs, Helm, and workload portability.

ECS/Fargate quick start pattern:

1. Push image to ECR.
2. Create ECS task definition with env vars/secrets.
3. Create ECS service (Fargate launch type) behind an ALB.

Both are valid; EKS manifests in this repo target Kubernetes.
