# AWS Quick Start Cheatsheet - Item Kafka POC
## One-Page Deployment Reference for Backend + Frontend + Infrastructure

**Date:** August 2026  
**Target Region:** `af-south-1` (adjustable)  
**Cluster Name:** `item-kafka-eks`  

---

## 🚀 Prerequisites (Install These First)

```powershell
# AWS CLI - credentials configured
aws configure

# Kubernetes tools
choco install -y kubectl eksctl helm docker

# Verify
aws sts get-caller-identity
kubectl version --client
eksctl version
```

---

## 1️⃣ PROVISION AWS INFRASTRUCTURE

### 1.1 Create EKS Cluster (2-3 min)

```powershell
eksctl create cluster `
  --name item-kafka-eks `
  --region af-south-1 `
  --version 1.29 `
  --nodegroup-name workers `
  --node-type t3.large `
  --nodes 2 `
  --nodes-min 2 `
  --nodes-max 6 `
  --enable-ssm-access

# Update kubeconfig
aws eks update-kubeconfig --region af-south-1 --name item-kafka-eks

# Verify cluster
kubectl get nodes
kubectl cluster-info
```

### 1.2 Create ECR Repositories

```powershell
# Backend repository
aws ecr create-repository `
  --repository-name item-kafka-backend `
  --region af-south-1

# Frontend repository
aws ecr create-repository `
  --repository-name item-kafka-ui `
  --region af-south-1

# Store your AWS account ID
$AWS_ACCOUNT_ID = (aws sts get-caller-identity --query Account --output text)
$AWS_REGION = "af-south-1"
$ECR_REGISTRY = "$AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com"

Write-Host "ECR Registry: $ECR_REGISTRY"
```

### 1.3 Create RDS MySQL Database

```powershell
# Create subnet group (use default VPC subnets)
aws rds create-db-subnet-group `
  --db-subnet-group-name item-kafka-subnet-group `
  --db-subnet-group-description "Item Kafka POC" `
  --subnet-ids subnet-xxxxx subnet-yyyyy `
  --region af-south-1

# Create RDS instance
aws rds create-db-instance `
  --db-instance-identifier item-kafka-mysql `
  --db-instance-class db.t3.micro `
  --engine mysql `
  --engine-version 8.0.35 `
  --master-username admin `
  --master-user-password 'YourSecurePassword123!' `
  --allocated-storage 20 `
  --db-subnet-group-name item-kafka-subnet-group `
  --publicly-accessible true `
  --multi-az false `
  --storage-encrypted true `
  --region af-south-1

# Wait for status "available" (5-10 minutes)
aws rds describe-db-instances `
  --db-instance-identifier item-kafka-mysql `
  --region af-south-1 `
  --query 'DBInstances[0].[DBInstanceStatus,Endpoint.Address]' `
  --output text

# Get endpoint
$MYSQL_ENDPOINT = aws rds describe-db-instances `
  --db-instance-identifier item-kafka-mysql `
  --region af-south-1 `
  --query 'DBInstances[0].Endpoint.Address' `
  --output text
  
Write-Host "MySQL Endpoint: $MYSQL_ENDPOINT"
```

### 1.4 Create Amazon MSK Kafka Cluster

```powershell
# Create Kafka security group (allow port 9092 from EKS VPC)
$SG_ID = (aws ec2 create-security-group `
  --group-name item-kafka-msk-sg `
  --description "MSK Kafka for Item POC" `
  --region af-south-1 `
  --query 'GroupId' `
  --output text)

# Allow inbound from EKS VPC
aws ec2 authorize-security-group-ingress `
  --group-id $SG_ID `
  --protocol tcp `
  --port 9092 `
  --cidr 10.0.0.0/8 `
  --region af-south-1

# Create MSK Cluster (simplified - broker nodes per AZ)
aws kafka create-cluster `
  --cluster-name item-kafka-cluster `
  --kafka-version 3.6.0 `
  --number-of-broker-nodes 3 `
  --broker-node-group-info InstanceType=kafka.t3.small,EbsStorageInfo={VolumeSize=100},SecurityGroups=$SG_ID `
  --encryption-info EncryptionInTransit={ClientBroker=TLS} `
  --region af-south-1

# Wait for cluster to be ACTIVE (~20 minutes)
aws kafka describe-cluster --cluster-arn <cluster-arn> --region af-south-1

# Get bootstrap servers
aws kafka get-bootstrap-brokers `
  --cluster-arn <cluster-arn> `
  --region af-south-1

# Store for later use
$MSK_BOOTSTRAP_SERVERS = "b-1.itemkafka.xxxxx.kafka.af-south-1.amazonaws.com:9092,..."
```

### 1.5 Create SQL Server (Optional Source Database)

```powershell
# Create RDS SQL Server instance
aws rds create-db-instance `
  --db-instance-identifier item-kafka-sqlserver `
  --db-instance-class db.t3.small `
  --engine sqlserver-ex `
  --engine-version "15.00" `
  --master-username admin `
  --master-user-password 'YourSecurePassword123!' `
  --allocated-storage 100 `
  --region af-south-1

# Wait for availability
```

---

## 2️⃣ BUILD & PUSH CONTAINER IMAGES

### 2.1 Backend Docker Image

```powershell
# Clone backend repo (if not in current directory)
git clone https://github.com/antonboshoff67-tech/WebFlux_Kafka-Producer-Consumer-POC backend
cd backend

# Build Docker image
docker build -t item-kafka-backend:latest .

# Tag for ECR
docker tag item-kafka-backend:latest $ECR_REGISTRY/item-kafka-backend:latest

# Login to ECR
aws ecr get-login-password --region $AWS_REGION | `
  docker login --username AWS --password-stdin $ECR_REGISTRY

# Push
docker push $ECR_REGISTRY/item-kafka-backend:latest
```

### 2.2 Frontend Docker Image

```powershell
cd ../frontend  # C:\Workspaces\ReactJS-UI-For-Item-Kafka-Producer-POC

# Build with API URL env var (adjust for your API endpoint)
docker build -t item-kafka-ui:latest `
  --build-arg VITE_API_BASE_URL=https://api.yourdomain.com .

# Tag and push
docker tag item-kafka-ui:latest $ECR_REGISTRY/item-kafka-ui:latest
docker push $ECR_REGISTRY/item-kafka-ui:latest
```

---

## 3️⃣ CONFIGURE KUBERNETES MANIFESTS

### 3.1 Update Backend Deployment Image

```powershell
cd ./k8s

# Edit backend-deployment.yaml
# Change image line to:
# image: 123456789012.dkr.ecr.af-south-1.amazonaws.com/item-kafka-backend:latest
```

### 3.2 Update ConfigMap with AWS Endpoints

Edit `backend-configmap.yaml`:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: item-kafka-config
  namespace: item-kafka-poc
data:
  # AWS RDS MySQL (replace with your endpoint)
  spring_mysql_jdbcUrl: "jdbc:mysql://item-kafka-mysql.c9akciq32.us-east-1.rds.amazonaws.com:3306/item_poc?useSSL=true&allowPublicKeyRetrieval=false"
  spring_mysql_username: "admin"
  
  # AWS MSK Kafka (replace with your bootstrap servers)
  spring_kafka_bootstrapServers: "b-1.itemkafka.xxxxx.kafka.af-south-1.amazonaws.com:9092,b-2.itemkafka.xxxxx.kafka.af-south-1.amazonaws.com:9092"
  spring_kafka_itemTopicName: "Item_Topic"
  
  # Optional: SQL Server source (if using RDS)
  spring_datasource_url: "jdbc:sqlserver://item-kafka-sqlserver.xxxxx.rds.amazonaws.com:1433;databaseName=ItemDB"
  
  # CORS for frontend domain
  item_cors_allowedOrigins: "https://ui.yourdomain.com"
  
  # Logging
  syslog_logging_host: "log-sink.yourdomain.com"
```

### 3.3 Update Backend Secrets

```powershell
# Create secret file (DO NOT commit)
# Edit backend-secret.yaml with your actual secrets:

$SECRET_YAML = @"
apiVersion: v1
kind: Secret
metadata:
  name: item-kafka-secrets
  namespace: item-kafka-poc
type: Opaque
stringData:
  mysql_password: "YourSecurePassword123!"
  mssql_password: "YourSecurePassword123!"
  jwt_private_key: |
    -----BEGIN PRIVATE KEY-----
    MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQC+...
    -----END PRIVATE KEY-----
  jwt_issuer: "item-kafka-producer"
"@

$SECRET_YAML | Out-File -FilePath .\backend-secret.yaml -Encoding UTF8
```

### 3.4 Create Frontend Deployment Manifest (if not exists)

```powershell
# Create frontend-deployment.yaml
$FRONTEND_DEPLOY = @"
apiVersion: apps/v1
kind: Deployment
metadata:
  name: item-kafka-ui
  namespace: item-kafka-poc
spec:
  replicas: 2
  selector:
    matchLabels:
      app: item-kafka-ui
  template:
    metadata:
      labels:
        app: item-kafka-ui
    spec:
      containers:
      - name: item-kafka-ui
        image: 123456789012.dkr.ecr.af-south-1.amazonaws.com/item-kafka-ui:latest
        ports:
        - containerPort: 3000
        env:
        - name: VITE_API_BASE_URL
          value: https://api.yourdomain.com
        resources:
          requests:
            memory: "128Mi"
            cpu: "100m"
          limits:
            memory: "512Mi"
            cpu: "500m"
        livenessProbe:
          httpGet:
            path: /
            port: 3000
          initialDelaySeconds: 30
          periodSeconds: 10
"@

$FRONTEND_DEPLOY | Out-File -FilePath .\frontend-deployment.yaml -Encoding UTF8
```

---

## 4️⃣ DEPLOY TO KUBERNETES

### 4.1 Create Namespace & Secrets

```powershell
# Create namespace
kubectl apply -f k8s/namespace.yaml

# Create secrets from file
kubectl apply -f k8s/backend-secret.yaml

# Verify
kubectl get secrets -n item-kafka-poc
```

### 4.2 Deploy Backend

```powershell
# ConfigMap
kubectl apply -f k8s/backend-configmap.yaml

# Deployment
kubectl apply -f k8s/backend-deployment.yaml

# Service
kubectl apply -f k8s/backend-service.yaml

# HPA (Horizontal Pod Autoscaler)
kubectl apply -f k8s/backend-hpa.yaml

# Ingress (ALB)
kubectl apply -f k8s/backend-ingress-alb.yaml

# Wait for pods
kubectl get pods -n item-kafka-poc -w

# Check logs
kubectl logs -n item-kafka-poc -l app=item-kafka-backend --tail=50 -f
```

### 4.3 Deploy Frontend

```powershell
# Deployment
kubectl apply -f k8s/frontend-deployment.yaml

# Service
kubectl apply -f k8s/frontend-service.yaml

# HPA
kubectl apply -f k8s/frontend-hpa.yaml

# Ingress
kubectl apply -f k8s/frontend-ingress-alb.yaml

# Wait for pods
kubectl get pods -n item-kafka-poc -w
```

---

## 5️⃣ CONFIGURE INGRESS & DNS

### 5.1 Get ALB URLs

```powershell
# Get ALB endpoints
kubectl get ingress -n item-kafka-poc

# Example output:
# NAME                   CLASS   HOSTS   ADDRESS                                          PORTS
# backend-ingress-alb    alb     *       k8s-itemkaf-backend-xxxxx.af-south-1.elb.amazonaws.com   80
# frontend-ingress-alb   alb     *       k8s-itemkaf-frontend-xxxxx.af-south-1.elb.amazonaws.com   80
```

### 5.2 Update Route53 DNS

```powershell
# Create Route53 records (replace *.elb.amazonaws.com URLs)
aws route53 change-resource-record-sets `
  --hosted-zone-id Z1234567890ABC `
  --change-batch '{
    "Changes": [
      {
        "Action": "CREATE",
        "ResourceRecordSet": {
          "Name": "api.yourdomain.com",
          "Type": "CNAME",
          "TTL": 300,
          "ResourceRecords": [{"Value": "k8s-itemkaf-backend-xxxxx.af-south-1.elb.amazonaws.com"}]
        }
      },
      {
        "Action": "CREATE",
        "ResourceRecordSet": {
          "Name": "ui.yourdomain.com",
          "Type": "CNAME",
          "TTL": 300,
          "ResourceRecords": [{"Value": "k8s-itemkaf-frontend-xxxxx.af-south-1.elb.amazonaws.com"}]
        }
      }
    ]
  }' --region $AWS_REGION
```

---

## 6️⃣ INITIALIZE KAFKA & DATABASE

### 6.1 Create Kafka Topic

```powershell
# SSH into a pod to create topic (or use AWS CLI)
kubectl run -it --rm kafka-client --image=confluentinc/cp-kafka:7.5.0 --restart=Never -- bash

# Inside pod:
kafka-topics --create \
  --bootstrap-server b-1.itemkafka.xxxxx.kafka.af-south-1.amazonaws.com:9092 \
  --topic Item_Topic \
  --partitions 3 \
  --replication-factor 3

# Verify
kafka-topics --list --bootstrap-server b-1.itemkafka.xxxxx.kafka.af-south-1.amazonaws.com:9092
```

### 6.2 Initialize Database

```powershell
# Connect to RDS MySQL
mysql -h $MYSQL_ENDPOINT -u admin -p

# Inside MySQL:
CREATE DATABASE IF NOT EXISTS item_poc;
USE item_poc;

-- Source the SQL scripts
SOURCE sql-scripts/03_mysql_item_sink_and_consumed_tables.sql;

# For SQL Server source (if used):
# SOURCE sql-scripts/02_mysql_item_source_seed_200.sql;
```

---

## 7️⃣ VALIDATION & TESTING

### 7.1 Check Pod Status

```powershell
# All pods running
kubectl get pods -n item-kafka-poc

# Backend pod logs
kubectl logs -n item-kafka-poc -l app=item-kafka-backend --tail=50

# Frontend pod logs
kubectl logs -n item-kafka-poc -l app=item-kafka-ui --tail=50

# Check HPA status
kubectl get hpa -n item-kafka-poc
```

### 7.2 Test Backend API

```powershell
# Get backend service endpoint
$BACKEND_URL = kubectl get svc -n item-kafka-poc item-kafka-backend -o jsonpath='{.status.loadBalancer.ingress[0].hostname}'

# Health check
curl http://$BACKEND_URL:8082/actuator/health

# Swagger UI
Write-Host "Swagger: http://$BACKEND_URL:8082/agent/swagger-ui.html"

# Publish items
curl -X POST http://$BACKEND_URL:8082/item-kafka/app/publish-items/v1

# Consume items
curl -X POST http://$BACKEND_URL:8082/item-kafka/consumer/manual-consume/v1
```

### 7.3 Test Frontend

```powershell
Write-Host "Frontend: https://ui.yourdomain.com"
```

---

## 8️⃣ MONITORING & SCALING

### 8.1 CloudWatch Logs

```powershell
# View backend logs
aws logs tail /aws/containerinsights/item-kafka-eks/application --follow

# View performance metrics
```

### 8.2 Scale Pods Manually

```powershell
# Backend
kubectl scale deployment/item-kafka-backend --replicas=4 -n item-kafka-poc

# Frontend
kubectl scale deployment/item-kafka-ui --replicas=3 -n item-kafka-poc
```

### 8.3 Monitor Resources

```powershell
# Node usage
kubectl top nodes

# Pod usage
kubectl top pods -n item-kafka-poc
```

---

## 9️⃣ CLEANUP (When Done)

```powershell
# Delete Kubernetes resources
kubectl delete namespace item-kafka-poc

# Delete EKS cluster (this removes all resources in VPC)
eksctl delete cluster --name item-kafka-eks --region af-south-1

# Delete RDS instances
aws rds delete-db-instance --db-instance-identifier item-kafka-mysql --skip-final-snapshot --region af-south-1
aws rds delete-db-instance --db-instance-identifier item-kafka-sqlserver --skip-final-snapshot --region af-south-1

# Delete MSK cluster
aws kafka delete-cluster --cluster-arn <cluster-arn> --region af-south-1

# Delete ECR repositories
aws ecr delete-repository --repository-name item-kafka-backend --region af-south-1 --force
aws ecr delete-repository --repository-name item-kafka-ui --region af-south-1 --force
```

---

## 🔑 Key Environment Variables Summary

| Variable | Value | Used By |
|---|---|---|
| `ITEM_KAFKA_BOOTSTRAP_SERVERS` | MSK brokers (9092) | Backend, Jobs |
| `ITEM_MYSQL_URL` | RDS MySQL endpoint | Backend, MySQL Job |
| `ITEM_MYSQL_USERNAME` | `admin` | Backend, MySQL Job |
| `ITEM_MYSQL_PASSWORD` | Your password | Backend, MySQL Job |
| `ITEM_MSSQL_URL` | SQL Server endpoint (optional) | Backend, MSSQL Job |
| `ITEM_JWT_PRIVATE_KEY` | Generated RSA key | JWT signing |
| `ITEM_CORS_ALLOWED_ORIGINS` | Frontend domain | CORS policy |
| `VITE_API_BASE_URL` | Backend API URL | React frontend |

---

## 📊 Architecture Summary

```
┌─────────────────────────────────────────────────┐
│  Route53 (DNS)                                  │
│  - api.yourdomain.com → Backend ALB             │
│  - ui.yourdomain.com  → Frontend ALB            │
└──────────────┬────────────────────────────────┘
               │
        ┌──────┴──────┐
        │             │
    ┌───▼────┐   ┌────▼────┐
    │Backend  │   │Frontend  │
    │ALB      │   │ALB       │
    └───┬────┘   └────┬─────┘
        │             │
   ┌────▼──────────────▼────────────────┐
   │       EKS Cluster                  │
   │  ┌──────────┐  ┌──────────┐       │
   │  │Backend   │  │Frontend  │       │
   │  │Pods (HPA)│  │Pods (HPA)│       │
   │  └──────┬───┘  └──────────┘       │
   │         │ (Spring Boot)           │
   └─────────┼───────────────────────┘
             │
        ┌────┴────┬─────────┬──────────┐
        │          │         │          │
   ┌────▼────┐ ┌──▼──┐ ┌───▼──┐  ┌───▼───┐
   │RDS MySQL│ │ MSK │ │ SQL  │  │Secrets│
   │(Sink)   │ │Kafka│ │Server│  │Manager│
   │         │ │     │ │(Src) │  │(JWT)  │
   └─────────┘ └─────┘ └──────┘  └───────┘
```

---

## 🆘 Troubleshooting

| Issue | Solution |
|---|---|
| Pods stuck in `Pending` | Check node resources: `kubectl top nodes` |
| Backend can't reach MSK | Check security group ingress rules (port 9092) |
| Frontend API calls fail | Verify `VITE_API_BASE_URL` env var, check CORS |
| Database connection error | Verify RDS endpoint, security group, credentials |
| Images not found in ECR | Verify push succeeded: `aws ecr describe-images --repository-name item-kafka-backend` |
| Ingress not showing IP | Wait 2-3 min, ALB provisioning takes time |

---

## 📚 Reference Links

- Backend EKS Guide: `EKS_README.md`
- Frontend EKS Guide: `../ReactJS-UI-For-Item-Kafka-Producer-POC/EKS_README.md`
- Full Architecture: `ARCHITECTURE.md`
- API Documentation: `API_DOCUMENTATION.md`
- Developer Guide: `DEVELOPER_GUIDE.md`

---

**Last Updated:** August 8, 2026  
**Tested on:** Windows PowerShell 5.1, eksctl 0.180+, kubectl 1.29, Kubernetes 1.29

