# AWS Deployment - Start Here
This is the **entry point** for deploying the Item Kafka Producer/Consumer/Flink backend
(this repo, the **Spring WebFlux / reactive** version) to AWS. Read this first, then follow
the links below in order.
## Which AWS doc do I need?
| I want to... | Read this |
|---|---|
| Understand the target AWS architecture (EKS + MSK + RDS + ALB) at a glance | `EKS_README.md` |
| Copy/paste a full, ordered set of PowerShell commands to provision everything and deploy | `AWS_QUICKSTART_CHEATSHEET.md` |
| Get a short summary of what was deployed / decisions made for this POC | `AWS_DEPLOYMENT_SUMMARY.md` |
| Edit the actual Kubernetes manifests (Deployment, Service, HPA, Ingress, ConfigMap, Secret) | `k8s/` folder |
| Understand environment variables / config properties used by the app | `SETUP_GUIDE.md` |
| See reactive REST endpoints to smoke-test after deployment | `API_DOCUMENTATION.md` |
| Understand WebFlux/R2DBC/reactor-kafka specifics before touching prod config | `WEBFLUX_TUTORIAL_GUIDE.md`, `WEBFLUX_INTERVIEW_SHEET.md` |
## Recommended order
1. `EKS_README.md` - read this first for the "why" and the high-level architecture
   (EKS for compute, Amazon MSK for Kafka, Amazon RDS MySQL via R2DBC for the sink database,
   ALB for ingress). It also covers the ECS/Fargate alternative if you dont want Kubernetes.
2. `AWS_QUICKSTART_CHEATSHEET.md` - the "how", step by step:
   - provision the EKS cluster, ECR repositories, RDS MySQL, Amazon MSK, optional RDS SQL Server
   - build and push the backend (and companion React UI) Docker images
   - configure `k8s/backend-configmap.yaml` and `k8s/backend-secret.example.yaml`
   - `kubectl apply` the manifests in `k8s/`
   - wire up Route53 DNS and validate with curl/Swagger, including the reactive
     `.../items/stream/v1` and `.../consume-stream/v1` SSE endpoints
   - monitor, scale, and clean up when you are done
3. `AWS_DEPLOYMENT_SUMMARY.md` - a short recap of what was actually deployed for this
   POC (region, cluster name, endpoints used) so you don't have to re-derive it from the
   cheatsheet every time.
## Before you start
- Have an AWS account with permissions for EKS, ECR, EC2, IAM, RDS, and MSK.
- Install: `aws` CLI, `kubectl`, `eksctl`, `helm` (optional), and Docker Desktop.
- Never commit real secrets - copy `k8s/backend-secret.example.yaml` to a local, git-ignored
  file and fill in real values, or use AWS Secrets Manager (see the Security notes
  section of `README.md`).
- R2DBC connection strings use a different URL format than JDBC
  (`r2dbc:mysql://<host>:3306/<db>`) - double-check `ITEM_MYSQL_URL` in
  `k8s/backend-configmap.yaml` uses the R2DBC form, not the JDBC form used by the
  Spring MVC sibling project.
## Companion frontend
The React UI (`ReactJS-UI-For-Item-Kafka-Producer-POC`) has its own `EKS_README.md` /
`AWS_*` docs for deploying the frontend container alongside this backend. Deploy the backend
first so you have an API URL to point the frontend's `VITE_API_BASE_URL` at.
## Related backend variant
This repo is the reactive Spring WebFlux implementation. The original Spring MVC
(blocking/servlet) implementation of the same POC lives in
`ConfluentCloud_Kafka-Producer-Consumer-POC` and has its own `AWS_README_START_HERE.md`
following the same structure - see `TECHNOLOGY_ARCHITECTURE_COMPARISON.md` in that repo for
a detailed pros/cons comparison of the two stacks.
