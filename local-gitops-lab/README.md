# Local GitOps Lab

## 1) Prereqs
- kind
- kubectl
- helm
- argocd

## 2) Cluster + Argo CD
```bash
kind create cluster --name gitops-lab
kubectl create namespace argocd
kubectl apply -n argocd -f https://raw.githubusercontent.com/argoproj/argo-cd/stable/manifests/install.yaml
kubectl apply --server-side -k 'https://github.com/argoproj/argo-cd/manifests/crds?ref=stable'
```

## 3) Push this folder to GitHub
Update repoURL in:
- argo/connect-source-dev1.yaml
- argo/connect-sink-dev1.yaml
- argo/schema-registry-dev1.yaml

## 4) Create destination namespace
```bash
kubectl create namespace data-platform
```

## 5) Apply Argo Applications
```bash
kubectl apply -f argo/connect-source-dev1.yaml
kubectl apply -f argo/connect-sink-dev1.yaml
kubectl apply -f argo/schema-registry-dev1.yaml
```

## 6) Access Argo UI
```bash
kubectl -n argocd port-forward svc/argocd-server 8080:443
kubectl -n argocd get secret argocd-initial-admin-secret -o jsonpath='{.data.password}' | base64 -d; echo
```
Then login with:
- user: admin
- password: output above
