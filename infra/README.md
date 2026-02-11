
ci-workflow.yaml
```
git private repo에 접근 
microk8s kubectl create secret generic github-token \
  --namespace argo \
  --from-literal=token=[github 토큰]
```