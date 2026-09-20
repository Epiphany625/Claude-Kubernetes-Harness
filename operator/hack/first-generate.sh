mkdir -p hack pkg/apis/CKH/v1alpha1 config/crd config/samples cmd/operator controller

kubectl version | grep Server
V=v0.35.0   # adjust to your server's minor version
go get k8s.io/api@$V k8s.io/apimachinery@$V k8s.io/client-go@$V k8s.io/code-generator@$V


controller-gen object:headerFile=hack/boilerplate.go.txt paths=./pkg/apis/...
controller-gen crd paths=./pkg/apis/... output:crd:dir=config/crd

chmod +x hack/update-codegen.sh

