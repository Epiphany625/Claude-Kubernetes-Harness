# operator -- CKH (ckh.io/v1alpha1)
#
# Generated output is not checked in. Run `make codegen` after touching
# anything under api/ or any +kubebuilder marker.

CONTROLLER_TOOLS_VERSION ?= v0.22.0
ENVTEST_VERSION          ?= v0.25.1
ENVTEST_K8S_VERSION      ?= 1.37.0

LOCALBIN       := $(CURDIR)/bin
CONTROLLER_GEN := $(LOCALBIN)/controller-gen
ENVTEST        := $(LOCALBIN)/setup-envtest

.DEFAULT_GOAL := help

.PHONY: help
help: ## Show this help
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

# --- Codegen --------------------------------------------------------------

.PHONY: codegen
codegen: generate manifests ## Everything controller-gen produces

.PHONY: generate
generate: $(CONTROLLER_GEN) ## DeepCopy methods -> api/v1alpha1/zz_generated.deepcopy.go
	$(CONTROLLER_GEN) object paths="./api/..."

.PHONY: manifests
manifests: $(CONTROLLER_GEN) ## CRD + RBAC yaml -> config/
	$(CONTROLLER_GEN) \
		crd output:crd:artifacts:config=config/crd \
		rbac:roleName=operator-role output:rbac:artifacts:config=config/rbac \
		paths="./..."

# --- Verify ---------------------------------------------------------------

.PHONY: tidy
tidy: ## Resolve dependencies (run this first, go.sum is not checked in)
	go mod tidy

.PHONY: test
test: $(ENVTEST) ## Run tests against a real apiserver via envtest
	KUBEBUILDER_ASSETS="$$($(ENVTEST) use $(ENVTEST_K8S_VERSION) --bin-dir $(LOCALBIN) -p path)" go test ./...

.PHONY: fmt
fmt: ## gofmt + go vet
	gofmt -w .
	go vet ./...

# --- Run ------------------------------------------------------------------

.PHONY: install
install: manifests ## Apply the CRD to the current cluster
	kubectl apply -f config/crd

.PHONY: run
run: generate ## Run the controller locally against the current kubeconfig
	go run ./cmd

# --- Tools ----------------------------------------------------------------

$(LOCALBIN):
	mkdir -p $(LOCALBIN)

$(CONTROLLER_GEN): | $(LOCALBIN)
	GOBIN=$(LOCALBIN) go install sigs.k8s.io/controller-tools/cmd/controller-gen@$(CONTROLLER_TOOLS_VERSION)

$(ENVTEST): | $(LOCALBIN)
	GOBIN=$(LOCALBIN) go install sigs.k8s.io/controller-runtime/tools/setup-envtest@$(ENVTEST_VERSION)

.PHONY: clean
clean: ## Remove tools and generated output
	rm -rf $(LOCALBIN) config/crd config/rbac api/v1alpha1/zz_generated.deepcopy.go
