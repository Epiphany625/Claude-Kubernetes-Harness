# alertprocessor
#
# `make test` is the gate that must always pass and must never need a cluster,
# a database, or Docker. Everything that does need one is a separate target.

IMAGE   ?= alertprocessor
TAG     ?= dev
VERSION ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo dev)
NS      ?= ckh

.DEFAULT_GOAL := help

.PHONY: help
help: ## Show this help
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

# --- Verify ---------------------------------------------------------------

.PHONY: test
test: ## Unit tests. No cluster, no database, no Docker.
	go test ./...

.PHONY: test-integration
test-integration: ## Integration tests against ephemeral Postgres + RabbitMQ (needs Docker)
	go test -tags=integration -timeout=20m ./...

.PHONY: test-e2e
test-e2e: ## End-to-end against the deployed pipeline (needs a cluster + ALERTPROCESSOR_POSTGRES_DSN)
	go test -tags=e2e -timeout=10m -v ./test/e2e/...

.PHONY: lint
lint: ## gofmt + go vet, across every build tag
	@unformatted=$$(gofmt -l .); \
	if [ -n "$$unformatted" ]; then \
		echo "gofmt needed in:"; echo "$$unformatted"; exit 1; \
	fi
	go vet ./...
	go vet -tags=integration ./...
	go vet -tags=e2e ./...

.PHONY: fmt
fmt: ## Rewrite source with gofmt
	gofmt -w .

.PHONY: check
check: lint test ## What CI runs

# --- Build ----------------------------------------------------------------

.PHONY: build
build: ## Build the binary into ./bin
	go build -trimpath -ldflags="-s -w -X main.version=$(VERSION)" -o bin/alertprocessor ./cmd/alertprocessor

.PHONY: image
image: ## Build the container image
	docker build --build-arg VERSION=$(VERSION) -t $(IMAGE):$(TAG) .

.PHONY: load
load: ## Build the image directly inside minikube's Docker daemon
	@# Deliberately NOT `make image && minikube image load`. That pair silently
	@# keeps the image already in the cluster when the tag has not changed --
	@# `--overwrite=true` does not help, and `minikube image rm` refuses while a
	@# container references it. The result is a rollout that reports success and
	@# runs the previous binary, which is an expensive way to debug a fix that
	@# was already correct.
	@#
	@# Building inside minikube's daemon repoints the tag at the new layers the
	@# way any local `docker build` would, whatever is currently running.
	eval $$(minikube docker-env) && \
		docker build --build-arg VERSION=$(VERSION) -t $(IMAGE):$(TAG) .

# --- Deploy ---------------------------------------------------------------

.PHONY: deploy
deploy: ## Apply the service manifests (not the Secret -- see ops/alertprocessor/secret.example.yaml)
	kubectl apply -f ../ops/alertprocessor/configmap.yaml
	kubectl apply -f ../ops/alertprocessor/deployment.yaml
	kubectl apply -f ../ops/alertprocessor/service.yaml
	kubectl apply -f ../ops/alertprocessor/servicemonitor.yaml

.PHONY: route
route: ## Point Alertmanager at the webhook (patch + AlertmanagerConfig)
	kubectl patch -n monitoring alertmanager monitoring-kube-prometheus-alertmanager \
		--type=merge --patch-file ../ops/alerting/alertmanager-patch.yaml
	kubectl apply -f ../ops/alerting/alertmanagerconfig.yaml
	kubectl apply -f ../ops/alerting/prometheusrule-ckh.yaml

.PHONY: restart
restart: ## Roll the deployment, e.g. after `make load`
	kubectl -n $(NS) rollout restart deployment/alertprocessor
	kubectl -n $(NS) rollout status deployment/alertprocessor --timeout=120s

.PHONY: logs
logs: ## Follow the service logs
	kubectl -n $(NS) logs -l app.kubernetes.io/name=alertprocessor -f --tail=100

# --- Inspect --------------------------------------------------------------

.PHONY: am-config
am-config: ## Print the Alertmanager config the operator actually generated
	@# prometheus-operator >= 0.78 stores this GZIPPED under `alertmanager.yaml.gz`.
	@# Older versions use a plain `alertmanager.yaml`. Try the compressed key first
	@# and fall back, so this works against either.
	@set -e; \
	S=alertmanager-monitoring-kube-prometheus-alertmanager-generated; \
	GZ=$$(kubectl -n monitoring get secret $$S -o jsonpath='{.data.alertmanager\.yaml\.gz}' 2>/dev/null); \
	if [ -n "$$GZ" ]; then \
		printf '%s' "$$GZ" | base64 -d | gunzip; \
	else \
		kubectl -n monitoring get secret $$S -o jsonpath='{.data.alertmanager\.yaml}' | base64 -d; \
	fi

.PHONY: smoke
smoke: ## Fire a real alert through the whole chain, then clean up
	kubectl apply -f ../ops/alerting/prometheusrule-smoketest.yaml
	@echo "Applied. The alert fires within ~30s and is delivered ~30s later (groupWait)."
	@echo "Watch:   make logs"
	@echo "Verify:  SELECT event_id, alert_id, status FROM event"
	@echo "           WHERE alert_name = 'AlertProcessorPipelineSmokeTest';"
	@echo "Then:    make smoke-clean"

.PHONY: smoke-clean
smoke-clean: ## Remove the smoke-test alert rule
	kubectl delete -f ../ops/alerting/prometheusrule-smoketest.yaml --ignore-not-found

.PHONY: rabbit-ui
rabbit-ui: ## Port-forward the RabbitMQ management UI to http://localhost:15672
	kubectl -n $(NS) port-forward svc/agent-rabbitmq 15672:15672

.PHONY: clean
clean: ## Remove build output
	rm -rf bin/
