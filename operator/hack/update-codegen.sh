#!/usr/bin/env bash
set -euo pipefail

SCRIPT_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
CODEGEN_PKG=$(go list -m -f '{{.Dir}}' k8s.io/code-generator)
MODULE=$(go list -m)

source "${CODEGEN_PKG}/kube_codegen.sh"

kube::codegen::gen_client \
  --with-watch \
  --output-dir "${SCRIPT_ROOT}/pkg/generated" \
  --output-pkg "${MODULE}/pkg/generated" \
  --boilerplate "${SCRIPT_ROOT}/hack/boilerplate.go.txt" \
  "${SCRIPT_ROOT}/pkg/apis"