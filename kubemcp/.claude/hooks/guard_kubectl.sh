#!/usr/bin/env bash
# PreToolUse(Bash): refuse destructive cluster commands against a non-local context.
#
# This repo carries live cluster credentials, and the e2e suite plus ordinary
# debugging both involve kubectl. A `kubectl delete` typed against whatever
# context happens to be current is the accident worth preventing -- the local
# ones (minikube, kind, Docker Desktop) are disposable, anything else is not.
#
# Exit 2 blocks the call and returns stderr to the agent; exit 0 allows it.
set -uo pipefail

INPUT=$(cat)
COMMAND=$(printf '%s' "$INPUT" | python3 -c 'import json,sys
try:
    data = json.load(sys.stdin)
except Exception:
    sys.exit(0)
print((data.get("tool_input") or {}).get("command", ""))' 2>/dev/null)

[[ -z "${COMMAND}" ]] && exit 0

# Only inspect commands that actually talk to a cluster.
if ! printf '%s' "${COMMAND}" | grep -qE '(^|[;&|[:space:]])(kubectl|helm)([[:space:]]|$)'; then
  exit 0
fi

# Destructive verbs. Reads (get/describe/logs/top/explain) are always fine.
if ! printf '%s' "${COMMAND}" | grep -qE '(kubectl[[:space:]]+(delete|drain|cordon|uncordon|taint|evict|replace|scale|patch|apply|edit|rollout[[:space:]]+(restart|undo))|helm[[:space:]]+(uninstall|delete|rollback|upgrade))'; then
  exit 0
fi

# An explicit context on the command line wins over the current context.
# kubectl spells it --context; helm spells it --kube-context.
CONTEXT=$(printf '%s' "${COMMAND}" \
  | grep -oE -- '--(kube-)?context[= ][^ ]+' | head -1 \
  | sed -E 's/--(kube-)?context[= ]//')
if [[ -z "${CONTEXT}" ]]; then
  CONTEXT=$(kubectl config current-context 2>/dev/null || echo "")
fi

if [[ -z "${CONTEXT}" ]]; then
  exit 0  # No cluster configured at all; nothing to protect.
fi

case "${CONTEXT}" in
  minikube|kind-*|docker-desktop|k3d-*|rancher-desktop|colima)
    exit 0
    ;;
esac

cat >&2 <<EOF
Blocked: this is a destructive cluster command and the current kubectl context
is "${CONTEXT}", which is not a recognised local/disposable cluster
(minikube, kind-*, docker-desktop, k3d-*, rancher-desktop, colima).

  ${COMMAND}

If this is intended, either switch context first:
  kubectl config use-context <local-cluster>
or ask the user to confirm the target cluster before re-running. Do not work
around this by re-running the same command with a different spelling.
EOF
exit 2
