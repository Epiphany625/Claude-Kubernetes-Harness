#!/usr/bin/env bash
# Stop: run the unit suite before finishing a turn.
#
# Unit tests only. The integration suite pulls container images and takes over a
# minute; the full gate is `make test-integration`.
#
# Skips entirely when no Go source changed, so documentation-only turns do not
# pay for it. Exit 2 reports failures back to the agent.
set -uo pipefail

MODULE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${MODULE_DIR}" || exit 0
command -v go >/dev/null 2>&1 || exit 0

# Nothing to check if no Go file in this module differs from HEAD.
if git rev-parse --git-dir >/dev/null 2>&1; then
  CHANGED=$(git status --porcelain -- '*.go' 2>/dev/null | head -1)
  UNTRACKED=$(git ls-files --others --exclude-standard -- '*.go' 2>/dev/null | head -1)
  [[ -z "${CHANGED}" && -z "${UNTRACKED}" ]] && exit 0
fi

OUTPUT=$(go test ./... 2>&1)
STATUS=$?

if [[ ${STATUS} -ne 0 ]]; then
  {
    echo "Unit tests are failing:"
    echo
    printf '%s\n' "${OUTPUT}" | grep -vE '^(ok|\?)' | tail -40
    echo
    echo "Fix these before finishing. Full gate: make check && make test-integration"
  } >&2
  exit 2
fi

# The suite must not have acquired a dependency. A test that needs Docker or a
# cluster belongs behind -tags=integration or -tags=e2e; catching the drift here
# is cheaper than discovering it in CI on a machine with no Docker.
if printf '%s' "${OUTPUT}" | grep -qiE 'docker|testcontainers|connection refused|port-forward'; then
  {
    echo "The unit suite appears to be reaching for a real dependency:"
    echo
    printf '%s\n' "${OUTPUT}" | grep -iE 'docker|testcontainers|connection refused|port-forward' | head -10
    echo
    echo "'make test' must pass with no cluster, no database and no Docker."
    echo "Move the test behind //go:build integration or //go:build e2e."
  } >&2
  exit 2
fi
exit 0
