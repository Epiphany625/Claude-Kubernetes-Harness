#!/usr/bin/env bash
# Stop: run the fast unit suite before finishing a turn.
#
# Unit tests only (~0.1s, no I/O). The integration suite starts HTTP servers and
# is too slow to run on every turn; the full gate is `uv run pytest`.
#
# Skips entirely when no Python source changed, so documentation-only turns do
# not pay for it. Exit 2 reports failures back to the agent.
set -uo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${PROJECT_DIR}" || exit 0
command -v uv >/dev/null 2>&1 || exit 0

# Nothing to check if no tracked Python file differs from HEAD.
if git rev-parse --git-dir >/dev/null 2>&1; then
  CHANGED=$(git status --porcelain -- '*.py' 2>/dev/null | head -1)
  UNTRACKED=$(git ls-files --others --exclude-standard -- '*.py' 2>/dev/null | head -1)
  [[ -z "${CHANGED}" && -z "${UNTRACKED}" ]] && exit 0
fi

OUTPUT=$(uv run --quiet pytest tests/unit -q --no-header -x 2>&1)
STATUS=$?

if [[ ${STATUS} -ne 0 ]]; then
  {
    echo "Unit tests are failing:"
    echo
    printf '%s\n' "${OUTPUT}" | tail -30
    echo
    echo "Fix these before finishing. Full gate: uv run pytest"
  } >&2
  exit 2
fi
exit 0
