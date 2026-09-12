#!/usr/bin/env bash
# PostToolUse(Write|Edit): format and auto-fix the single Python file just written.
#
# Scoped to one file so it stays fast (~50ms) and never reformats work in
# progress elsewhere. Exits 0 regardless: this is a convenience, not a gate.
set -uo pipefail

INPUT=$(cat)
FILE=$(printf '%s' "$INPUT" | python3 -c 'import json,sys
try:
    data = json.load(sys.stdin)
except Exception:
    sys.exit(0)
path = (data.get("tool_input") or {}).get("file_path", "")
print(path)' 2>/dev/null)

[[ -z "${FILE}" || "${FILE}" != *.py || ! -f "${FILE}" ]] && exit 0

# Only touch files inside this project.
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
case "${FILE}" in
  "${PROJECT_DIR}"/*) ;;
  *) exit 0 ;;
esac

cd "${PROJECT_DIR}" || exit 0
command -v uv >/dev/null 2>&1 || exit 0

uv run --quiet ruff format "${FILE}" >/dev/null 2>&1
uv run --quiet ruff check --fix --quiet "${FILE}" >/dev/null 2>&1

# Surface anything ruff could not fix, as context rather than as a failure.
REMAINING=$(uv run --quiet ruff check --quiet "${FILE}" 2>&1 | head -20)
if [[ -n "${REMAINING}" ]]; then
  printf 'ruff still reports issues in %s:\n%s\n' "${FILE}" "${REMAINING}"
fi
exit 0
