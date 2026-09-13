#!/usr/bin/env bash
# PostToolUse(Write|Edit): gofmt the single Go file just written.
#
# Scoped to one file so it stays fast and never reformats work in progress
# elsewhere. Exits 0 regardless: this is a convenience, not a gate.
set -uo pipefail

INPUT=$(cat)
FILE=$(printf '%s' "$INPUT" | python3 -c 'import json,sys
try:
    data = json.load(sys.stdin)
except Exception:
    sys.exit(0)
print((data.get("tool_input") or {}).get("file_path", ""))' 2>/dev/null)

[[ -z "${FILE}" || "${FILE}" != *.go || ! -f "${FILE}" ]] && exit 0

# Only touch files inside this module.
MODULE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
case "${FILE}" in
  "${MODULE_DIR}"/*) ;;
  *) exit 0 ;;
esac

command -v gofmt >/dev/null 2>&1 || exit 0

gofmt -w "${FILE}" >/dev/null 2>&1

# Surface a compile error in the file's package as context rather than as a
# failure -- gofmt succeeds on code that does not build, and knowing that now is
# better than discovering it at the Stop hook.
cd "${MODULE_DIR}" || exit 0
command -v go >/dev/null 2>&1 || exit 0

# Strip the module prefix to get a package path. Plain string manipulation
# rather than `realpath --relative-to`, which is GNU-only and absent on macOS.
PKG_DIR=$(dirname "${FILE}")
REL="./${PKG_DIR#"${MODULE_DIR}"/}"
[[ "${REL}" == "./${MODULE_DIR}" ]] && REL="."

# `go vet` rather than `go build`: it compiles the test files too, so a broken
# _test.go is reported here instead of at the Stop hook.
VET_OUTPUT=$(go vet "${REL}" 2>&1 | head -10)
if [[ -n "${VET_OUTPUT}" ]]; then
  printf 'go vet reports problems in %s:\n%s\n' "${REL}" "${VET_OUTPUT}"
fi
exit 0
