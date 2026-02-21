#!/usr/bin/env bash
if [[ -z "${BASH_VERSION:-}" ]]; then
  exec /usr/bin/env bash "$0" "$@"
fi
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
DOCS_DIR="${REPO_DIR}/docs"
DELETE_ALL=0

usage() {
  cat <<'EOF'
Usage:
  toool/s/cleanup-generated.sh [--all]

Default: delete non-gzipped counterparts and temp files only.
--all: also delete gzipped assets and shard directories.
EOF
}

for arg in "$@"; do
  case "${arg}" in
    --all) DELETE_ALL=1 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown arg: ${arg}"; usage; exit 1 ;;
  esac
done

log() { printf "%s\n" "$*"; }
warn() { log "⚠️  $*"; }
pass() { log "✅ $*"; }

confirm_rm() {
  local target="$1"
  if [[ ! -e "${target}" ]]; then
    warn "Missing: ${target}"
    return 0
  fi
  printf "\n➡️  Delete %s? [y/N]: " "${target}"
  local reply=""
  read -r reply || true
  if [[ "${reply}" != "y" && "${reply}" != "Y" ]]; then
    warn "Skipped: ${target}"
    return 0
  fi
  rm -rf "${target}"
  pass "Deleted: ${target}"
}

log "🧹 HN Backup Tape cleanup (generated assets)"
log "---------------------------------------"

confirm_rm "${DOCS_DIR}/static-manifest.json"
confirm_rm "${DOCS_DIR}/static-manifest.json.gz.tmp"

confirm_rm "${DOCS_DIR}/static-user-stats-manifest.json"
confirm_rm "${DOCS_DIR}/static-user-stats-manifest.json.gz.tmp"

confirm_rm "${DOCS_DIR}/archive-index.json"
confirm_rm "${DOCS_DIR}/archive-index.json.gz.tmp"

confirm_rm "${DOCS_DIR}/cross-shard-index.bin"
confirm_rm "${DOCS_DIR}/cross-shard-index.bin.gz.tmp"
confirm_rm "${DOCS_DIR}/cross-shard-index.sqlite"

if [[ "${DELETE_ALL}" -eq 1 ]]; then
  confirm_rm "${DOCS_DIR}/static-shards"
  confirm_rm "${DOCS_DIR}/static-user-stats-shards"
  confirm_rm "${DOCS_DIR}/static-manifest.json.gz"
  confirm_rm "${DOCS_DIR}/static-user-stats-manifest.json.gz"
  confirm_rm "${DOCS_DIR}/archive-index.json.gz"
  confirm_rm "${DOCS_DIR}/cross-shard-index.bin.gz"
  confirm_rm "${DOCS_DIR}/cross-shard-index.json.gz"
  confirm_rm "${DOCS_DIR}/cross-shard-index.sqlite.gz"
fi

log ""
pass "Cleanup complete"
