#!/usr/bin/env bash
if [[ -z "${BASH_VERSION:-}" ]]; then
  exec /usr/bin/env bash "${0}" "$@"
fi
set -euo pipefail
shopt -s nullglob

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
DOCS_DIR="${REPO_DIR}/docs"
RAW_DIR_PRIMARY="${REPO_DIR}/data/raw"
RAW_DIR_ALT="${REPO_DIR}/toool/data/raw"
USE_STAGING=0

CUSTOM_DOMAIN="${CUSTOM_DOMAIN:-staticnews.dosaygo.com}"
EXPECTED_CNAME="${EXPECTED_CNAME:-static-news-dtg.pages.dev}"
PAGES_PROJECT_NAME="${PAGES_PROJECT_NAME:-static-news}"

for arg in "$@"; do
  case "${arg}" in
    --use-staging) USE_STAGING=1 ;;
    -h|--help)
      cat <<'EOF'
Usage: toool/s/predeploy-checks.sh [--use-staging]
  --use-staging  Run ETL from ./data/static-staging-hn.sqlite and skip raw download
EOF
      exit 0
      ;;
    *) echo "Unknown arg: ${arg}"; exit 1 ;;
  esac
done

log() { printf "%s\n" "$*"; }
pass() { log "✅ $*"; }
fail() { log "❌ $*"; exit 1; }
step() { printf "\n🔎 %s\n" "$*"; }
warn() { log "⚠️  $*"; }

pause() {
  printf "\n➡️  %s" "${1}"
  if [[ -n "${AUTO_RUN:-}" ]]; then
    printf " [auto]\n"
    return 0
  fi
  read -r
}

confirm_step() {
  local msg="${1}"; shift
  printf "\n➡️  %s [Enter=run, s=skip]: " "${msg}"
  local reply=""
  if [[ -n "${AUTO_RUN:-}" ]]; then
    printf " [auto]\n"
  else
    read -r reply || true
  fi
  if [[ "${reply:-}" == "s" || "${reply:-}" == "S" ]]; then
    warn "Skipped: ${msg}"
    return 0
  fi
  "$@"
}

in_repo() {
  (cd "${REPO_DIR}" && "$@")
}

require_cmd() {
  local cmd="${1}"
  command -v "${cmd}" >/dev/null 2>&1 || fail "Missing required command: ${cmd}"
}

require_file() {
  local path="${1}"
  [[ -f "${path}" ]] || fail "Missing file: ${path}"
}

gzip_test() {
  local path="${1}"
  gzip -t "${path}" >/dev/null 2>&1 || fail "gzip failed: ${path}"
}

gzip_replace() {
  local src="${1}"
  local dst="${2}"
  local tmp="${dst}.tmp"
  gzip -9 -c "${src}" > "${tmp}"
  if ! gzip -t "${tmp}" >/dev/null 2>&1; then
    rm -f "${tmp}"
    fail "gzip failed: ${dst}"
  fi
  mv "${tmp}" "${dst}"
  rm -f "${src}"
}

lock_file() {
  local path="${1}"
  chmod 444 "${path}" || fail "Failed to lock file: ${path}"
}

count_glob() {
  local pattern="${1}"
  local -a arr=()
  # shellcheck disable=SC2206
  arr=(${pattern})
  printf "%s" "${#arr[@]}"
}

validate_manifest_shards() {
  local manifest_path="${1}"
  local shards_dir="${2}"
  local label="${3}"

  local shard_files
  shard_files="$(MANIFEST_PATH="${manifest_path}" node - <<'NODE'
const fs = require('fs');
const zlib = require('zlib');
const p = process.env.MANIFEST_PATH || process.argv[1];
if (!p) throw new Error('Missing manifest path');
let raw = fs.readFileSync(p);
if (p.endsWith('.gz')) raw = zlib.gunzipSync(raw);
const m = JSON.parse(raw.toString('utf8'));
const shards = Array.isArray(m.shards) ? m.shards : [];
for (const s of shards) {
  if (s && s.file) process.stdout.write(String(s.file) + '\n');
}
NODE
)"

  local -a files=()
  while IFS= read -r line; do
    [[ -n "${line}" ]] && files+=("${line}")
  done <<<"${shard_files}"

  [[ "${#files[@]}" -gt 0 ]] || fail "${label} manifest has no shards: ${manifest_path}"

  step "Validating ${label} shards from manifest ($(printf "%s" "${#files[@]}") files)"
  local i=0
  for f in "${files[@]}"; do
    i=$((i+1))
    local full="${shards_dir}/${f}"
    printf "\r🧪 %s %d/%d" "${label}" "${i}" "${#files[@]}"
    [[ -f "${full}" ]] || fail "Missing ${label} shard file: ${full}"
    gzip_test "${full}"
  done
  printf "\r"
  pass "${label} shards OK (${#files[@]})"
}

check_cname() {
  local domain="${1}"
  local expected="${2}"
  local cname=""
  if command -v dig >/dev/null 2>&1; then
    cname="$(dig +short CNAME "${domain}" | head -n 1 | tr -d '\r' | sed 's/\.$//')"
  elif command -v nslookup >/dev/null 2>&1; then
    cname="$(nslookup -type=CNAME "${domain}" 2>/dev/null | awk '/canonical name/ {print ${NF}}' | head -n 1 | tr -d '\r' | sed 's/\.$//')"
  else
    warn "No dig/nslookup available; skipping CNAME check"
    return 0
  fi

  if [[ -z "${cname}" ]]; then
    fail "CNAME not found for ${domain}"
  fi
  if [[ "${cname}" != "${expected}" ]]; then
    fail "CNAME mismatch for ${domain}: expected ${expected}, got ${cname}"
  fi
  pass "CNAME OK: ${domain} → ${cname}"
}

log "🧭 Static News pre‑deploy checklist"
log "---------------------------------"

step "Checking prerequisites"
require_cmd bash
require_cmd gzip
require_cmd node
require_cmd rg
if ! command -v wrangler >/dev/null 2>&1; then
  warn "wrangler not found (deploy step can be skipped)"
fi
pass "Core commands available"

step "Checking raw data"
mkdir -p "${RAW_DIR_PRIMARY}" "${RAW_DIR_ALT}"
RAW_DIR="${RAW_DIR_PRIMARY}"

if [[ "${USE_STAGING}" -eq 1 ]]; then
  if [[ -f "${REPO_DIR}/data/static-staging-hn.sqlite" ]]; then
    pass "Using staging DB: ${REPO_DIR}/data/static-staging-hn.sqlite"
  else
    fail "Staging DB not found: ${REPO_DIR}/data/static-staging-hn.sqlite"
  fi
  warn "Skipping raw download; ETL will run with --from-staging"
  confirm_step "Run full ETL now? (etl-hn.js --gzip --from-staging)" in_repo node ./etl-hn.js --gzip --from-staging
else
  raw_primary_count="$(count_glob "${RAW_DIR_PRIMARY}/*.json.gz")"
  raw_alt_count="$(count_glob "${RAW_DIR_ALT}/*.json.gz")"

  if [[ "${raw_primary_count}" -gt 0 ]]; then
    pass "BigQuery extract present: ${raw_primary_count} raw files in data/raw"
  elif [[ "${raw_alt_count}" -gt 0 ]]; then
    RAW_DIR="${RAW_DIR_ALT}"
    pass "BigQuery extract present: ${raw_alt_count} raw files in toool/data/raw"
    warn "Using toool/data/raw; ETL will be run with --data \"${RAW_DIR}\""
  else
    warn "No raw data found in data/raw/*.json.gz (or toool/data/raw/*.json.gz)"
    confirm_step "Run download script now? (download_hn.sh)" bash -lc "cd \"${REPO_DIR}\" && bash ./download_hn.sh"
    raw_primary_count="$(count_glob "${RAW_DIR_PRIMARY}/*.json.gz")"
    if [[ "${raw_primary_count}" -gt 0 ]]; then
      pass "Downloaded ${raw_primary_count} raw files"
    else
      warn "No raw data available; ETL step may fail if you run it."
    fi
  fi

  confirm_step "Run full ETL now? (etl-hn.js --gzip)" in_repo node ./etl-hn.js --gzip --data "${RAW_DIR}"
fi

confirm_step "Rebuild archive index now? (build-archive-index.js)" in_repo node ./build-archive-index.js
confirm_step "Gzip archive-index.json (-9)" gzip_replace "${DOCS_DIR}/archive-index.json" "${DOCS_DIR}/archive-index.json.gz"

confirm_step "Rebuild cross-shard index now? (build-cross-shard-index.mjs --binary)" in_repo node ./toool/s/build-cross-shard-index.mjs --binary
confirm_step "Gzip cross-shard-index.bin (-9)" gzip_replace "${DOCS_DIR}/cross-shard-index.bin" "${DOCS_DIR}/cross-shard-index.bin.gz"

confirm_step "Rebuild user stats now? (build-user-stats.mjs --gzip --target-mb 15)" in_repo node ./toool/s/build-user-stats.mjs --gzip --target-mb 15
confirm_step "Gzip static-user-stats-manifest.json (-9)" gzip_replace "${DOCS_DIR}/static-user-stats-manifest.json" "${DOCS_DIR}/static-user-stats-manifest.json.gz"

confirm_step "Gzip static-manifest.json (-9)" gzip_replace "${DOCS_DIR}/static-manifest.json" "${DOCS_DIR}/static-manifest.json.gz"

step "Checking required files"
require_file "${DOCS_DIR}/index.html"
require_file "${DOCS_DIR}/static.html"
require_file "${DOCS_DIR}/static-manifest.json.gz"
require_file "${DOCS_DIR}/archive-index.json.gz"
require_file "${DOCS_DIR}/static-user-stats-manifest.json.gz"
require_file "${DOCS_DIR}/cross-shard-index.bin.gz"
pass "Core files present"

step "Checking content shards"
mkdir -p "${DOCS_DIR}/static-shards"
shard_count="$(count_glob "${DOCS_DIR}/static-shards/*.gz")"
[[ "${shard_count}" -gt 0 ]] || fail "No content shards found"
pass "Found ${shard_count} content shard files"

step "Checking user stats"
mkdir -p "${DOCS_DIR}/static-user-stats-shards"
user_shard_count="$(count_glob "${DOCS_DIR}/static-user-stats-shards/*.gz")"
[[ "${user_shard_count}" -gt 0 ]] || fail "No user stats shards found"
pass "Found ${user_shard_count} user stats shard files"

step "Validating gzip assets"
gzip_test "${DOCS_DIR}/static-manifest.json.gz"
gzip_test "${DOCS_DIR}/archive-index.json.gz"
gzip_test "${DOCS_DIR}/static-user-stats-manifest.json.gz"
gzip_test "${DOCS_DIR}/cross-shard-index.bin.gz"
pass "Core gzip assets OK"

if [[ -f "${DOCS_DIR}/static-manifest.json" ]]; then
  validate_manifest_shards "${DOCS_DIR}/static-manifest.json" "${DOCS_DIR}/static-shards" "content"
else
  warn "docs/static-manifest.json missing; validating via docs/static-manifest.json.gz"
  validate_manifest_shards "${DOCS_DIR}/static-manifest.json.gz" "${DOCS_DIR}/static-shards" "content"
fi
if [[ -f "${DOCS_DIR}/static-user-stats-manifest.json" ]]; then
  validate_manifest_shards "${DOCS_DIR}/static-user-stats-manifest.json" "${DOCS_DIR}/static-user-stats-shards" "user-stats"
else
  warn "docs/static-user-stats-manifest.json missing; validating via docs/static-user-stats-manifest.json.gz"
  validate_manifest_shards "${DOCS_DIR}/static-user-stats-manifest.json.gz" "${DOCS_DIR}/static-user-stats-shards" "user-stats"
fi

step "Quick sanity checks"
if ! rg -q "cross-shard-index.bin.gz" "${DOCS_DIR}/index.html"; then
  fail "index.html missing cross-shard-index.bin.gz reference"
fi
if ! rg -q "static-manifest.json.gz" "${DOCS_DIR}/index.html"; then
  fail "index.html missing static-manifest.json.gz reference"
fi
if ! rg -q "static-user-stats-manifest.json.gz" "${DOCS_DIR}/index.html"; then
  fail "index.html missing static-user-stats-manifest.json.gz reference"
fi
pass "HTML references OK"

step "Validating JSON manifests"
node - "${DOCS_DIR}/archive-index.json.gz" "${DOCS_DIR}/static-user-stats-manifest.json.gz" <<'NODE'
const fs = require('fs');
const zlib = require('zlib');
for (const p of process.argv.slice(1)) {
  let raw = fs.readFileSync(p);
  if (p.endsWith('.gz')) raw = zlib.gunzipSync(raw);
  const j = JSON.parse(raw.toString('utf8'));
  if (!j || typeof j !== 'object') throw new Error(`Invalid JSON object: ${p}`);
}
NODE
pass "Manifests parse as JSON"

node - "${DOCS_DIR}/static-user-stats-manifest.json.gz" <<'NODE'
const fs = require('fs');
const zlib = require('zlib');
let raw = fs.readFileSync(process.argv[1]);
if (process.argv[1].endsWith('.gz')) raw = zlib.gunzipSync(raw);
const m = JSON.parse(raw.toString('utf8'));
if (!Array.isArray(m.shards) || m.shards.length === 0) throw new Error('user stats manifest missing shards');
if (!Array.isArray(m.user_growth) || m.user_growth.length === 0) throw new Error('user stats manifest missing user_growth');
if (!Array.isArray(m.user_active) || m.user_active.length === 0) throw new Error('user stats manifest missing user_active');
NODE
pass "User stats manifest includes growth + MAU"

step "Manifest gzip summary"
for m in "${DOCS_DIR}/static-manifest.json" "${DOCS_DIR}/archive-index.json" "${DOCS_DIR}/static-user-stats-manifest.json"; do
  if [[ -f "${m}.gz" ]]; then
    pass "Gz present: $(basename "${m}.gz")"
  else
    warn "Missing gz: $(basename "${m}.gz")"
  fi
done

step "Lock core gzip assets (chmod 444)"
for f in "${DOCS_DIR}/static-manifest.json.gz" \
  "${DOCS_DIR}/archive-index.json.gz" \
  "${DOCS_DIR}/static-user-stats-manifest.json.gz" \
  "${DOCS_DIR}/cross-shard-index.bin.gz"; do
  if [[ -f "${f}" ]]; then
    lock_file "${f}"
    pass "Locked: $(basename "${f}")"
  else
    warn "Missing: $(basename "${f}")"
  fi
done

pause "Smoke UI locally (/ , ?view=query, ?view=user&id=pg, ?view=archive). Press Enter to continue..."

step "Checking custom domain CNAME"
confirm_step "Check DNS now? (${CUSTOM_DOMAIN} → ${EXPECTED_CNAME})" check_cname "${CUSTOM_DOMAIN}" "${EXPECTED_CNAME}"

if command -v wrangler >/dev/null 2>&1; then
  confirm_step "Deploy now? (wrangler pages deploy)" in_repo wrangler pages deploy docs --project-name "${PAGES_PROJECT_NAME}" --commit-dirty=true
else
  warn "wrangler not found; skipping deploy step"
fi

log ""
pass "Pre‑deploy checklist complete 🎉"
