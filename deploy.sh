#!/usr/bin/env bash
# Deploy the AI visibility tracker from Dropbox to Kestra on the Beelink.
#
#   ./deploy.sh              deploy every flow, then seed the prompt sets
#   ./deploy.sh flow         deploy every *.yml flow in this folder
#   ./deploy.sh dashboard    create the custom dashboards
#   ./deploy.sh run          trigger a tracker run (Claude only)
#   ./deploy.sh check        verify auth and list what is deployed
#
# Prompt sets are seeded by running the bootstrap_prompts flow rather than
# calling the KV API directly: the API token is denied KV, but a flow runs
# server-side with full namespace access.
#
# Auth lives in a local kestra.auth file next to this script:
#
#   KESTRA_TOKEN=your_api_token
#   TENANT=main
#
# or, for OSS basic auth:
#
#   KESTRA_USER=you@example.com
#   KESTRA_PASS=yourpassword
#   TENANT=main
#
# The LLM API keys are NOT here. On EE they live in the namespace secret
# store; on OSS they come from SECRET_* env vars on the server.

set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Host-specific settings live in kestra.auth, which is gitignored, so this
# script stays free of anyone's IP addresses and usernames. Defaults below are
# only fallbacks; see kestra.auth.example.
HOST="${HOST:-user@kestra-host}"
REMOTE_DIR="${REMOTE_DIR:-~/kestra}"
KESTRA="${KESTRA:-http://kestra-host:8080}"
NS="${NS:-kaiwaehner.blog}"

TENANT="main"
KESTRA_USER=""
KESTRA_PASS=""
KESTRA_TOKEN=""

if [ -f "$HERE/kestra.auth" ]; then
  # shellcheck disable=SC1091
  . "$HERE/kestra.auth"
else
  echo "Missing $HERE/kestra.auth" >&2
  echo "Copy kestra.auth.example to kestra.auth and fill it in." >&2
  exit 1
fi

if [ "$HOST" = "user@kestra-host" ] || [ "$KESTRA" = "http://kestra-host:8080" ]; then
  echo "HOST and KESTRA are still placeholders. Set them in kestra.auth." >&2
  exit 1
fi

AUTH=()
if [ -n "$KESTRA_TOKEN" ]; then
  AUTH=(-H "Authorization: Bearer $KESTRA_TOKEN")
elif [ -n "$KESTRA_USER" ]; then
  AUTH=(-u "$KESTRA_USER:$KESTRA_PASS")
else
  echo "Set either KESTRA_TOKEN or KESTRA_USER/KESTRA_PASS in kestra.auth" >&2
  exit 1
fi

API="$KESTRA/api/v1/$TENANT"

say()  { printf '\n==> %s\n' "$1"; }
warn() { printf '    %s\n' "$1" >&2; }

# api METHOD URL [curl args...]
# Prints the response body. Exits with a readable message on any non-2xx.
# This is the fix for the original bug: `curl -sf ... >/dev/null` hid a 403
# behind a silent exit, so a failed KV write looked exactly like success.
api() {
  local method="$1" url="$2"; shift 2
  local tmp code
  tmp="$(mktemp)"
  code="$(curl -s -o "$tmp" -w '%{http_code}' --max-time 60 \
            -X "$method" "$url" "${AUTH[@]}" "$@" || echo 000)"

  if [ "$code" -ge 200 ] && [ "$code" -lt 300 ]; then
    cat "$tmp"; rm -f "$tmp"; return 0
  fi

  local body; body="$(head -c 500 "$tmp")"; rm -f "$tmp"
  echo >&2
  case "$code" in
    000) warn "No response from $KESTRA (is it running?)" ;;
    401) warn "HTTP 401 - token rejected. Check KESTRA_TOKEN in kestra.auth." ;;
    403) warn "HTTP 403 - authenticated but not permitted for:"
         warn "  $method $url"
         warn "The API token needs permission for this resource."
         warn "In the UI: your user menu, API Tokens, or check the role"
         warn "attached to your account for KV / FLOW / EXECUTION access." ;;
    404) warn "HTTP 404 - not found. Wrong tenant ('$TENANT') or namespace?" ;;
    409) warn "HTTP 409 - already exists." ;;
    *)   warn "HTTP $code from $method $url" ;;
  esac
  [ -n "$body" ] && warn "server said: $body"
  exit 1
}

check_auth() {
  api GET "$API/flows/search" >/dev/null
}

# Deploy one flow file. PUT updates, POST creates; try update first.
# A Kestra flow always has a top-level `id:` and a top-level `tasks:` key,
# both unindented. That is enough to identify one without parsing YAML, which
# keeps this script free of a PyYAML dependency.
is_flow() {
  grep -qE '^id:[[:space:]]*[A-Za-z_]' "$1" && grep -qE '^tasks:[[:space:]]*$' "$1"
}

flow_id() {
  sed -nE 's/^id:[[:space:]]*([A-Za-z0-9_.-]+).*/\1/p' "$1" | head -1
}

deploy_one_flow() {
  local file="$1" id
  id="$(flow_id "$file")"
  if [ -z "$id" ]; then
    warn "could not read flow id from $(basename "$file")"
    return 1
  fi

  local tmp code
  tmp="$(mktemp)"
  code="$(curl -s -o "$tmp" -w '%{http_code}' \
            -X PUT "$API/flows/$NS/$id" "${AUTH[@]}" \
            -H "Content-Type: application/x-yaml" \
            --data-binary "@$file")"
  rm -f "$tmp"

  if [ "$code" -ge 200 ] && [ "$code" -lt 300 ]; then
    echo "    $id: updated"
  else
    api POST "$API/flows" \
      -H "Content-Type: application/x-yaml" \
      --data-binary "@$file" >/dev/null
    echo "    $id: created"
  fi
}

push_flow() {
  shopt -s nullglob
  local found=0
  say "Deploying flows"
  for f in "$HERE"/*.yml "$HERE"/*.yaml; do
    [ -f "$f" ] || continue
    if is_dashboard "$f"; then
      echo "    skipped $(basename "$f") (dashboard - use ./deploy.sh dashboard)"
      continue
    fi
    if ! is_flow "$f"; then
      echo "    skipped $(basename "$f") (not a Kestra flow)"
      continue
    fi
    found=1
    scp -q "$f" "$HOST:$REMOTE_DIR/"
    deploy_one_flow "$f"
  done
  [ "$found" -eq 1 ] || warn "No flow YAML found in $HERE"
}

# Dashboards use a separate API from flows. A dashboard file has `title:` and
# `charts:` at the top level, never `tasks:`.
is_dashboard() {
  grep -qE '^title:[[:space:]]*\S' "$1" && grep -qE '^charts:[[:space:]]*$' "$1"
}

push_dashboards() {
  shopt -s nullglob
  local found=0
  say "Deploying dashboards"
  for f in "$HERE"/*.yml "$HERE"/*.yaml; do
    [ -f "$f" ] || continue
    is_dashboard "$f" || continue
    found=1
    local name tmp code body
    name="$(basename "$f")"
    # Deliberately not using api() here: it exits the script on failure, which
    # is right for a deploy step but wrong when we want to report and continue.
    tmp="$(mktemp)"
    code="$(curl -s -o "$tmp" -w '%{http_code}' --max-time 30 \
              -X POST "$API/dashboards" "${AUTH[@]}" \
              -H "Content-Type: application/x-yaml" \
              --data-binary "@$f" || echo 000)"
    body="$(head -c 300 "$tmp")"; rm -f "$tmp"

    if [ "$code" -ge 200 ] && [ "$code" -lt 300 ]; then
      echo "    $name: created"
    else
      warn "$name: HTTP $code"
      [ -n "$body" ] && warn "  $body"
      warn "  If it already exists, edit it in the UI under Dashboards."
    fi
  done
  [ "$found" -eq 1 ] || echo "    no dashboard files found"
}

seed_prompts() {
  say "Running bootstrap_prompts to seed the KV store"
  local out id
  out="$(api POST "$API/executions/$NS/bootstrap_prompts")"
  id="$(printf '%s' "$out" | python3 -c "import json,sys;print(json.load(sys.stdin)['id'])" 2>/dev/null || echo "")"
  echo "    execution ${id:-started}"
  echo "    $KESTRA/ui/$TENANT/executions"
}




trigger_run() {
  say "Triggering a Claude-only run"
  local out id
  out="$(api POST "$API/executions/$NS/blog_monitor" \
          -F "article=oceanbase" -F 'engines=["anthropic"]')"
  id="$(printf '%s' "$out" | python3 -c "import json,sys;print(json.load(sys.stdin)['id'])" 2>/dev/null || echo "")"
  if [ -n "$id" ]; then
    echo "    execution $id"
    echo "    $KESTRA/ui/$TENANT/executions/$NS/blog_monitor/$id"
  else
    echo "    $KESTRA/ui/$TENANT/executions"
  fi
}

do_check() {
  say "Checking $KESTRA as tenant '$TENANT'"
  api GET "$API/flows/search" >/dev/null
  echo "    auth: OK"
  api GET "$API/flows/$NS/blog_monitor" >/dev/null 2>&1 \
    && echo "    flow: deployed" || echo "    flow: NOT deployed (run ./deploy.sh flow)"
  local keys
  keys="$(api GET "$API/namespaces/$NS/kv" || echo "")"
  echo "    kv keys: $(printf '%s' "$keys" | python3 -c "
import json,sys
try:
    d=json.load(sys.stdin)
    print(', '.join(x.get('key',str(x)) if isinstance(x,dict) else str(x) for x in d) or 'none')
except Exception:
    print('none')" 2>/dev/null)"
}

case "${1:-all}" in
  flow)    check_auth; push_flow ;;
  dashboard) check_auth; push_dashboards ;;
  run)     check_auth; trigger_run ;;
  check)   do_check ;;
  all)     check_auth; push_flow; push_dashboards ;;
  *)       echo "usage: $0 [all|flow|dashboard|run|check]" >&2; exit 1 ;;
esac

say "Done. UI: $KESTRA/ui/$TENANT"
