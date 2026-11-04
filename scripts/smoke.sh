#!/usr/bin/env bash
# Smoke test against a running compose stack: create a key, make a chat call.
set -euo pipefail

GATEWAY=${GATEWAY:-http://127.0.0.1:8080}
ADMIN=${ADMIN:-http://127.0.0.1:8081}
ADMIN_TOKEN=${ADMIN_TOKEN:-dev-admin-token}

created=$(curl -fsS -X POST "$ADMIN/admin/keys" \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -d '{"name":"smoke","budget_usd":1}')
key=$(printf '%s' "$created" | sed -n 's/.*"key":"\([^"]*\)".*/\1/p')
if [[ -z "$key" ]]; then
  echo "could not create key: $created" >&2
  exit 1
fi
echo "created key ${key:0:9}..."

echo "--- GET /v1/models"
curl -fsS "$GATEWAY/v1/models" -H "Authorization: Bearer $key"
echo "--- POST /v1/chat/completions"
curl -fsS -D - "$GATEWAY/v1/chat/completions" \
  -H "Authorization: Bearer $key" \
  -H "Content-Type: application/json" \
  -d '{"model":"mock-small","messages":[{"role":"user","content":"hello gateway"}],"max_tokens":16}'
echo
echo "--- POST /v1/chat/completions (stream, Anthropic-format mock)"
curl -fsS -N "$GATEWAY/v1/chat/completions" \
  -H "Authorization: Bearer $key" \
  -H "Content-Type: application/json" \
  -d '{"model":"mock-claude","stream":true,"stream_options":{"include_usage":true},"messages":[{"role":"user","content":"hello gateway"}],"max_tokens":5}'
