#!/usr/bin/env bash
set -euo pipefail

image="${1:-harness-core:smoke}"
if ! command -v docker >/dev/null 2>&1; then
  echo 'docker is required for container smoke' >&2
  exit 2
fi
if ! docker image inspect "$image" >/dev/null 2>&1; then
  docker build --tag "$image" .
fi

container="harness-core-smoke-$$"
cleanup() { docker rm --force "$container" >/dev/null 2>&1 || true; }
trap cleanup EXIT
docker run --detach --name "$container" --publish 18080:8080 \
  --env HARNESS_MODE=dev \
  --env HARNESS_DATABASE_TYPE=sqlite \
  --env HARNESS_SQLITE_PATH=/data/core.db \
  --env HARNESS_DEV_HEADER_AUTH=true \
  "$image" >/dev/null

for _ in $(seq 1 60); do
  if curl --silent --fail http://127.0.0.1:18080/healthz >/tmp/harness-health.json; then
    break
  fi
  sleep 1
done
curl --silent --fail http://127.0.0.1:18080/healthz | grep -q '"status":"ok"'
curl --silent --fail http://127.0.0.1:18080/readyz | grep -q '"status":"ready"'

# Publish a provider-neutral mock profile, create a real session, then run a
# no-tool turn. This exercises the actual HTTP API and receives real SSE frames
# without an LLM key or network provider call.
headers=(--header 'X-Harness-Tenant: smoke' --header 'X-Harness-Subject: smoke' --header 'Content-Type: application/json')
scope='[{"kind":"global","id":"global"},{"kind":"deployment","id":"default"},{"kind":"product","id":"default"},{"kind":"tenant","id":"smoke"},{"kind":"user","id":"smoke"}]'
publish_body="{\"scope\":$scope,\"layer\":{\"profile_id\":\"container.smoke\",\"name\":\"Container smoke\",\"model\":{\"provider\":\"mock\",\"model\":\"mock\"}}}"
curl --silent --show-error --fail --request POST "${headers[@]}" --data "$publish_body" \
  http://127.0.0.1:18080/v1/profiles/container.smoke/publish >/tmp/harness-publish.json
session_json="$(curl --silent --show-error --fail --request POST "${headers[@]}" --data '{"profile_id":"container.smoke"}' \
  http://127.0.0.1:18080/v1/sessions)"
session_id="$(python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])' <<<"$session_json")"
test -n "$session_id"
curl --silent --show-error --fail --max-time 15 --no-buffer --dump-header /tmp/harness-sse.headers \
  --request POST "${headers[@]}" --header 'Accept: text/event-stream' \
  --data '{"message":"container smoke"}' \
  "http://127.0.0.1:18080/v1/sessions/$session_id/runs" >/tmp/harness-sse.log
grep -Eiq '^content-type: *text/event-stream' /tmp/harness-sse.headers
grep -Eq '^event: ' /tmp/harness-sse.log
grep -Eq '^data: ' /tmp/harness-sse.log
echo 'container HTTP/readiness/real-session/SSE smoke passed'
