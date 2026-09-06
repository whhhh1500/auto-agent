#!/usr/bin/env bash
set -euo pipefail

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
mkdir -p "$tmp/repo"
for path in LICENSE CONTRIBUTING.md SECURITY.md CODE_OF_CONDUCT.md CHANGELOG.md SUPPORT.md RELEASE.md README.md go.mod go.sum Dockerfile docker-compose.yml .dockerignore cmd examples internal pkg scripts docs openapi .github; do
  cp -R "$path" "$tmp/repo/"
done
(cd "$tmp/repo" && go list ./... && go test -count=1 -timeout 600s ./... && go build ./...)
