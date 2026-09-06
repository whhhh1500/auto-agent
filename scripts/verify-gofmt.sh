#!/usr/bin/env bash
set -euo pipefail

failed=0
while IFS= read -r file; do
  diff="$(gofmt -d "$file")"
  if [[ -n "$diff" ]]; then
    printf '%s\n' "$diff"
    failed=1
  fi
done < <(find . -type f -name '*.go' -not -path './vendor/*' -print | sort)

if [[ "$failed" -ne 0 ]]; then
  echo 'gofmt check failed; run gofmt -w on the files above.' >&2
  exit 1
fi
