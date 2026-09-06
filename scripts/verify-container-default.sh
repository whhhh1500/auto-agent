#!/usr/bin/env bash
set -euo pipefail

grep -Eq '^ENV HARNESS_MODE=production[[:space:]]*$' Dockerfile
grep -Eq -- '--env HARNESS_MODE=dev' scripts/container-smoke.sh
grep -Eq '^      HARNESS_MODE: dev[[:space:]]*$' docker-compose.yml
echo 'container default is production; development mode is explicit in smoke configuration'
