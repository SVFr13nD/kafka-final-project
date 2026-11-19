#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
command -v docker >/dev/null || { echo 'Install Docker with Compose v2 first'; exit 1; }
docker compose version
mkdir -p .runtime evidence
# Existing Kafka/HDFS volumes and certificates are preserved.
docker compose --profile tools run --build --rm tools
docker compose --profile tools build
docker compose up -d primary-1 primary-2 primary-3 secondary-1 secondary-2 secondary-3 postgres namenode datanode alert-receiver prometheus alertmanager grafana
docker compose run --rm bootstrap
docker compose up -d filter mirror connect catalog rec-reader outbox lake
docker compose run --rm verify ready
printf '\nReady. Run: bash scripts/demo.sh\nGrafana: http://localhost:3000 (admin / marketplace-demo)\n'
