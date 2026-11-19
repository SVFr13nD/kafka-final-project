#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p evidence
exec > >(tee evidence/verification.log) 2>&1
docker compose run --rm verify topology
docker compose run --rm shop services.acl_check
docker compose run --rm verify metrics
# Late ban retracts a product already in the catalog; unban restores it.
docker compose run --rm moderator services.cli ban add p001
docker compose run --rm verify absent --product p001
docker compose run --rm client services.cli recommend --user alice
docker compose run --rm moderator services.cli ban remove p001
docker compose run --rm verify present --product p001
# Always restart the broker, including an interrupted or failed check.
trap 'docker compose start primary-3 >/dev/null' EXIT
docker compose stop primary-3
docker compose run --rm verify alerts --status firing
docker compose run --rm shop
docker compose start primary-3
docker compose run --rm verify metrics
docker compose run --rm verify alerts --status resolved
trap - EXIT
for cluster in primary secondary; do
  docker compose exec -T "$cluster-1" /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$cluster-1:9092" --command-config /certs/client.properties --describe > "evidence/$cluster-topics.txt"
done
docker compose logs --no-color filter mirror connect lake outbox catalog rec-reader > evidence/services.log
docker compose ps > evidence/services.txt
printf '\nALL INTEGRATION CHECKS PASSED\n'
