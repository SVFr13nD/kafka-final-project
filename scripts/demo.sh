#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p evidence
exec > >(tee evidence/demo.log) 2>&1
docker compose run --rm moderator services.cli ban add p006
docker compose run --rm verify banned --product p006
docker compose run --rm shop
for event in 'alice view p001' 'alice cart p001' 'alice purchase p002' 'bob view p003' 'bob cart p003' 'bob purchase p004'; do
  read -r user kind product <<< "$event"
  docker compose run --rm client services.cli event "$kind" "$product" --user "$user"
done
docker compose run --rm client services.cli search 'часы' --user alice
docker compose run --rm verify data
docker compose run --rm analytics
docker compose run --rm verify recommendations
docker compose run --rm client services.cli recommend --user alice
docker compose run --rm client services.cli recommend --user bob
