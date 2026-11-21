#!/usr/bin/env bash
# Bring up Kafka + Pinot, create the `fills` topic and the Pinot upsert table.
set -euo pipefail
cd "$(dirname "$0")/.."

PARTITIONS="${PARTITIONS:-3}"
CONTROLLER="${CONTROLLER:-http://localhost:9000}"

docker compose up -d

echo "waiting for kafka..."
until docker exec fillrec-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list >/dev/null 2>&1; do sleep 2; done
docker exec fillrec-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --if-not-exists --topic fills --partitions "$PARTITIONS" --replication-factor 1

echo "waiting for pinot controller..."
until curl -sf "$CONTROLLER/health" >/dev/null; do sleep 3; done
until curl -sf "http://localhost:8000/health" >/dev/null; do sleep 3; done

curl -sf -X POST -H 'Content-Type: application/json' "$CONTROLLER/schemas?override=true" -d @pinot/fills_schema.json; echo
curl -sf -X POST -H 'Content-Type: application/json' "$CONTROLLER/tables" -d @pinot/fills_table.json \
  || curl -sf -X PUT -H 'Content-Type: application/json' "$CONTROLLER/tables/fills" -d @pinot/fills_table.json
echo
echo "ready: Pinot UI at $CONTROLLER, broker at http://localhost:8000"
