#!/usr/bin/env bash
# Proposal mapping: "2. DATA INGESTION LAYER - Kafka Topics Used"
# Runs once via the `kafka-topics-init` service, then exits.
set -e

BOOTSTRAP="kafka:9092"
TOPICS=(
    "transactions_raw"
    "transactions_stream"
    "fraud_predictions"
    "alerts"
    "enriched_transactions"
)

echo "Waiting for Kafka at ${BOOTSTRAP}..."
until /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server "${BOOTSTRAP}" >/dev/null 2>&1; do
    sleep 3
done

for topic in "${TOPICS[@]}"; do
    echo "Creating topic: ${topic}"
    /opt/kafka/bin/kafka-topics.sh --create \
        --topic "${topic}" \
        --bootstrap-server "${BOOTSTRAP}" \
        --partitions 3 \
        --replication-factor 1 \
        || echo "  (already exists, skipping)"
done

echo "All fraud-detection Kafka topics are ready."
