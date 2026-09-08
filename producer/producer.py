"""
Credit Card Fraud Detection - Data Source Layer
-------------------------------------------------
Batch / One-pass Data Source

Reads the real CSV incrementally, one row at a time, and publishes
each transaction to Kafka topic `transactions_raw`.

The full 4.7GB CSV is NEVER loaded into RAM.

The producer processes the CSV exactly once and exits when
the end of the file is reached.

Environment variables:
  KAFKA_BOOTSTRAP   default kafka:9092
  RAW_TOPIC         default transactions_raw
  EVENTS_PER_SEC    default 500
  DATASET_PATH      default /data/raw/credit_card_fraud.csv
"""

from __future__ import annotations

import csv
import json
import os
import time
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

KAFKA_BOOTSTRAP = os.environ.get(
    "KAFKA_BOOTSTRAP",
    "kafka:9092",
)

RAW_TOPIC = os.environ.get(
    "RAW_TOPIC",
    "transactions_raw",
)

EVENTS_PER_SEC = float(
    os.environ.get(
        "EVENTS_PER_SEC",
        "500",
    )
)

DATASET_PATH = os.environ.get(
    "DATASET_PATH",
    "/data/raw/credit_card_fraud.csv",
)


# ---------------------------------------------------------------------------
# Real CSV schema - 34 columns
# ---------------------------------------------------------------------------

FIELDNAMES = [
    "transaction_amount",
    "transaction_amount_bin",
    "avg_amount_deviation_sigma",
    "merchant_category",
    "merchant_risk_score",
    "merchant_category_vs_history",
    "geo_velocity_kmh",
    "geo_velocity_bin",
    "geo_distance_km",
    "geo_distance_bin",
    "merchant_location",
    "country_consistency",
    "ip_address_type",
    "device_fingerprint_match",
    "session_duration_sec",
    "session_duration_bin",
    "network_carrier_type",
    "cards_on_device_30d",
    "cvv_match_status",
    "three_ds_auth_result",
    "tokenization_used",
    "failed_attempts_before_success",
    "transaction_velocity_1h",
    "time_of_transaction",
    "account_credential_change_recency",
    "card_present_cnp",
    "order_shipping_speed",
    "account_age_days",
    "chargeback_history_count",
    "customer_id",
    "transaction_id",
    "segment",
    "customer_type",
    "is_fraud",
]


# ---------------------------------------------------------------------------
# Kafka connection
# ---------------------------------------------------------------------------

def connect_kafka(
    retries: int = 30,
    delay: float = 5.0,
) -> KafkaProducer:

    for attempt in range(1, retries + 1):

        try:

            print(
                f"Connecting to Kafka at {KAFKA_BOOTSTRAP}..."
            )

            return KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,

                value_serializer=lambda value:
                    json.dumps(value).encode("utf-8"),

                key_serializer=lambda key:
                    key.encode("utf-8")
                    if key
                    else None,
            )

        except NoBrokersAvailable:

            print(
                f"Kafka not ready yet "
                f"(attempt {attempt}/{retries}), "
                f"retrying..."
            )

            time.sleep(delay)

    raise RuntimeError(
        "Could not connect to Kafka after retries"
    )


# ---------------------------------------------------------------------------
# Real CSV -> Kafka
# ---------------------------------------------------------------------------

def process_csv_once(
    producer: KafkaProducer,
):
    """
    Read the real CSV one row at a time, publish each row to Kafka,
    and stop when the end of the file is reached.

    The full 4.7GB CSV is NEVER loaded into RAM.
    """

    # -----------------------------------------------------------------------
    # Check dataset exists
    # -----------------------------------------------------------------------

    if not os.path.isfile(DATASET_PATH):

        raise FileNotFoundError(
            f"Dataset not found: {DATASET_PATH}"
        )

    # -----------------------------------------------------------------------
    # Validate CSV header
    # -----------------------------------------------------------------------

    with open(
        DATASET_PATH,
        newline="",
        encoding="utf-8",
    ) as f:

        reader = csv.DictReader(f)

        if reader.fieldnames is None:

            raise ValueError(
                "CSV file has no header."
            )

        missing_columns = [
            field
            for field in FIELDNAMES
            if field not in reader.fieldnames
        ]

        if missing_columns:

            raise ValueError(
                "CSV is missing expected columns: "
                + ", ".join(missing_columns)
            )

    # -----------------------------------------------------------------------
    # Calculate delay between messages
    # -----------------------------------------------------------------------

    sleep_between = (
        1.0 / EVENTS_PER_SEC
        if EVENTS_PER_SEC > 0
        else 0
    )

    sent = 0

    print(
        f"Processing and streaming from {DATASET_PATH} "
        f"to topic '{RAW_TOPIC}' "
        f"at ~{EVENTS_PER_SEC} events/sec "
        f"(will stop at EOF) ..."
    )

    # -----------------------------------------------------------------------
    # Process CSV exactly once
    # -----------------------------------------------------------------------

    with open(
        DATASET_PATH,
        newline="",
        encoding="utf-8",
    ) as f:

        reader = csv.DictReader(f)

        for row in reader:

            # Keep all original 34 columns
            event = {
                field: row.get(field, "")
                for field in FIELDNAMES
            }

            # Add ingestion timestamp
            event["event_timestamp"] = (
                datetime.now(timezone.utc).isoformat()
            )

            # ----------------------------------------------------------------
            # Validate transaction ID
            # ----------------------------------------------------------------

            transaction_id = event.get(
                "transaction_id"
            )

            if not transaction_id:

                raise ValueError(
                    "Row is missing transaction_id"
                )

            # ----------------------------------------------------------------
            # Send real transaction to Kafka
            # ----------------------------------------------------------------

            producer.send(
                RAW_TOPIC,
                key=transaction_id,
                value=event,
            )

            sent += 1

            # ----------------------------------------------------------------
            # Flush every 500 messages
            # ----------------------------------------------------------------

            if sent % 500 == 0:

                producer.flush()

                print(
                    f"Sent {sent} transactions "
                    f"to '{RAW_TOPIC}'"
                )

            # ----------------------------------------------------------------
            # Control streaming rate
            # ----------------------------------------------------------------

            if sleep_between > 0:

                time.sleep(sleep_between)

    # -----------------------------------------------------------------------
    # Finished
    # -----------------------------------------------------------------------

    print(
        f"Finished! Successfully sent all "
        f"{sent} transactions. Exiting."
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():

    producer = connect_kafka()

    try:

        process_csv_once(producer)

    finally:

        # Make sure all buffered messages are delivered
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()