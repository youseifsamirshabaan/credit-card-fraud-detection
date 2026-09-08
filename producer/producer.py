"""
Credit Card Fraud Detection - Data Source Layer
-------------------------------------------------
Plays the role of "1. DATA SOURCES" from the proposal:

  A. Historical Data  -> writes a batch of transactions to HDFS /raw
                         (stand-in for the 15M-row / 4.67GB CreditTransact
                         dataset from Mendeley Data - see README for how to
                         plug in the real file instead).
  B. Real-time Data    -> continuously streams transaction events to the
                         Kafka topic `transactions_raw` at a configurable
                         rate (EVENTS_PER_SEC), exactly like the "Python
                         Kafka Producer" described in the proposal.

Runs forever as its own container (see docker-compose.yml: `producer`).

Env vars (see .env):
  KAFKA_BOOTSTRAP        default kafka:9092
  RAW_TOPIC              default transactions_raw
  EVENTS_PER_SEC         default 5   (proposal range: 100-1000, kept low
                                      here so a laptop can keep up - bump
                                      it up for a demo)
  HISTORICAL_ROWS        default 20000 (rows written once to HDFS /raw)
  NAMENODE_WEBHDFS       default http://namenode:9870
  DATASET_PATH           optional path to a real CreditTransact-style CSV
                         mounted under /data - if present, real rows are
                         streamed instead of synthetic ones.
"""
from __future__ import annotations

import csv
import io
import json
import os
import random
import time
import uuid
from datetime import datetime, timedelta, timezone

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from hdfs import InsecureClient

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
RAW_TOPIC = os.environ.get("RAW_TOPIC", "transactions_raw")
EVENTS_PER_SEC = float(os.environ.get("EVENTS_PER_SEC", "5"))
HISTORICAL_ROWS = int(os.environ.get("HISTORICAL_ROWS", "20000"))
NAMENODE_WEBHDFS = os.environ.get("NAMENODE_WEBHDFS", "http://namenode:9870")
DATASET_PATH = os.environ.get("DATASET_PATH", "/data/creditcard_transactions.csv")
HDFS_RAW_DIR = "/raw"
HDFS_HISTORICAL_FILE = f"{HDFS_RAW_DIR}/creditcard_transactions_historical.csv"
HDFS_MARKER_FILE = f"{HDFS_RAW_DIR}/_historical_load_complete"

MERCHANT_CATEGORIES = [
    "grocery", "electronics", "travel", "restaurant", "fashion",
    "gaming", "utilities", "pharmacy", "fuel", "online_marketplace",
]
CARD_TYPES = ["VISA", "MASTERCARD", "AMEX", "MEZA"]
# Weighted so most traffic looks "local" - a handful of foreign countries
# make the fraud-by-country dashboard panel interesting.
COUNTRIES = (["EG"] * 70) + (["SA", "AE", "US", "GB", "DE", "FR", "NG", "RU"] * 5)

FIELDNAMES = [
    "transaction_id", "timestamp", "amount", "merchant_id",
    "merchant_category", "card_type", "country", "city",
    "device_id", "ip_address", "fraud_label",
]


def make_transaction(ts: datetime | None = None) -> dict:
    ts = ts or datetime.now(timezone.utc)
    country = random.choice(COUNTRIES)
    hour = ts.hour
    amount = round(random.lognormvariate(3.2, 1.1), 2)  # skewed, mostly small amounts
    is_odd_hour = hour in (1, 2, 3, 4)
    is_foreign = country != "EG"
    is_high_amount = amount > 800

    # base fraud rate ~4.9% to match the proposal's dashboard, boosted by
    # the classic risk signals so the model has real signal to learn from.
    fraud_score = 0.02
    if is_foreign:
        fraud_score += 0.10
    if is_odd_hour:
        fraud_score += 0.08
    if is_high_amount:
        fraud_score += 0.12
    fraud_label = 1 if random.random() < fraud_score else 0

    return {
        "transaction_id": str(uuid.uuid4()),
        "timestamp": ts.isoformat(),
        "amount": amount,
        "merchant_id": f"M{random.randint(1000, 9999)}",
        "merchant_category": random.choice(MERCHANT_CATEGORIES),
        "card_type": random.choice(CARD_TYPES),
        "country": country,
        "city": "Cairo" if country == "EG" else "Foreign",
        "device_id": f"D{random.randint(100000, 999999)}",
        "ip_address": f"{random.randint(1,223)}.{random.randint(0,255)}."
                       f"{random.randint(0,255)}.{random.randint(0,255)}",
        "fraud_label": fraud_label,
    }


def load_historical_batch_to_hdfs():
    """Loads a batch of 'historical' transactions into HDFS /raw once.

    If a real CreditTransact-style CSV is mounted at DATASET_PATH, its rows
    are copied (up to HISTORICAL_ROWS) instead of synthetic ones - this is
    how you swap the lab-sized synthetic data for the real Mendeley dataset.
    """
    client = InsecureClient(NAMENODE_WEBHDFS, user="root")
    client.makedirs(HDFS_RAW_DIR)
    client.makedirs("/stream")
    client.makedirs("/processed")
    client.makedirs("/features")
    client.makedirs("/models")
    client.makedirs("/predictions")

    if client.status(HDFS_MARKER_FILE, strict=False):
        print("Historical data already loaded into HDFS /raw, skipping.")
        return

    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=FIELDNAMES)
    writer.writeheader()

    if os.path.exists(DATASET_PATH):
        print(f"Found real dataset at {DATASET_PATH}, loading up to "
              f"{HISTORICAL_ROWS} rows into HDFS /raw ...")
        with open(DATASET_PATH, newline="") as f:
            reader = csv.DictReader(f)
            for i, row in enumerate(reader):
                if i >= HISTORICAL_ROWS:
                    break
                writer.writerow({k: row.get(k, "") for k in FIELDNAMES})
    else:
        print(f"No real dataset mounted at {DATASET_PATH} - generating "
              f"{HISTORICAL_ROWS} synthetic historical rows instead.")
        start = datetime.now(timezone.utc) - timedelta(days=30)
        for i in range(HISTORICAL_ROWS):
            ts = start + timedelta(seconds=i * 3)
            writer.writerow(make_transaction(ts))

    with client.write(HDFS_HISTORICAL_FILE, overwrite=True, encoding="utf-8") as out:
        out.write(buf.getvalue())
    with client.write(HDFS_MARKER_FILE, overwrite=True, encoding="utf-8") as out:
        out.write(f"loaded_at={datetime.now(timezone.utc).isoformat()}\n")

    print(f"Historical batch written to hdfs://{HDFS_HISTORICAL_FILE}")


def connect_kafka(retries: int = 30, delay: float = 5.0) -> KafkaProducer:
    for attempt in range(1, retries + 1):
        try:
            return KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
            )
        except NoBrokersAvailable:
            print(f"Kafka not ready yet (attempt {attempt}/{retries}), retrying...")
            time.sleep(delay)
    raise RuntimeError("Could not connect to Kafka after retries")


def stream_forever(producer: KafkaProducer):
    real_rows = None
    if os.path.exists(DATASET_PATH):
        with open(DATASET_PATH, newline="") as f:
            real_rows = list(csv.DictReader(f))
        print(f"Streaming will replay {len(real_rows)} rows from {DATASET_PATH}")

    sleep_between = 1.0 / EVENTS_PER_SEC if EVENTS_PER_SEC > 0 else 1.0
    sent = 0
    idx = 0
    print(f"Streaming to topic '{RAW_TOPIC}' at ~{EVENTS_PER_SEC} events/sec ...")
    while True:
        if real_rows:
            row = dict(real_rows[idx % len(real_rows)])
            row["timestamp"] = datetime.now(timezone.utc).isoformat()
            event = {k: row.get(k, "") for k in FIELDNAMES}
            idx += 1
        else:
            event = make_transaction()

        producer.send(RAW_TOPIC, key=event["transaction_id"], value=event)
        sent += 1
        if sent % 100 == 0:
            producer.flush()
            print(f"Sent {sent} transactions to '{RAW_TOPIC}'")
        time.sleep(sleep_between)


def main():
    try:
        load_historical_batch_to_hdfs()
    except Exception as exc:  # HDFS may still be starting up - don't crash the producer
        print(f"Warning: could not write historical batch to HDFS yet ({exc}). "
              f"Will retry on next restart; continuing with live streaming.")

    producer = connect_kafka()
    stream_forever(producer)


if __name__ == "__main__":
    main()
