# Mini Big Data Lab — HDFS · Spark · Kafka · Kafka UI · Airflow · Postgres · Superset

A single-node teaching stack sized for a 16GB laptop. Everything runs in one
Docker network (`bigdata-net`) so students can point tools at each other by
hostname (`namenode`, `spark-master`, `kafka`, etc).

On top of the generic teaching labs below, this stack also runs the full
**Credit Card Fraud Detection** pipeline end-to-end (see
`fraud-detection-system.md`/the architecture image if you have it) —
producer → Kafka → Spark Structured Streaming + MLlib → HDFS/Postgres →
Superset, orchestrated by Airflow. Jump to
["Fraud detection pipeline"](#fraud-detection-pipeline-full-integration)
below for that part specifically.

> **Image note:** Spark uses `bde2020/spark-master` / `bde2020/spark-worker`
> (Spark 3.3.0) and Kafka uses the official `apache/kafka` image (KRaft mode).
> Bitnami's free `bitnami/spark` and `bitnami/kafka` tags were discontinued in
> August 2025 (moved behind a paid "Bitnami Secure Images" subscription), so
> this stack avoids them entirely. The Jupyter and Airflow images install
> `pyspark==3.3.0` to match the cluster's Spark version exactly — if you ever
> bump `SPARK_IMAGE_TAG` in `.env`, update `PYSPARK_VERSION` and
> `SPARK_KAFKA_PACKAGE` to match, or Spark clients will fail to connect.
> The always-on streaming job uses `bde2020/spark-submit` (same family,
> just a submit-and-run entrypoint). Superset uses the official
> `apache/superset` image with a thin custom Dockerfile that adds the
> `psycopg2-binary` driver so it can query `postgres-dw`.

## Architecture

```
                        ┌─────────────────────┐
                        │      Airflow         │
                        │ (webserver+scheduler) │  :8082
                        └──────────┬───────────┘
                                   │ orchestrates
        ┌──────────────────────────┼──────────────────────────┐
        │                          │                          │
   ┌────▼────┐              ┌──────▼──────┐            ┌──────▼──────┐
   │  HDFS   │◄────────────►│    Spark    │◄──────────►│    Kafka    │
   │ NN+DN   │   read/write │ master+wkr  │  streaming │  (KRaft)    │
   │ :9870   │              │   :8080     │            │   :9092     │
   └─────────┘              └──────┬──────┘            └──────┬──────┘
                                   │                          │
                             ┌─────▼─────┐              ┌─────▼─────┐
                             │  Jupyter  │              │ Kafka UI  │
                             │   :8888   │              │   :8090   │
                             └───────────┘              └───────────┘
```

## Quick start

```bash
cd bigdata-lab
docker compose up -d --build
```

First boot takes a few minutes (image pulls + Airflow DB migration). Check status:

```bash
docker compose ps
```

## Service URLs (default ports, edit in `.env` if they collide)

| Service           | URL                          | Notes                          |
|-------------------|------------------------------|---------------------------------|
| HDFS Namenode UI  | http://localhost:9870        | browse filesystem, live nodes  |
| HDFS Datanode UI  | http://localhost:9864        |                                  |
| Spark Master UI   | http://localhost:8080        | see running/completed jobs     |
| Spark Worker UI   | http://localhost:8081        |                                  |
| Jupyter Lab       | http://localhost:8888        | token: `lab`                   |
| Kafka UI          | http://localhost:8090        | topics, messages, consumer groups |
| Kafka (in-network)| kafka:9092                   | for containers on bigdata-net  |
| Kafka (from host) | localhost:9094               | for CLI tools on your laptop   |
| Airflow           | http://localhost:8082        | user/pass in `.env` (admin/admin) |
| Postgres (serving)| localhost:5433                | fraud_etl / fraud_etl, db `frauddb` |
| Superset          | http://localhost:8088        | user/pass in `.env` (admin/admin) |

## Resource footprint (16GB profile)

This is tuned to comfortably fit 16GB total, leaving headroom for your OS:
- Spark worker: 3GB / 2 cores (adjust `SPARK_WORKER_MEMORY` / `SPARK_WORKER_CORES` in compose)
- Single HDFS datanode, single Kafka broker (combined controller+broker, KRaft, no Zookeeper)
- Airflow LocalExecutor (no Redis/Celery worker containers)

If things feel tight, stop services you're not actively teaching with, e.g.:
```bash
docker compose stop airflow-webserver airflow-scheduler
```

## Lab structure

### 1. HDFS labs
```bash
docker exec -it namenode hdfs dfs -mkdir -p /labs/demo
docker exec -it namenode hdfs dfs -put /etc/hosts /labs/demo/
docker exec -it namenode hdfs dfs -ls /labs/demo
docker exec -it namenode hdfs dfs -cat /labs/demo/hosts
```
Good topics: block replication (set to 1 here since we have 1 datanode — explain
why production clusters use 3), permissions, `hdfs fsck`, the NameNode UI.

### 2. Kafka labs
The official `apache/kafka` image keeps its CLI scripts under `/opt/kafka/bin/`:
```bash
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --create --topic demo \
  --bootstrap-server kafka:9092 --partitions 3 --replication-factor 1

docker exec -it kafka /opt/kafka/bin/kafka-console-producer.sh --topic demo --bootstrap-server kafka:9092
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh --topic demo --bootstrap-server kafka:9092 --from-beginning
```
Use Kafka UI (http://localhost:8090) to visually show partitions, offsets, and
consumer group lag — much easier for students than raw CLI output.

### 3. Spark labs
Three ready-made jobs in `spark/apps/` (mounted into the master/worker at
`/opt/spark-apps` and into Jupyter at `~/work/spark-apps`):

- `batch_wordcount.py` — batch processing, HDFS in/out
- `streaming_kafka_to_hdfs.py` — Structured Streaming, Kafka → HDFS
- `spark_sql_demo.py` — Spark SQL over a CSV in HDFS

Each file has run instructions in its docstring. Submit with:
```bash
docker exec -it spark-master spark-submit --master spark://spark-master:7077 \
  /opt/spark-apps/batch_wordcount.py
```
Or run interactively from Jupyter — see `00_environment_check.ipynb` for the
connection boilerplate (note: connect to `spark://spark-master:7077`, not
local mode, so students see it as a real cluster).

### 4. Integration / pipeline labs
`airflow/dags/pipeline_hdfs_kafka_spark.py` is a 3-task DAG:
`ensure_hdfs_dirs → produce_kafka_events → spark_batch_job`, all wired through
Airflow. Trigger it from the Airflow UI (http://localhost:8082) and watch task
logs to see the whole path lit up in one place. Good scaffold for students to
extend (e.g. add a streaming sensor task, add a Slack/email notify task, swap
in `SparkSubmitOperator` once they're comfortable with the basics).

## Use-case coverage checklist
- **Batch**: `batch_wordcount.py`, HDFS CLI labs
- **Streaming**: `streaming_kafka_to_hdfs.py`, Kafka producer/consumer labs
- **Spark SQL**: `spark_sql_demo.py`
- **Pipeline/orchestration**: Airflow DAG tying all three systems together
- **ML (MLlib)**: `train_fraud_model.py` (Logistic Regression / Random Forest)
- **Real-time inference + alerting**: `streaming_fraud_pipeline.py`
- **Serving DB + BI**: `postgres-dw` + Superset dashboards
- **Full fraud-detection integration**: see
  [Fraud detection pipeline](#fraud-detection-pipeline-full-integration)

## Fraud detection pipeline (full integration)

Every box in the proposal/architecture diagram is now a real, wired-up
service — nothing is a stub. Layer by layer:

| Proposal layer | What runs it | Where |
|---|---|---|
| 1A/1B Data Sources | `producer` container | `producer/producer.py` |
| 2 Data Ingestion (Kafka topics) | `kafka-topics-init` (one-shot) | `kafka/create_topics.sh` |
| 3 Processing (clean, feature engineer, MLlib inference, alerts) | `spark-streaming-job` container | `spark/apps/streaming_fraud_pipeline.py` |
| 4A Data Lake (HDFS/Parquet) | `namenode` + `datanode` | `/raw /stream /processed /features /models /predictions` |
| 4B Serving Database | `postgres-dw` container | `postgres-dw/init.sql` |
| 5A/5B ML + Business Analytics | `spark/apps/train_fraud_model.py` (batch) + `postgres-dw` views | — |
| 6 Visualization | `superset` container | pre-wired to `postgres-dw` on first boot |
| Orchestration | Airflow DAG `pipeline_fraud_detection` | `airflow/dags/pipeline_fraud_detection.py` |

**Kafka topics created:** `transactions_raw`, `transactions_stream`,
`fraud_predictions`, `alerts`, `enriched_transactions`.

### Bring it up (first run)

```bash
cd bigdata-lab
docker compose up -d --build
```

This starts everything, but the streaming job needs a trained model before
it can do useful work, and training needs at least one historical batch in
HDFS. Order of events on a fresh `up`:

1. `namenode`/`kafka` come up, `kafka-topics-init` creates the 5 topics.
2. `producer` starts, writes a historical batch to HDFS `/raw`
   (synthetic by default — see `data/README.md` for using the real
   Mendeley dataset), then starts streaming to `transactions_raw`.
3. `spark-streaming-job` will crash-loop until a model exists at
   `hdfs:///models/fraud_model` — that's expected on first boot.
4. Train the model once (give the producer ~30s to finish the historical
   write first):
   ```bash
   docker exec -it spark-master spark-submit \
     --master spark://spark-master:7077 \
     /opt/spark-apps/train_fraud_model.py
   ```
5. `spark-streaming-job` picks up the model on its next restart:
   ```bash
   docker compose restart spark-streaming-job
   ```
6. Open Superset (http://localhost:8088, admin/admin) → **Data → Datasets**
   → the "Fraud Serving DB" connection is already there → add a dataset on
   `predictions` (or the `fraud_rate_over_time` / `top_fraudulent_merchants`
   / `fraud_by_country` / `fraud_by_card_type` views) and build charts.

After that first setup, everything runs on its own:
- `producer` and `spark-streaming-job` are `restart: unless-stopped` —
  transactions keep flowing and getting scored continuously.
- The Airflow DAG `pipeline_fraud_detection` (schedule: weekly, or trigger
  manually from http://localhost:8082) re-trains the model on the latest
  `/raw` data — restart `spark-streaming-job` afterwards to pick it up.

### Verifying it end-to-end

```bash
# transactions flowing in
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --topic transactions_raw --bootstrap-server kafka:9092 --max-messages 3

# predictions coming out
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --topic fraud_predictions --bootstrap-server kafka:9092 --max-messages 3

# predictions landing in the serving DB
docker exec -it postgres-dw psql -U fraud_etl -d frauddb \
  -c "SELECT * FROM predictions ORDER BY processed_at DESC LIMIT 5;"

# logs if something looks stuck
docker compose logs -f producer spark-streaming-job superset-init
```

### Fraud-detection-specific troubleshooting
- **`spark-streaming-job` keeps restarting**: almost always means
  `hdfs:///models/fraud_model` doesn't exist yet — run
  `train_fraud_model.py` once (step 4 above).
- **Superset has no "Fraud Serving DB" connection**: `superset-init` didn't
  finish — check `docker compose logs superset-init`; it's safe to re-run
  with `docker compose up -d superset-init`.
- **Postgres tables empty**: confirm `spark-streaming-job` is actually
  running (`docker compose ps`) and check its logs — the most common cause
  is the Kafka/Postgres `--packages` download failing on a slow connection
  on first submit (it re-tries on container restart).
- **Want more/less traffic**: change `PRODUCER_EVENTS_PER_SEC` in `.env`
  and `docker compose restart producer`.

## Resetting between class sessions
```bash
docker compose down -v   # wipes HDFS/Kafka/Postgres/Superset volumes - fresh start
docker compose up -d --build
```
Drop `-v` if you want data to persist across sessions instead.

## Troubleshooting
- **`pip install` fails/times out while building the Airflow image**: `pyspark`
  is a large download (~300MB); on a slow or unstable connection it can get
  interrupted. Just re-run `docker compose up -d --build` — Docker reuses
  already-downloaded layers, and the Dockerfile is set to retry pip 8 times
  with a longer timeout. If it keeps failing, try building just that image
  first so you get a clearer error: `docker compose build airflow-init`.
- **Airflow webserver keeps restarting**: check `docker compose logs airflow-init`
  first — it must finish successfully before webserver/scheduler start.
- **Spark job can't reach HDFS**: confirm you're using `hdfs://namenode:9000/...`
  paths, not `localhost` — containers talk to each other by service name.
- **Kafka client from host laptop**: use `localhost:9094` (the `EXTERNAL`
  listener), not `9092` (in-network only).
- **Port already in use**: change the relevant `*_PORT` value in `.env` and
  re-run `docker compose up -d`.
