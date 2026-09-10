# Credit Card Fraud Detection System

##  Overview

A Big Data and Machine Learning system for detecting potentially fraudulent credit card transactions through a real-time streaming pipeline.

The system combines **Kafka, Apache Spark, HDFS, PostgreSQL, Apache Superset, Airflow, Docker, and Machine Learning** into one end-to-end architecture.

The main objective is to ingest transaction events, process them in real time, apply a trained fraud detection model, store the results, and provide a monitoring dashboard.

---

##  System Architecture

```text
                    ┌─────────────────────┐
                    │  Transaction CSV    │
                    │   Data Source        │
                    └──────────┬──────────┘
                               │
                               ▼
                    ┌─────────────────────┐
                    │  Python Producer    │
                    │  Streaming Events   │
                    └──────────┬──────────┘
                               │
                               ▼
                    ┌─────────────────────┐
                    │       Kafka         │
                    │ transactions_raw    │
                    └──────────┬──────────┘
                               │
                               ▼
              ┌────────────────────────────────┐
              │       Spark Structured         │
              │          Streaming             │
              │                                │
              │ Cleaning + Feature Engineering │
              │ + ML Inference                 │
              └───────────────┬────────────────┘
                              │
               ┌──────────────┼──────────────┐
               │              │              │
               ▼              ▼              ▼
        ┌────────────┐  ┌────────────┐  ┌─────────────┐
        │    HDFS    │  │ PostgreSQL │  │    Kafka    │
        │ Predictions│  │ Predictions│  │ Predictions │
        └────────────┘  └─────┬──────┘  └─────────────┘
                              │
                              ▼
                       ┌─────────────┐
                       │  Superset   │
                       │  Dashboard  │
                       └─────────────┘

                 ┌─────────────────────┐
                 │       Airflow       │
                 │ Weekly Orchestration│
                 └──────────┬──────────┘
                            │
                            ▼
                    Spark Model Training
```

---

##  Technologies

| Technology              | Purpose                                         |
| ----------------------- | ----------------------------------------------- |
| Python                  | Data producer and application logic             |
| Apache Kafka            | Real-time event ingestion and streaming         |
| Apache Spark            | Distributed processing and streaming            |
| Spark MLlib             | Fraud classification model                      |
| HDFS                    | Distributed data storage                        |
| PostgreSQL              | Analytical serving layer                        |
| Apache Superset         | Fraud monitoring dashboard                      |
| Apache Airflow          | Pipeline orchestration and scheduled retraining |
| Docker / Docker Compose | Containerization and service management         |
| Jupyter                 | Development and experimentation                 |
| Kafka UI                | Kafka monitoring                                |

---

##  Project Structure

```text
credit-card-fraud-detection/
│
├── docker-compose.yml
├── .env
├── README.md
│
├── producer/
│   ├── producer.py
│   ├── Dockerfile
│   └── requirements.txt
│
├── kafka/
│   └── create_topics.sh
│
├── spark/
│   ├── Dockerfile
│   ├── Dockerfile.master
│   ├── Dockerfile.worker
│   └── apps/
│       ├── train_fraud_model.py
│       ├── streaming_fraud_pipeline.py
│       ├── streaming_kafka_to_hdfs.py
│       ├── batch_wordcount.py
│       └── spark_sql_demo.py
│
├── hadoop/
│   └── hadoop.env
│
├── postgres-dw/
│   └── init.sql
│
├── superset/
│   ├── Dockerfile
│   ├── bootstrap.sh
│   └── superset_config.py
│
├── airflow/
│   ├── dags/
│   │   ├── pipeline_fraud_detection.py
│   │   └── pipeline_hdfs_kafka_spark.py
│   └── docker/
│
├── jupyter/
│   ├── Dockerfile
│   └── notebooks/
│
└── data/
    └── README.md
```

---

##  Data Flow

### 1. Data Source

The system uses a historical credit card transaction dataset stored locally and mounted into the Producer container.

The dataset contains transaction and behavioral features such as:

* Transaction amount
* Merchant category
* Merchant risk score
* Geographic velocity
* Geographic distance
* Device fingerprint match
* CVV status
* 3-D Secure authentication result
* Transaction velocity
* Failed attempts
* Account age
* Chargeback history
* Customer information
* Fraud label

The raw historical dataset is stored in HDFS under:

```text
/raw/creditcard_transactions_historical.csv
```

---

### 2. Python Producer

The Producer reads the transaction CSV incrementally and publishes transactions to Kafka.

Each event contains the transaction features together with an event timestamp.

Kafka topic:

```text
transactions_raw
```

The Producer simulates real-time transaction arrival rather than loading the entire dataset into Kafka at once.

---

### 3. Apache Kafka

Kafka acts as the real-time ingestion layer.

Main topics:

```text
transactions_raw
transactions_stream
enriched_transactions
fraud_predictions
alerts
```

The `transactions_raw` topic is configured with multiple partitions to support parallel processing.

---

### 4. Spark Structured Streaming

Spark continuously consumes transactions from Kafka.

The streaming pipeline performs:

1. Reading Kafka events
2. Parsing transaction data
3. Data cleaning
4. Feature preparation
5. Loading the trained ML model
6. Fraud prediction
7. Generating fraud probabilities
8. Writing predictions to downstream systems

The streaming application loads the model from:

```text
hdfs://namenode:9000/models/fraud_model
```

---

##  Machine Learning

The project uses a **Random Forest Classifier** implemented with Spark MLlib.

The model uses categorical and numerical transaction features.

### Categorical features

Examples include:

```text
merchant_category
merchant_risk_score
geo_velocity_bin
geo_distance_bin
merchant_location
country_consistency
ip_address_type
device_fingerprint_match
cvv_match_status
three_ds_auth_result
tokenization_used
time_of_transaction
card_present_cnp
order_shipping_speed
```

### Numerical features

Examples include:

```text
transaction_amount
avg_amount_deviation_sigma
geo_velocity_kmh
geo_distance_km
session_duration_sec
cards_on_device_30d
failed_attempts_before_success
account_age_days
chargeback_history_count
```

Categorical features are transformed using:

```text
StringIndexer → OneHotEncoder
```

and combined with numerical features using:

```text
VectorAssembler
```

The final pipeline contains a **Random Forest Classifier**.

The trained model is saved to:

```text
/models/fraud_model
```

---

##  Storage Layer

### HDFS

HDFS is used for distributed storage of raw and processed data.

Main directories:

```text
/raw
/stream
/processed
/features
/models
/predictions
/checkpoints
```

---

### PostgreSQL

PostgreSQL acts as the serving/analytical database for fraud predictions.

Main tables include:

```text
predictions
alerts
```

Additional SQL views are used by Superset for dashboard reporting.

---

##  Apache Superset

Superset provides the monitoring and analytics layer.

The dashboard exposes key fraud metrics such as:

* Total transactions
* Fraud transactions
* Fraud rate
* Total transaction amount
* Fraud amount
* Critical alerts
* Suspicious transactions
* Merchant-level fraud analysis

Superset reads the live analytical data from PostgreSQL.

---

##  Apache Airflow

Airflow provides the orchestration layer.

The main DAG is:

```text
pipeline_fraud_detection
```

The workflow is:

```text
ensure_hdfs_dirs
        ↓
check_pipeline_health
        ↓
train_or_update_model
        ↓
notify_dashboards_ready
```

The DAG is scheduled weekly.

The training task uses the same official Spark training script used by the project:

```text
spark/apps/train_fraud_model.py
```

This keeps scheduled retraining consistent with the model used by the streaming inference service.

---

##  Docker

All major project services are containerized using Docker Compose.

The stack includes:

```text
Kafka
Kafka UI
Spark Master
Spark Worker
Spark Streaming Job
Hadoop NameNode
Hadoop DataNode
PostgreSQL
Superset
Airflow
Jupyter
Producer
```

---

# ▶️ Running the Project

## Prerequisites

Install:

* Docker Desktop
* Docker Compose
* Git

Make sure Docker Desktop is running.

---

## 1. Clone the repository

```bash
git clone <repository-url>
cd credit-card-fraud-detection
```

---

## 2. Configure environment variables

Create a local `.env` file.

Do not commit `.env` to GitHub because it contains local configuration and credentials.

The repository intentionally ignores:

```text
.env
data/credit_card_fraud.csv
airflow/logs/
__pycache__/
.ipynb_checkpoints/
```

---

## 3. Start the complete stack

```bash
docker compose up -d --build
```

Check the services:

```bash
docker compose ps
```

All required services should be running.

---

## 4. Access the services

### Superset

```text
http://localhost:8088
```

### Airflow

```text
http://localhost:8082
```

### Kafka UI

```text
http://localhost:8090
```

### Spark Master UI

```text
http://localhost:8080
```

### Spark Worker UI

```text
http://localhost:8081
```

### Jupyter

```text
http://localhost:8888
```

---

##  End-to-End Pipeline Test

After starting the stack, verify the complete flow:

```text
CSV
 ↓
Python Producer
 ↓
Kafka
 ↓
Spark Structured Streaming
 ↓
Random Forest ML Model
 ↓
Fraud Prediction
 ↓
HDFS + PostgreSQL + Kafka
 ↓
Superset Dashboard
```

Useful commands:

### Check running containers

```bash
docker compose ps
```

### Check Producer logs

```bash
docker compose logs -f producer
```

### Check Spark Streaming logs

```bash
docker compose logs -f spark-streaming-job
```

### Check HDFS predictions

```bash
docker compose exec namenode hdfs dfs -ls -h /predictions
```

### Check PostgreSQL predictions

```bash
docker compose exec postgres-dw \
psql -U fraud_etl -d frauddb \
-c "SELECT COUNT(*) AS predictions_count FROM predictions;"
```

### Check Kafka topics

```bash
docker compose exec kafka \
kafka-topics.sh --bootstrap-server kafka:9092 --list
```

---

##  Important Notes

### Do not use

```bash
docker compose down -v
```

during normal project operation.

The `-v` option removes Docker volumes and can delete persisted project data.

To stop the stack safely:

```bash
docker compose down
```

To start it again:

```bash
docker compose up -d
```

---

##  Current Project Status

| Component              | Status |
| ---------------------- | ------ |
| Docker Compose         | ✅      |
| Python Producer        | ✅      |
| Kafka                  | ✅      |
| Spark Streaming        | ✅      |
| Spark MLlib            | ✅      |
| Random Forest Model    | ✅      |
| HDFS                   | ✅      |
| PostgreSQL             | ✅      |
| Superset               | ✅      |
| Airflow                | ✅      |
| End-to-End Integration | ✅      |
| GitHub Repository      | ✅      |

---

##  Project Objective

The final system demonstrates how a real-world fraud detection workflow can be implemented using a modern Big Data architecture.

Instead of processing transactions only as a static batch, the system provides a streaming-oriented architecture where incoming transactions can be:

**ingested → processed → scored → stored → monitored**

using distributed and containerized technologies.

---

##  Team

**Credit Card Fraud Detection System**

Youseif (Leader)
Ahmed
Hadeer
Yasmein
Hania

Big Data / Data Engineering Graduation Project

Technologies:

```text
Kafka
Spark
Hadoop / HDFS
Spark MLlib
PostgreSQL
Superset
Airflow
Docker
Python
```

---

##  Future Improvements

Possible future enhancements include:

* Training the final model on the complete historical dataset
* Hyperparameter tuning
* Advanced fraud detection models
* Model monitoring and drift detection
* Better alert notification mechanisms
* Additional Superset visualizations
* Kafka replication across multiple brokers
* Cloud deployment
* CI/CD automation
