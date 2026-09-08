# Real-Time Credit Card Fraud Detection System

A real-time **Credit Card Fraud Detection System** built using Big Data technologies, Machine Learning, and a scalable data-processing architecture.

The system ingests transaction data through **Apache Kafka**, processes it using **Apache Spark Structured Streaming**, applies a trained **Spark MLlib** model for fraud prediction, stores data in **HDFS and PostgreSQL**, and provides analytics through **Apache Superset**. **Apache Airflow** is used for workflow orchestration and model retraining.

---

## Project Overview

Credit card fraud detection requires processing large volumes of transactions while identifying suspicious activity with low latency.

This project implements an end-to-end Big Data pipeline that can:

* Generate and ingest transaction data.
* Stream transactions through Kafka.
* Process and transform transactions using Spark.
* Perform feature engineering.
* Apply a Machine Learning model for fraud prediction.
* Generate fraud alerts for high-risk transactions.
* Store raw and processed data in HDFS.
* Store prediction results in PostgreSQL.
* Visualize fraud analytics using Superset.
* Orchestrate scheduled workflows using Airflow.

### End-to-End Flow

```text
                    ┌──────────────────┐
                    │     Producer     │
                    │ Python Simulator │
                    └────────┬─────────┘
                             │
                             ▼
                    ┌──────────────────┐
                    │      Kafka       │
                    │ transactions_raw │
                    └────────┬─────────┘
                             │
                             ▼
              ┌─────────────────────────────┐
              │      Spark Streaming        │
              │                             │
              │  • Data Cleaning            │
              │  • Feature Engineering      │
              │  • MLlib Inference          │
              │  • Fraud Detection          │
              │  • Alert Generation         │
              └─────────────┬───────────────┘
                            │
              ┌─────────────┼─────────────┐
              │             │             │
              ▼             ▼             ▼
          ┌────────┐   ┌──────────┐   ┌──────────┐
          │  HDFS  │   │PostgreSQL│   │  Kafka   │
          │  Data  │   │ Serving  │   │ Predictions│
          │  Lake  │   │    DB    │   │ & Alerts │
          └────────┘   └─────┬────┘   └──────────┘
                              │
                              ▼
                       ┌────────────┐
                       │  Superset  │
                       │ Dashboards │
                       └────────────┘

                     ┌────────────┐
                     │  Airflow   │
                     │Orchestration│
                     └────────────┘
```

---

##  Architecture

The project is organized into several layers:

| Layer             | Technology                        | Responsibility                                            |
| ----------------- | --------------------------------- | --------------------------------------------------------- |
| Data Generation   | Python                            | Generate historical and real-time transactions            |
| Data Ingestion    | Apache Kafka                      | Stream transactions through Kafka topics                  |
| Stream Processing | Apache Spark Structured Streaming | Clean, transform, and process transactions                |
| Machine Learning  | Spark MLlib                       | Train and apply the fraud detection model                 |
| Data Lake         | HDFS                              | Store raw, processed, feature, model, and prediction data |
| Serving Database  | PostgreSQL                        | Store predictions and analytics data                      |
| Visualization     | Apache Superset                   | Build fraud analytics dashboards                          |
| Orchestration     | Apache Airflow                    | Schedule and orchestrate data workflows                   |
| Development       | Jupyter                           | Interactive data exploration and Spark development        |
| Deployment        | Docker Compose                    | Run the complete platform as containers                   |

---

##  Technology Stack

* **Python**
* **Apache Kafka**
* **Apache Spark 3.3.0**
* **Spark Structured Streaming**
* **Spark MLlib**
* **Apache Hadoop / HDFS**
* **Apache Airflow**
* **PostgreSQL**
* **Apache Superset**
* **JupyterLab**
* **Docker & Docker Compose**

---

##  Project Structure

```text
credit-card-fraud-detection/
│
├── airflow/
│   ├── dags/
│   │   ├── pipeline_fraud_detection.py
│   │   └── pipeline_hdfs_kafka_spark.py
│   └── docker/
│
├── data/
│   └── README.md
│
├── docs/
│   └── PROJECT_PLAN.md
│
├── hadoop/
│   └── hadoop.env
│
├── jupyter/
│   ├── Dockerfile
│   └── notebooks/
│
├── kafka/
│   └── create_topics.sh
│
├── postgres-dw/
│   └── init.sql
│
├── producer/
│   ├── producer.py
│   ├── Dockerfile
│   └── requirements.txt
│
├── spark/
│   └── apps/
│       ├── train_fraud_model.py
│       ├── streaming_fraud_pipeline.py
│       ├── streaming_kafka_to_hdfs.py
│       ├── batch_wordcount.py
│       └── spark_sql_demo.py
│
├── superset/
│   ├── Dockerfile
│   ├── bootstrap.sh
│   └── superset_config.py
│
├── .gitignore
├── docker-compose.yml
└── README.md
```

---

##  Data Pipeline

### 1. Data Generation

The Python producer generates transaction data for both:

* Historical batch processing.
* Continuous real-time streaming.

Historical data is stored in HDFS under:

```text
/raw
```

Real-time transactions are sent to Kafka:

```text
transactions_raw
```

---

### 2. Kafka Ingestion

Kafka acts as the real-time ingestion layer.

The system creates the following topics:

```text
transactions_raw
transactions_stream
fraud_predictions
alerts
enriched_transactions
```

Each topic is configured with multiple partitions to demonstrate distributed streaming concepts.

---

### 3. Spark Structured Streaming

The main streaming application is:

```text
spark/apps/streaming_fraud_pipeline.py
```

The streaming pipeline performs:

```text
Kafka
  ↓
JSON Parsing
  ↓
Data Cleaning
  ↓
Feature Engineering
  ↓
MLlib Prediction
  ↓
Fraud Classification
  ↓
Predictions + Alerts
  ↓
HDFS / PostgreSQL / Kafka
```

The pipeline continuously processes incoming transactions and applies the trained fraud detection model.

---

##  Machine Learning

The model training application is:

```text
spark/apps/train_fraud_model.py
```

The project uses **Spark MLlib** for model training and evaluation.

The current training pipeline uses **Logistic Regression** and stores the trained model in HDFS:

```text
/models/fraud_model
```

### Model Evaluation

During the project integration test, the trained Logistic Regression model achieved:

| Metric             | Result |
| ------------------ | -----: |
| AUC                | 0.7203 |
| Accuracy           | 0.9240 |
| Weighted Precision | 0.8538 |
| Weighted Recall    | 0.9240 |
| F1 Score           | 0.8875 |

These results are based on the historical data used during the integration test.

---

##  Fraud Alerts

When the streaming model identifies a transaction as high risk, the system generates an alert.

Alerts are published to the Kafka topic:

```text
alerts
```

and are also available through the downstream storage layer for analytics.

This allows the system to support near real-time fraud monitoring.

---

##  HDFS Data Lake

HDFS is used as the project's distributed storage layer.

The main directories are:

```text
/raw
/stream
/processed
/features
/models
/predictions
```

### Storage Responsibilities

| Directory      | Purpose                         |
| -------------- | ------------------------------- |
| `/raw`         | Historical/raw transaction data |
| `/stream`      | Streaming transaction data      |
| `/processed`   | Cleaned and processed data      |
| `/features`    | Feature-engineered data         |
| `/models`      | Trained ML models               |
| `/predictions` | Fraud prediction results        |

---

##  PostgreSQL Serving Layer

PostgreSQL is used as the serving database for downstream analytics.

The main prediction data is stored in:

```text
predictions
```

The database also provides analytical views such as:

```text
fraud_rate_over_time
top_fraudulent_merchants
fraud_by_country
fraud_by_card_type
```

These views can be connected directly to Superset for visualization.

---

##  Visualization — Apache Superset

Apache Superset provides the analytics and dashboard layer.

The Superset instance is pre-configured to connect to the PostgreSQL serving database.

Possible dashboard metrics include:

* Total transactions
* Fraudulent transactions
* Fraud rate
* Fraud over time
* Fraud by country
* Fraud by card type
* Top fraudulent merchants
* High-risk transactions

Superset:

```text
PostgreSQL
     ↓
   Superset
     ↓
Dashboards & Analytics
```

---

##  Orchestration — Apache Airflow

Apache Airflow is used to orchestrate the project workflows.

The main fraud detection DAG is:

```text
airflow/dags/pipeline_fraud_detection.py
```

The project also contains:

```text
airflow/dags/pipeline_hdfs_kafka_spark.py
```

Airflow can be used to automate tasks such as:

* Preparing HDFS directories.
* Producing data.
* Running Spark batch jobs.
* Retraining the fraud detection model.

The fraud detection workflow is designed for scheduled model retraining.

---

#  Getting Started

## Prerequisites

Make sure the following are installed:

* Docker
* Docker Compose
* Git

A machine with approximately **16 GB RAM** is recommended for running the complete stack locally.

---

## 1. Clone the Repository

```bash
git clone https://github.com/youseifsamirshabaan/credit-card-fraud-detection.git
cd credit-card-fraud-detection
```

---

## 2. Start the Platform

```bash
docker compose up -d --build
```

Check the running services:

```bash
docker compose ps
```

---

## 3. Train the Fraud Detection Model

On the first run, the streaming service requires a trained model.

After the producer has loaded the historical data into HDFS, run:

```bash
docker compose run --rm spark-streaming-job \
  /spark/bin/spark-submit \
  /opt/spark-apps/train_fraud_model.py
```

The model will be stored at:

```text
hdfs://namenode:9000/models/fraud_model
```

---

## 4. Restart the Streaming Pipeline

After training the model:

```bash
docker compose restart spark-streaming-job
```

The streaming pipeline will then consume transactions from Kafka and perform real-time fraud detection.

---

#  End-to-End Verification

### Check incoming transactions

```bash
docker exec -it kafka \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --topic transactions_raw \
  --bootstrap-server kafka:9092 \
  --max-messages 3
```

### Check fraud predictions

```bash
docker exec -it kafka \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --topic fraud_predictions \
  --bootstrap-server kafka:9092 \
  --max-messages 3
```

### Check PostgreSQL predictions

```bash
docker exec -it postgres-dw \
  psql -U fraud_etl -d frauddb \
  -c "SELECT * FROM predictions ORDER BY processed_at DESC LIMIT 5;"
```

### Check HDFS

```bash
docker exec -it namenode \
  hdfs dfs -ls /
```

### Check streaming logs

```bash
docker compose logs -f spark-streaming-job
```

---

#  Service URLs

After starting the project, the main services are available at:

| Service       | URL                   |
| ------------- | --------------------- |
| HDFS NameNode | http://localhost:9870 |
| HDFS DataNode | http://localhost:9864 |
| Spark Master  | http://localhost:8080 |
| Spark Worker  | http://localhost:8081 |
| JupyterLab    | http://localhost:8888 |
| Kafka UI      | http://localhost:8090 |
| Airflow       | http://localhost:8082 |
| Superset      | http://localhost:8088 |
| PostgreSQL    | `localhost:5433`      |

Default development credentials are configured through the project's environment configuration.

**Do not commit `.env` or production credentials to the repository.**

---

#  Project Verification

The complete pipeline was successfully tested locally.

The integration test verified:

```text
Producer
   ↓
Kafka
   ↓
Spark Structured Streaming
   ↓
MLlib Model
   ↓
Fraud Prediction
   ↓
HDFS / PostgreSQL / Kafka
```

During testing, the producer successfully sent thousands of transactions to Kafka, while the Spark streaming application continuously processed incoming batches.

Example Spark streaming batches included:

```text
[batch 1] 31 transactions
[batch 2] 81 transactions
[batch 3] 69 transactions
[batch 4] 74 transactions
[batch 5] 75 transactions
...
```

The trained model was successfully saved to:

```text
hdfs://namenode:9000/models/fraud_model
```

This confirms that the core real-time processing pipeline is operational.

---

#  Additional Big Data Components

The repository also contains educational Big Data examples demonstrating:

### Batch Processing

```text
spark/apps/batch_wordcount.py
```

### Kafka → HDFS Streaming

```text
spark/apps/streaming_kafka_to_hdfs.py
```

### Spark SQL

```text
spark/apps/spark_sql_demo.py
```

These examples complement the main fraud detection pipeline and demonstrate fundamental Big Data processing concepts.

---

#  Data & Security

The actual transaction dataset is intentionally **not committed to GitHub**.

The repository uses `.gitignore` rules to prevent committing:

```text
.env
*.csv
*.zip
*.parquet
airflow/logs/
```

This keeps credentials, potentially sensitive datasets, and generated runtime files outside the public repository.

For local development, refer to:

```text
data/README.md
```

for dataset information and setup instructions.

---

#  Useful Docker Commands

### View running containers

```bash
docker compose ps
```

### View logs

```bash
docker compose logs -f
```

### View specific service logs

```bash
docker compose logs -f producer
docker compose logs -f spark-streaming-job
docker compose logs -f kafka
```

### Stop the project

```bash
docker compose down
```

This stops and removes the containers while keeping persistent Docker volumes.

### Full reset

```bash
docker compose down -v
```

>  This removes the project's persistent volumes, including stored HDFS, Kafka, PostgreSQL, and Superset data.

---

#  Project Objectives

The project demonstrates how Big Data technologies can be combined to build a real-time Machine Learning application.

The main objectives are:

* Implement real-time data ingestion using Kafka.
* Process streaming data using Spark Structured Streaming.
* Apply distributed data processing concepts.
* Perform feature engineering using Spark.
* Train and evaluate a Machine Learning model using Spark MLlib.
* Detect potentially fraudulent transactions in real time.
* Store large-scale data using HDFS.
* Provide a serving layer using PostgreSQL.
* Build analytics dashboards using Superset.
* Automate workflows using Airflow.
* Containerize the complete system using Docker Compose.

---

#  Team

This project was developed as part of the **NTI Big Data Training / Graduation Project**.

### Team Roles

| Team Member | Responsibility                                                                 |
| ----------- | ------------------------------------------------------------------------------ |
| Youseif     | Team Leader, System Integration, Architecture, Git/GitHub, Docker & Deployment |
| Yasmin      | Data Ingestion, Python Producer & Kafka                                        |
| Hadeer      | Spark Structured Streaming & Stream Processing                                 |
| Ahmed       | Spark Batch, HDFS/PostgreSQL Integration & Visualization                       |
| Hania       | Machine Learning, Feature Engineering & Model Training                         |

---

#  Documentation

The detailed project plan is available in:

```text
docs/PROJECT_PLAN.md
```

It contains the project planning, architecture, responsibilities, and implementation details.

---

#  Future Improvements

Possible future improvements include:

* Compare multiple ML algorithms and optimize the fraud classifier.
* Improve model performance through advanced feature engineering.
* Add model monitoring and drift detection.
* Add authentication and role-based access to dashboards.
* Deploy the system on a cloud-based Big Data platform.
* Introduce multiple Kafka brokers for higher availability.
* Add automated model validation before deployment.
* Add real-time monitoring and notification services.
* Implement CI/CD for automated testing and deployment.

---

#  Conclusion

This project demonstrates a complete **end-to-end Big Data and Machine Learning pipeline** for real-time credit card fraud detection.

By combining **Kafka, Spark, MLlib, HDFS, PostgreSQL, Superset, Airflow, and Docker**, the system provides a practical architecture for ingesting, processing, predicting, storing, and visualizing transaction data in near real time.

---

## Technologies

```text
Python
Apache Kafka
Apache Spark
Spark Structured Streaming
Spark MLlib
Apache Hadoop
HDFS
Apache Airflow
PostgreSQL
Apache Superset
JupyterLab
Docker
Docker Compose
```
