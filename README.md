# IoT Streaming Pipeline — Kafka + PySpark + BigQuery

![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-231F20?style=flat&logo=apachekafka&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-E25A1C?style=flat&logo=apachespark&logoColor=white)
![Google BigQuery](https://img.shields.io/badge/BigQuery-4285F4?style=flat&logo=googlebigquery&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=flat&logo=docker&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=flat&logo=python&logoColor=white)

End-to-end IoT data streaming pipeline that collects sensor telemetry from thousands of devices, processes streams in real-time with PySpark Structured Streaming, and loads analytics-ready data into Google BigQuery for BI reporting.

## Architecture

```mermaid
graph TD
    A[IoT Sensors<br/>Temperature / Humidity / GPS] --> B[MQTT Broker<br/>Eclipse Mosquitto]
    B --> C[Kafka Connect<br/>MQTT Source Connector]
    C --> D[Apache Kafka<br/>Device Topics]
    D --> E[PySpark Structured Streaming<br/>Micro-batch Processing]
    E --> F[Google BigQuery<br/>Analytics Warehouse]
    E --> G[Alert Engine<br/>Threshold Violations]
    F --> H[Looker Studio<br/>IoT Dashboard]
```

## Features

- MQTT-to-Kafka bridge for IoT device telemetry ingestion
- PySpark Structured Streaming with watermarking and late data handling
- BigQuery partitioned tables for cost-effective analytics
- Real-time anomaly detection and alerting
- Schema evolution support via Confluent Schema Registry
- End-to-end exactly-once delivery guarantee

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Device Protocol | MQTT (Eclipse Mosquitto) |
| Message Bus | Apache Kafka + Kafka Connect |
| Processing | PySpark Structured Streaming |
| Data Warehouse | Google BigQuery |
| Visualization | Looker Studio |
| Orchestration | Docker Compose |

## Prerequisites

- Docker & Docker Compose
- Python 3.10+
- Google Cloud account with BigQuery enabled
- GCP Service Account JSON key

## Quick Start

```bash
git clone https://github.com/zulham-tech/iot-streaming-kafka-pyspark-bigquery.git
cd iot-streaming-kafka-pyspark-bigquery
cp .env.example .env  # add your GCP credentials
docker compose up -d
python producers/iot_simulator.py  # simulate IoT devices
```

## Project Structure

```
.
├── producers/           # IoT device simulators
├── kafka_connect/       # MQTT Source Connector configs
├── spark_jobs/          # PySpark streaming jobs
├── bigquery/            # DDL scripts & table schemas
├── alerts/              # Threshold-based alert engine
├── docker-compose.yml
└── requirements.txt
```

## Author

**Ahmad Zulham Hamdan** — [LinkedIn](https://linkedin.com/in/ahmad-zulham-665170279) | [GitHub](https://github.com/zulham-tech)
