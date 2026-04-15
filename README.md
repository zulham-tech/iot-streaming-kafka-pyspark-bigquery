# Real-Time IoT Streaming Pipeline

**Stack:** Open-Meteo API -> Kafka -> PySpark -> PostgreSQL -> BigQuery -> Airflow

## Key Metrics
- Less than 60 seconds end-to-end latency
- 7,200 events/day processed continuously
- 95%+ data completeness enforced via quality gate
- 5-min tumbling windows with z-score anomaly detection

## Architecture
```
Open-Meteo API (5 sensor nodes, 60s interval)
    |
Kafka [iot-weather-sensors, 5 partitions, GZIP]
    |
PySpark Structured Streaming
    -> watermark 2min -> 5-min window -> z-score anomaly
    |
PostgreSQL (operational) + BigQuery (analytics, hourly sync)
```

## Tech Stack
Python, Apache Kafka 3.6, PySpark 3.5, Apache Airflow 2.8, PostgreSQL, Google BigQuery, Docker

## Quick Start
```bash
docker compose up -d
# Airflow: http://localhost:8080 (admin/admin)
# Kafka UI: http://localhost:9090
```

## Author
Ahmad Zulham Hamdan | https://linkedin.com/in/ahmad-zulham-665170279
