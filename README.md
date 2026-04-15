# Real-Time IoT Streaming Pipeline | Kafka Â· PySpark Â· BigQuery

> **Type:** Streaming | **Stack:** Open-Meteo API â†’ Kafka â†’ PySpark â†’ PostgreSQL â†’ BigQuery â†’ Airflow

## Overview
Built end-to-end streaming pipeline monitoring weather conditions across 5 Indonesian cities in real-time.

## Key Metrics
- **<60 seconds** end-to-end latency
- **7,200 events/day** processed continuously
- **95%+** data completeness enforced via quality gate
- **5-min tumbling windows** with z-score anomaly detection

## Architecture
```
Open-Meteo API (5 sensor nodes Â· 60s interval)
    â†“
Kafka [iot-weather-sensors Â· 5 partitions Â· GZIP]
    â†“ (PySpark Structured Streaming)
PySpark â†’ watermark 2min â†’ 5-min window â†’ z-score anomaly
    â†“ (foreachBatch)
PostgreSQL (operational) + BigQuery (analytics Â· hourly sync)
```

## Tech Stack
Python Â· Apache Kafka 3.6 Â· PySpark 3.5 Â· Apache Airflow 2.8 Â· PostgreSQL Â· Google BigQuery Â· Docker

## Quick Start
```bash
docker compose up -d
# Airflow â†’ http://localhost:8080 (admin/admin)
# Kafka UI â†’ http://localhost:9090
```

## Author
**Ahmad Zulham Hamdan** | [LinkedIn](https://linkedin.com/in/ahmad-zulham-hamdan-665170279) | [GitHub](https://github.com/zulham-tech)
