# immo-lakehouse
immo-lakehouse

DVF real-estate lakehouse pipeline built with MinIO, Nessie, Apache Spark, and ClickHouse.

This project ingests French DVF (Demandes de Valeurs Foncières) data, builds a Lakehouse architecture using Iceberg, computes aggregated metrics, and exposes them in ClickHouse for analytics.

The entire pipeline is reproducible and orchestrated via a Makefile.

Architecture
<img width="3748" height="165" alt="mermaid-diagram" src="https://github.com/user-attachments/assets/cc9d62c0-3720-4140-966b-22b1bcc32c6b" />
Stack components:
| Component      | Role                         |
| -------------- | ---------------------------- |
| **MinIO**      | S3-compatible object storage |
| **Nessie**     | Iceberg catalog              |
| **Spark**      | transformation engine        |
| **ClickHouse** | analytical database          |

Data Lake Layout
MinIO bucket structure:
bucket/
 ├─ raw/
 │   └─ dvf/
 │       └─ ingest=<timestamp>/
 │           └─ year=YYYY/
 │               └─ dep=XX/
 │                   *.csv.gz
 │
 ├─ warehouse/
 │   └─ iceberg/
 │       └─ immo/
 │           dvf_silver/
 │
 └─ gold/
     └─ m_price_m2_commune_month/
         └─ year=YYYY/
             └─ dep=XX/
                 *.parquet

Repository Structure:
immo-lake/
│
├─ Makefile
├─ README.md
│
├─ scripts/
│   └─ ingest_dvf_landES_40.py
│
├─ spark/
│   ├─ spark_bronze_to_silver_iceberg.py
│   └─ spark_silver_to_gold_parquet.py
│
└─ sql/
    └─ clickhouse_schema.sql

Prerequisites
Infrastructure running (via docker or Portainer):
MinIO
Nessie
Spark

Server tools:
docker
python3
make
ClickHouse
