# immo-lakehouse

DVF real-estate lakehouse pipeline built with **MinIO, Nessie, Apache Spark, and ClickHouse**.

This project ingests French **DVF (Demandes de Valeurs Foncières)** data, builds a **Lakehouse architecture using Iceberg**, computes aggregated metrics, and exposes them in **ClickHouse** for analytics.

The entire pipeline is reproducible and orchestrated via a **Makefile**.

---

## Architecture

```mermaid
flowchart LR
A[DVF data.gouv.fr] --> B[MinIO raw]
B --> C[Spark Bronze → Silver]
C --> D[Iceberg table<br>nessie.immo.dvf_silver]
D --> E[Spark Silver → Gold]
E --> F[Parquet Gold datasets]
F --> G[ClickHouse mart]
```

## Stack
| Component  | Role                         |
| ---------- | ---------------------------- |
| MinIO      | S3-compatible object storage |
| Nessie     | Iceberg catalog              |
| Spark      | transformation engine        |
| ClickHouse | analytical database          |

## Data Lake Layout
### MinIO bucket structure:
```
lake/
├─ raw/
│  └─ dvf/
│     └─ ingest=<timestamp>/
│        └─ year=YYYY/
│           └─ dep=XX/
│              *.csv.gz
│
├─ warehouse/
│  └─ iceberg/
│     └─ immo/
│        dvf_silver/
│
└─ gold/
   └─ m_price_m2_commune_month/
      └─ year=YYYY/
         └─ dep=XX/
            *.parquet
```
## Repository Structure
```
immo-lakehouse/
│
├─ Makefile
├─ README.md
│
├─ scripts/
│  └─ ingest_dvf_landES_40.py
│
├─ spark/
│  ├─ spark_bronze_to_silver_iceberg.py
│  └─ spark_silver_to_gold_parquet.py
│
└─ sql/
   └─ clickhouse_schema.sql
```
## Prerequisites
Infrastructure stack running (Docker / Portainer):
```
MinIO
Nessie
Spark
ClickHouse
```
Server tools:
```
docker
python3
make
```
Environment configuration files:
```
.env
.env.docker
```
Example configuration:
```
MINIO_ENDPOINT=http://127.0.0.1:9100
MINIO_ACCESS_KEY=xxx
MINIO_SECRET_KEY=xxx
MINIO_BUCKET=lake

NESSIE_ENDPOINT=http://nessie:19120
NESSIE_REF=main
```
## Pipeline Workflow

Preview execution plan:
```
make plan
```
Run the full pipeline:
```
make apply INGEST_ID=$(date -u +%Y-%m-%dT%H%M%SZ)
```
Pipeline execution order:
```
ingest-all
silver-all
gold-all
ch-init
ch-load-all
```
## Bronze Layer

DVF ingestion into MinIO.

Data is stored under:
```
raw/dvf/ingest=<id>/year=YYYY/dep=XX/
```
Example:
```
make ingest YEAR=2024
```
## Silver Layer (Lakehouse)

Spark transforms Bronze data into an Iceberg table.

Table:
```
nessie.immo.dvf_silver
```
Partitioning:
```
dep
annee
ingest_id
```
## Gold Layer (Aggregations)

Spark computes aggregated real-estate metrics:
```
- price per m²
- per commune
- per month
- per property type
```
Output location:
```
gold/m_price_m2_commune_month/year=YYYY/dep=XX/
```
## ClickHouse Mart

Gold datasets are loaded into ClickHouse.

Main table:
```
immo.m_price_m2_commune_month
```
Derived analytical views:
```
v_price_m2_commune_month
v_price_m2_commune_year
v_price_m2_commune_year_current
```
## Cleaning / Reset

Reset lakehouse:
```
make clean
```
Removes:
```
MinIO raw/
MinIO warehouse/
MinIO gold/
Nessie catalog
```
Full reset:
```
make clean-all
```
Removes:
```
MinIO data
Nessie catalog
ClickHouse database
```
## Nessie Catalog Reset

If Iceberg metadata disappears but Nessie still references the table, the catalog may become inconsistent.

Example failure case:
```
catalog → table exists
storage → metadata missing
```
The cleanup process resets Nessie RocksDB:
```
rm -rf /tmp/nessie
```
This forces a fresh catalog state.

## Example End-to-End Run
```
make clean-all
make plan
make apply INGEST_ID=$(date -u +%Y-%m-%dT%H%M%SZ)
```
Expected pipeline flow:
```
raw → warehouse → gold → ClickHouse
```
## Dataset Scope

Current ingestion scope:
```
Department: Landes (40)
Years: 2020–2025
```
Configured in the Makefile:
```
DEPARTMENT=40
YEARS=2020 2021 2022 2023 2024 2025
```
## Example Query

Top communes by yearly price per m²:
```SQL
SELECT
    nom_commune,
    annee,
    prix_m2_median_year
FROM immo.v_price_m2_commune_year
ORDER BY prix_m2_median_year DESC
LIMIT 20;
```
## License

MIT
