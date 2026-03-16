# immo-lakehouse

DVF real-estate lakehouse pipeline built with **MinIO, Nessie, Apache Spark, and ClickHouse**.

This project ingests French **DVF (Demandes de Valeurs Foncières)** data, builds a **Lakehouse architecture using Iceberg**, computes aggregated metrics, and exposes them in **ClickHouse** for analytics.

The entire pipeline is reproducible and orchestrated via a **Makefile**.

---

# Architecture

```mermaid
flowchart LR
A[DVF data.gouv.fr] --> B[MinIO raw]
B --> C[Spark Bronze → Silver]
C --> D[Iceberg table<br>nessie.immo.dvf_silver]
D --> E[Spark Silver → Gold]
E --> F[Parquet Gold datasets]
F --> G[ClickHouse mart]
