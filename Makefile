SHELL := /bin/bash

INGEST_ID ?= $(shell date -u +%Y-%m-%dT%H%M%SZ)
DEPARTMENT ?= 40
YEARS ?= 2020 2021 2022 2023 2024 2025

ENV_HOST ?= .env
ENV_DOCKER ?= .env.docker
include $(ENV_HOST)
export

MINIO_ENDPOINT_DOCKER ?= http://minio:9000

NESSIE_CONTAINER ?= $(shell docker ps -qf name=nessie | head -n 1)
NESSIE_DATA_DIR ?= /tmp/nessie

CH_CONTAINER ?= $(shell docker ps -qf name=clickhouse | head -n 1)
SPARK_MASTER_CONTAINER ?= $(shell (docker ps -qf name=spark || true) | head -n 1)

SPARK_PACKAGES ?= org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,org.apache.hadoop:hadoop-aws:3.3.6

.PHONY: plan apply check print spark-sync \
        ingest ingest-all \
        silver silver-all \
        gold gold-all \
        ch-init ch-load ch-load-all \
        validate \
	clean clean-all clean-minio clean-lh clean-ch

plan: check print
	@echo "PLAN:"
	@echo "  apply -> ingest-all + silver-all + gold-all + ch-init + ch-load-all + validate"
	@echo "  scope: dep=$(DEPARTMENT) years=$(YEARS) ingest_id=$(INGEST_ID)"

apply:
	$(MAKE) ingest-all DEPARTMENT=$(DEPARTMENT) YEARS="$(YEARS)" INGEST_ID=$(INGEST_ID)
	$(MAKE) silver-all DEPARTMENT=$(DEPARTMENT) YEARS="$(YEARS)" INGEST_ID=$(INGEST_ID)
	$(MAKE) gold-all   DEPARTMENT=$(DEPARTMENT) YEARS="$(YEARS)" INGEST_ID=$(INGEST_ID)
	$(MAKE) ch-init
	$(MAKE) ch-load-all DEPARTMENT=$(DEPARTMENT) YEARS="$(YEARS)" INGEST_ID=$(INGEST_ID)
	$(MAKE) validate

check:
	@command -v docker >/dev/null || (echo "docker missing" && exit 1)
	@docker ps >/dev/null || (echo "docker not running / no permission" && exit 1)
	@test -f $(ENV_HOST) || (echo "Missing $(ENV_HOST)" && exit 1)
	@test -f $(ENV_DOCKER) || (echo "Missing $(ENV_DOCKER)" && exit 1)
	@test -n "$(CH_CONTAINER)" || (echo "ClickHouse container not found" && exit 1)
	@test -n "$(SPARK_MASTER_CONTAINER)" || (echo "Spark container not found" && exit 1)
	@echo "OK"

print:
	@echo "INGEST_ID=$(INGEST_ID)"
	@echo "DEPARTMENT=$(DEPARTMENT)"
	@echo "YEARS=$(YEARS)"
	@echo "MINIO_ENDPOINT=$(MINIO_ENDPOINT)"
	@echo "MINIO_ENDPOINT_DOCKER=$(MINIO_ENDPOINT_DOCKER)"
	@echo "MINIO_BUCKET=$(MINIO_BUCKET)"
	@echo "CH_CONTAINER=$(CH_CONTAINER)"
	@echo "SPARK_MASTER_CONTAINER=$(SPARK_MASTER_CONTAINER)"

ingest:
	@test -n "$(YEAR)" || (echo "Missing YEAR=YYYY" && exit 1)
	python3 scripts/ingest_dvf_landES_40.py \
	  --year "$(YEAR)" --dep "$(DEPARTMENT)" \
	  --ingest-id "$(INGEST_ID)" \
	  --minio-endpoint "$(MINIO_ENDPOINT)" --access-key "$(MINIO_ACCESS_KEY)" --secret-key "$(MINIO_SECRET_KEY)" \
	  --bucket "$(MINIO_BUCKET)"

ingest-all:
	@for y in $(YEARS); do \
	  echo "== ingest $$y dep=$(DEPARTMENT) ingest=$(INGEST_ID) =="; \
	  $(MAKE) ingest YEAR=$$y DEPARTMENT=$(DEPARTMENT) INGEST_ID=$(INGEST_ID); \
	done

silver: spark-sync
	@test -n "$(YEAR)" || (echo "Missing YEAR=YYYY" && exit 1)
	@set -a; source $(ENV_DOCKER); set +a; \
	docker exec -it $(SPARK_MASTER_CONTAINER) spark-submit \
	  --master spark://spark:7077 \
	  --packages "$(SPARK_PACKAGES)" \
	  /opt/bitnami/spark/work/spark_bronze_to_silver_iceberg.py \
	  --minio-endpoint "$$MINIO_ENDPOINT" --access-key "$$MINIO_ACCESS_KEY" --secret-key "$$MINIO_SECRET_KEY" \
	  --bucket "$$MINIO_BUCKET" \
	  --nessie "$$NESSIE_ENDPOINT" --ref "$$NESSIE_REF" \
	  --year "$(YEAR)" --dep "$(DEPARTMENT)" --ingest-id "$(INGEST_ID)" --mode overwrite_partitions

silver-all:
	@for y in $(YEARS); do \
	  echo "== silver $$y dep=$(DEPARTMENT) ingest=$(INGEST_ID) =="; \
	  $(MAKE) silver YEAR=$$y DEPARTMENT=$(DEPARTMENT) INGEST_ID=$(INGEST_ID); \
	done

gold: spark-sync
	@test -n "$(YEAR)" || (echo "Missing YEAR=YYYY" && exit 1)
	@set -a; source $(ENV_DOCKER); set +a; \
	docker exec -it $(SPARK_MASTER_CONTAINER) spark-submit \
	  --master spark://spark:7077 \
	  --packages "$(SPARK_PACKAGES)" \
	  /opt/bitnami/spark/work/spark_silver_to_gold_parquet.py \
	  --minio-endpoint "$$MINIO_ENDPOINT" --access-key "$$MINIO_ACCESS_KEY" --secret-key "$$MINIO_SECRET_KEY" \
	  --bucket "$$MINIO_BUCKET" \
	  --nessie "$$NESSIE_ENDPOINT" --ref "$$NESSIE_REF" \
	  --year "$(YEAR)" --dep "$(DEPARTMENT)" --ingest-id "$(INGEST_ID)"

gold-all:
	@for y in $(YEARS); do \
	  echo "== gold $$y dep=$(DEPARTMENT) ingest=$(INGEST_ID) =="; \
	  $(MAKE) gold YEAR=$$y DEPARTMENT=$(DEPARTMENT) INGEST_ID=$(INGEST_ID); \
	done

ch-init:
	docker exec -i $(CH_CONTAINER) clickhouse-client < sql/clickhouse_schema.sql

ch-load:
	@test -n "$(YEAR)" || (echo "Missing YEAR=YYYY" && exit 1)
	@test -n "$(INGEST_ID)" || (echo "Missing INGEST_ID=..." && exit 1)
	@echo "CH load dep=$(DEPARTMENT) year=$(YEAR) ingest=$(INGEST_ID)"
	@docker exec -i $(CH_CONTAINER) clickhouse-client --multiquery --query "\
	ALTER TABLE immo.m_price_m2_commune_month DELETE \
	  WHERE annee=toUInt16($(YEAR)) AND dep='$(DEPARTMENT)' AND ingest_id='$(INGEST_ID)'; \
	INSERT INTO immo.m_price_m2_commune_month \
	(code_commune, annee, mois, type_local, nb_ventes, prix_m2_median, prix_m2_p25, prix_m2_p75, ingest_id, dep) \
	SELECT code_commune, annee, mois, type_local, nb_ventes, prix_m2_median, prix_m2_p25, prix_m2_p75, \
	       '$(INGEST_ID)' AS ingest_id, '$(DEPARTMENT)' AS dep \
	FROM s3( \
	  '$(MINIO_ENDPOINT_DOCKER)/$(MINIO_BUCKET)/gold/m_price_m2_commune_month/year=$(YEAR)/dep=$(DEPARTMENT)/*.parquet', \
	  '$(MINIO_ACCESS_KEY)', '$(MINIO_SECRET_KEY)', 'Parquet' \
	);"

ch-load-all:
	@for y in $(YEARS); do \
	  echo "== ch-load $$y dep=$(DEPARTMENT) ingest=$(INGEST_ID) =="; \
	  $(MAKE) ch-load YEAR=$$y DEPARTMENT=$(DEPARTMENT) INGEST_ID=$(INGEST_ID); \
	done

validate:
	docker exec -it $(CH_CONTAINER) clickhouse-client --query \
	"SELECT dep, annee, count() FROM immo.m_price_m2_commune_month GROUP BY dep, annee ORDER BY dep, annee;"

clean:
	$(MAKE) clean-minio
	$(MAKE) clean-lh

clean-all:
	$(MAKE) clean-minio
	$(MAKE) clean-lh
	$(MAKE) clean-ch

clean-minio:
	@echo "Cleaning MinIO prefixes raw/, warehouse/, gold/ in bucket $(MINIO_BUCKET)"
	@mc alias set lake "$(MINIO_ENDPOINT)" "$(MINIO_ACCESS_KEY)" "$(MINIO_SECRET_KEY)" >/dev/null
	@mc rm --recursive --force "lake/$(MINIO_BUCKET)/raw" || true
	@mc rm --recursive --force "lake/$(MINIO_BUCKET)/warehouse" || true
	@mc rm --recursive --force "lake/$(MINIO_BUCKET)/gold" || true
	@echo "MinIO cleaned."

clean-lh:
	@NESSIE_CONTAINER=$$(docker ps -qf name=nessie | head -n 1); \
	test -n "$$NESSIE_CONTAINER" || (echo "Nessie container not found" && exit 1); \
	echo "Resetting Nessie RocksDB in $$NESSIE_CONTAINER"; \
	docker stop "$$NESSIE_CONTAINER" >/dev/null; \
	docker start "$$NESSIE_CONTAINER" >/dev/null 2>&1 || true; \
	docker stop "$$NESSIE_CONTAINER" >/dev/null; \
	docker run --rm --volumes-from "$$NESSIE_CONTAINER" alpine sh -lc "rm -rf /tmp/nessie"; \
	docker start "$$NESSIE_CONTAINER" >/dev/null; \
	echo "Lakehouse cleaned."
	

clean-ch:
	@test -n "$(CH_CONTAINER)" || (echo "ClickHouse container not found" && exit 1)
	@echo "Dropping ClickHouse database: immo"
	@docker exec -i $(CH_CONTAINER) clickhouse-client --multiquery --query "\
		DROP VIEW IF EXISTS immo.v_price_m2_commune_year_current; \
		DROP VIEW IF EXISTS immo.v_price_m2_commune_year; \
		DROP VIEW IF EXISTS immo.v_price_m2_commune_month; \
		DROP TABLE IF EXISTS immo.m_price_m2_commune_month; \
		DROP TABLE IF EXISTS immo.ref_commune; \
		DROP DATABASE IF EXISTS immo;"
	@echo "ClickHouse cleaned."

spark-sync:
	@test -n "$(SPARK_MASTER_CONTAINER)" || (echo "Spark container not found" && exit 1)
	docker cp scripts/spark_bronze_to_silver_iceberg.py $(SPARK_MASTER_CONTAINER):/opt/bitnami/spark/work/spark_bronze_to_silver_iceberg.py
	docker cp scripts/spark_silver_to_gold_parquet.py $(SPARK_MASTER_CONTAINER):/opt/bitnami/spark/work/spark_silver_to_gold_parquet.py
