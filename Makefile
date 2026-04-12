SHELL := /bin/bash

INGEST_ID ?= $(shell date -u +%Y-%m-%dT%H%M%SZ)
DEPARTMENTS ?= 40
YEARS ?= 2020 2021 2022 2023 2024 2025

ifeq ($(DEPARTMENTS),all)
  DEPARTMENTS_RESOLVED := $(shell seq -w 01 95)
else
  DEPARTMENTS_RESOLVED := $(DEPARTMENTS)
endif

ENV ?= .env
include $(ENV)
export

CH_CONTAINER ?= $(shell docker ps -qf name=clickhouse | head -n 1)
SPARK_MASTER_CONTAINER ?= $(shell (docker ps -qf name=spark || true) | head -n 1)

SPARK_PACKAGES ?= org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,org.apache.hadoop:hadoop-aws:3.3.6

# Examples:
#   make plan
#   make apply
#   make apply DEPARTMENTS=40
#   make apply DEPARTMENTS="33 40 75"
#   make apply DEPARTMENTS=all YEARS="2024 2025"

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
	@echo "  scope: deps=$(DEPARTMENTS_RESOLVED) years=$(YEARS) ingest_id=$(INGEST_ID)"

apply:
	$(MAKE) ingest-all DEPARTMENTS="$(DEPARTMENTS)" YEARS="$(YEARS)" INGEST_ID="$(INGEST_ID)"
	$(MAKE) silver-all DEPARTMENTS="$(DEPARTMENTS)" YEARS="$(YEARS)" INGEST_ID="$(INGEST_ID)"
	$(MAKE) gold-all   DEPARTMENTS="$(DEPARTMENTS)" YEARS="$(YEARS)" INGEST_ID="$(INGEST_ID)"
	$(MAKE) ch-init
	$(MAKE) ch-load-all DEPARTMENTS="$(DEPARTMENTS)" YEARS="$(YEARS)" INGEST_ID="$(INGEST_ID)"
	$(MAKE) validate

check:
	@command -v docker >/dev/null || (echo "docker missing" && exit 1)
	@docker ps >/dev/null || (echo "docker not running / no permission" && exit 1)
	@command -v parallel >/dev/null || (echo "parallel missing — apt install parallel/yum install parallel/ dnf install parallel" && exit 1)
	@test -f $(ENV) || (echo "Missing $(ENV)" && exit 1)
	@test -n "$(CH_CONTAINER)" || (echo "ClickHouse container not found" && exit 1)
	@test -n "$(SPARK_MASTER_CONTAINER)" || (echo "Spark container not found" && exit 1)
	@echo "OK"

print:
	@echo "ENV=$(ENV)"
	@echo "INGEST_ID=$(INGEST_ID)"
	@echo "DEPARTMENTS=$(DEPARTMENTS)"
	@echo "DEPARTMENTS_RESOLVED=$(DEPARTMENTS_RESOLVED)"
	@echo "YEARS=$(YEARS)"
	@echo "MINIO_ENDPOINT=$(MINIO_ENDPOINT)"
	@echo "MINIO_ENDPOINT_DOCKER=$(MINIO_ENDPOINT_DOCKER)"
	@echo "MINIO_BUCKET=$(MINIO_BUCKET)"
	@echo "NESSIE_ENDPOINT=$(NESSIE_ENDPOINT)"
	@echo "NESSIE_REF=$(NESSIE_REF)"
	@echo "CH_CONTAINER=$(CH_CONTAINER)"
	@echo "SPARK_MASTER_CONTAINER=$(SPARK_MASTER_CONTAINER)"

logs: 
	mkdir -p logs

ingest:
	@test -n "$(YEAR)" || (echo "Missing YEAR=YYYY" && exit 1)
	@test -n "$(DEPARTMENT)" || (echo "Missing DEPARTMENT=XX" && exit 1)
	python3 scripts/ingest_dvf.py \
	  --year "$(YEAR)" \
	  --dep "$(DEPARTMENT)" \
	  --ingest-id "$(INGEST_ID)" \
	  --minio-endpoint "$(MINIO_ENDPOINT)" \
	  --access-key "$(MINIO_ACCESS_KEY)" \
	  --secret-key "$(MINIO_SECRET_KEY)" \
	  --bucket "$(MINIO_BUCKET)"

ingest-all: logs
	parallel \
        --jobs 4 \
        --halt soon,fail=1 \
        --joblog logs/ingest-$(INGEST_ID).log \
        $(MAKE) ingest YEAR={1} DEPARTMENT={2} INGEST_ID="$(INGEST_ID)" \
        ::: $(YEARS) \
        ::: $(DEPARTMENTS_RESOLVED)

silver: spark-sync
	@test -n "$(YEAR)" || (echo "Missing YEAR=YYYY" && exit 1)
	@test -n "$(DEPARTMENT)" || (echo "Missing DEPARTMENT=XX" && exit 1)
	docker exec -i $(SPARK_MASTER_CONTAINER) spark-submit \
	  --master spark://spark:7077 \
	  --packages "$(SPARK_PACKAGES)" \
	  /opt/bitnami/spark/work/spark_bronze_to_silver_iceberg.py \
	  --minio-endpoint "$(MINIO_ENDPOINT_DOCKER)" \
	  --access-key "$(MINIO_ACCESS_KEY)" \
	  --secret-key "$(MINIO_SECRET_KEY)" \
	  --bucket "$(MINIO_BUCKET)" \
	  --nessie "$(NESSIE_ENDPOINT)" \
	  --ref "$(NESSIE_REF)" \
	  --year "$(YEAR)" \
	  --dep "$(DEPARTMENT)" \
	  --ingest-id "$(INGEST_ID)" \
	  --mode overwrite_partitions

silver-all:
	@for d in $(DEPARTMENTS_RESOLVED); do \
	  for y in $(YEARS); do \
	    echo "== silver $$y dep=$$d ingest=$(INGEST_ID) =="; \
	    $(MAKE) silver YEAR=$$y DEPARTMENT=$$d INGEST_ID="$(INGEST_ID)"; \
	  done; \
	done

gold: spark-sync
	@test -n "$(YEAR)" || (echo "Missing YEAR=YYYY" && exit 1)
	@test -n "$(DEPARTMENT)" || (echo "Missing DEPARTMENT=XX" && exit 1)
	docker exec -i $(SPARK_MASTER_CONTAINER) spark-submit \
	  --master spark://spark:7077 \
	  --packages "$(SPARK_PACKAGES)" \
	  /opt/bitnami/spark/work/spark_silver_to_gold_parquet.py \
	  --minio-endpoint "$(MINIO_ENDPOINT_DOCKER)" \
	  --access-key "$(MINIO_ACCESS_KEY)" \
	  --secret-key "$(MINIO_SECRET_KEY)" \
	  --bucket "$(MINIO_BUCKET)" \
	  --nessie "$(NESSIE_ENDPOINT)" \
	  --ref "$(NESSIE_REF)" \
	  --year "$(YEAR)" \
	  --dep "$(DEPARTMENT)" \
	  --ingest-id "$(INGEST_ID)"

gold-all:
	@for d in $(DEPARTMENTS_RESOLVED); do \
	  for y in $(YEARS); do \
	    echo "== gold $$y dep=$$d ingest=$(INGEST_ID) =="; \
	    $(MAKE) gold YEAR=$$y DEPARTMENT=$$d INGEST_ID="$(INGEST_ID)"; \
	  done; \
	done

ch-init:
	docker exec -i $(CH_CONTAINER) clickhouse-client < sql/clickhouse_schema.sql

ch-load:
	@test -n "$(YEAR)" || (echo "Missing YEAR=YYYY" && exit 1)
	@test -n "$(DEPARTMENT)" || (echo "Missing DEPARTMENT=XX" && exit 1)
	@test -n "$(INGEST_ID)" || (echo "Missing INGEST_ID=..." && exit 1)
	@echo "CH load dep=$(DEPARTMENT) year=$(YEAR) ingest=$(INGEST_ID)"
	@docker exec -i $(CH_CONTAINER) clickhouse-client --multiquery --query "\
	ALTER TABLE immo.m_price_m2_commune_month DELETE \
	  WHERE annee=toUInt16($(YEAR)) AND dep='$(DEPARTMENT)' AND ingest_id='$(INGEST_ID)'; \
	INSERT INTO immo.m_price_m2_commune_month \
	(code_commune, nom_commune, annee, mois, type_local, nb_ventes, prix_m2_median, prix_m2_p25, prix_m2_p75, ingest_id, dep) \
	SELECT code_commune, nom_commune, annee, mois, type_local, nb_ventes, prix_m2_median, prix_m2_p25, prix_m2_p75, \
	       '$(INGEST_ID)' AS ingest_id, '$(DEPARTMENT)' AS dep \
	FROM s3( \
	  '$(MINIO_ENDPOINT_DOCKER)/$(MINIO_BUCKET)/gold/m_price_m2_commune_month/year=$(YEAR)/dep=$(DEPARTMENT)/*.parquet', \
	  '$(MINIO_ACCESS_KEY)', '$(MINIO_SECRET_KEY)', 'Parquet' \
	);"

ch-load-all:
	@for d in $(DEPARTMENTS_RESOLVED); do \
	  for y in $(YEARS); do \
	    echo "== ch-load $$y dep=$$d ingest=$(INGEST_ID) =="; \
	    $(MAKE) ch-load YEAR=$$y DEPARTMENT=$$d INGEST_ID="$(INGEST_ID)"; \
	  done; \
	done

validate:
	docker exec -i $(CH_CONTAINER) clickhouse-client --query \
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
	echo "Resetting Nessie $$NESSIE_CONTAINER"; \
	docker stop "$$NESSIE_CONTAINER" >/dev/null; \
	docker start "$$NESSIE_CONTAINER" >/dev/null; \
	echo "Lakehouse cleaned."

clean-ch:
	@test -n "$(CH_CONTAINER)" || (echo "ClickHouse container not found" && exit 1)
	@echo "Dropping ClickHouse database: immo"
	@docker exec -i $(CH_CONTAINER) clickhouse-client --multiquery --query "\
		DROP VIEW IF EXISTS immo.v_price_m2_commune_month_bi; \
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
