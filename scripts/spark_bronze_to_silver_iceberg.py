#!/usr/bin/env python3
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit


def table_exists(spark: SparkSession, full_table: str) -> bool:
    # full_table like "nessie.immo.dvf_silver"
    try:
        spark.sql(f"DESCRIBE {full_table}")
        return True
    except Exception:
        return False


def column_exists(spark: SparkSession, full_table: str, col_name: str) -> bool:
    # DESCRIBE returns rows with col_name / data_type / comment
    rows = spark.sql(f"DESCRIBE {full_table}").collect()
    for r in rows:
        c = r[0]
        if c is None:
            continue
        c = str(c).strip()
        if not c or c.startswith("#"):
            continue
        if c == col_name:
            return True
    return False


def ensure_column_exists(spark: SparkSession, full_table: str, col_name: str, col_type: str) -> None:
    if not column_exists(spark, full_table, col_name):
        spark.sql(f"ALTER TABLE {full_table} ADD COLUMN {col_name} {col_type}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--minio-endpoint", required=True)
    ap.add_argument("--access-key", required=True)
    ap.add_argument("--secret-key", required=True)
    ap.add_argument("--bucket", required=True)
    ap.add_argument("--nessie", required=True)
    ap.add_argument("--ref", required=True)
    ap.add_argument("--year", required=True, type=int)
    ap.add_argument("--dep", required=True)
    ap.add_argument("--ingest-id", required=True)
    ap.add_argument("--mode", choices=["append", "overwrite_partitions"], default="append")
    args = ap.parse_args()

    nessie_uri = args.nessie.rstrip("/")
    if not (nessie_uri.endswith("/api/v2") or nessie_uri.endswith("/api/v1")):
        nessie_uri = nessie_uri + "/api/v2"

    spark = (
        SparkSession.builder.appName("dvf-bronze-to-silver")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
        .config("spark.sql.catalog.nessie.uri", nessie_uri)
        .config("spark.sql.catalog.nessie.ref", args.ref)
        .config("spark.sql.catalog.nessie.warehouse", f"s3a://{args.bucket}/warehouse/iceberg")
        .config("spark.hadoop.fs.s3a.endpoint", args.minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", args.access_key)
        .config("spark.hadoop.fs.s3a.secret.key", args.secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false" if args.minio_endpoint.startswith("http://") else "true")
        .getOrCreate()
    )

    spark.sql("CREATE DATABASE IF NOT EXISTS nessie.immo")

    full_table = "nessie.immo.dvf_silver"

    # Create table if needed (with ingest_id)
    spark.sql(f"""
      CREATE TABLE IF NOT EXISTS {full_table} (
        date_mutation date,
        valeur_fonciere double,
        code_commune string,
        type_local string,
        surface_reelle_bati double,
        dep string,
        annee int,
        ingest_id string
      )
      USING iceberg
      PARTITIONED BY (dep, annee, ingest_id)
    """)

    # If table pre-existed without ingest_id, add it
    ensure_column_exists(spark, full_table, "ingest_id", "string")

    bronze_glob = f"s3a://{args.bucket}/raw/dvf/ingest={args.ingest_id}/year={args.year}/dep={args.dep}/*.csv.gz"
    print(f"Reading bronze from: {bronze_glob}")

    df = spark.read.option("header", True).csv(bronze_glob)

    # geo-dvf latest: snake_case columns
    df2 = (
        df.withColumn("date_mutation", col("date_mutation").cast("date"))
          .withColumn("valeur_fonciere", col("valeur_fonciere").cast("double"))
          .withColumn("surface_reelle_bati", col("surface_reelle_bati").cast("double"))
          .withColumn("code_commune", col("code_commune").cast("string"))
          .withColumn("type_local", col("type_local").cast("string"))
          .withColumn("dep", col("code_departement").cast("string"))
          .withColumn("annee", col("date_mutation").substr(1, 4).cast("int"))
          .withColumn("ingest_id", lit(args.ingest_id))
    )

    out_df = (
        df2.select(
            "date_mutation", "valeur_fonciere", "code_commune", "type_local",
            "surface_reelle_bati", "dep", "annee", "ingest_id"
        )
        .where(col("dep") == args.dep)
        .where(col("annee") == args.year)
    )

    writer = out_df.writeTo(full_table)
    if args.mode == "overwrite_partitions":
        writer.overwritePartitions()
    else:
        writer.append()

    print("OK: wrote to nessie.immo.dvf_silver")
    spark.stop()


if __name__ == "__main__":
    main()
