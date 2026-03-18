CREATE DATABASE IF NOT EXISTS immo;

CREATE TABLE IF NOT EXISTS immo.m_price_m2_commune_month
(
    code_commune String,
    nom_commune String,
    annee UInt16,
    mois UInt8,
    type_local LowCardinality(String),
    nb_ventes UInt32,
    prix_m2_median Float64,
    prix_m2_p25 Float64,
    prix_m2_p75 Float64,
    ingest_id String DEFAULT '',
    dep FixedString(2) DEFAULT ''
)
ENGINE = MergeTree
PARTITION BY (annee, mois)
ORDER BY (code_commune, type_local)
SETTINGS index_granularity = 8192;


CREATE OR REPLACE VIEW immo.v_price_m2_commune_month AS
SELECT
    dep,
    ingest_id,
    code_commune,
    nom_commune,
    annee,
    mois,
    type_local,
    nb_ventes,
    prix_m2_median,
    prix_m2_p25,
    prix_m2_p75
FROM immo.m_price_m2_commune_month;

CREATE OR REPLACE VIEW immo.v_price_m2_commune_year AS
SELECT
    dep,
    ingest_id,
    code_commune,
    nom_commune,
    annee,
    type_local,
    sum(nb_ventes) AS nb_ventes_year,
    quantileExactWeighted(0.50)(prix_m2_median, nb_ventes) AS prix_m2_median_year,
    quantileExactWeighted(0.25)(prix_m2_median, nb_ventes) AS prix_m2_p25_year,
    quantileExactWeighted(0.75)(prix_m2_median, nb_ventes) AS prix_m2_p75_year
FROM immo.m_price_m2_commune_month
GROUP BY
    dep,
    ingest_id,
    code_commune,
    nom_commune,
    annee,
    type_local;


CREATE OR REPLACE VIEW immo.v_price_m2_commune_year_bi AS
SELECT
    dep,
    code_commune,
    nom_commune,
    ingest_id,
    annee,
    type_local,
    nb_ventes_year,
    prix_m2_median_year,
    prix_m2_p25_year,
    prix_m2_p75_year
FROM immo.v_price_m2_commune_year;


CREATE OR REPLACE VIEW immo.v_price_m2_commune_year_current AS
SELECT
    dep,
    nom_commune,
    code_commune,
    ingest_id,
    annee,
    type_local,
    nb_ventes_year,
    prix_m2_median_year,
    prix_m2_p25_year,
    prix_m2_p75_year
FROM immo.v_price_m2_commune_year
WHERE ingest_id =
(
    SELECT max(ingest_id)
    FROM immo.m_price_m2_commune_month
);
