CREATE DATABASE IF NOT EXISTS immo;

CREATE TABLE IF NOT EXISTS immo.m_price_m2_commune_month
(
    code_commune String,
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


CREATE TABLE IF NOT EXISTS immo.ref_commune
(
    code_commune String,
    nom_commune String,
    dep String
)
ENGINE = MergeTree
ORDER BY (dep, code_commune)
SETTINGS index_granularity = 8192;


CREATE OR REPLACE VIEW immo.v_price_m2_commune_month AS
SELECT
    m.dep,
    m.ingest_id,
    m.code_commune,
    r.nom_commune,
    m.annee,
    m.mois,
    m.type_local,
    m.nb_ventes,
    m.prix_m2_median,
    m.prix_m2_p25,
    m.prix_m2_p75
FROM immo.m_price_m2_commune_month AS m
LEFT JOIN immo.ref_commune AS r
    ON r.dep = m.dep
   AND r.code_commune = m.code_commune;


CREATE OR REPLACE VIEW immo.v_price_m2_commune_year AS
SELECT
    m.dep,
    m.ingest_id,
    m.code_commune,
    r.nom_commune,
    m.annee,
    m.type_local,
    sum(m.nb_ventes_month) AS nb_ventes_year,
    quantileExactWeighted(0.50)(m.prix_m2_median, m.nb_ventes_month) AS prix_m2_median_year,
    quantileExactWeighted(0.25)(m.prix_m2_median, m.nb_ventes_month) AS prix_m2_p25_year,
    quantileExactWeighted(0.75)(m.prix_m2_median, m.nb_ventes_month) AS prix_m2_p75_year
FROM
(
    SELECT
        dep,
        ingest_id,
        code_commune,
        annee,
        type_local,
        prix_m2_median,
        nb_ventes AS nb_ventes_month
    FROM immo.m_price_m2_commune_month
) AS m
LEFT JOIN immo.ref_commune AS r
    ON r.dep = m.dep
   AND r.code_commune = m.code_commune
GROUP BY
    m.dep,
    m.ingest_id,
    m.code_commune,
    r.nom_commune,
    m.annee,
    m.type_local;


CREATE OR REPLACE VIEW immo.v_price_m2_commune_year_bi AS
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
