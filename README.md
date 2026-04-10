# 🏠 Immo Lakehouse — From Raw Data to Real Estate Insights

![GitHub repo size](https://img.shields.io/github/repo-size/limeconsulting/immo-lakehouse)
![GitHub last commit](https://img.shields.io/github/last-commit/limeconsulting/immo-lakehouse)
![GitHub license](https://img.shields.io/github/license/limeconsulting/immo-lakehouse)
![GitHub stars](https://img.shields.io/github/stars/limeconsulting/immo-lakehouse)

> A production-ready data product transforming French DVF data into actionable insights.
---

## ⚡ What you get

- 📊 Interactive dashboards (Superset)
- 📈 Price trends over time
- 🗺️ Multi-department analysis
- 🔎 Filtering by commune, property type, year

---

## 📸 Dashboard preview

_Add screenshots here (Superset dashboards)_

---

## 🧱 End-to-end architecture

```
Public DVF data
↓
MinIO (S3 storage)
↓
Iceberg (table format via Nessie)
↓
Spark (data processing)
↓
ClickHouse (analytics engine)
↓
Superset (BI dashboard)
```

---

## 🚀 Key capabilities

### Multi-department support

```bash
make apply DEPARTMENTS="33 40 75"
```
→ compare multiple regions instantly

### Multi-year analysis
```bash
YEARS="2020 2021 2022 2023 2024 2025"
```
→ detect long-term trends

### End-to-end pipeline
```bash
make apply
```
→ ingest → transform → serve → visualize

### 📊 Example use cases
#### 🏡 Investment analysis
```
Identify high-growth communes
Compare price per m² across regions
```
#### 🏗️ Market monitoring
```
Track monthly trends
Detect slowdowns or spikes
```
#### 🧭 Territorial comparison
```
Compare departments (urban vs coastal vs rural)
Analyze transaction volumes
```
### ⚙️ Quickstart
```bash
cp .env.example .env
docker-compose up -d
make apply DEPARTMENTS="40"
```
Access:
```
Superset → http://localhost:8088
ClickHouse → http://localhost:8124
```
### 🔐 Enterprise-ready (by design)
```
HTTPS (via reverse proxy)
SSO integration (OAuth / LDAP / SAML)
RBAC (data access control)
network isolation
```
### 💡 Positioning

This is not just a data pipeline.

It’s a data product:
```
Data → Processing → Insight → Decision
```
### 🧭 Roadmap
```bash
 dashboard UX improvements
 row-level security
 additional datasets
 deployment templates (cloud / on-prem)
```
### 🧠 Philosophy
```
Under-promise → over-deliver
```
### 📬 Contact
```
Lime Consulting
https://www.lime-consulting.com
```
