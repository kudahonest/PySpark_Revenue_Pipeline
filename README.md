# Project 2: PySpark Revenue Analytics Pipeline (Databricks)

## Business Problem

When datasets exceed tens of millions of rows, SQL alone becomes slow and impractical. This project demonstrates an end-to-end **PySpark pipeline on Databricks** — the
same architecture used in production for large-scale analytics engagements — ingesting Parquet files from cloud storage, applying multi-stage transformations, running 
reconciliation logic, and writing results as Delta tables.

**Why Parquet?** Parquet is the standard file format for big data analytics. It is columnar (reads only the columns you need), compressed (smaller storage), and natively
supported by Databricks, Azure Data Lake, and AWS S3.

---

## Dataset

Uses UK government open data (ONS Retail Sales Index + synthetic transaction extracts) saved as Parquet to simulate ingestion from Azure Storage Explorer into Databricks — 
the exact workflow used professionally.

---

## Pipeline Stages

```
[Parquet files on storage]
        │
        ▼
[Stage 1] Ingest — read Parquet, log row counts, and schema
        │
        ▼
[Stage 2] Validate — null checks, duplicate detection, quality scorecard
        │  (pipeline halts if critical columns fail)
        ▼
[Stage 3] Cleanse — trim, cast, round, filter, deduplicate
        │
        ▼
[Stage 4] Three-way reconciliation — join sales + GL + bank, classify
        │
        ▼
[Stage 5] Anomaly detection — z-score flagging within product categories
        │
        ▼
[Stage 6] Output — write to Delta table + CSV summary
```

---

## Key Techniques Demonstrated

**PySpark / Databricks:**
- `spark.read.parquet()` — Parquet file ingestion
- `Window` functions: `row_number`, `partitionBy`, `orderBy`
- `F.when / F.otherwise` — CASE-equivalent classification
- `groupBy + agg` — aggregation pipeline
- `F.datediff`, `F.date_format`, `F.coalesce` — date and null handling
- `.write.mode("overwrite").parquet()` — Delta/Parquet output
- Statistical anomaly detection using `F.mean` + `F.stddev`

**Engineering:**
- Modular function design — each stage is independently testable
- Logging throughout for audit trail
- Config-driven (paths, thresholds as variables — not hardcoded)
- Adaptive query execution enabled for performance at scale

---

## How to Run

**On Databricks Community Edition (free):**
1. Create a free account at community.cloud.databricks.com
2. Create a new cluster (default settings are fine)
3. Import `revenue_analytics_pipeline.py` as a notebook
4. Update `CONFIG` paths to your storage locations
5. Run all cells

**Locally (with PySpark installed):**
```bash
pip install pyspark
python revenue_analytics_pipeline.py
```

---

## How This Maps to Real Work

In production at BDO UK, I use this exact architecture to:
- Ingest Parquet files from Azure Storage Explorer into Databricks
- Apply multi-stage SQL and PySpark transformations at 300M+ row scale
- Automate end-to-end analytics jobs as scheduled Databricks pipelines
- Write results to Delta tables for downstream Power BI consumption

This project is the public, illustrative version of that workflow.

---

## Tools Used

`PySpark` &nbsp; `Databricks` &nbsp; `Parquet` &nbsp; `Delta Lake` &nbsp;
`Azure Storage` &nbsp; `Python` &nbsp; `Window Functions` &nbsp; `ETL Pipeline Design`
