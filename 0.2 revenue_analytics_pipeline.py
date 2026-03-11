# ============================================================
# PROJECT: PySpark Revenue Analytics Pipeline
# FILE:    revenue_analytics_pipeline.py
# PURPOSE: End-to-end pipeline — ingest Parquet files from
#          storage, transform, reconcile, and output results.
#
# DESIGNED FOR: Databricks (Community Edition or workspace)
# ALSO RUNS IN: Any PySpark environment (local, AWS, Azure)
#
# PIPELINE STAGES:
#   1. Ingest raw Parquet files
#   2. Data quality validation
#   3. Cleansing & standardisation
#   4. Multi-source join & reconciliation
#   5. Anomaly detection
#   6. Output results as Delta table / CSV
#
# BUSINESS CONTEXT:
#   This pipeline replicates the analytical workflow used
#   in large-scale revenue reconciliation engagements —
#   handling 100M+ row datasets where SQL alone is too slow
#   and Databricks/PySpark is the appropriate tool.
# ============================================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, DateType, IntegerType
)
import logging

# ── SETUP ────────────────────────────────────────────────────────────────────

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s — %(levelname)s — %(message)s')
log = logging.getLogger(__name__)

spark = SparkSession.builder \
    .appName("RevenueReconciliationPipeline") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ── CONFIG ───────────────────────────────────────────────────────────────────
# In production: read from a config file or Databricks secret scope

CONFIG = {
    "source_path":      "/mnt/data/raw/sales_transactions/",
    "gl_path":          "/mnt/data/raw/gl_postings/",
    "bank_path":        "/mnt/data/raw/bank_settlements/",
    "output_path":      "/mnt/data/processed/reconciliation/",
    "reporting_period": "2024",          # filter to this year
    "variance_tolerance": 0.01,          # £0.01 tolerance for rounding
    "anomaly_threshold_std": 2.0,        # flag beyond 2 standard deviations
}


# ════════════════════════════════════════════════════════════
# STAGE 1: INGEST PARQUET FILES
# ════════════════════════════════════════════════════════════

def ingest_parquet(path: str, description: str):
    """
    Read Parquet files from a storage path.
    Logs row count and schema for audit trail.
    """
    log.info(f"Ingesting {description} from: {path}")
    df = spark.read.parquet(path)
    row_count = df.count()
    log.info(f"  {description}: {row_count:,} rows, {len(df.columns)} columns")
    return df


def ingest_all_sources():
    sales = ingest_parquet(CONFIG["source_path"], "Sales Transactions")
    gl    = ingest_parquet(CONFIG["gl_path"],     "GL Postings")
    bank  = ingest_parquet(CONFIG["bank_path"],   "Bank Settlements")
    return sales, gl, bank


# ════════════════════════════════════════════════════════════
# STAGE 2: DATA QUALITY VALIDATION
# ════════════════════════════════════════════════════════════

def validate_quality(df, name: str, critical_cols: list):
    """
    Run a quality scorecard on a dataframe.
    Returns a summary dict and logs results.
    Raises an exception if critical columns have nulls.
    """
    log.info(f"Running quality validation on: {name}")
    total = df.count()

    results = {"table": name, "total_rows": total, "checks": {}}

    for col in critical_cols:
        null_count = df.filter(F.col(col).isNull()).count()
        null_pct   = round(100.0 * null_count / total, 2) if total > 0 else 0
        results["checks"][col] = {"nulls": null_count, "null_pct": null_pct}
        log.info(f"  {col}: {null_count:,} nulls ({null_pct}%)")

        if null_count > 0 and col in critical_cols[:2]:   # first two = critical
            raise ValueError(
                f"QUALITY FAIL: Critical column '{col}' in '{name}' "
                f"has {null_count} nulls. Pipeline halted."
            )

    # Duplicate check
    key_col = critical_cols[0]
    dup_count = total - df.select(key_col).distinct().count()
    results["duplicates"] = dup_count
    log.info(f"  Duplicate {key_col}: {dup_count:,}")

    return results


# ════════════════════════════════════════════════════════════
# STAGE 3: CLEANSING & STANDARDISATION
# ════════════════════════════════════════════════════════════

def cleanse_sales(df):
    """Standardise and clean the sales transactions dataset."""
    return (
        df
        # Trim whitespace from string fields
        .withColumn("transaction_id",    F.trim(F.col("transaction_id")))
        .withColumn("customer_id",       F.trim(F.upper(F.col("customer_id"))))
        .withColumn("product_category",  F.trim(F.upper(F.col("product_category"))))

        # Cast dates
        .withColumn("transaction_date",  F.to_date(F.col("transaction_date")))

        # Ensure amounts are non-null and rounded to 2dp
        .withColumn("gross_revenue",     F.round(F.coalesce(F.col("gross_revenue"), F.lit(0)), 2))
        .withColumn("net_revenue",       F.round(F.coalesce(F.col("net_revenue"),   F.lit(0)), 2))
        .withColumn("tax_amount",        F.round(F.coalesce(F.col("tax_amount"),    F.lit(0)), 2))

        # Filter to reporting period
        .filter(F.year(F.col("transaction_date")) == int(CONFIG["reporting_period"]))

        # Drop confirmed duplicates (keep latest modified)
        .withColumn("rn", F.row_number().over(
            Window.partitionBy("transaction_id")
                  .orderBy(F.col("last_modified_date").desc())
        ))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )


def cleanse_gl(df):
    """Standardise GL postings."""
    return (
        df
        .withColumn("source_reference", F.trim(F.col("source_reference")))
        .withColumn("posting_date",     F.to_date(F.col("posting_date")))
        .withColumn("net_amount",       F.round(F.col("net_amount"), 2))
        .withColumn("debit_amount",     F.round(F.col("debit_amount"), 2))
        # Revenue accounts only
        .filter(F.col("account_type") == "REVENUE")
        .filter(F.year(F.col("posting_date")) == int(CONFIG["reporting_period"]))
        # Aggregate to transaction level (GL may have multiple journals)
        .groupBy("source_reference", "posting_date")
        .agg(
            F.sum("net_amount").alias("gl_net_amount"),
            F.sum("debit_amount").alias("gl_gross_amount"),
            F.sum("tax_posted").alias("gl_tax_amount"),
            F.count("journal_id").alias("journal_count")
        )
    )


def cleanse_bank(df):
    """Standardise bank settlements."""
    return (
        df
        .withColumn("reference_id",     F.trim(F.col("reference_id")))
        .withColumn("settlement_date",  F.to_date(F.col("settlement_date")))
        .withColumn("settled_amount",   F.round(F.col("settled_amount"), 2))
        .filter(F.year(F.col("settlement_date")) == int(CONFIG["reporting_period"]))
        .groupBy("reference_id", "settlement_date")
        .agg(
            F.sum("settled_amount").alias("bank_settled_amount"),
            F.sum("fee_deducted").alias("bank_fees"),
            F.count("*").alias("settlement_count")
        )
    )


# ════════════════════════════════════════════════════════════
# STAGE 4: THREE-WAY RECONCILIATION JOIN
# ════════════════════════════════════════════════════════════

def three_way_reconciliation(sales, gl, bank):
    """
    Join all three sources and categorise each transaction.
    """
    tol = CONFIG["variance_tolerance"]

    joined = (
        sales
        .join(gl,   sales.transaction_id == gl.source_reference,   "left")
        .join(bank, sales.transaction_id == bank.reference_id,      "left")
        .select(
            sales.transaction_id,
            sales.customer_id,
            sales.transaction_date,
            sales.product_category,
            sales.net_revenue.alias("source_net"),
            sales.gross_revenue.alias("source_gross"),
            gl.gl_net_amount,
            gl.gl_gross_amount,
            gl.posting_date,
            bank.bank_settled_amount,
            bank.settlement_date,
        )
    )

    # Add variance columns
    with_variances = (
        joined
        .withColumn("source_vs_gl_variance",
            F.round(
                F.coalesce(F.col("source_net"), F.lit(0))
                - F.coalesce(F.col("gl_net_amount"), F.lit(0)),
            2))
        .withColumn("gl_vs_bank_variance",
            F.round(
                F.coalesce(F.col("gl_net_amount"), F.lit(0))
                - F.coalesce(F.col("bank_settled_amount"), F.lit(0)),
            2))
        .withColumn("days_to_post",
            F.datediff(F.col("posting_date"), F.col("transaction_date")))
        .withColumn("days_to_settle",
            F.datediff(F.col("settlement_date"), F.col("posting_date")))
    )

    # Categorise
    reconciled = with_variances.withColumn(
        "reconciliation_status",
        F.when(F.col("gl_net_amount").isNull(),          "MISSING_IN_GL")
         .when(F.col("bank_settled_amount").isNull(),    "MISSING_IN_BANK")
         .when(
            (F.abs("source_vs_gl_variance") > tol) &
            (F.abs("gl_vs_bank_variance")   > tol),     "MULTI_SYSTEM_VARIANCE")
         .when(F.abs("source_vs_gl_variance") > tol,    "GL_VARIANCE")
         .when(F.abs("gl_vs_bank_variance")   > tol,    "BANK_VARIANCE")
         .when(
            (F.abs("source_vs_gl_variance") <= tol) &
            (F.abs("gl_vs_bank_variance")   <= tol) &
            (F.col("days_to_post")           > 5),      "TIMING_DIFFERENCE")
         .otherwise("MATCHED")
    )

    return reconciled


# ════════════════════════════════════════════════════════════
# STAGE 5: ANOMALY DETECTION
# ════════════════════════════════════════════════════════════

def detect_anomalies(df):
    """
    Flag transactions that are statistical outliers
    within their product category.
    """
    std_threshold = CONFIG["anomaly_threshold_std"]

    # Calculate mean and stddev per category
    stats = df.groupBy("product_category").agg(
        F.mean("source_net").alias("mean_val"),
        F.stddev("source_net").alias("std_val")
    )

    # Join stats back
    with_stats = df.join(stats, "product_category")

    # Calculate z-score and flag anomalies
    return with_stats.withColumn(
        "z_score",
        F.round(
            (F.col("source_net") - F.col("mean_val"))
            / F.when(F.col("std_val") == 0, F.lit(1)).otherwise(F.col("std_val")),
        3)
    ).withColumn(
        "anomaly_flag",
        F.when(F.abs(F.col("z_score")) > std_threshold, "ANOMALY")
         .otherwise("NORMAL")
    ).withColumn(
        "anomaly_priority",
        F.when(F.abs(F.col("z_score")) > std_threshold * 2, "HIGH")
         .when(F.abs(F.col("z_score")) > std_threshold,      "MEDIUM")
         .otherwise("NONE")
    )


# ════════════════════════════════════════════════════════════
# STAGE 6: SUMMARISE & OUTPUT
# ════════════════════════════════════════════════════════════

def produce_summary(df):
    """Executive-level reconciliation summary by period."""
    return (
        df
        .withColumn("period", F.date_format(F.col("transaction_date"), "yyyy-MM"))
        .groupBy("period")
        .agg(
            F.count("*").alias("total_transactions"),
            F.sum("source_net").alias("total_source_revenue"),
            F.sum(F.when(F.col("reconciliation_status") == "MATCHED",
                         F.col("source_net")).otherwise(0))
              .alias("matched_revenue"),
            F.sum(F.when(F.col("reconciliation_status") != "MATCHED",
                         F.abs(F.col("source_vs_gl_variance"))).otherwise(0))
              .alias("total_unreconciled"),
            F.count(F.when(F.col("anomaly_flag") == "ANOMALY", True))
              .alias("anomalies_detected")
        )
        .withColumn("reconciliation_rate_pct",
            F.round(
                100.0 * F.col("matched_revenue") / F.col("total_source_revenue"), 2
            ))
        .withColumn("rag_status",
            F.when(F.col("reconciliation_rate_pct") >= 99, "GREEN")
             .when(F.col("reconciliation_rate_pct") >= 95, "AMBER")
             .otherwise("RED"))
        .orderBy("period")
    )


def write_outputs(reconciled, summary):
    """Write results to output location."""
    out = CONFIG["output_path"]

    reconciled.write.mode("overwrite").parquet(f"{out}transaction_level/")
    log.info(f"Transaction-level output written to: {out}transaction_level/")

    summary.write.mode("overwrite").parquet(f"{out}summary/")
    log.info(f"Summary output written to: {out}summary/")

    # Also write summary as CSV for easy consumption
    summary.coalesce(1).write.mode("overwrite").option("header", "true") \
           .csv(f"{out}summary_csv/")
    log.info(f"Summary CSV written to: {out}summary_csv/")


# ════════════════════════════════════════════════════════════
# MAIN PIPELINE ORCHESTRATION
# ════════════════════════════════════════════════════════════

def run_pipeline():
    log.info("=" * 60)
    log.info("REVENUE RECONCILIATION PIPELINE — START")
    log.info("=" * 60)

    # Stage 1: Ingest
    sales_raw, gl_raw, bank_raw = ingest_all_sources()

    # Stage 2: Validate
    validate_quality(sales_raw, "Sales",  ["transaction_id", "net_revenue", "transaction_date"])
    validate_quality(gl_raw,    "GL",     ["source_reference", "net_amount"])
    validate_quality(bank_raw,  "Bank",   ["reference_id", "settled_amount"])

    # Stage 3: Cleanse
    log.info("Cleansing all sources...")
    sales_clean = cleanse_sales(sales_raw)
    gl_clean    = cleanse_gl(gl_raw)
    bank_clean  = cleanse_bank(bank_raw)

    # Stage 4: Reconcile
    log.info("Running three-way reconciliation...")
    reconciled  = three_way_reconciliation(sales_clean, gl_clean, bank_clean)

    # Stage 5: Anomaly detection
    log.info("Detecting anomalies...")
    reconciled  = detect_anomalies(reconciled)

    # Stage 6: Output
    summary = produce_summary(reconciled)
    write_outputs(reconciled, summary)

    # Print summary to console / notebook
    log.info("\n" + "=" * 40)
    log.info("PIPELINE COMPLETE — SUMMARY:")
    log.info("=" * 40)
    summary.show(truncate=False)

    log.info("Revenue Reconciliation Pipeline finished successfully.")
    return reconciled, summary


if __name__ == "__main__":
    reconciled_df, summary_df = run_pipeline()
