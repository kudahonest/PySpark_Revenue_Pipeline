# ============================================================
# PROJECT 2: PySpark Revenue Analytics Pipeline
# FILE:      data_quality_checks.py
# PURPOSE:   Standalone data quality module.
#            Import and use in any PySpark pipeline.
#
# USAGE:
#   from data_quality_checks import DataQualityChecker
#   checker = DataQualityChecker(spark)
#   report  = checker.full_report(df, "sales_transactions")
#   checker.assert_no_critical_nulls(df, ["transaction_id"])
# ============================================================

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from typing import List, Dict, Optional
import logging

log = logging.getLogger(__name__)


class DataQualityChecker:
    """
    Reusable data quality assessment for PySpark DataFrames.
    Produces scorecards, flags issues, and can halt pipelines
    on critical failures.
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def null_report(self, df: DataFrame, name: str = "DataFrame") -> DataFrame:
        """
        Return a DataFrame showing null counts and % per column.
        """
        total = df.count()
        null_counts = [
            F.sum(F.col(c).isNull().cast("int")).alias(c)
            for c in df.columns
        ]
        nulls = df.select(null_counts).collect()[0].asDict()

        rows = [
            (col, nulls[col], round(100.0 * nulls[col] / total, 2) if total > 0 else 0.0)
            for col in df.columns
        ]
        schema = ["column_name", "null_count", "null_pct"]
        result = self.spark.createDataFrame(rows, schema=schema)

        log.info(f"Null report for {name} ({total:,} rows):")
        result.show(truncate=False)
        return result

    def duplicate_report(self, df: DataFrame, key_cols: List[str]) -> Dict:
        """
        Check for duplicate records on specified key columns.
        Returns a dict with counts and a sample DataFrame.
        """
        total      = df.count()
        distinct   = df.select(key_cols).distinct().count()
        dup_count  = total - distinct

        duplicates = (
            df.groupBy(key_cols)
              .count()
              .filter(F.col("count") > 1)
              .orderBy(F.col("count").desc())
        )

        log.info(f"Duplicate report on {key_cols}:")
        log.info(f"  Total rows:    {total:,}")
        log.info(f"  Distinct keys: {distinct:,}")
        log.info(f"  Duplicate rows:{dup_count:,}")

        return {
            "total_rows": total,
            "distinct_keys": distinct,
            "duplicate_row_count": dup_count,
            "duplicate_keys": duplicates
        }

    def value_range_check(self,
                          df: DataFrame,
                          col: str,
                          min_val: Optional[float] = None,
                          max_val: Optional[float] = None) -> DataFrame:
        """
        Flag records where a numeric column is outside expected range.
        """
        condition = F.lit(False)
        if min_val is not None:
            condition = condition | (F.col(col) < min_val)
        if max_val is not None:
            condition = condition | (F.col(col) > max_val)

        out_of_range = df.filter(condition)
        count = out_of_range.count()
        log.info(f"Range check on '{col}' [{min_val}, {max_val}]: {count:,} out-of-range rows")
        return out_of_range

    def date_range_check(self, df: DataFrame, date_col: str,
                         start: str, end: str) -> DataFrame:
        """Flag records where a date column falls outside expected range."""
        out = df.filter(
            (F.col(date_col) < F.lit(start)) |
            (F.col(date_col) > F.lit(end))
        )
        log.info(f"Date range check on '{date_col}' [{start} to {end}]: "
                 f"{out.count():,} out-of-range rows")
        return out

    def assert_no_critical_nulls(self, df: DataFrame, critical_cols: List[str]):
        """
        Raise ValueError if any critical column contains nulls.
        Use this as a pipeline gate — halt processing if data is bad.
        """
        for col in critical_cols:
            null_count = df.filter(F.col(col).isNull()).count()
            if null_count > 0:
                raise ValueError(
                    f"CRITICAL QUALITY FAILURE: Column '{col}' has {null_count:,} nulls. "
                    f"Pipeline halted. Investigate source data before proceeding."
                )
        log.info(f"Critical null check PASSED for: {critical_cols}")

    def full_report(self, df: DataFrame, name: str,
                    key_cols: Optional[List[str]] = None) -> Dict:
        """
        Run all quality checks and return a summary dictionary.
        """
        log.info(f"\n{'='*50}")
        log.info(f"DATA QUALITY REPORT: {name}")
        log.info(f"{'='*50}")

        total = df.count()
        log.info(f"Total rows: {total:,}")
        log.info(f"Columns:    {len(df.columns)}")

        null_df  = self.null_report(df, name)
        dup_info = self.duplicate_report(df, key_cols or [df.columns[0]])

        # Summary stats for numeric columns
        numeric_cols = [f.name for f in df.schema.fields
                        if str(f.dataType) in ("DoubleType", "FloatType",
                                               "LongType", "IntegerType",
                                               "DecimalType")]
        if numeric_cols:
            log.info(f"\nNumeric column summary:")
            df.select(numeric_cols).describe().show()

        return {
            "table":           name,
            "total_rows":      total,
            "column_count":    len(df.columns),
            "null_report":     null_df,
            "duplicate_info":  dup_info,
        }


# ── STANDALONE USAGE EXAMPLE ─────────────────────────────────────────────────

if __name__ == "__main__":
    spark = SparkSession.builder.appName("DQExample").getOrCreate()

    # Create a small example DataFrame
    data = [
        ("TXN-001", "C001", "2024-01-05",  2000.00),
        ("TXN-002", "C002", "2024-01-08",   500.00),
        ("TXN-002", "C002", "2024-01-08",   500.00),   # duplicate
        ("TXN-003", None,   "2024-01-10",  2000.00),   # null customer
        ("TXN-004", "C001", "2024-01-15",    -5.00),   # negative — suspicious
    ]
    df = spark.createDataFrame(data,
         ["transaction_id", "customer_id", "transaction_date", "net_revenue"])

    checker = DataQualityChecker(spark)
    report  = checker.full_report(df, "example_sales", key_cols=["transaction_id"])

    # This would raise ValueError if run on the example data:
    # checker.assert_no_critical_nulls(df, ["transaction_id", "customer_id"])

    checker.value_range_check(df, "net_revenue", min_val=0)
