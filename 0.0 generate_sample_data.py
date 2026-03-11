# ============================================================
# PROJECT 2: PySpark Revenue Analytics Pipeline
# FILE:      generate_sample_data.py
# PURPOSE:   Generate realistic sample Parquet files so you
#            can run the full pipeline without needing a
#            real data source.
#
# RUN THIS FIRST before running the main pipeline.
# Output: three Parquet folders in /tmp/sample_data/
# ============================================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
import random
from datetime import date, timedelta

spark = SparkSession.builder \
    .appName("GenerateSampleData") \
    .getOrCreate()

random.seed(42)

# ── CONFIG ───────────────────────────────────────────────────
N_TRANSACTIONS = 50_000          # adjust up to test at scale
OUTPUT_BASE    = "/tmp/sample_data"

CATEGORIES = ["Electronics", "Furniture", "Software", "Services", "Clothing"]
CUSTOMERS  = [f"C{i:04d}" for i in range(1, 201)]   # 200 customers
PRODUCTS   = [f"P{i:03d}"  for i in range(1, 51)]    # 50 products
REGIONS    = ["London", "Midlands", "North", "South", "Scotland"]


def random_date(start_year=2024, end_year=2024):
    start = date(start_year, 1, 1)
    end   = date(end_year, 12, 31)
    delta = end - start
    return start + timedelta(days=random.randint(0, delta.days))


# ── SALES TRANSACTIONS ───────────────────────────────────────

print(f"Generating {N_TRANSACTIONS:,} sales transactions...")

sales_rows = []
for i in range(1, N_TRANSACTIONS + 1):
    cat      = random.choice(CATEGORIES)
    base_val = {
        "Electronics": random.uniform(100, 2000),
        "Furniture":   random.uniform(50,  800),
        "Software":    random.uniform(200, 1500),
        "Services":    random.uniform(300, 3000),
        "Clothing":    random.uniform(20,  300),
    }[cat]

    # Inject anomalies (~1%)
    if random.random() < 0.01:
        base_val *= random.uniform(10, 50)

    txn_date   = random_date()
    gross      = round(base_val * 1.2, 2)
    net        = round(base_val, 2)
    tax        = round(gross - net, 2)

    # Some transactions are subscriptions (have service dates)
    svc_start  = txn_date.isoformat() if cat in ["Software", "Services"] and random.random() > 0.5 else None
    svc_end    = (txn_date + timedelta(days=365)).isoformat() if svc_start else None

    # ~2% duplicates
    txn_id = f"TXN-{i:07d}"
    if random.random() < 0.02:
        txn_id = f"TXN-{random.randint(1, i):07d}"

    sales_rows.append((
        txn_id,
        random.choice(CUSTOMERS),
        random.choice(PRODUCTS),
        cat,
        txn_date.isoformat(),
        random.randint(1, 10),
        round(base_val / random.randint(1, 10), 2),
        gross, net, tax,
        svc_start, svc_end,
        "SYSTEM_A",
        f"2024-{random.randint(1,12):02d}-{random.randint(1,28):02d} 09:00:00"
    ))

sales_schema = StructType([
    StructField("transaction_id",   StringType()),
    StructField("customer_id",      StringType()),
    StructField("product_code",     StringType()),
    StructField("product_category", StringType()),
    StructField("transaction_date", StringType()),
    StructField("quantity",         IntegerType()),
    StructField("unit_price",       DoubleType()),
    StructField("gross_revenue",    DoubleType()),
    StructField("net_revenue",      DoubleType()),
    StructField("tax_amount",       DoubleType()),
    StructField("service_start_date", StringType()),
    StructField("service_end_date",   StringType()),
    StructField("source_system",    StringType()),
    StructField("last_modified_date", StringType()),
])

sales_df = spark.createDataFrame(sales_rows, schema=sales_schema)
sales_df.write.mode("overwrite").parquet(f"{OUTPUT_BASE}/sales_transactions")
print(f"  Sales: {sales_df.count():,} rows written")


# ── GL POSTINGS ──────────────────────────────────────────────

print("Generating GL postings...")

# Read back sales to align on transaction IDs
sales_ids = [r.transaction_id for r in
             spark.read.parquet(f"{OUTPUT_BASE}/sales_transactions")
             .select("transaction_id", "net_revenue", "gross_revenue",
                     "tax_amount", "transaction_date")
             .distinct().collect()]

gl_rows = []
for j, row in enumerate(
    spark.read.parquet(f"{OUTPUT_BASE}/sales_transactions")
        .select("transaction_id","net_revenue","gross_revenue","tax_amount","transaction_date")
        .distinct().collect()
):
    # ~3% missing from GL
    if random.random() < 0.03:
        continue
    # ~2% GL variance (wrong amount posted)
    multiplier = random.uniform(0.95, 1.05) if random.random() < 0.02 else 1.0
    post_date  = (date.fromisoformat(row.transaction_date)
                  + timedelta(days=random.randint(0, 3))).isoformat()

    gl_rows.append((
        f"GL-{j+1:07d}",
        f"JNL-{j+1:07d}",
        row.transaction_id,
        "REVENUE",
        post_date,
        round(row.gross_revenue * multiplier, 2),
        round(row.net_revenue * multiplier, 2),
        round(row.tax_amount * multiplier, 2),
        "auto_post" if random.random() > 0.1 else "manual"
    ))

gl_schema = StructType([
    StructField("posting_id",       StringType()),
    StructField("journal_id",       StringType()),
    StructField("source_reference", StringType()),
    StructField("account_type",     StringType()),
    StructField("posting_date",     StringType()),
    StructField("debit_amount",     DoubleType()),
    StructField("net_amount",       DoubleType()),
    StructField("tax_posted",       DoubleType()),
    StructField("posted_by",        StringType()),
])

gl_df = spark.createDataFrame(gl_rows, schema=gl_schema)
gl_df.write.mode("overwrite").parquet(f"{OUTPUT_BASE}/gl_postings")
print(f"  GL: {gl_df.count():,} rows written")


# ── BANK SETTLEMENTS ─────────────────────────────────────────

print("Generating bank settlements...")

bank_rows = []
for k, row in enumerate(
    spark.read.parquet(f"{OUTPUT_BASE}/sales_transactions")
        .select("transaction_id","net_revenue","transaction_date")
        .distinct().collect()
):
    # ~2% missing from bank
    if random.random() < 0.02:
        continue
    settle_date = (date.fromisoformat(row.transaction_date)
                   + timedelta(days=random.randint(1, 5))).isoformat()
    fee = round(row.net_revenue * 0.01, 2)

    bank_rows.append((
        f"BNK-{k+1:07d}",
        row.transaction_id,
        settle_date,
        round(row.net_revenue, 2),
        fee,
        random.choice(["CARD", "BACS", "CHAPS"])
    ))

bank_schema = StructType([
    StructField("settlement_id",    StringType()),
    StructField("reference_id",     StringType()),
    StructField("settlement_date",  StringType()),
    StructField("settled_amount",   DoubleType()),
    StructField("fee_deducted",     DoubleType()),
    StructField("payment_method",   StringType()),
])

bank_df = spark.createDataFrame(bank_rows, schema=bank_schema)
bank_df.write.mode("overwrite").parquet(f"{OUTPUT_BASE}/bank_settlements")
print(f"  Bank: {bank_df.count():,} rows written")

print(f"\nAll sample data written to: {OUTPUT_BASE}/")
print("Update CONFIG paths in revenue_analytics_pipeline.py to:")
print(f'  "source_path": "{OUTPUT_BASE}/sales_transactions/"')
print(f'  "gl_path":     "{OUTPUT_BASE}/gl_postings/"')
print(f'  "bank_path":   "{OUTPUT_BASE}/bank_settlements/"')
