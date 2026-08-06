# Fabric notebook source
# "B2S - Xray" — Bronze-to-Silver for the Xray source. The Bronze extractor
# already landed ONE flat record per test run (not deeply nested JSON like the
# Jira issues pull), so this notebook is light: type the columns, drop rows with
# no run id, deduplicate, land to Silver. NO API calls. Run AFTER "S2B - Xray".
# Attach Bronze and Silver lakehouses, plus env_medallion_toolkit.

# CELL ********************
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType
import fabric_medallion_toolkit as fmt

BRONZE_SCHEMA = "Bronze.xray"
SILVER_SCHEMA = "Silver.xray"

# land_records (used by S2B - Xray) never writes per-field columns -- every
# record lands as a single JSON STRING inside raw_data, plus generic metadata
# (entity, extracted_at, ingest_date, load_id, primary_key, source_system).
# That's true for every source using the wheel's standard Bronze landing, not
# just Xray -- Jira's B2S notebook unpacks the same shape via auto_standardize
# (schema-inferred, handles Jira's deep nesting). Xray's record is a simple
# FLAT dict with no nested objects, so an explicit schema here is simpler and
# more deterministic than inference -- it's exactly the dict shape
# S2B - Xray's `records.append({...})` writes, kept in sync with it by hand.
RAW_SCHEMA = StructType([
    StructField("run_id", StringType()),
    StructField("execution_issue_id", StringType()),
    StructField("execution_issue_key", StringType()),
    StructField("execution_summary", StringType()),
    StructField("test_issue_id", StringType()),
    StructField("test_issue_key", StringType()),
    StructField("status_name", StringType()),
    StructField("test_type", StringType()),
    StructField("started_on", StringType()),
    StructField("finished_on", StringType()),
    StructField("executed_by_id", StringType()),
])

# CELL ********************
if not spark.catalog.tableExists(f"{BRONZE_SCHEMA}.test_runs"):
    print("No Bronze.xray.test_runs table -- S2B - Xray hasn't run or landed nothing. Nothing to do.")
else:
    bronze_df = spark.table(f"{BRONZE_SCHEMA}.test_runs")

    # Unpack raw_data -- this is the step that was missing. Everything below
    # this point is unchanged from before: it just now runs against real
    # columns instead of a raw_data string that had none of these names on it.
    df = bronze_df.withColumn("parsed", F.from_json(F.col("raw_data"), RAW_SCHEMA)).select("parsed.*")

    # Timestamps arrive as ISO-8601 strings ("2026-03-14T09:22:00Z"). to_timestamp
    # parses them; a bad/empty string becomes NULL rather than erroring the batch.
    clean = (
        df
        .withColumn("started_on",  F.to_timestamp("started_on"))
        .withColumn("finished_on", F.to_timestamp("finished_on"))
        # An unfinished run (TO DO / EXECUTING) has a null finished_on -- that's
        # real, not dirty, so it's kept. A null RUN ID, though, means a record we
        # can't key or dedupe, so those are dropped.
        .filter(F.col("run_id").isNotNull())
    )

    # A test run id is globally unique in Xray, so it's the dedupe key. The
    # watermark can legitimately re-pull an execution whose results changed, so
    # the same run_id can land twice across runs -- keep the latest by
    # finished_on (nulls last, so a now-finished run supersedes its earlier
    # unfinished landing).
    from pyspark.sql.window import Window
    w = Window.partitionBy("run_id").orderBy(F.col("finished_on").desc_nulls_last())
    deduped = (
        clean
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    fmt.write_silver_table(spark, deduped, f"{SILVER_SCHEMA}.test_runs", merge_key="run_id")
    print(f"B2S - Xray complete: {deduped.count()} test runs in {SILVER_SCHEMA}.test_runs")
