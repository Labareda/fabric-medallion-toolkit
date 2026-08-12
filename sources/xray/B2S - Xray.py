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

    # upsert_delta -- NOT write_silver_table, which doesn't exist in this
    # wheel (my earlier mistake). It's the same MERGE-based upsert save_watermark
    # uses internally: creates the table on first write, MERGEs on subsequent
    # runs. Needs key_cols as a list, and -- like Config.xray/Config.jira before
    # it -- it creates the TABLE but not the SCHEMA, so Silver.xray must exist
    # first (idempotent, harmless once it does).
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SILVER_SCHEMA}")
    fmt.upsert_delta(spark, deduped, f"{SILVER_SCHEMA}.test_runs", key_cols=["run_id"])
    print(f"B2S - Xray complete: {deduped.count()} test runs in {SILVER_SCHEMA}.test_runs")

# CELL ********************
# --- Statuses: same raw_data unpack, different (simpler) shape -- config, not transactional ---
STATUS_RAW_SCHEMA = StructType([
    StructField("name", StringType()),
    StructField("description", StringType()),
    StructField("color", StringType()),
])

if not spark.catalog.tableExists(f"{BRONZE_SCHEMA}.statuses"):
    print("No Bronze.xray.statuses table -- S2B - Xray hasn't run its getStatuses step yet. Nothing to do.")
else:
    statuses_df = (
        spark.table(f"{BRONZE_SCHEMA}.statuses")
        .withColumn("parsed", F.from_json(F.col("raw_data"), STATUS_RAW_SCHEMA))
        .select("parsed.*")
        .filter(F.col("name").isNotNull())
    )
    # DEDUPE ON NAME -- missing this was the actual bug. S2B - Xray's
    # getStatuses call lands the full status list EVERY run via land_records
    # (append-only, no watermark -- deliberately, since it's small config
    # data), so Bronze.xray.statuses accumulates a fresh copy of every status
    # row on every S2B run: multiple "PASSED" rows, multiple "FAILED" rows,
    # etc. Reading that whole history straight into upsert_delta (key_cols=
    # ["name"]) then hands MERGE several source rows all claiming the same
    # target row -- exactly DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW.
    # dropDuplicates keeps one arbitrary-but-consistent row per name; status
    # config rarely changes, so which landing "wins" doesn't matter in
    # practice, only that MERGE sees one row per key.
    statuses_df = statuses_df.dropDuplicates(["name"])
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SILVER_SCHEMA}")
    fmt.upsert_delta(spark, statuses_df, f"{SILVER_SCHEMA}.statuses", key_cols=["name"])
    print(f"B2S - Xray complete: {statuses_df.count()} statuses in {SILVER_SCHEMA}.statuses")
