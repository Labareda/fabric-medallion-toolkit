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

# CELL ********************
# --- Test Sets / Test Plans / Preconditions: same flat "container linked to
# a test" shape S2B - Xray's extract_container_with_tests() lands. Column
# names below are the container-specific ones Gold already reads (e.g.
# Gold - Dim Test Set.py / Gold - Fact Test.py select test_set_issue_id etc
# straight off Silver.xray.test_sets) -- kept exactly so no Gold change is
# needed just from this notebook landing the table for the first time.
CONTAINER_RAW_SCHEMA = StructType([
    StructField("container_issue_id", StringType()),
    StructField("container_issue_key", StringType()),
    StructField("container_summary", StringType()),
    StructField("test_issue_id", StringType()),
    StructField("test_issue_key", StringType()),
    StructField("_pk", StringType()),
])

def standardize_container_table(bronze_table: str, silver_table: str, id_col: str, key_col: str, summary_col: str):
    if not spark.catalog.tableExists(f"{BRONZE_SCHEMA}.{bronze_table}"):
        print(f"No {BRONZE_SCHEMA}.{bronze_table} table -- S2B - Xray hasn't run this step yet. Nothing to do.")
        return
    raw = (
        spark.table(f"{BRONZE_SCHEMA}.{bronze_table}")
        .withColumn("parsed", F.from_json(F.col("raw_data"), CONTAINER_RAW_SCHEMA))
        .select("parsed.*")
        # A container issue can legitimately land with no linked tests (see
        # S2B's "zero linked tests" branch) -- container_issue_id must still
        # be present, but test_issue_id staying null is real, not dirty.
        .filter(F.col("container_issue_id").isNotNull())
        .dropDuplicates(["_pk"])
        .withColumnRenamed("container_issue_id", id_col)
        .withColumnRenamed("container_issue_key", key_col)
        .withColumnRenamed("container_summary", summary_col)
        .drop("_pk")
    )
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SILVER_SCHEMA}")
    fmt.upsert_delta(spark, raw, f"{SILVER_SCHEMA}.{silver_table}", key_cols=[id_col, "test_issue_id"])
    print(f"B2S - Xray complete: {raw.count()} rows in {SILVER_SCHEMA}.{silver_table}")

standardize_container_table("test_sets", "test_sets", "test_set_issue_id", "test_set_issue_key", "test_set_summary")
standardize_container_table("test_plans", "test_plans", "test_plan_issue_id", "test_plan_issue_key", "test_plan_summary")
standardize_container_table("preconditions", "preconditions", "precondition_issue_id", "precondition_issue_key", "precondition_summary")

# CELL ********************
# --- Test details: type + steps for each Test issue. steps_json stays a JSON
# string in Silver too (not exploded to one-row-per-step) -- nothing in Gold
# needs individual steps yet, and exploding pre-emptively would be an
# unused table shape. Parse it downstream with from_json if/when a report
# needs step-level detail. ---
TESTS_RAW_SCHEMA = StructType([
    StructField("test_issue_id", StringType()),
    StructField("test_issue_key", StringType()),
    StructField("test_summary", StringType()),
    StructField("test_type", StringType()),
    StructField("steps_json", StringType()),
])

if not spark.catalog.tableExists(f"{BRONZE_SCHEMA}.tests"):
    print(f"No {BRONZE_SCHEMA}.tests table -- S2B - Xray hasn't run this step yet. Nothing to do.")
else:
    tests_df = (
        spark.table(f"{BRONZE_SCHEMA}.tests")
        .withColumn("parsed", F.from_json(F.col("raw_data"), TESTS_RAW_SCHEMA))
        .select("parsed.*")
        .filter(F.col("test_issue_id").isNotNull())
        .dropDuplicates(["test_issue_id"])
    )
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SILVER_SCHEMA}")
    fmt.upsert_delta(spark, tests_df, f"{SILVER_SCHEMA}.tests", key_cols=["test_issue_id"])
    print(f"B2S - Xray complete: {tests_df.count()} rows in {SILVER_SCHEMA}.tests")
