# PATCH INSTRUCTIONS -- Xray Test Sets extraction
# ================================================
#
# TWO EDITS. One in S2B - Xray, one in B2S - Xray. Both simple, both follow
# exactly the pattern already there for statuses -- flat unpaginated pull for
# statuses; this one paginates (Test Sets can be many, each with many tests).
#
# =====================================================================
# EDIT 1: S2B - Xray.py
# =====================================================================
# Add THIS ENTIRE CELL just before the final "S2B - Xray complete." print:

# CELL ********************
# --- Test Sets: Xray's TestSet -> member Tests relationship ---
# Nowhere else in the pipeline does this membership live. Not in
# jira.issue_links (that carries coverage, not membership), not on the Test
# Set issue's own fields (verified against real data -- no member-list column
# exists on Test Set rows). Xray's GraphQL getTestSets query is the only
# source.
#
# Cost: getTestSets + tests = 2 resolvers, well under the 25 budget. Both
# paginated -- getTestSets with start/limit at outer level; tests(limit) is
# nested per test set and (like testRuns in the executions pull) has NO
# paginator of its own beyond its initial limit, so a set with >100 tests
# would truncate. Warned about below if it ever happens; a getTestSet
# follow-up pass could be added the same way overflow test runs are handled
# in the executions pull. None of this client's test sets are near that
# size yet.
test_set_records = []
ts_start = 0
TS_PAGE = 100
TESTS_PAGE = 100

while True:
    q = f"""{{
      getTestSets(limit: {TS_PAGE}, start: {ts_start}) {{
        total
        results {{
          issueId
          jira(fields: ["key", "summary"])
          tests(limit: {TESTS_PAGE}) {{
            total
            results {{
              issueId
              jira(fields: ["key"])
            }}
          }}
        }}
      }}
    }}"""
    data = run_graphql(token, q)
    block = data["getTestSets"]
    sets = block.get("results", []) or []
    if not sets:
        break

    for ts in sets:
        ts_id = ts.get("issueId")
        ts_key = (ts.get("jira") or {}).get("key")
        ts_summary = (ts.get("jira") or {}).get("summary")
        tests_block = ts.get("tests") or {}
        for t in (tests_block.get("results") or []):
            test_key = (t.get("jira") or {}).get("key")
            test_set_records.append({
                "test_set_issue_id":  ts_id,
                "test_set_issue_key": ts_key,
                "test_set_summary":   ts_summary,
                "test_issue_id":      t.get("issueId"),
                "test_issue_key":     test_key,
            })
        tt = tests_block.get("total", 0)
        if tt and tt > TESTS_PAGE:
            print(f"  WARNING: test set {ts_key} has {tt} tests; only first {TESTS_PAGE} landed")

    ts_start += TS_PAGE
    if ts_start >= block.get("total", 0):
        break

print(f"[test_sets] collected {len(test_set_records)} test-set-membership records")

# CELL ********************
# --- Land test sets to Bronze ---
if test_set_records:
    test_sets_entity = fmt.EntityConfig(
        entity_name="test_sets", endpoint_path="", pagination_style="none",
        records_json_path="", natural_key_field="test_set_issue_id",
    )
    count = fmt.land_records(spark, test_set_records, source_name=SOURCE_NAME,
                             entity=test_sets_entity, bronze_schema=SCHEMA)
    print(f"[test_sets] landed {count} records")


# =====================================================================
# EDIT 2: B2S - Xray.py
# =====================================================================
# Add THIS ENTIRE CELL at the end (after the statuses block):

# CELL ********************
# --- Test Sets: same raw_data unpack pattern as test_runs and statuses ---
TEST_SETS_RAW_SCHEMA = StructType([
    StructField("test_set_issue_id",  StringType()),
    StructField("test_set_issue_key", StringType()),
    StructField("test_set_summary",   StringType()),
    StructField("test_issue_id",      StringType()),
    StructField("test_issue_key",     StringType()),
])

if not spark.catalog.tableExists(f"{BRONZE_SCHEMA}.test_sets"):
    print("No Bronze.xray.test_sets table -- S2B - Xray hasn't run its getTestSets step yet.")
else:
    ts_df = (
        spark.table(f"{BRONZE_SCHEMA}.test_sets")
        .withColumn("parsed", F.from_json(F.col("raw_data"), TEST_SETS_RAW_SCHEMA))
        .select("parsed.*")
        .filter(F.col("test_set_issue_id").isNotNull())
        .filter(F.col("test_issue_id").isNotNull())
    )
    # A test set landing multiple times across S2B runs (no watermark, deliberate)
    # would produce duplicates. Same dedup fix already applied to statuses.
    ts_df = ts_df.dropDuplicates(["test_set_issue_id", "test_issue_id"])
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SILVER_SCHEMA}")
    fmt.upsert_delta(spark, ts_df, f"{SILVER_SCHEMA}.test_sets",
                     key_cols=["test_set_issue_id", "test_issue_id"])
    print(f"B2S - Xray: {ts_df.count()} test-set-membership rows in {SILVER_SCHEMA}.test_sets")
