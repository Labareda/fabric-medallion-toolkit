# Fabric notebook source
# "S2B - Xray" — Source-to-Bronze for the Xray test-management source.
# Extraction only: authenticates against Xray Cloud, pulls test executions and
# their test runs via the GraphQL API, lands raw JSON into Bronze. NO
# standardization here -- that's "B2S - Xray"'s job. Run this FIRST.
#
# WHY THIS IS A SEPARATE NOTEBOOK AND NOT A jira.json ENTITY:
# The Jira source uses fmt.RestExtractor, which is config-driven and assumes a
# conventional REST paginator (GET, query-param cursors, a records JSON path).
# Xray Cloud is none of those -- it's a bearer-token handshake, POST bodies
# carrying GraphQL, and cursor pagination that nests MULTIPLICATIVELY under a
# 25-resolver-per-call budget. That doesn't fit the config schema, so the
# extraction is hand-written here. It still lands via fmt.land_records, so
# everything DOWNSTREAM (B2S, Gold) treats it like any other Bronze table.
#
# Attach Bronze and Config lakehouses, plus env_medallion_toolkit.
# Config is used both for the watermark control table (Config.xray.watermarks)
# and for xray.json (client_id/secret), the same lakehouse jira.json lives in.

# CELL ********************
from datetime import datetime, timezone
import json
import time
import requests
from notebookutils import mssparkutils
import fabric_medallion_toolkit as fmt

# CELL ********************
SOURCE_NAME = "xray"
SCHEMA = "Bronze.xray"  # raw test-run data lands here, unchanged
# Watermark control table lives under Config, alongside Jira's, for one place
# to look for pipeline state -- NOT the same physical table as Jira's,
# deliberately: the wheel's watermark table is keyed on entity_name ALONE
# (no source_name column), so two sources sharing one physical table would
# silently collide if they ever had a same-named entity. Config.xray keeps
# them co-located under one lakehouse while staying in separate schemas.
WATERMARK_SCHEMA = "Config.xray"

# upsert_delta (which save_watermark calls) creates the WATERMARKS TABLE on
# first write, but assumes the SCHEMA it lives in already exists -- it never
# creates that itself. Bronze.xray got created implicitly the first time
# land_records wrote test_runs there; Config.xray never has, since Config
# previously only ever held jira.json as a loose file, no schema. Left
# unhandled, the first save_watermark call fails with a confusing
# SCHEMA_NOT_FOUND (Spark's fallback parsing of an unresolvable 3-part name
# produces a garbled error, not a clear "schema missing" message). Creating
# it here is idempotent -- harmless to run on every execution once it exists.
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {WATERMARK_SCHEMA}")

# esshtransform.atlassian.net is a standard Atlassian Cloud host, so the GLOBAL
# Xray endpoints apply. A residency instance would use us./eu./au. prefixes on
# BOTH urls -- change both together if the client ever migrates.
XRAY_AUTH_URL    = "https://xray.cloud.getxray.app/api/v2/authenticate"
XRAY_GRAPHQL_URL = "https://xray.cloud.getxray.app/api/v2/graphql"

# --- Secret resolution: a config FILE in the Config lakehouse, same pattern
# as jira.json, NOT a Fabric Connection (repeatedly failed with "Artifact
# Connection does not exist" for this client and was abandoned) and NOT Key
# Vault (not available to this client either). The client_id/client_secret
# live in xray.json's plaintext, protected only by who has access to the
# Config lakehouse -- so treat that lakehouse's permissions as the real
# security boundary, and never grant Files access there casually.
#
# sources/xray/xray.json in this repo is a TEMPLATE with placeholder values
# only (safe to commit). The REAL file -- with the real client_id/secret --
# lives ONLY in the Config lakehouse's Files area, uploaded there directly
# (Config lakehouse -> Files -> upload xray.json), never through git.
#
# Get the ABFS path the same way as jira.json: Config lakehouse -> Files ->
# right-click xray.json -> "Copy ABFS path", and paste it below.
CONFIG_ABFS_PATH = "abfss://<workspace>@onelake.dfs.fabric.microsoft.com/Config.Lakehouse/Files/xray.json"

def load_xray_config() -> dict:
    config_json_text = mssparkutils.fs.head(CONFIG_ABFS_PATH, 10 * 1024 * 1024)  # 10MB is plenty
    return json.loads(config_json_text)

# The Xray API key is generated per Jira USER (Jira -> Xray Settings -> API
# Keys). The extract sees ONLY what that user can see, so it must belong to a
# service account (or someone with visibility of every in-scope project) --
# otherwise projects vanish from the results with NO error.

# Full load reaches back to this date on first run; subsequent runs are
# incremental from the saved watermark (see the watermark cell below).
INITIAL_WATERMARK = "2026-01-01T00:00:00Z"

# Xray page caps. 100 is the API maximum for both levels. The nested testRuns
# query below costs 4 resolvers (getTestExecutions + testRuns + status +
# testType) -- well under the 25 ceiling, so no query-splitting is needed for
# this field set. Adding nested steps{} or evidence{} would change that.
EXEC_PAGE  = 100
RUNS_PAGE  = 100

# CELL ********************
def get_token() -> str:
    """Exchange client_id/client_secret for a bearer token (valid 24h)."""
    config = load_xray_config()
    auth = config["auth"]
    client_id = auth["client_id"]
    client_secret = auth["secret"]
    resp = requests.post(
        XRAY_AUTH_URL,
        headers={"Content-Type": "application/json", "Accept": "text/plain"},
        data=json.dumps({"client_id": client_id, "client_secret": client_secret}),
        timeout=30,
    )
    resp.raise_for_status()
    # Xray returns the token as a JSON string ("eyJ..."); .json() parses off
    # the surrounding quotes. Matches Xray's own official Python snippet.
    return resp.json()

def run_graphql(token: str, query: str, retries: int = 5) -> dict:
    """POST a GraphQL query, with backoff on 429s AND on read timeouts.

    A wide page (100 executions each nested up to 100 runs, or the 100x100
    container/linked-test pages added for test_sets/test_plans/preconditions)
    can genuinely take Xray's server longer than 60s to assemble -- that's a
    slow response, not a dead connection, so it's retried with backoff like a
    429 rather than left to kill the whole run. 120s (was 60s) as the
    per-attempt cap gives the server more room before even the first retry.
    """
    for attempt in range(retries):
        try:
            resp = requests.post(
                XRAY_GRAPHQL_URL,
                headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                data=json.dumps({"query": query}),
                timeout=120,
            )
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as exc:
            wait = 2 ** attempt
            print(f"  {type(exc).__name__} ({exc}), retrying in {wait}s")
            time.sleep(wait)
            continue
        if resp.status_code == 429:
            wait = 2 ** attempt
            print(f"  429 rate-limited, backing off {wait}s")
            time.sleep(wait)
            continue
        resp.raise_for_status()
        body = resp.json()
        # GraphQL returns HTTP 200 even for query errors -- they're in a top-level
        # "errors" array. Surface them instead of silently landing empty data.
        if "errors" in body and body["errors"]:
            raise RuntimeError(f"Xray GraphQL error: {body['errors']}")
        return body["data"]
    raise RuntimeError(f"Xray GraphQL still failing (rate-limited or timing out) after {retries} attempts")

# CELL ********************
# --- Watermark: only pull executions MODIFIED since the last successful run ---
# Xray's getTestExecutions accepts a JQL filter, and executions are Jira issues,
# so "updated" works exactly as it does in the Jira pull. A test run's result is
# recorded on its execution, so a re-run bumps the execution's updated date --
# meaning this watermark correctly re-pulls an execution whose results changed,
# not just newly-created ones.
watermark_entity = fmt.EntityConfig(
    entity_name="test_runs", endpoint_path="", pagination_style="none",
    records_json_path="", natural_key_field="run_id",
)
try:
    saved = fmt.get_watermark(spark, SOURCE_NAME, watermark_entity, bronze_schema=WATERMARK_SCHEMA)
    watermark = saved or INITIAL_WATERMARK
except Exception:
    watermark = INITIAL_WATERMARK

# Xray JQL wants the datetime without the 'T'/'Z' and quoted.
#
# SINGLE quotes here, deliberately -- JQL accepts either single or double
# quotes for string literals. This whole jql string gets embedded inside
# ANOTHER pair of double quotes below (`jql: "{jql}"` in the GraphQL query),
# so if jql itself used double quotes they'd collide with the wrapping ones
# and produce syntactically broken GraphQL -- which is exactly what caused
# the "400 Bad Request" on the /graphql call (the auth call succeeds first,
# so a 400 specifically on /graphql means the query body itself, not the
# credentials, was malformed).
wm_jql = watermark.replace("T", " ").replace("Z", "")[:16]
jql = f"issuetype = 'Test Execution' AND updated >= '{wm_jql}' ORDER BY updated ASC"
print(f"[test_runs] pulling executions updated >= {wm_jql}")

# CELL ********************
# --- Extract: page executions, and for each, page its test runs ---
# One flat record per (execution x test run). start/limit cursors on each level.
# The execution and test Jira keys are pulled via jira(fields:[...]) -- these
# are the ONLY join back to the model: Test_Issue_Key and Execution_Issue_Key
# both resolve to the Dim_Issue you already have (tests and executions are Jira
# issues), so no new issue dimension is needed.
extraction_started_at = datetime.now(timezone.utc)
token = get_token()
records = []
overflow_executions = []  # (exec_issue_id, exec_key, exec_summary, total_runs) for the second pass below
exec_start = 0

while True:
    q = f"""{{
      getTestExecutions(jql: "{jql}", limit: {EXEC_PAGE}, start: {exec_start}) {{
        total
        results {{
          issueId
          jira(fields: ["key", "summary"])
          testRuns(limit: {RUNS_PAGE}) {{
            total
            results {{
              id
              status {{ name }}
              testType {{ name }}
              startedOn
              finishedOn
              executedById
              test {{ issueId jira(fields: ["key"]) }}
            }}
          }}
        }}
      }}
    }}"""
    data = run_graphql(token, q)
    block = data["getTestExecutions"]
    execs = block.get("results", []) or []
    if not execs:
        break

    for ex in execs:
        exec_key = (ex.get("jira") or {}).get("key")
        exec_summary = (ex.get("jira") or {}).get("summary")
        runs_block = ex.get("testRuns") or {}
        for run in (runs_block.get("results") or []):
            test = run.get("test") or {}
            records.append({
                "run_id": run.get("id"),
                "execution_issue_id": ex.get("issueId"),
                "execution_issue_key": exec_key,
                "execution_summary": exec_summary,
                "test_issue_id": test.get("issueId"),
                "test_issue_key": (test.get("jira") or {}).get("key"),
                "status_name": (run.get("status") or {}).get("name"),
                "test_type": (run.get("testType") or {}).get("name"),
                "started_on": run.get("startedOn"),
                "finished_on": run.get("finishedOn"),
                "executed_by_id": run.get("executedById"),
            })
        # If an execution has MORE than 100 runs, this first pass truncates at
        # 100 -- collect it here so the follow-up pass below can fetch the rest
        # via getTestRuns, which has its own independent start/limit pagination.
        rt = runs_block.get("total", 0)
        if rt and rt > RUNS_PAGE:
            overflow_executions.append((ex.get("issueId"), exec_key, exec_summary, rt))
            print(f"  NOTE: execution {exec_key} has {rt} runs; {RUNS_PAGE} landed here, "
                  f"remaining {rt - RUNS_PAGE} will be fetched in the follow-up pass")

    exec_start += EXEC_PAGE
    if exec_start >= block.get("total", 0):
        break

print(f"[test_runs] collected {len(records)} test-run records from first pass")

# CELL ********************
# --- Follow-up pass: fetch runs beyond the first 100 for any execution that overflowed ---
# getTestExecutions -> testRuns has no pagination cursor of its own beyond its
# initial limit, so an execution with >100 runs can't be paged further through
# that path. getTestRuns is a separate TOP-LEVEL query with its own documented
# start/limit pagination (Xray's own docs show total/limit/start on its
# response), and it accepts testExecIssueIds to scope to one execution -- so
# it's used here purely to fill the gap the first pass left, not to re-fetch
# runs already collected.
for exec_issue_id, exec_key, exec_summary, total_runs in overflow_executions:
    offset = RUNS_PAGE
    while offset < total_runs:
        q = f"""{{
          getTestRuns(testExecIssueIds: ["{exec_issue_id}"], limit: {RUNS_PAGE}, start: {offset}) {{
            total
            results {{
              id
              status {{ name }}
              testType {{ name }}
              startedOn
              finishedOn
              executedById
              test {{ issueId jira(fields: ["key"]) }}
            }}
          }}
        }}"""
        data = run_graphql(token, q)
        block = data["getTestRuns"]
        for run in (block.get("results") or []):
            test = run.get("test") or {}
            records.append({
                "run_id": run.get("id"),
                "execution_issue_id": exec_issue_id,
                "execution_issue_key": exec_key,
                "execution_summary": exec_summary,
                "test_issue_id": test.get("issueId"),
                "test_issue_key": (test.get("jira") or {}).get("key"),
                "status_name": (run.get("status") or {}).get("name"),
                "test_type": (run.get("testType") or {}).get("name"),
                "started_on": run.get("startedOn"),
                "finished_on": run.get("finishedOn"),
                "executed_by_id": run.get("executedById"),
            })
        offset += RUNS_PAGE
    print(f"  [test_runs] fetched remaining {total_runs - RUNS_PAGE} runs for execution {exec_key}")

print(f"[test_runs] collected {len(records)} test-run records total (after follow-up pass)")

# CELL ********************
# --- Land to Bronze, and advance the watermark ---
if records:
    count = fmt.land_records(spark, records, source_name=SOURCE_NAME,
                             entity=watermark_entity, bronze_schema=SCHEMA)
    print(f"[test_runs] landed {count} records")
    # Watermark forward to when THIS run started, not to max(finished_on) --
    # an execution can be updated without any run finishing, and we don't want
    # to skip it next time.
    new_wm = extraction_started_at.strftime("%Y-%m-%dT%H:%M:%SZ")
    fmt.save_watermark(spark, SOURCE_NAME, watermark_entity, new_wm, bronze_schema=WATERMARK_SCHEMA)
    print(f"[test_runs] watermark advanced to {new_wm}")
else:
    print("[test_runs] no records this run -- watermark unchanged")

# CELL ********************
# --- Statuses: Xray's status CONFIGURATION, not transactional data ---
# getStatuses is a flat, unpaginated top-level query (Xray's own docs and
# Postman collection never show it with limit/start -- it just returns every
# status defined for the instance in one call). It gives name/description/
# color -- real config, not the hand-typed guess Dim_TestStatus used to run
# on. It does NOT give Final or Coverage Status, though: neither field
# appears anywhere in Xray's documented schema for getStatuses OR for any
# nested status{} selection elsewhere in their API (checked against their
# official docs and Postman collection, not assumed) -- those two are
# Settings-screen-only and have to stay hand-maintained in Gold - Dim_TestStatus.
# No watermark needed -- this is small, rarely changes, and a full pull each
# run is cheap and simpler than tracking incremental changes to five rows.
statuses_data = run_graphql(token, "{ getStatuses { name description color } }")
status_records = statuses_data.get("getStatuses") or []

statuses_entity = fmt.EntityConfig(
    entity_name="statuses", endpoint_path="", pagination_style="none",
    records_json_path="", natural_key_field="name",
)
count = fmt.land_records(spark, status_records, source_name=SOURCE_NAME,
                         entity=statuses_entity, bronze_schema=SCHEMA)
print(f"[statuses] landed {count} records")

# CELL ********************
# --- Shared shape: Test Sets, Test Plans and Preconditions are all "a
# container issue with a paginated connection of linked Test issues" in
# Xray's GraphQL schema (getTestSets/getTestPlans/getPreconditions all
# expose a `tests(limit, start)` connection the same way getTestExecutions
# exposes `testRuns`). One helper covers all three instead of tripling the
# same pagination/overflow logic.
CONTAINER_PAGE = 100
LINKED_TESTS_PAGE = 100

def extract_container_with_tests(token: str, query_name: str, entity_label: str) -> list:
    """Pulls every issue of a container type (test set / test plan /
    precondition) together with the Test issues linked to it. Returns one
    flat record per (container, linked test) pair."""
    out = []
    start = 0
    while True:
        q = f"""{{
          {query_name}(limit: {CONTAINER_PAGE}, start: {start}) {{
            total
            results {{
              issueId
              jira(fields: ["key", "summary"])
              tests(limit: {LINKED_TESTS_PAGE}) {{
                total
                results {{ issueId jira(fields: ["key"]) }}
              }}
            }}
          }}
        }}"""
        data = run_graphql(token, q)
        block = data[query_name]
        containers = block.get("results", []) or []
        if not containers:
            break

        for c in containers:
            c_key = (c.get("jira") or {}).get("key")
            c_summary = (c.get("jira") or {}).get("summary")
            tests_block = c.get("tests") or {}
            linked = tests_block.get("results") or []
            if not linked:
                # A container with zero linked tests still needs to exist in
                # Silver (e.g. an empty Test Set), so it lands with null test
                # columns rather than being dropped entirely.
                out.append({
                    "container_issue_id": c.get("issueId"), "container_issue_key": c_key,
                    "container_summary": c_summary, "test_issue_id": None, "test_issue_key": None,
                })
            for t in linked:
                out.append({
                    "container_issue_id": c.get("issueId"), "container_issue_key": c_key,
                    "container_summary": c_summary,
                    "test_issue_id": t.get("issueId"),
                    "test_issue_key": (t.get("jira") or {}).get("key"),
                })
            # Same >100-linked-tests gap as test_runs -- getTestSets/getTestPlans/
            # getPreconditions' nested `tests` connection has no cursor of its
            # own beyond the initial limit. Flagged, not silently truncated;
            # revisit with a getTests(testSetId/testPlanId/preconditionId:)
            # follow-up pass if this ever actually fires for this client.
            total_linked = tests_block.get("total", 0)
            if total_linked and total_linked > LINKED_TESTS_PAGE:
                print(f"  WARNING: {entity_label} {c_key} has {total_linked} linked tests; "
                      f"only the first {LINKED_TESTS_PAGE} landed (no follow-up pass yet)")

        start += CONTAINER_PAGE
        if start >= block.get("total", 0):
            break

    print(f"[{entity_label}] collected {len(out)} (container, test) pairs")
    return out

# CELL ********************
# --- Test Sets: replaces whatever previously produced Silver.xray.test_sets
# (no notebook in this repo did -- see Orchestration - Jira and Xray.py's
# "ASSUMPTION TO CHECK" comment). This is a full pull, not watermarked: Xray's
# getTestSets has no jql/updated filter documented for it, and test-set
# membership is small enough to re-pull in full each run. ---
test_set_records = extract_container_with_tests(token, "getTestSets", "test_sets")
test_sets_entity = fmt.EntityConfig(
    entity_name="test_sets", endpoint_path="", pagination_style="none",
    records_json_path="", natural_key_field="_pk",
)
for r in test_set_records:
    r["_pk"] = f"{r['container_issue_id']}::{r.get('test_issue_id') or ''}"
count = fmt.land_records(spark, test_set_records, source_name=SOURCE_NAME,
                         entity=test_sets_entity, bronze_schema=SCHEMA)
print(f"[test_sets] landed {count} records")

# CELL ********************
# --- Test Plans: same shape as Test Sets, same full-pull reasoning. ---
test_plan_records = extract_container_with_tests(token, "getTestPlans", "test_plans")
test_plans_entity = fmt.EntityConfig(
    entity_name="test_plans", endpoint_path="", pagination_style="none",
    records_json_path="", natural_key_field="_pk",
)
for r in test_plan_records:
    r["_pk"] = f"{r['container_issue_id']}::{r.get('test_issue_id') or ''}"
count = fmt.land_records(spark, test_plan_records, source_name=SOURCE_NAME,
                         entity=test_plans_entity, bronze_schema=SCHEMA)
print(f"[test_plans] landed {count} records")

# CELL ********************
# --- Preconditions: same shape again. ---
precondition_records = extract_container_with_tests(token, "getPreconditions", "preconditions")
preconditions_entity = fmt.EntityConfig(
    entity_name="preconditions", endpoint_path="", pagination_style="none",
    records_json_path="", natural_key_field="_pk",
)
for r in precondition_records:
    r["_pk"] = f"{r['container_issue_id']}::{r.get('test_issue_id') or ''}"
count = fmt.land_records(spark, precondition_records, source_name=SOURCE_NAME,
                         entity=preconditions_entity, bronze_schema=SCHEMA)
print(f"[preconditions] landed {count} records")

# CELL ********************
# --- Test details: the Test issues themselves -- type and steps -- not their
# links to other containers (those are covered above). getTests has no jql
# updated-filter documented either (unlike getTestExecutions), so this is
# also a full pull. Steps are returned as a nested list; landed as a JSON
# string (steps_json) rather than exploded, since B2S can parse/explode them
# without a schema change here if the step count grows. ---
test_records = []
t_start = 0
while True:
    q = f"""{{
      getTests(limit: {CONTAINER_PAGE}, start: {t_start}) {{
        total
        results {{
          issueId
          jira(fields: ["key", "summary"])
          testType {{ name }}
          steps {{ id action data result }}
        }}
      }}
    }}"""
    data = run_graphql(token, q)
    block = data["getTests"]
    results = block.get("results", []) or []
    if not results:
        break
    for t in results:
        test_records.append({
            "test_issue_id": t.get("issueId"),
            "test_issue_key": (t.get("jira") or {}).get("key"),
            "test_summary": (t.get("jira") or {}).get("summary"),
            "test_type": (t.get("testType") or {}).get("name"),
            "steps_json": json.dumps(t.get("steps") or []),
        })
    t_start += CONTAINER_PAGE
    if t_start >= block.get("total", 0):
        break

print(f"[tests] collected {len(test_records)} test detail records")
tests_entity = fmt.EntityConfig(
    entity_name="tests", endpoint_path="", pagination_style="none",
    records_json_path="", natural_key_field="test_issue_id",
)
count = fmt.land_records(spark, test_records, source_name=SOURCE_NAME,
                         entity=tests_entity, bronze_schema=SCHEMA)
print(f"[tests] landed {count} records")

# CELL ********************
print("S2B - Xray complete.")
