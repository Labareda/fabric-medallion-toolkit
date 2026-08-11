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
# Config is used here only for the watermark control table (Config.xray.watermarks)
# -- Xray has no config file of its own, unlike Jira.

# CELL ********************
from datetime import datetime, timezone
import json
import time
import requests
from notebookutils import mssparkutils
import notebookutils
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

# --- Secret resolution: a Fabric CONNECTION (Workspace -> Manage connections
# -> New connection -> Web V2 -> Base Url = https://xray.cloud.getxray.app ->
# Authentication method = Basic -> username = the Xray API key's Client ID,
# password = the Client Secret -> tick "Allow Code-First Artifacts like
# Notebooks..." -> then add this connection under the NOTEBOOK's own
# Connections pane, not just the workspace). NOT Key Vault (not available to
# this client) and NOT a Variable library (that store is plain, git-trackable
# configuration, not an encrypted secret store).
#
# This DID work once credential resolution and the notebook-level attachment
# were both correctly in place -- earlier "Artifact Connection does not
# exist" failures were resolved by attaching the connection under this
# notebook's own Connections pane and starting a fresh Spark session, not by
# any code change.
XRAY_CONNECTION_ID = "6d3f6ca5-1107-4a17-a76a-e3684376ceac"

def get_credential_field(connection_id: str, field_name: str) -> str:
    raw = notebookutils.connections.getCredential(connection_id)
    credential_data = json.loads(raw["credential"])["credentialData"]
    # The exact field name Fabric uses for a Basic-auth connection's
    # username/password isn't publicly documented, so this matches
    # case-insensitively against common aliases rather than one exact string.
    aliases = {"password", "secret", "key", "value"} if field_name == "password" else {"username", "user"}
    for item in credential_data:
        if item.get("name", "").lower() in aliases:
            return item["value"]
    available = [item.get("name") for item in credential_data]
    raise KeyError(
        f"Could not find a field matching '{field_name}' in connection {connection_id}. "
        f"Fields actually present: {available}"
    )

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
    client_id = get_credential_field(XRAY_CONNECTION_ID, "username")      # Client ID
    client_secret = get_credential_field(XRAY_CONNECTION_ID, "password")  # Client Secret
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
    """POST a GraphQL query, with backoff on the 429s Xray hands out freely."""
    for attempt in range(retries):
        resp = requests.post(
            XRAY_GRAPHQL_URL,
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            data=json.dumps({"query": query}),
            timeout=60,
        )
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
    raise RuntimeError(f"Xray GraphQL still rate-limited after {retries} attempts")

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
print("S2B - Xray complete.")
