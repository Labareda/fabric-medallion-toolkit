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

# CELL ********************
from datetime import datetime, timezone
import json
import time
import requests
from notebookutils import mssparkutils
import fabric_medallion_toolkit as fmt

# CELL ********************
SOURCE_NAME = "xray"
SCHEMA = "Bronze.xray"

# esshtransform.atlassian.net is a standard Atlassian Cloud host, so the GLOBAL
# Xray endpoints apply. A residency instance would use us./eu./au. prefixes on
# BOTH urls -- change both together if the client ever migrates.
XRAY_AUTH_URL    = "https://xray.cloud.getxray.app/api/v2/authenticate"
XRAY_GRAPHQL_URL = "https://xray.cloud.getxray.app/api/v2/graphql"

# --- Secret resolution: PLAINTEXT, matching how Jira is currently configured ---
# The Fabric "Connection inside Notebook" preview feature was tried first (see
# git history) but consistently failed with "Artifact Connection does not
# exist" from Fabric's own token service, regardless of correct connection ID,
# the notebook-access checkbox, the connection being attached under this
# notebook's own Connections pane, and a fresh Spark session -- all confirmed
# correct. That points to a bug or tenant limitation in the PREVIEW feature
# itself, not a configuration error here, so continuing to debug it blind
# wasn't a good use of delivery time. This client also has no Azure Key Vault
# access, so Key Vault isn't an option either.
#
# Fallback: hardcode the two secrets here, same tradeoff already accepted for
# Jira's token -- this file goes into a git repo, so the values are exposed
# to anyone with repo read access, for as long as the repository holds this
# commit in its history (removing the line later does NOT remove it from
# past commits). Acceptable only because the repo is private with limited
# access; rotate both values periodically as a partial mitigation.
#
# Revisit the Fabric Connection route later (possibly via Microsoft support,
# since this looks like a product bug) once there's time to debug it properly
# outside delivery pressure.
XRAY_CLIENT_ID = "<paste the Xray API key's Client ID here>"
XRAY_CLIENT_SECRET = "<paste the Xray API key's Client Secret here>"

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
    resp = requests.post(
        XRAY_AUTH_URL,
        headers={"Content-Type": "application/json", "Accept": "text/plain"},
        data=json.dumps({"client_id": XRAY_CLIENT_ID, "client_secret": XRAY_CLIENT_SECRET}),
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
    saved = fmt.get_watermark(spark, SOURCE_NAME, watermark_entity, bronze_schema=SCHEMA)
    watermark = saved or INITIAL_WATERMARK
except Exception:
    watermark = INITIAL_WATERMARK

# Xray JQL wants the datetime without the 'T'/'Z' and quoted.
wm_jql = watermark.replace("T", " ").replace("Z", "")[:16]
jql = f'issuetype = "Test Execution" AND updated >= "{wm_jql}" ORDER BY updated ASC'
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
        # If an execution has MORE than 100 runs, this pull truncates at 100.
        # None in this instance are near that, so a per-execution runs paginator
        # is deliberately omitted -- add one here if that ever changes.
        rt = runs_block.get("total", 0)
        if rt and rt > RUNS_PAGE:
            print(f"  WARNING: execution {exec_key} has {rt} runs; only first {RUNS_PAGE} landed")

    exec_start += EXEC_PAGE
    if exec_start >= block.get("total", 0):
        break

print(f"[test_runs] collected {len(records)} test-run records")

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
    fmt.save_watermark(spark, SOURCE_NAME, watermark_entity, new_wm, bronze_schema=SCHEMA)
    print(f"[test_runs] watermark advanced to {new_wm}")
else:
    print("[test_runs] no records this run -- watermark unchanged")

# CELL ********************
print("S2B - Xray complete.")
