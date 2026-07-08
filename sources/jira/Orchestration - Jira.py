# Fabric notebook source
# "Orchestration - Jira" — runs the full Jira pipeline end to end, in
# order: Source-to-Bronze, Bronze-to-Silver, then Silver-to-Gold (once
# those exist), then refreshes the SQL Analytics Endpoint for the
# lakehouses that changed. No Teams alerting logic here -- this notebook's
# only job is to run the steps and either finish cleanly or fail with the
# real error intact. The PIPELINE that schedules this notebook handles the
# Teams alert on failure (see pipeline setup notes).
#
# Add S2G notebooks to PIPELINE_STEPS below once you've built them --
# currently only S2B and B2S exist for Jira.
#
# Needs env_medallion_toolkit attached (for fmt.refresh_sql_endpoint) --
# no lakehouse attached, since each step notebook it calls already has its
# own attachments configured.

# CELL ********************
from notebookutils import mssparkutils
import fabric_medallion_toolkit as fmt

# CELL ********************
# --- The pipeline, in order. Add Gold notebooks here once built, e.g.:
# {"name": "S2G - dim_project", "timeout_seconds": 1800},
PIPELINE_STEPS = [
    {"name": "S2B - Jira", "timeout_seconds": 3600},
    {"name": "B2S - Jira", "timeout_seconds": 3600},
]

# --- SQL Analytics Endpoint refresh, run after all steps succeed. Add
# "Gold" here too, once it exists, if you want both refreshed.
LAKEHOUSES_TO_REFRESH = ["Silver"]

# CELL ********************
for step in PIPELINE_STEPS:
    step_name = step["name"]
    print(f"--- Running {step_name} ---")
    try:
        # useRootDefaultLakehouse=True bypasses Fabric's default behavior of
        # blocking a child notebook run when its default lakehouse differs
        # from the parent's -- this orchestration notebook has no lakehouse
        # attached itself, and S2B/B2S each attach different ones, so without
        # this the run would be blocked outright.
        mssparkutils.notebook.run(step_name, step["timeout_seconds"], {"useRootDefaultLakehouse": True})
        print(f"--- {step_name} succeeded ---")
    except Exception as exc:
        # The pipeline running this notebook can only see THIS notebook's
        # own failure message -- it has no visibility into which internal
        # step (S2B/B2S/S2G) actually broke. Prefixing the step name here
        # guarantees that's unambiguous in whatever the pipeline captures
        # (and shows in the Teams alert), rather than depending on however
        # Fabric happens to format the underlying notebook-run exception.
        raise RuntimeError(f"Failed at step '{step_name}': {exc}") from exc

# CELL ********************
print("--- Refreshing SQL Analytics Endpoint metadata ---")
for lh_name in LAKEHOUSES_TO_REFRESH:
    try:
        result = fmt.refresh_sql_endpoint(mssparkutils, lh_name)
        print(f"[{lh_name}] SQL Endpoint refresh result: {result}")
    except Exception as exc:
        # Best-effort: don't fail the whole orchestration over a sync lag
        # issue -- the underlying data is already correct either way.
        print(f"[{lh_name}] SQL Endpoint refresh WARNING (data itself is unaffected): {exc}")

# CELL ********************
print("Orchestration complete. All steps succeeded.")
