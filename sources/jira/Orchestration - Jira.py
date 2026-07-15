# Fabric notebook source
# "Orchestration - Jira" — single entry point: Source -> Bronze -> Silver ->
# Gold dimensions -> Gold facts -> SQL Endpoint refresh. One execution model
# throughout: a dependency graph, topologically sorted, run strictly one
# notebook at a time.
#
# Needs env_medallion_toolkit attached. No lakehouse attached itself -- each
# step notebook attaches its own.

# CELL ********************
from notebookutils import mssparkutils
from datetime import datetime, timezone
import fabric_medallion_toolkit as fmt

# CELL ********************
SOURCE_TO_BRONZE_STEPS = ["S2B - Jira"]
BRONZE_TO_SILVER_STEPS = ["B2S - Jira"]

DIMENSION_NOTEBOOKS = {
    "Gold - Dim_Date": [],
    "Gold - Dim_Project": [],
    "Gold - Dim_Resource": [],
    "Gold - Dim_IssueType": [],
    "Gold - Dim_Status": [],
    "Gold - Dim_Priority": [],
    "Gold - Dim_Board": [],
    # Dim_Sprint resolves Board_Key, so Dim_Board must exist first. Without this
    # declared, topological_sort's alphabetical tie-break runs Sprint before
    # Board (B < S is true, so this one would happen to work today) -- but the
    # ordering is only GUARANTEED when the dependency is declared. Don't rely on
    # the alphabet.
    "Gold - Dim_Sprint": ["Gold - Dim_Board"],
    # Dim_Issue joins Dim_Project for Sort_Path's project-code root prefix. This
    # one the alphabet actively gets WRONG (I < P), and it's the exact
    # TABLE_OR_VIEW_NOT_FOUND this pipeline used to die on.
    "Gold - Dim_Issue": ["Gold - Dim_Project"],
}

# Facts declare dependencies on OTHER FACTS only -- depending on every dimension
# is added automatically inside build_medallion_run_order, not restated per fact.
FACT_NOTEBOOKS = {
    # Fact_StatusHistory FIRST: Fact_Issue reads Actual_Start_Date back out of
    # it (Jira has no actual-start field -- it only has a changelog). This is the
    # one non-obvious ordering constraint in the whole pipeline.
    "Gold - Fact_StatusHistory": [],
    "Gold - Fact_Issue": ["Gold - Fact_StatusHistory"],
    "Gold - Fact_ResourceAllocation": [],
    "Gold - Bridge_IssueSprint": [],
    "Gold - Bridge_IssueLink": [],
    # Was built but never scheduled -- added here so comment-based reporting
    # (communications delivery, code-review turnaround) actually refreshes.
    "Gold - Fact_Comment": [],
}

LAKEHOUSES_TO_REFRESH = ["Silver", "Gold"]

# PARAMETERS CELL -- a Fabric pipeline "Execute Notebook" activity can inject
# a value for RUN_ID_OVERRIDE here (tag this cell as a parameters cell in the
# notebook's cell properties). Leave it None for normal scheduled runs.
# Set it to a failed run's exact run_id (visible in orchestration_log) to
# resume that specific run instead of starting a fresh one.
RUN_ID_OVERRIDE = None

# RUN_ID must be unique PER TRIGGER, not per day, since this pipeline can run
# more than once a day. date-only run_ids broke that: a second same-day
# trigger would find every step already marked "succeeded" under that same
# date and skip the ENTIRE run, refreshing nothing. Timestamp-to-the-second
# gives every trigger its own run_id, so an intra-day re-run always does a
# full fresh pass -- unless RUN_ID_OVERRIDE is explicitly set to resume a
# specific earlier (failed) run.
RUN_LOG = "Gold.gold.orchestration_log"
RUN_ID = RUN_ID_OVERRIDE or datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
FORCE_FULL_RERUN = False

# CELL ********************
run_order = fmt.build_medallion_run_order(
    source_to_bronze=SOURCE_TO_BRONZE_STEPS,
    bronze_to_silver=BRONZE_TO_SILVER_STEPS,
    dimensions=DIMENSION_NOTEBOOKS,
    facts=FACT_NOTEBOOKS,
)
print("Computed run order:", run_order)

already_completed = set() if FORCE_FULL_RERUN else fmt.get_completed_steps(spark, RUN_LOG, RUN_ID)
if already_completed:
    print(f"Resuming run_id '{RUN_ID}' -- skipping: {sorted(already_completed)}")

# CELL ********************
for step_name in run_order:
    if step_name in already_completed:
        print(f"--- Skipping {step_name} (already succeeded) ---")
        continue

    print(f"--- Running {step_name} ---")
    # useRootDefaultLakehouse=True: this notebook has no lakehouse attached and
    # each step attaches its own, which Fabric otherwise blocks.
    #
    # The step's own exception propagates unchanged (so the real error survives),
    # prefixed with which step died -- the calling pipeline can only see THIS
    # notebook's failure message. The failure LOG write is wrapped separately so
    # a logging problem can never mask the real failure.
    try:
        mssparkutils.notebook.run(step_name, 3600, {"useRootDefaultLakehouse": True})
        print(f"--- {step_name} succeeded ---")
        fmt.log_step_status(spark, RUN_LOG, RUN_ID, step_name, "succeeded")
    except Exception as exc:
        try:
            fmt.log_step_status(spark, RUN_LOG, RUN_ID, step_name, "failed")
        except Exception:
            pass
        raise RuntimeError(f"Failed at step '{step_name}': {exc}") from exc

# CELL ********************
fmt.refresh_sql_endpoints(mssparkutils, LAKEHOUSES_TO_REFRESH)

# CELL ********************
print("Orchestration complete. All steps succeeded.")
