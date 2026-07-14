# Fabric notebook source
# "Orchestration - Jira" — the single entry point for the whole pipeline:
# Source -> Bronze -> Silver -> Gold dimensions -> Gold facts -> SQL
# Endpoint refresh, then exit. One consistent execution model throughout
# (a dependency graph, topologically sorted, run strictly one notebook at
# a time -- never in parallel), rather than mixing a flat list for
# Bronze/Silver with a different mechanism for Gold.
#
# Needs env_medallion_toolkit attached (for fmt.build_medallion_run_order
# and fmt.refresh_sql_endpoint) -- no lakehouse attached itself, since each
# step notebook it calls already has its own attachments configured.

# CELL ********************
from notebookutils import mssparkutils
import fabric_medallion_toolkit as fmt

# CELL ********************
# --- Declare every step here, in the shape natural to what it actually is.
# Add a new source's Bronze/Silver notebooks, a new dimension, or a new
# fact by editing ONLY this cell -- nothing below needs to change.

SOURCE_TO_BRONZE_STEPS = ["S2B - Jira"]   # add more here later, e.g. "S2B - Navision"
BRONZE_TO_SILVER_STEPS = ["B2S - Jira"]   # same idea, per source

DIMENSION_NOTEBOOKS = {
    "Gold - Dim_Date": [],
    "Gold - Dim_Project": [],
    "Gold - Dim_Resource": [],
    "Gold - Dim_IssueType": [],
    "Gold - Dim_Status": [],
    "Gold - Dim_Priority": [],
    "Gold - Dim_Board": [],
    # Dim_Issue joins to Dim_Project (for Sort_Path's project-code root
    # prefix) -- it must run AFTER Dim_Project exists, not just after
    # Silver. Without this declared explicitly, topological_sort's
    # alphabetical tie-break runs "Dim - Issue" before "Dim - Project"
    # (I < P), which is exactly the TABLE_OR_VIEW_NOT_FOUND error this
    # notebook used to hit.
    "Gold - Dim_Issue": ["Gold - Dim_Project"],
}

# Facts only declare dependencies on OTHER FACTS here -- depending on every
# dimension is automatic (handled inside build_medallion_run_order below),
# not restated per fact.
FACT_NOTEBOOKS = {
    "Gold - Fact_Issue": [],
    "Gold - Fact_Comment": [],
    "Gold - Fact_Worklog": [],
    # Bridge_IssueResource replaces the old Bridge_IssuePeopleInvolved: same
    # idea, but it now carries the LEAD (assignee) as well as the people
    # involved, one row per issue x person with Is_Lead/Is_Involved flags.
    "Gold - Bridge_IssueResource": [],
    "Gold - Bridge_IssueLink": [],
    # Fact_ResourceAllocation now joins Bridge_IssueResource to pick up
    # Issue_Resource_Key (its FK to the bridge, and the model's single
    # resource path) -- so the bridge must exist BEFORE it runs. Declaring
    # this is not optional: without it, topological_sort's alphabetical
    # tie-break happily runs Fact_ResourceAllocation first (B < F is only
    # true for the bridge, but nothing GUARANTEES the ordering), and the
    # join hits TABLE_OR_VIEW_NOT_FOUND on a clean build.
    "Gold - Fact_ResourceAllocation": ["Gold - Fact_Issue", "Gold - Bridge_IssueResource"],
}

LAKEHOUSES_TO_REFRESH = ["Silver", "Gold"]

# RUN_LOG records each step's outcome so a re-run can skip whatever
# already succeeded, instead of repeating the whole pipeline after a
# failure partway through.
#
# RUN_ID scopes what counts as "already done" -- it defaults to today's
# date, so re-running LATER THE SAME DAY (e.g. retrying after fixing a
# bug) skips already-succeeded steps, but a FRESH scheduled run tomorrow
# gets a new run_id and reruns everything against today's data, which is
# what a recurring pipeline should normally do. Set RUN_ID to a fixed
# string manually if you want to resume a specific earlier attempt
# instead of today's.
#
# FORCE_FULL_RERUN=True ignores the log entirely and reruns every step,
# regardless of RUN_ID -- use this if you deliberately want a clean run
# (e.g. after a schema change you know invalidates prior results).
from datetime import date

RUN_LOG = "Gold.gold.orchestration_log"
RUN_ID = str(date.today())
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
    print(f"Resuming run_id '{RUN_ID}' -- already succeeded, will be skipped: {sorted(already_completed)}")

# CELL ********************
for step_name in run_order:
    if step_name in already_completed:
        print(f"--- Skipping {step_name} (already succeeded this run) ---")
        continue

    print(f"--- Running {step_name} ---")
    # useRootDefaultLakehouse=True bypasses Fabric's default behavior of
    # blocking a child notebook run when its default lakehouse differs
    # from the parent's -- this orchestration notebook has no lakehouse
    # attached itself, and each step below attaches its own, so without
    # this the run would be blocked outright.
    #
    # No try/except around the run itself: if a step fails, its exception
    # (which already contains that notebook's own real error) should
    # propagate all the way up unchanged, so this notebook's own failure
    # carries the real error, not a summarized/re-wrapped version of it --
    # EXCEPT we prefix which step failed, since the pipeline calling this
    # notebook can only see THIS notebook's own failure message, not which
    # internal step actually broke. The log write on failure IS wrapped,
    # separately, so a logging problem never masks the real step failure.
    try:
        mssparkutils.notebook.run(step_name, 3600, {"useRootDefaultLakehouse": True})
        print(f"--- {step_name} succeeded ---")
        fmt.log_step_status(spark, RUN_LOG, RUN_ID, step_name, "succeeded")
    except Exception as exc:
        try:
            fmt.log_step_status(spark, RUN_LOG, RUN_ID, step_name, "failed")
        except Exception:
            pass  # logging the failure must never hide the real one below
        raise RuntimeError(f"Failed at step '{step_name}': {exc}") from exc

# CELL ********************
fmt.refresh_sql_endpoints(mssparkutils, LAKEHOUSES_TO_REFRESH)

# CELL ********************
print("Orchestration complete. All steps succeeded.")
