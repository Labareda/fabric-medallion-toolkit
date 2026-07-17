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
    "Gold - Fact_Issue": [],
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
# notebook's cell properties). Leave it None for normal runs. Set it only for
# the rare case of resuming ONE specific older run_id by hand -- normal
# resume-after-failure is automatic (see below), so you almost never need this.
RUN_ID_OVERRIDE = None

# RUN_LOG lives in its OWN lakehouse ("Config" here -- rename to whatever
# you actually call it), not Gold. This is pipeline-engineering metadata
# (which run succeeded/failed, when), not business data -- it shouldn't
# clutter the Gold lakehouse a business user or Power BI browses, and it
# shouldn't be at risk of getting wiped along with Gold during a full Gold
# rebuild (or vice versa: resetting run history shouldn't require touching
# Gold at all). log_step_status/get_completed_steps/resolve_run_id are fully
# generic about this -- log_table is just a string, any lakehouse works.
#
# One-time setup: create this lakehouse in the workspace and attach it to
# this notebook (Notebook -> Lakehouses -> Add), same as Bronze/Silver/Gold
# already are, before running with this value.
RUN_LOG = "Config.control.orchestration_log"
FORCE_FULL_RERUN = False

# CELL ********************
run_order = fmt.build_medallion_run_order(
    source_to_bronze=SOURCE_TO_BRONZE_STEPS,
    bronze_to_silver=BRONZE_TO_SILVER_STEPS,
    dimensions=DIMENSION_NOTEBOOKS,
    facts=FACT_NOTEBOOKS,
)
print("Computed run order:", run_order)

# resolve_run_id decides fresh-vs-resume automatically:
#   - if the LAST run in the log didn't finish every step (it failed or was
#     interrupted), REUSE its run_id -> this trigger resumes it and skips
#     what already succeeded. So a failure at 09:00 re-triggered at 10:00
#     picks up where it stopped, NOT a full rerun.
#   - if the last run completed everything (or there's no log yet), get a
#     NEW run_id -> a full fresh pass. So a successful 09:00 run followed by
#     a deliberate 10:00 run reprocesses everything.
# No date-vs-timestamp tradeoff, and no manual run_id bookkeeping.
RUN_ID = fmt.resolve_run_id(spark, RUN_LOG, run_order, override=RUN_ID_OVERRIDE)

already_completed = set() if FORCE_FULL_RERUN else fmt.get_completed_steps(spark, RUN_LOG, RUN_ID)
if already_completed:
    print(f"Resuming run_id '{RUN_ID}' -- skipping: {sorted(already_completed)}")
else:
    print(f"Starting fresh run_id '{RUN_ID}'")

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
