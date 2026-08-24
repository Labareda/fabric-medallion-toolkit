# Fabric notebook source
# "Orchestration - Jira and Xray" — single entry point: Source -> Bronze ->
# Silver -> Gold dimensions -> Gold facts -> SQL Endpoint refresh. One
# execution model throughout: a dependency graph, topologically sorted, run
# strictly one notebook at a time.
#
# Needs env_medallion_toolkit attached. No lakehouse attached itself -- each
# step notebook attaches its own.
#
# CHANGES FROM "Orchestration - Jira":
#   + Xray source added as a second extraction branch (see OPTIONAL_STEPS --
#     an Xray outage no longer blocks the Jira reporting refresh)
#   + Dim_Resolution, Dim_Resource_Role, Dim_Team, Dim_Test_Status
#   + Fact_Issue_History, Fact_Worklog
#   ~ renamed: Fact_ResourceAllocation -> Fact_Resource_Allocation
#   - removed: Dim_Board, Dim_Sprint, Bridge_IssueSprint (the client barely
#     uses sprints; trend now comes from Fact_Issue_History instead of sprint
#     burndown). Delete the notebooks too, or they rot unscheduled -- which is
#     exactly how Fact_Comment ended up built-but-never-run.
#   - removed: Fact_Comment (comment_body is the largest table in the model
#     and powers none of the requested reports). If comment ACTIVITY is wanted
#     later, build it without the body and add it back here.
#   - removed: Dim_Version, Bridge_Issue_Link, Bridge_Issue_Label,
#     Bridge_Test_Set_Test -- notebooks deleted from sources/Gold; nothing
#     schedules them any more. Dim_Link_Type has no consumer left either
#     (it existed to be joined by Bridge_Issue_Link) -- keeping it here as a
#     built-but-unused dimension unless issue-link traceability reporting
#     comes back into scope, in which case delete it too.
#   ~ ONE test fact now: Gold - Fact_Test (run grain, Is_Latest flag).
#     This replaced a long churn of designs (test-set-membership Fact_Test
#     -> Fact_Test_Run + Fact_Test_Coverage + Dim_Test_Set -> this). The
#     client's own conclusion drove it: they want ONE test table with
#     everything + a flag to pick the current row, not several. Coverage
#     (which requirement a test covers), Test Set / Plan / Execution
#     membership, and blocking all live in Bridge_Issue_Link now -- they're
#     issue-to-issue links, and the bridge carries each linked test's
#     latest status denormalised so "select any issue -> see linked tests
#     + results" works as plain columns. Dim_Test_Set was dropped (a Test
#     Set is just a Dim_Issue row); "requirements with no test" is a
#     measure on Dim_Issue, not a fact table.
#   - removed: Dim_Resolution -- not required by any report. Notebook
#     deleted from sources/Gold; Fact_Issue no longer joins it or carries
#     Resolution_Key.
#   + Fact_Resource_Day_Allocation -- resource/day capacity for conflict
#     detection on the Resource report. Reads Fact_Resource_Allocation AND
#     Fact_Issue (declared as a dependency below) -- the one remaining
#     fact-reads-fact exception in the model.
#   + Bridge_Issue_Link -- one row per issue per link record, any link type,
#     both directions (mirrors Jira's own Linked work items panel). Built
#     for the Blocked Tests report (client wants blocked tests plus the
#     work items blocking them; Dim_Issue.Predecessor_Issue_Code is a
#     comma-joined string, not report-friendly) but general-purpose --
#     filter Link_Type_Name/Direction for any relation, not just Blocks.
#     A Blocks-only Bridge_Issue_Blocks existed for one commit and was
#     removed again immediately -- this table is a strict superset of it
#     (filter Link_Type_Name='Blocks' AND Direction='Inward' for the exact
#     same rows), so the dedicated one was redundant, not a real
#     simplification. Also gives Dim_Link_Type its first real consumer --
#     it's carried no relationship since the original Bridge_Issue_Link
#     was deleted a few commits back.

# CELL ********************
from notebookutils import mssparkutils
import fabric_medallion_toolkit as fmt

# CELL ********************
# Both sources land in Bronze first, then both standardize into Silver.
# build_medallion_run_order makes every silver step depend on ALL bronze
# steps, so this ordering is a real barrier, not a coincidence of the list.
#
# RESOLVED: xray.test_sets was previously orphaned -- no notebook in this
# repo produced it, even though it existed in Silver with real data (built by
# something since deleted/never committed). S2B/B2S - Xray now produce it
# (plus test_plans, preconditions, tests) directly, so this is no longer a
# guess.
SOURCE_TO_BRONZE_STEPS = ["S2B - Jira", "S2B - Xray"]
BRONZE_TO_SILVER_STEPS = ["B2S - Jira", "B2S - Xray"]

DIMENSION_NOTEBOOKS = {
    "Gold - Dim_Date": [],
    "Gold - Dim_Project": [],
    "Gold - Dim_Resource": [],
    "Gold - Dim_Resource_Role": [],
    "Gold - Dim_Team": [],
    "Gold - Dim_IssueType": [],
    "Gold - Dim_Status": [],
    "Gold - Dim_Priority": [],
    "Gold - Dim_Link_Type": [],
    "Gold - Dim Test Status": [],
    # Dim_Issue joins Dim_Project for Sort_Path's project-code root prefix.
    # The alphabet actively gets this WRONG (I < P), and it's the exact
    # TABLE_OR_VIEW_NOT_FOUND this pipeline used to die on.
    "Gold - Dim_Issue": ["Gold - Dim_Project"],
    # Dim_Test = all descriptive attributes of a Test in one table (Jira
    # issue attrs + Xray test_type/steps). Joins Dim_Project for the project
    # name. Xray-dependent (reads Silver.xray.tests) -> OPTIONAL below.
    "Gold - Dim_Test": ["Gold - Dim_Project"],
}

# Facts declare dependencies on OTHER FACTS only -- depending on every
# dimension is added automatically inside build_medallion_run_order, not
# restated per fact.
#
# Every one of these is an empty list, and that is worth stating rather than
# leaving implicit: NO fact in this model reads another fact. Each one goes
# Silver -> dimensions -> itself. Verified by checking which Gold tables each
# notebook actually references. If a future fact does read another (a
# snapshot built off Fact_Issue, say), declare it here -- the alphabet will
# not save you.
FACT_NOTEBOOKS = {
    "Gold - Fact_Issue": [],
    "Gold - Fact_Issue_History": [],
    "Gold - Fact_Resource_Allocation": [],
    "Gold - Fact_Worklog": [],
    "Gold - Bridge_Issue_Link": [],
    # The ONE test fact -- run grain, Is_Latest flag. Reads only
    # Silver.xray.test_runs plus dimensions. (Consolidated the old
    # Fact_Test_Run + Fact_Test_Coverage into this single table.)
    "Gold - Fact_Test": [],
    # Reads Gold.gold.fact_resource_allocation AND fact_issue -- the other
    # exception. Must run after both.
    "Gold - Fact_Resource_Day_Allocation": ["Gold - Fact_Resource_Allocation", "Gold - Fact_Issue"],
}

# --- Steps allowed to fail without killing the run -------------------------
# Xray is a separate product with a separate API and its own auth. Without
# this, an Xray token expiry at 02:00 takes down the ENTIRE programme
# refresh -- timeline, RAID, resourcing, everything -- because one testing
# table couldn't build. The Jira side has no dependency on Xray, so there is
# no reason for that coupling to exist.
#
# A soft-failed step logs "failed", is reported loudly at the end, and gets
# retried on the next run. It does NOT count towards run completeness (see
# REQUIRED_STEPS below), which is the part that actually matters -- more on
# that there.
#
# Set to an empty set for strict fail-fast on everything.
OPTIONAL_STEPS = {
    "S2B - Xray",
    "B2S - Xray",
    "Gold - Dim Test Status",
    "Gold - Dim_Test",
    "Gold - Fact_Test",
}

LAKEHOUSES_TO_REFRESH = ["Silver", "Gold"]

# Fact_Issue_History windows over every changelog row in the instance and is
# the slowest step here by some margin. 3600s was fine for the old notebook
# set; give it room rather than discovering the timeout in production.
STEP_TIMEOUT_SECONDS = 7200

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
# Gold at all).
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

# --- Why resolve_run_id is given REQUIRED_STEPS, not run_order -------------
# resolve_run_id resumes the last run if it didn't complete EVERY step it is
# given. Hand it the full run_order and a soft-failed optional step makes the
# run permanently "incomplete" -- so tomorrow's scheduled trigger RESUMES
# instead of starting fresh, skipping every already-succeeded Jira step and
# retrying only Xray. The programme dashboards would then quietly stop
# refreshing until someone fixed Xray, which is the precise failure this
# optional-step machinery exists to prevent.
#
# Measuring completeness on the required steps only keeps both behaviours
# intact: a genuine Jira failure still resumes; a flaky Xray run doesn't
# freeze the pipeline.
REQUIRED_STEPS = [s for s in run_order if s not in OPTIONAL_STEPS]

RUN_ID = fmt.resolve_run_id(spark, RUN_LOG, REQUIRED_STEPS, override=RUN_ID_OVERRIDE)

already_completed = set() if FORCE_FULL_RERUN else fmt.get_completed_steps(spark, RUN_LOG, RUN_ID)
if already_completed:
    print(f"Resuming run_id '{RUN_ID}' -- skipping: {sorted(already_completed)}")
else:
    print(f"Starting fresh run_id '{RUN_ID}'")

# CELL ********************
soft_failures = []

for step_name in run_order:
    if step_name in already_completed:
        print(f"--- Skipping {step_name} (already succeeded) ---")
        continue

    print(f"--- Running {step_name} ---")
    # useRootDefaultLakehouse=True: this notebook has no lakehouse attached and
    # each step attaches its own, which Fabric otherwise blocks.
    #
    # For REQUIRED steps the exception propagates unchanged (so the real error
    # survives), prefixed with which step died -- the calling pipeline can only
    # see THIS notebook's failure message. The failure LOG write is wrapped
    # separately so a logging problem can never mask the real failure.
    try:
        mssparkutils.notebook.run(
            step_name, STEP_TIMEOUT_SECONDS, {"useRootDefaultLakehouse": True}
        )
        print(f"--- {step_name} succeeded ---")
        fmt.log_step_status(spark, RUN_LOG, RUN_ID, step_name, "succeeded")
    except Exception as exc:
        try:
            fmt.log_step_status(spark, RUN_LOG, RUN_ID, step_name, "failed")
        except Exception:
            pass

        if step_name in OPTIONAL_STEPS:
            # Recorded, reported, retried next run -- but not fatal.
            print(f"--- {step_name} FAILED (optional -- continuing) ---")
            print(f"    {exc}")
            soft_failures.append((step_name, str(exc)))
            continue

        raise RuntimeError(f"Failed at step '{step_name}': {exc}") from exc

# CELL ********************
# Refresh runs even after a soft failure: the Jira tables DID rebuild and the
# endpoint should reflect them. Skipping the refresh here would mean an Xray
# problem silently held back good Jira data, which is the coupling this whole
# arrangement is meant to remove.
fmt.refresh_sql_endpoints(mssparkutils, LAKEHOUSES_TO_REFRESH)

# CELL ********************
if soft_failures:
    print("=" * 70)
    print(f"Orchestration completed with {len(soft_failures)} OPTIONAL step failure(s).")
    print("Required steps all succeeded; Jira-sourced tables are current.")
    print("These will be retried on the next run:")
    for name, err in soft_failures:
        print(f"  - {name}: {err[:200]}")
    print("")
    print("Reporting affected while these are stale:")
    print("  Test Report, testing coverage, dashboards 9-11 (test-related visuals).")
    print("  Timeline, RAID, requirements, resourcing and governance are unaffected.")
    print("=" * 70)
else:
    print("Orchestration complete. All steps succeeded.")
