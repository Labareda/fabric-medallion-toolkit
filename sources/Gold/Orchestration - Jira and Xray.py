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
#   + Dim_Resolution, Dim_Role, Dim_Team, Dim_Test_Status
#   + Fact_Issue_History, Fact_Worklog, Fact_Test_Run
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
#   + Dim Test Set, Fact Test, Fact Test Coverage -- these notebooks existed
#     in sources/Gold but were never actually scheduled here (the exact
#     "built but never run" trap warned about above for Fact_Comment). Now
#     wired in, all OPTIONAL (Xray-dependent) like the rest of the test
#     tables, so the Test Report actually gets fresh data.
#   ~ Fact_Test_Run now consolidates what used to be two overlapping,
#     same-grain run tables (fact_test_run / fact_test_run_history).

# CELL ********************
from notebookutils import mssparkutils
import fabric_medallion_toolkit as fmt

# CELL ********************
# Both sources land in Bronze first, then both standardize into Silver.
# build_medallion_run_order makes every silver step depend on ALL bronze
# steps, so this ordering is a real barrier, not a coincidence of the list.
#
# ASSUMPTION TO CHECK: the Xray notebook names below are a guess -- Silver
# already has xray.test_runs / test_sets / statuses, so something is
# producing them. Rename these two to match whatever it actually is.
SOURCE_TO_BRONZE_STEPS = ["S2B - Jira", "S2B - Xray"]
BRONZE_TO_SILVER_STEPS = ["B2S - Jira", "B2S - Xray"]

DIMENSION_NOTEBOOKS = {
    "Gold - Dim_Date": [],
    "Gold - Dim_Project": [],
    "Gold - Dim_Resource": [],
    "Gold - Dim_Role": [],
    "Gold - Dim_Team": [],
    "Gold - Dim_IssueType": [],
    "Gold - Dim_Status": [],
    "Gold - Dim_Priority": [],
    "Gold - Dim_Resolution": [],
    "Gold - Dim_Link_Type": [],
    "Gold - Dim Test Status": [],
    "Gold - Dim Test Set": [],
    # Dim_Issue joins Dim_Project for Sort_Path's project-code root prefix.
    # The alphabet actively gets this WRONG (I < P), and it's the exact
    # TABLE_OR_VIEW_NOT_FOUND this pipeline used to die on. Still the only
    # dimension-to-dimension dependency in the model.
    "Gold - Dim_Issue": ["Gold - Dim_Project"],
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
    "Gold - Fact_Test_Run": [],
    "Gold - Fact Test": [],
    # Reads Gold.gold.fact_test (set_stats CTE) -- the one fact that reads
    # another fact in this model, so it's the one exception to "facts only
    # depend on other facts here, never implicitly."
    "Gold - Fact Test Coverage": ["Gold - Fact Test"],
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
    "Gold - Dim Test Set",
    "Gold - Fact_Test_Run",
    "Gold - Fact Test",
    "Gold - Fact Test Coverage",
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
