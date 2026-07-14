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
    "Gold - Fact_ResourceAllocation": ["Gold - Fact_Issue"],
    "Gold - Bridge_IssuePeopleInvolved": [],
    "Gold - Bridge_IssueLink": [],
}

LAKEHOUSES_TO_REFRESH = ["Silver", "Gold"]

# CELL ********************
run_order = fmt.build_medallion_run_order(
    source_to_bronze=SOURCE_TO_BRONZE_STEPS,
    bronze_to_silver=BRONZE_TO_SILVER_STEPS,
    dimensions=DIMENSION_NOTEBOOKS,
    facts=FACT_NOTEBOOKS,
)
print("Computed run order:", run_order)

# CELL ********************
for step_name in run_order:
    print(f"--- Running {step_name} ---")
    # useRootDefaultLakehouse=True bypasses Fabric's default behavior of
    # blocking a child notebook run when its default lakehouse differs
    # from the parent's -- this orchestration notebook has no lakehouse
    # attached itself, and each step below attaches its own, so without
    # this the run would be blocked outright.
    #
    # No try/except here on purpose: if a step fails, its exception
    # (which already contains that notebook's own real error) should
    # propagate all the way up unchanged, so this notebook's own failure
    # carries the real error, not a summarized/re-wrapped version of it --
    # EXCEPT we prefix which step failed, since the pipeline calling this
    # notebook can only see THIS notebook's own failure message, not which
    # internal step actually broke.
    try:
        mssparkutils.notebook.run(step_name, 3600, {"useRootDefaultLakehouse": True})
        print(f"--- {step_name} succeeded ---")
    except Exception as exc:
        raise RuntimeError(f"Failed at step '{step_name}': {exc}") from exc

# CELL ********************
fmt.refresh_sql_endpoints(mssparkutils, LAKEHOUSES_TO_REFRESH)

# CELL ********************
print("Orchestration complete. All steps succeeded.")
