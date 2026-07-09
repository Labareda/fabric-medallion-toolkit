# Fabric notebook source
# "Orchestration - Jira" — the single entry point for the whole pipeline:
# Source -> Bronze -> Silver -> Gold dimensions -> Gold facts -> SQL
# Endpoint refresh, then exit. One consistent execution model throughout
# (a dependency graph, topologically sorted, run strictly one notebook at
# a time -- never in parallel), rather than mixing a flat list for
# Bronze/Silver with a different mechanism for Gold.
#
# Needs env_medallion_toolkit attached (for fmt.topological_sort and
# fmt.refresh_sql_endpoint) -- no lakehouse attached itself, since each
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

DIMENSION_NOTEBOOKS = [
    "Gold - Dim_Date",
    "Gold - Dim_Project",
    "Gold - Dim_User",
    "Gold - Dim_IssueType",
    "Gold - Dim_Status",
    "Gold - Dim_Priority",
    "Gold - Dim_Board",
]

# Facts only declare dependencies on OTHER FACTS here -- depending on every
# dimension is automatic (see the merge step below), not restated per fact.
FACT_NOTEBOOKS = {
    "Gold - Fact_Issue": [],
    # "Gold - Fact_ResourceAllocation": ["Gold - Fact_Issue"],  # add once built
}

LAKEHOUSES_TO_REFRESH = ["Silver", "Gold"]

# CELL ********************
# --- Merge everything into ONE dependency graph, ONE execution model ---

full_dependencies = {}

for step in SOURCE_TO_BRONZE_STEPS:
    full_dependencies[step] = []

for step in BRONZE_TO_SILVER_STEPS:
    full_dependencies[step] = list(SOURCE_TO_BRONZE_STEPS)

for dim in DIMENSION_NOTEBOOKS:
    full_dependencies[dim] = list(BRONZE_TO_SILVER_STEPS)

for fact, fact_deps in FACT_NOTEBOOKS.items():
    full_dependencies[fact] = DIMENSION_NOTEBOOKS + fact_deps

run_order = fmt.topological_sort(full_dependencies)
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
