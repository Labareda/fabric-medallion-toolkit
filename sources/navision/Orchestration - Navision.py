# Fabric notebook source

# MARKDOWN ********************

# ## Orchestration - Navision -- single entry point
#
# Runs the Navision medallion in dependency order:
#
#     B2S - Navision            (Bronze -> Silver: clean every ess_* table)
#       -> Gold - Dim_Date
#       -> Gold - Dim_Account
#       -> Gold - Dim_Benefit_Type
#         -> Gold - Fact_GL      (depends on dim_account + dim_benefit_type)
#
# There is NO source-to-Bronze step: Navision Bronze is a mirror created outside
# this pipeline, so Bronze is already populated. The order is built with
# fmt.build_medallion_run_order (same as the Jira orchestration): every dimension
# runs after Silver, every fact after all dimensions.
#
# Needs env_medallion_toolkit attached. No lakehouse attached here -- each step
# notebook uses its own (useRootDefaultLakehouse=True). Make sure the step
# notebooks are named exactly as below and have their SILVER_SCHEMA / GOLD_SCHEMA
# set, and that the silver / Gold lakehouses are available to them.

# MARKDOWN ********************

# ##### Step configuration

# CELL ********************

from notebookutils import mssparkutils
import fabric_medallion_toolkit as fmt

# Navision Bronze is a mirror (external) -- nothing to extract, so no S2B step.
SOURCE_TO_BRONZE_STEPS = []
BRONZE_TO_SILVER_STEPS = ["B2S - Navision"]

# Dimensions declare dependencies on OTHER dimensions only (all are []):
# they were deliberately made independent, so any order works.
DIMENSION_NOTEBOOKS = {
    "Gold - Dim_Date":         [],
    "Gold - Dim_Account":      [],
    "Gold - Dim_Benefit_Type": [],
}

# Facts auto-depend on EVERY dimension (added inside build_medallion_run_order),
# so fact_gl runs after all three dims. No fact reads another fact -> [].
FACT_NOTEBOOKS = {
    "Gold - Fact_GL": [],
}

# Notebook display names in Fabric, Lakehouse display names to refresh, timeout.
STEP_TIMEOUT_SECONDS  = 7200
LAKEHOUSES_TO_REFRESH = ["silver", "Gold"]   # adjust to your Silver / Gold lakehouse names

# MARKDOWN ********************

# ##### Compute the run order

# CELL ********************

run_order = fmt.build_medallion_run_order(
    source_to_bronze=SOURCE_TO_BRONZE_STEPS,
    bronze_to_silver=BRONZE_TO_SILVER_STEPS,
    dimensions=DIMENSION_NOTEBOOKS,
    facts=FACT_NOTEBOOKS,
)
print("Computed run order:")
for i, step in enumerate(run_order, 1):
    print(f"  {i}. {step}")

# MARKDOWN ********************

# ##### Run each step in order, then refresh the endpoints

# CELL ********************

import time

run_start = time.time()
failed = []

for i, step in enumerate(run_order, start=1):
    print(f"--- [{i}/{len(run_order)}] Running {step} ---")
    t0 = time.time()
    try:
        # useRootDefaultLakehouse=True: this notebook has no lakehouse attached;
        # each step uses its own default.
        mssparkutils.notebook.run(step, STEP_TIMEOUT_SECONDS, {"useRootDefaultLakehouse": True})
        print(f"    {step} succeeded ({time.time() - t0:.0f}s)")
    except Exception as exc:
        print(f"    {step} FAILED ({time.time() - t0:.0f}s): {exc}")
        failed.append(step)
        # Fail fast -- every Navision step is required, and a later step would
        # just read half-built data. Fix and re-run.
        raise RuntimeError(f"Orchestration failed at step '{step}': {exc}") from exc

# All steps succeeded: refresh the SQL endpoints so the new tables are queryable.
fmt.refresh_sql_endpoints(mssparkutils, LAKEHOUSES_TO_REFRESH)
print(f"\nNavision orchestration complete: {len(run_order)} steps in "
      f"{(time.time() - run_start) / 60:.1f} minutes.")
