# Fabric notebook source
# "Orchestration - Jira" — runs the full Jira pipeline end to end, in
# order: Source-to-Bronze, Bronze-to-Silver, then Silver-to-Gold (once
# those exist). No alerting logic here -- this notebook's only job is to
# run the steps and either finish cleanly or fail with the real error
# intact. The PIPELINE that schedules this notebook handles the Teams
# alert on failure (see pipeline setup notes).
#
# Add S2G notebooks to PIPELINE_STEPS below once you've built them --
# currently only S2B and B2S exist for Jira.
#
# This notebook needs no lakehouses attached itself -- each step notebook
# it calls already has its own attachments configured.

# CELL ********************
from notebookutils import mssparkutils

# CELL ********************
# --- The pipeline, in order. Add Gold notebooks here once built, e.g.:
# {"name": "S2G - dim_project", "timeout_seconds": 1800},
PIPELINE_STEPS = [
    {"name": "S2B - Jira", "timeout_seconds": 3600},
    {"name": "B2S - Jira", "timeout_seconds": 3600},
]

# CELL ********************
for step in PIPELINE_STEPS:
    step_name = step["name"]
    print(f"--- Running {step_name} ---")
    # useRootDefaultLakehouse=True bypasses Fabric's default behavior of
    # blocking a child notebook run when its default lakehouse differs
    # from the parent's -- this orchestration notebook has no lakehouse
    # attached itself, and S2B/B2S each attach different ones, so without
    # this the run would be blocked outright.
    #
    # No try/except here on purpose: if a step fails, its exception
    # (which already contains that notebook's own real error) should
    # propagate all the way up unchanged, so this notebook's own failure
    # -- and the pipeline Notebook activity running it -- carries the
    # real error, not a summarized/re-wrapped version of it.
    mssparkutils.notebook.run(step_name, step["timeout_seconds"], {"useRootDefaultLakehouse": True})
    print(f"--- {step_name} succeeded ---")

# CELL ********************
print("Orchestration complete. All steps succeeded.")
