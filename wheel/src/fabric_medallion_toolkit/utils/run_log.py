 """
Lets an orchestration notebook resume after a failure without re-running
steps that already succeeded -- generic to any pipeline built on
build_medallion_run_order, not Jira-specific.

Not a general "skip if it succeeded yesterday" cache: a run_id scopes
what counts as "already done". Pass the SAME run_id when resuming a
failed run (so already-succeeded steps are skipped) and a NEW run_id for
each fresh scheduled run (so everything re-runs against today's data,
which is almost always what a recurring pipeline should do).
"""

from datetime import datetime, timezone
from typing import Set

from pyspark.sql import functions as F


def log_step_status(spark, log_table: str, run_id: str, step_name: str, status: str) -> None:
    """
    Appends one row recording a step's outcome for this run_id.
    status: "succeeded" or "failed" (any string is accepted and stored,
    but only "succeeded" rows are treated as complete by
    get_completed_steps -- a "failed" row deliberately does NOT let a
    later resume skip that step).
    """
    row = spark.createDataFrame(
        [(run_id, step_name, status, datetime.now(timezone.utc))],
        schema="run_id string, step_name string, status string, logged_at timestamp",
    )
    row.write.format("delta").mode("append").saveAsTable(log_table)


def get_completed_steps(spark, log_table: str, run_id: str) -> Set[str]:
    """
    Returns the set of step names that have a "succeeded" row logged
    under this run_id. Returns an empty set (not an error) if log_table
    doesn't exist yet -- the very first run of a new pipeline has no log
    to read, which should just mean "nothing is done yet", not a crash.

    A step that appears with BOTH a "failed" and a later "succeeded" row
    for the same run_id (i.e. you fixed something and it succeeded on a
    later attempt within the same run_id) counts as completed -- only the
    most recent status per step matters, not whether it ever failed.
    """
    try:
        log_df = spark.table(log_table).filter(F.col("run_id") == run_id)
    except Exception:
        return set()

    if log_df.rdd.isEmpty():
        return set()

    latest_per_step = (
        log_df.groupBy("step_name")
        .agg(F.max_by("status", "logged_at").alias("latest_status"))
    )
    completed = latest_per_step.filter(F.col("latest_status") == "succeeded")
    return {r["step_name"] for r in completed.select("step_name").collect()}
