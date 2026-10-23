# Fabric notebook source

# MARKDOWN ********************

# # Bronze → Silver — Navision
#
# Copies the Navision tables (Parquet, one folder per table, under `Files/navision_Bronze` in the **`navision_bronze`** lakehouse) into the shared **`silver`** lakehouse under a **`navision`** schema folder, doing only the minimum to make Silver clean:
#
# * **skip empty tables** — a 0-row table is not written
# * **drop fully-empty columns** — columns that are entirely null/blank are removed
# * **Fabric-safe dates** — the ancient-date rebase is set so reads/writes don't error, and any date/timestamp outside `0001-01-01 .. 9999-12-31` is nulled so the SQL endpoint can sync the table
#
# Everything else (trimming, casting, duplicate flags) is intentionally left out. All the logic is in the toolkit's `fmt.run_file_silver`, which also discovers the tables, processes them concurrently, and prunes Silver tables whose Bronze source is gone. Adding another tabular source is the same call with a different `bronze_lakehouse` / `silver_schema`.
#
# **Environment:** `env_medallion_toolkit` attached (wheel ≥ 0.3.74), with the **`navision_bronze`** and **`silver`** lakehouses added to the notebook.

# CELL ********************

import fabric_medallion_toolkit as fmt

reports = fmt.run_file_silver(
    bronze_lakehouse="navision_bronze",
    bronze_subpath="navision_Bronze",      # folder under Files/ (case-sensitive)
    silver_lakehouse="silver",             # shared Silver lakehouse
    silver_schema="navision",              # schema folder -> Tables/navision/<table>
    exclude_prefixes=("$", "dbo_"),        # NAV system tables + SQL-mirror artifacts
    max_workers=8,                         # process tables concurrently (~1595 tables)
    # skip=["Some_Big_Table"],             # explicit tables to leave out
)

# CELL ********************

# Run summary
written = [r for r in reports if r.get("written")]
skipped = [r for r in reports if not r.get("written")]
print(f"{len(written)} table(s) written to Silver, {len(skipped)} skipped/failed")
for r in skipped:
    reason = r.get("error") or ("0 rows" if r.get("rows") == 0 else "skipped")
    print(f"  - {r['table']}: {reason}")
