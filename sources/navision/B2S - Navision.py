# Fabric notebook source

# MARKDOWN ********************

# # Bronze → Silver — Navision
#
# Cleanses the Navision tables (Parquet, one folder per table, under `Files/navision_Bronze` in the **`navision_bronze`** lakehouse) into the shared **`silver`** lakehouse under a **`navision`** schema folder.
#
# All the logic lives in the toolkit — `fmt.run_file_silver` discovers the table folders, cleanses each (trim, empty→null, drop fully-empty columns, flag duplicates, skip empty tables), writes Silver, and prunes Silver tables whose Bronze source is gone. Adding another tabular source is the same call with a different `bronze_lakehouse` / `silver_schema`.
#
# **Environment:** `env_medallion_toolkit` attached (rebuild/upload the wheel so it includes `run_file_silver`), with the **`navision_bronze`** and **`silver`** lakehouses added to the notebook.

# CELL ********************

import fabric_medallion_toolkit as fmt

reports = fmt.run_file_silver(
    bronze_lakehouse="navision_bronze",
    bronze_subpath="navision_Bronze",      # folder under Files/ (case-sensitive)
    silver_lakehouse="silver",             # shared Silver lakehouse
    silver_schema="navision",              # schema folder -> Tables/navision/<table>
    exclude_prefixes=("$", "dbo_"),        # NAV system tables + SQL-mirror artifacts
    cast=False,                            # keep codes that look numeric as strings
    prune=True,                            # drop Silver tables whose source is gone
    overrides={
        # "Customer":        {"pk": ["No_"], "no_trim": ["Comment"]},
        # "Some_Big_Table":  {"skip": True},
        # "Prices":          {"cast": True},            # opt in to casting per table
    },
)

# CELL ********************

# Run summary
written = [r for r in reports if r.get("written")]
skipped = [r for r in reports if not r.get("written")]
print(f"{len(written)} table(s) written to Silver, {len(skipped)} skipped/failed")
for r in skipped:
    reason = r.get("error") or ("0 rows" if r.get("rows") == 0 else "skipped")
    print(f"  - {r['table']}: {reason}")
