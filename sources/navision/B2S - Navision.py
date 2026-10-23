# Fabric notebook source

# MARKDOWN ********************

# # Bronze → Silver — Navision
#
# Basic, generic cleansing of the Navision tables sitting as **Parquet, one folder per table, under Files in the `navision_bronze` lakehouse** (already tabular — no JSON to shred, so the `fabric_medallion_toolkit` Silver helpers for API data don't apply; this is a column-type-driven pass instead).
#
# **Goal: clean data in Silver — no empty tables, no empty columns.**
#
# **Environment:** needs `env_medallion_toolkit` attached (same Spark environment as the Jira pipeline), plus the **`navision_bronze`** and **`navision_silver`** lakehouses added to the notebook (Explorer panel). The table list is discovered by listing the Bronze Files folder — there is no discovery config / `NB_DiscoverTables` dependency any more.
#
# Cleansing applied to every table, driven by column type:
# 1. **Trim** leading/trailing whitespace on every string column
# 2. **Normalise empty strings** (`""`, `"   "`) to real nulls
# 3. **Drop fully-empty columns** — any column 100% null across every row (caught after trim+null, so all-whitespace columns go too)
# 4. **Skip empty tables** — a table with 0 rows is NOT written to Silver (no empty Silver tables)
# 5. **Cast** string columns that are cleanly numeric- or date-shaped back to their real type (only if *every* non-null value converts)
# 6. **Flag duplicates** — `_is_full_duplicate` always; `_is_pk_duplicate` only when a PK is supplied via `SILVER_OVERRIDES` (no discovery config to supply one). Rows are KEPT, never dropped
# 7. Add `_silver_processed_at`

# CELL ********************

# Needs env_medallion_toolkit attached so fabric_medallion_toolkit + notebookutils import.
import fabric_medallion_toolkit as fmt
from notebookutils import mssparkutils

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType, IntegerType, LongType, ShortType, ByteType,
    DoubleType, FloatType, DecimalType, DateType, TimestampType, BooleanType,
)
from datetime import datetime
import time

spark = SparkSession.builder.getOrCreate()
log = fmt.get_logger("b2s.navision")

# Navision came from an old SQL Server; some datetimes predate Spark's Gregorian
# cutover. CORRECTED reads/writes them as-is instead of raising READ_ANCIENT_DATETIME.
for _key in ("spark.sql.parquet.datetimeRebaseModeInRead",
             "spark.sql.parquet.int96RebaseModeInRead",
             "spark.sql.parquet.datetimeRebaseModeInWrite",
             "spark.sql.parquet.int96RebaseModeInWrite"):
    try:
        spark.conf.set(_key, "CORRECTED")
    except Exception:
        pass

# CELL ********************

# -- PARAMETERS ----------------------------------------------------------------

# Read from Bronze Files, write to Silver Tables. Both are separate Lakehouses,
# so each is resolved to its ABFS storage path by DISPLAY NAME at runtime -- no
# GUID hand-typed, and nothing relies on the session's single default Lakehouse.
BRONZE_LAKEHOUSE_NAME = "navision_bronze"
SILVER_LAKEHOUSE_NAME = "navision_silver"

# Write Silver tables under a schema/folder named after the source database, so
# one Silver lakehouse can hold several databases side by side
# (Tables/navision/<table>, Tables/pensionplus/<table>, ...). This needs the
# Silver lakehouse to be SCHEMA-ENABLED (so Tables/<schema>/<table> registers
# as schema.table on the SQL endpoint). Blank = write at the Tables/ root.
SILVER_SCHEMA = "navision"

# Folder UNDER Files/ that holds one sub-folder per table (blank = Files root).
# NOTE: OneLake paths are CASE-SENSITIVE -- the folder is "navision_Bronze"
# (capital B), not "navision_bronze".
#   "navision_Bronze"  ->  Files/navision_Bronze/<Table>/<parquet part files>
BRONZE_FILES_SUBPATH = "navision_Bronze"

# Table folders whose name starts with any of these prefixes are SKIPPED (not
# written to Silver). Matched case-insensitively. Defaults:
#   "$"    -- Navision/BC internal system tables ($ndo$cachesync, $ndo$dbproperty, ...)
#   "dbo_" -- the SQL mirror's own schema-flattened artifact tables (dbo_xxx),
#             created by the mirroring, not real Navision business data.
# Set to () to keep everything.
EXCLUDE_TABLE_PREFIXES = ("$", "dbo_")

# Blank = process every table folder discovered. Set a list to limit a run.
TABLES_TO_PROCESS: list[str] = []

# Remove Silver tables whose Bronze source folder no longer exists (full
# overwrite keeps tables current but never deletes one whose source is gone).
# Conservative: only drops Silver tables absent from the Bronze discovery, and
# refuses to run if discovery found nothing (guard against wiping Silver).
PRUNE_SILVER = True

# Per-table adjustments. Every key optional:
#   "skip": True                      -- don't process this table at all
#   "pk_override": ["col1", "col2"]   -- enables PK-duplicate flagging for this table
#                                        (there is no discovery config to supply a PK)
#   "no_trim_columns": ["notes"]      -- leave these string columns exactly as entered
#   "no_cast_columns": ["ref_code"]   -- don't type-cast these, even if numeric/date-shaped
SILVER_OVERRIDES: dict[str, dict] = {
    # "Customer": {"pk_override": ["No_"]},
}

SEP  = "\u2500" * 72
SEP2 = "\u2501" * 72


def sanitise(col_name: str) -> str:
    return col_name.replace(" ", "_").replace(".", "_").replace("$", "_") \
                   .replace("/", "_").replace("-", "_").replace("(", "_").replace(")", "_")

# CELL ********************

# -- Resolve Lakehouse paths + discover tables ---------------------------------

def _lh_get(obj, key):
    """Field access that works whether lakehouse.list() returns dicts or objects."""
    if isinstance(obj, dict):
        return obj.get(key)
    return getattr(obj, key, None)


def resolve_lakehouse_path(lakehouse_name: str) -> str:
    """ABFS storage path for a Lakehouse, by its Workspace display name. In Fabric
    the path lives under .properties.abfsPath (not a top-level field), so look
    there as well as on the object/dict directly."""
    lakehouses = mssparkutils.lakehouse.list()
    for lh in lakehouses:
        name = _lh_get(lh, "displayName") or _lh_get(lh, "name")
        if name == lakehouse_name:
            props = _lh_get(lh, "properties") or {}
            path = _lh_get(lh, "abfsPath") or _lh_get(props, "abfsPath")
            if path:
                return path
            raise ValueError(
                f"Lakehouse '{lakehouse_name}' was found but had no abfsPath. "
                f"Available fields: {list(lh.keys()) if isinstance(lh, dict) else dir(lh)}; "
                f"properties: {props}"
            )
    raise ValueError(
        f"Lakehouse '{lakehouse_name}' not found among "
        f"{[(_lh_get(l, 'displayName') or _lh_get(l, 'name')) for l in lakehouses]}. Check the name "
        f"matches the Workspace exactly and the Lakehouse is added to this notebook (Explorer panel)."
    )


_bronze_path           = resolve_lakehouse_path(BRONZE_LAKEHOUSE_NAME)
_silver_lakehouse_path = resolve_lakehouse_path(SILVER_LAKEHOUSE_NAME)
_bronze_files_base     = f"{_bronze_path}/Files"
_bronze_files_root     = _bronze_files_base + (f"/{BRONZE_FILES_SUBPATH}" if BRONZE_FILES_SUBPATH else "")
# Silver tables land under the database schema folder (Tables/<schema>/<table>).
_silver_tables_root    = f"{_silver_lakehouse_path}/Tables" + (f"/{SILVER_SCHEMA}" if SILVER_SCHEMA else "")


def _list_dir_names(path: str) -> list:
    """Immediate sub-folder names under a path. Skips hidden/system folders
    (_delta_log, _SUCCESS, dot-folders)."""
    names = []
    for e in mssparkutils.fs.ls(path):
        is_dir = getattr(e, "isDir", None)
        if is_dir is None:
            is_dir = getattr(e, "isDirectory", False)
        name = (getattr(e, "name", "") or "").rstrip("/")
        if is_dir and name and not name.startswith((".", "_")):
            names.append(name)
    return sorted(names)


def discover_tables(root: str) -> list:
    """One sub-folder per table under the Bronze Files root. Drops folders whose
    name starts with any EXCLUDE_TABLE_PREFIXES ($ndo$ system tables, dbo_ mirror
    artifacts). If the root doesn't exist, list what IS under Files/ so
    BRONZE_FILES_SUBPATH can be set."""
    try:
        names = _list_dir_names(root)
        if EXCLUDE_TABLE_PREFIXES:
            _pref = tuple(p.lower() for p in EXCLUDE_TABLE_PREFIXES)
            excluded = [n for n in names if n.lower().startswith(_pref)]
            names    = [n for n in names if not n.lower().startswith(_pref)]
            if excluded:
                print(f"Excluding {len(excluded)} table(s) matching {EXCLUDE_TABLE_PREFIXES} "
                      f"(e.g. {excluded[:3]}) -- set EXCLUDE_TABLE_PREFIXES=() to keep them")
        return names
    except Exception as e:
        if "PathNotFound" not in str(e) and "Not Found" not in str(e) and "FileNotFound" not in str(e):
            raise
        try:
            available = _list_dir_names(_bronze_files_base)
        except Exception:
            available = ["<could not list Files/>"]
        raise ValueError(
            f"Path not found: {root}  ||  "
            f"Folders directly under Files/ in {BRONZE_LAKEHOUSE_NAME}: {available}  ||  "
            f"Set BRONZE_FILES_SUBPATH to the one holding the Navision tables "
            f"(or '' if they sit at the Files/ root; if one level deeper, e.g. 'navision_bronze/dbo')."
        )


ALL_TABLES = discover_tables(_bronze_files_root)
print(f"Discovered {len(ALL_TABLES)} table folder(s) under {_bronze_files_root}")

tables  = TABLES_TO_PROCESS if TABLES_TO_PROCESS else ALL_TABLES
tables  = [t for t in tables if not SILVER_OVERRIDES.get(t, {}).get("skip")]
skipped = [t for t in (TABLES_TO_PROCESS or ALL_TABLES)
           if SILVER_OVERRIDES.get(t, {}).get("skip")]
if skipped:
    print(f"Skipping {len(skipped)} table(s) per SILVER_OVERRIDES: {skipped}")
print(f"Silver tables -> {SILVER_LAKEHOUSE_NAME} / Tables/{SILVER_SCHEMA or '(root)'} ({_silver_tables_root})")

# CELL ********************

# -- Read (Bronze Files, Parquet) + write (Silver Tables, Delta) ---------------

def read_table(table_name: str):
    """Read one table's Parquet folder from Bronze Files. Column names are
    sanitised so odd characters don't break the Delta write / SQL endpoint."""
    df = spark.read.parquet(f"{_bronze_files_root}/{table_name}")
    for col in df.columns:
        safe = sanitise(col)
        if safe != col:
            df = df.withColumnRenamed(col, safe)
    return df


def write_silver_table(df, table_name: str):
    """Full overwrite -- Silver always reflects the latest cleansing pass over
    current Bronze; nothing is appended. Writes under the database schema folder
    (Tables/<SILVER_SCHEMA>/<table>) in the resolved Silver Lakehouse path."""
    full_path = f"{_silver_tables_root}/{sanitise(table_name)}"
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(full_path)
    return full_path

# CELL ********************

# -- Cleansing logic -----------------------------------------------------------

NUMERIC_TYPES = (IntegerType, LongType, ShortType, ByteType,
                 DoubleType, FloatType, DecimalType)


def trim_string_columns(df, exclude: list):
    """Trim leading/trailing whitespace on every string column, except excluded."""
    for field in df.schema.fields:
        if field.name in exclude:
            continue
        if isinstance(field.dataType, StringType):
            df = df.withColumn(field.name, F.trim(F.col(field.name)))
    return df


def empty_strings_to_null(df):
    """An empty string and a null mean the same thing for data quality; normalise
    so a column doesn't read '0% null' when it's actually full of empty strings."""
    for field in df.schema.fields:
        if isinstance(field.dataType, StringType):
            df = df.withColumn(
                field.name,
                F.when(F.col(field.name) == "", None).otherwise(F.col(field.name)),
            )
    return df


def drop_fully_empty_columns(df, total: int):
    """Drop any column 100% null across every row. Run AFTER trim +
    empty_strings_to_null, so all-whitespace / all-empty-string columns are
    caught too. Single aggregation pass. Returns (df, dropped_column_names)."""
    if total == 0:
        return df, []

    null_count_exprs = [
        F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c)
        for c in df.columns if not c.startswith("_")
    ]
    if not null_count_exprs:
        return df, []

    null_counts = df.agg(*null_count_exprs).collect()[0].asDict()
    fully_empty_cols = [c for c, n in null_counts.items() if n == total]
    if fully_empty_cols:
        df = df.drop(*fully_empty_cols)
    return df, fully_empty_cols


def cast_misformatted_columns(df, exclude: list):
    """String columns that are entirely numeric- or date-shaped get cast to their
    real type. Only cast if EVERY non-null value converts cleanly -- one bad
    value and the column is left as-is, never silently turned into null.
    (Parquet already carries real types, so this mostly helps the few text
    columns that hold numbers/dates as strings.)"""
    for field in df.schema.fields:
        col = field.name
        if col in exclude or not isinstance(field.dataType, StringType):
            continue

        if df.filter(F.col(col).isNotNull()).count() == 0:
            continue

        numeric_ok = df.filter(
            F.col(col).isNotNull() & ~F.col(col).rlike(r"^-?[\d,]+\.?\d*$")
        ).count() == 0
        if numeric_ok:
            df = df.withColumn(col, F.regexp_replace(F.col(col), ",", "").cast(DoubleType()))
            continue

        # ISO-ish dates only -- avoids guessing ambiguous DD/MM vs MM/DD.
        date_candidate = df.withColumn("_tmp_date", F.col(col).cast(DateType()))
        date_ok = date_candidate.filter(
            F.col(col).isNotNull() & F.col("_tmp_date").isNull()
        ).count() == 0
        if date_ok:
            df = df.withColumn(col, F.col(col).cast(DateType()))

    if "_tmp_date" in df.columns:
        df = df.drop("_tmp_date")
    return df


def flag_duplicates(df, total: int, pk_cols: list):
    """Add _is_full_duplicate (always) and _is_pk_duplicate (only when pk_cols are
    given via SILVER_OVERRIDES). Rows are flagged, never dropped."""
    from pyspark.sql.window import Window

    if total == 0:
        return df.withColumn("_is_full_duplicate", F.lit(False)) \
                 .withColumn("_is_pk_duplicate", F.lit(False))

    df = df.withColumn("_is_full_duplicate",
                       F.count("*").over(Window.partitionBy(*df.columns)) > 1)

    available_pks = [sanitise(p) for p in pk_cols if sanitise(p) in df.columns]
    if available_pks:
        df = df.withColumn("_is_pk_duplicate",
                           F.count("*").over(Window.partitionBy(*available_pks)) > 1)
    else:
        df = df.withColumn("_is_pk_duplicate", F.lit(False))
    return df


def cleanse_table(table_name: str) -> dict:
    """Full cleansing pass for one table, written to Silver. A table with 0 rows
    is skipped -- NOT written -- so Silver has no empty tables."""
    df = read_table(table_name)
    total = df.count()

    if total == 0:
        return {"table": table_name, "rows": 0, "silver_path": None,
                "skipped_empty_table": True, "full_duplicates_flagged": 0,
                "pk_duplicates_flagged": 0, "pk_used": [], "dropped_empty_columns": []}

    overrides = SILVER_OVERRIDES.get(table_name, {})
    pk_cols = overrides.get("pk_override", [])          # no discovery config any more
    no_trim = set(sanitise(c) for c in overrides.get("no_trim_columns", []))
    no_cast = set(sanitise(c) for c in overrides.get("no_cast_columns", []))

    # Order matters: trim FIRST so an all-whitespace value becomes "" -> null and
    # is then caught by drop_fully_empty_columns.
    df = trim_string_columns(df, exclude=no_trim)
    df = empty_strings_to_null(df)
    df, dropped_cols = drop_fully_empty_columns(df, total)
    df = cast_misformatted_columns(df, exclude=no_cast)
    df = flag_duplicates(df, total, pk_cols)
    df = df.withColumn("_silver_processed_at", F.current_timestamp())

    full_dup_count = df.filter(F.col("_is_full_duplicate")).count()
    pk_dup_count   = df.filter(F.col("_is_pk_duplicate")).count()
    silver_path = write_silver_table(df, table_name)

    return {"table": table_name, "rows": total, "silver_path": silver_path,
            "skipped_empty_table": False, "full_duplicates_flagged": full_dup_count,
            "pk_duplicates_flagged": pk_dup_count, "pk_used": pk_cols,
            "dropped_empty_columns": dropped_cols}

# CELL ********************

# -- MAIN LOOP -----------------------------------------------------------------

run_id    = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
run_start = time.time()
n_tables  = len(tables)

print(f"\n{SEP2}")
print(f"  Cleansing {n_tables} tables: Bronze -> Silver (full overwrite)")
print(SEP2)

results, failed = [], []

for i, table_name in enumerate(tables, start=1):
    t0 = time.time()
    try:
        result = cleanse_table(table_name)
        results.append(result)
        elapsed = time.time() - t0
        if result["skipped_empty_table"]:
            print(f"  [{i}/{n_tables}] {table_name}: 0 rows -- skipped, not written to Silver  ({elapsed:.1f}s)")
        else:
            dropped_note = (f"  ({len(result['dropped_empty_columns'])} empty column(s) dropped)"
                            if result["dropped_empty_columns"] else "")
            print(f"  [{i}/{n_tables}] {table_name}: {result['rows']:,} rows -> "
                  f"{result['silver_path']}  (full dup: {result['full_duplicates_flagged']:,}, "
                  f"PK dup: {result['pk_duplicates_flagged']:,}){dropped_note}  {elapsed:.1f}s")
    except Exception as e:
        elapsed = time.time() - t0
        print(f"  [{i}/{n_tables}] {table_name}: FAILED -- {e}  ({elapsed:.1f}s)")
        failed.append(table_name)

total_elapsed = time.time() - run_start

print(f"\n{SEP2}")
print(f"  Bronze -> Silver complete: {len(results)} tables in {total_elapsed/60:.1f} minutes")
skipped_empty = [r["table"] for r in results if r["skipped_empty_table"]]
if skipped_empty:
    print(f"  {len(skipped_empty)} table(s) skipped (0 rows, not written): {skipped_empty}")
if failed:
    print(f"  {len(failed)} table(s) failed: {failed}")
no_pk_tables = [r["table"] for r in results if not r["skipped_empty_table"] and not r["pk_used"]]
print(f"  Tables with no PK override (full-row duplicate check only): {len(no_pk_tables)}")
print(SEP2)

# -- Prune Silver tables whose Bronze source folder is gone --------------------
# Overwrite keeps each table current but never REMOVES a Silver table whose
# Bronze source has been deleted. Drop any Silver table not in the current
# Bronze discovery (ALL_TABLES). Skip-listed and empty tables are still in
# Bronze, so they stay; a table that only FAILED to read this run also stays
# (it's in ALL_TABLES). Hard guard: if discovery found nothing, do NOT prune --
# that almost always means a wrong path, and pruning would wipe all of Silver.
if PRUNE_SILVER and not ALL_TABLES:
    print("  Prune skipped: Bronze discovery is empty (guard against wiping Silver on a bad path).")
elif PRUNE_SILVER:
    _expected = {sanitise(t) for t in ALL_TABLES}
    try:
        _silver_entries = mssparkutils.fs.ls(_silver_tables_root)
    except Exception:
        _silver_entries = []  # schema folder doesn't exist yet (first run) -> nothing to prune
    pruned = []
    for e in _silver_entries:
        name = (getattr(e, "name", "") or "").rstrip("/")
        is_dir = getattr(e, "isDir", None)
        if is_dir is None:
            is_dir = getattr(e, "isDirectory", False)
        if not is_dir or not name or name.startswith(("_", ".")):
            continue  # skip system/log folders (e.g. a future _b2s_run_log)
        if name not in _expected:
            mssparkutils.fs.rm(f"{_silver_tables_root}/{name}", True)
            pruned.append(name)
    if pruned:
        print(f"  Pruned {len(pruned)} Silver table(s) no longer in Bronze: {pruned}")
    else:
        print("  Prune: no stale Silver tables to remove.")
    print(SEP2)

# Refresh the Silver SQL endpoint so the cleaned tables are queryable straight
# away -- same closing step as the Jira orchestration.
try:
    fmt.refresh_sql_endpoints(mssparkutils, [SILVER_LAKEHOUSE_NAME])
    print("Silver SQL endpoint refresh triggered.")
except Exception as e:
    print(f"Silver SQL endpoint refresh skipped: {e}")
