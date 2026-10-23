"""
Minimal Bronze -> Silver pass for ALREADY-TABULAR sources (mirrored / copied
tables that arrive with real columns, NOT the JSON `raw_data` that
auto_standardize / explode_nested_array handle). Reusable across sources so
adding one is a few lines, not a copied notebook.

It does ONLY what's needed for a clean Silver mirror -- nothing more:
  1. SKIP EMPTY TABLES      -- a 0-row table is not written (no empty Silver tables)
  2. DROP EMPTY COLUMNS     -- a column that is entirely null/blank is dropped
  3. FABRIC-SAFE DATES      -- the ancient-date rebase is set so reads/writes don't
                               throw, and any date/timestamp outside Fabric's
                               representable range (0001-01-01 .. 9999-12-31) is
                               nulled so the SQL endpoint can sync the table.

Deliberately NOT done (not required): trimming, empty-string->null rewriting,
type casting, duplicate flags, processed-at columns. Data that stays is written
through unchanged apart from out-of-range dates being nulled.

Built for MANY tables: tables are processed CONCURRENTLY (max_workers) and each
table is ~2 Spark actions (one agg for the row/empty-column check, plus the write).
"""

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, DateType, TimestampType

from fabric_medallion_toolkit.utils.logging_utils import get_logger

logger = get_logger("silver.clean_tabular")

# Fabric's SQL endpoint maps Delta date/timestamp to T-SQL date/datetime2, whose
# range is 0001-01-01 .. 9999-12-31. Values outside it can't sync, so they're nulled.
_FABRIC_MIN = "0001-01-01"
_FABRIC_MAX = "9999-12-31"

# Rebase keys so pre-1582 (Julian) dates read/write as-is instead of throwing
# READ_ANCIENT_DATETIME -- common in data copied from old SQL Server / Navision.
_REBASE_KEYS = (
    "spark.sql.parquet.datetimeRebaseModeInRead",
    "spark.sql.parquet.int96RebaseModeInRead",
    "spark.sql.parquet.datetimeRebaseModeInWrite",
    "spark.sql.parquet.int96RebaseModeInWrite",
)


def set_date_rebase(spark):
    """Set CORRECTED rebase so ancient dates don't error. Safe to call repeatedly."""
    for k in _REBASE_KEYS:
        try:
            spark.conf.set(k, "CORRECTED")
        except Exception:
            pass


def sanitise(name: str) -> str:
    """Column/table name -> a name safe for a Delta path and the SQL endpoint."""
    for ch in (" ", ".", "$", "/", "-", "(", ")"):
        name = name.replace(ch, "_")
    return name


def _fabric_safe_date(col, is_ts):
    cast = TimestampType() if is_ts else DateType()
    lo = F.lit(_FABRIC_MIN).cast(cast)
    hi = F.lit(_FABRIC_MAX + (" 23:59:59" if is_ts else "")).cast(cast)
    return F.when((col < lo) | (col > hi), None).otherwise(col)


def clean_tabular(df, *, fix_dates=True):
    """Minimal cleanse of one already-tabular DataFrame. Returns (clean_df, report).

      1. report the row count (caller skips the write when it's 0)
      2. drop columns that are entirely null or blank (string "" / whitespace)
      3. null date/timestamp values outside the Fabric-safe range (if fix_dates)

    Nothing else is changed. report keys: rows, dropped_empty_columns, date_columns.
    """
    report = {"rows": 0, "dropped_empty_columns": [], "date_columns": []}
    data_cols = [c for c in df.columns if not c.startswith("_")]

    # ONE pass: row count + per-column count of MEANINGFUL values (not null, and
    # for strings not blank). A column with zero meaningful values is "empty".
    def _nonempty(field):
        v = F.col(field.name)
        meaningful = (v.isNotNull() & (F.trim(v) != "")
                      if isinstance(field.dataType, StringType) else v.isNotNull())
        return F.sum(F.when(meaningful, 1).otherwise(0)).alias(field.name)

    agg = [F.count(F.lit(1)).alias("__rows__")]
    agg += [_nonempty(f) for f in df.schema.fields if not f.name.startswith("_")]
    row = df.agg(*agg).collect()[0].asDict()

    total = int(row["__rows__"] or 0)
    report["rows"] = total
    if total == 0:
        return df, report

    dropped = [c for c in data_cols if (row[c] or 0) == 0]
    if dropped:
        df = df.drop(*dropped)
        report["dropped_empty_columns"] = dropped

    if fix_dates:
        for f in df.schema.fields:
            if isinstance(f.dataType, DateType):
                df = df.withColumn(f.name, _fabric_safe_date(F.col(f.name), False))
                report["date_columns"].append(f.name)
            elif isinstance(f.dataType, TimestampType):
                df = df.withColumn(f.name, _fabric_safe_date(F.col(f.name), True))
                report["date_columns"].append(f.name)

    return df, report


# ---------------------------------------------------------------------------
# run_file_silver -- discovery + read + clean + write + prune over a Lakehouse
# ---------------------------------------------------------------------------

def _get_spark(spark):
    if spark is not None:
        return spark
    from pyspark.sql import SparkSession
    return SparkSession.builder.getOrCreate()


def _get_mssparkutils(mssparkutils):
    if mssparkutils is not None:
        return mssparkutils
    from notebookutils import mssparkutils as _m
    return _m


def _attr(obj, key):
    if isinstance(obj, dict):
        return obj.get(key)
    return getattr(obj, key, None)


def _is_dir(entry):
    v = _attr(entry, "isDir")
    return _attr(entry, "isDirectory") if v is None else v


def resolve_lakehouse_path(mssparkutils, lakehouse_name):
    """ABFS storage path of a Lakehouse, by Workspace display name (the path is
    under .properties.abfsPath in Fabric, not a top-level field)."""
    lakehouses = mssparkutils.lakehouse.list()
    for lh in lakehouses:
        if (_attr(lh, "displayName") or _attr(lh, "name")) == lakehouse_name:
            props = _attr(lh, "properties") or {}
            path = _attr(lh, "abfsPath") or _attr(props, "abfsPath")
            if path:
                return path
            raise ValueError(f"Lakehouse '{lakehouse_name}' found but had no abfsPath.")
    names = [(_attr(l, "displayName") or _attr(l, "name")) for l in lakehouses]
    raise ValueError(
        f"Lakehouse '{lakehouse_name}' not found among {names}. Check the name matches the "
        f"Workspace exactly and the Lakehouse is added to this notebook."
    )


def _list_dir_names(mssparkutils, path):
    names = []
    for e in mssparkutils.fs.ls(path):
        name = (_attr(e, "name") or "").rstrip("/")
        if _is_dir(e) and name and not name.startswith((".", "_")):
            names.append(name)
    return sorted(names)


def _sanitise_columns(df):
    renames = {c: sanitise(c) for c in df.columns if sanitise(c) != c}
    if not renames:
        return df
    return df.select(*[F.col(c).alias(renames.get(c, c)) for c in df.columns])


def run_file_silver(bronze_lakehouse, bronze_subpath="", *, silver_lakehouse,
                    silver_schema="", tables=None, exclude_prefixes=("$",),
                    skip=(), fix_dates=True, file_format="parquet",
                    prune=True, refresh_endpoint=True, max_workers=8,
                    spark=None, mssparkutils=None):
    """Clean every one-folder-per-table dataset under a Bronze Lakehouse Files
    path into a Silver Lakehouse. Both Lakehouses resolved by display name.

    bronze_subpath  : folder under Files/ with the per-table folders ("" = root).
    silver_schema   : schema folder under Silver Tables/ ("" = root).
    tables          : limit to these (None = all discovered).
    exclude_prefixes: skip table folders starting with any of these (case-insensitive).
    skip            : explicit table names to skip.
    fix_dates       : null date/timestamp values outside the Fabric-safe range.
    max_workers     : tables processed concurrently (1 = sequential).
    prune           : drop Silver tables whose Bronze source folder is gone.

    Empty tables are skipped (never written). Returns a list of per-table reports.
    """
    spark = _get_spark(spark)
    msu = _get_mssparkutils(mssparkutils)
    set_date_rebase(spark)
    prefixes = tuple(p.lower() for p in (exclude_prefixes or ()))
    skip_set = set(skip or ())

    bronze_path = resolve_lakehouse_path(msu, bronze_lakehouse)
    silver_path = resolve_lakehouse_path(msu, silver_lakehouse)
    files_base = f"{bronze_path}/Files"
    files_root = files_base + (f"/{bronze_subpath}" if bronze_subpath else "")
    silver_tables_root = f"{silver_path}/Tables" + (f"/{silver_schema}" if silver_schema else "")

    try:
        discovered = _list_dir_names(msu, files_root)
    except Exception as e:
        try:
            available = _list_dir_names(msu, files_base)
        except Exception:
            available = ["<could not list Files/>"]
        raise ValueError(
            f"Path not found: {files_root}  ||  Folders under Files/ in {bronze_lakehouse}: "
            f"{available}  ||  Set bronze_subpath to one of these (or '' for the Files/ root)."
        ) from e

    if prefixes:
        excluded = [t for t in discovered if t.lower().startswith(prefixes)]
        discovered = [t for t in discovered if not t.lower().startswith(prefixes)]
        if excluded:
            logger.info(f"Excluding {len(excluded)} table(s) matching {exclude_prefixes} (e.g. {excluded[:3]})")

    to_process = [t for t in (tables or discovered) if t not in skip_set]
    logger.info(f"Discovered {len(discovered)} table(s); processing {len(to_process)} "
                f"with max_workers={max_workers}")

    def _one(t):
        try:
            reader = spark.read.format(file_format)
            if file_format == "csv":
                reader = reader.option("header", "true")
            df = _sanitise_columns(reader.load(f"{files_root}/{t}"))
            clean, report = clean_tabular(df, fix_dates=fix_dates)
            report["table"] = t
            if report["rows"] == 0:
                report["written"] = False
                logger.info(f"{t}: 0 rows -- skipped")
            else:
                out = f"{silver_tables_root}/{sanitise(t)}"
                clean.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(out)
                report["written"] = True
                report["silver_path"] = out
                logger.info(f"{t}: {report['rows']} rows -> Silver "
                            f"(dropped {len(report['dropped_empty_columns'])} empty col(s))")
            return report
        except Exception as e:
            logger.error(f"{t}: FAILED -- {e}")
            return {"table": t, "written": False, "error": str(e)}

    if max_workers and max_workers > 1 and len(to_process) > 1:
        from concurrent.futures import ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            reports = list(ex.map(_one, to_process))
    else:
        reports = [_one(t) for t in to_process]

    written = sum(1 for r in reports if r.get("written"))
    logger.info(f"Done: {written} written, {len(reports) - written} skipped/failed")

    if prune:
        _prune_silver(msu, silver_tables_root, {sanitise(t) for t in discovered})

    if refresh_endpoint:
        try:
            from fabric_medallion_toolkit.utils import refresh_sql_endpoints
            refresh_sql_endpoints(msu, [silver_lakehouse])
        except Exception as e:
            logger.warning(f"Silver endpoint refresh skipped: {e}")

    return reports


def _prune_silver(mssparkutils, silver_tables_root, expected_names):
    """Drop Silver table folders not in the current Bronze discovery. Guard:
    never prune when discovery is empty."""
    if not expected_names:
        logger.warning("Prune skipped: Bronze discovery is empty (guard against wiping Silver).")
        return []
    try:
        entries = mssparkutils.fs.ls(silver_tables_root)
    except Exception:
        return []
    pruned = []
    for e in entries:
        name = (_attr(e, "name") or "").rstrip("/")
        if not _is_dir(e) or not name or name.startswith(("_", ".")):
            continue
        if name not in expected_names:
            mssparkutils.fs.rm(f"{silver_tables_root}/{name}", True)
            pruned.append(name)
    if pruned:
        logger.info(f"Pruned {len(pruned)} Silver table(s) no longer in Bronze: {pruned}")
    return pruned
