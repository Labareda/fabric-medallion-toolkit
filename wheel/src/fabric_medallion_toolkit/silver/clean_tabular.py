"""
Generic cleansing for ALREADY-TABULAR Bronze -- mirrored / copied source tables
that arrive with real columns, NOT the JSON `raw_data` that auto_standardize and
explode_nested_array handle. This is the reusable version of the per-source
"Bronze -> Silver clean" notebooks (e.g. Navision), so adding a new tabular
source is a few lines rather than a copied notebook.

Two entry points:
  * clean_tabular(df, ...)   -- PURE transform, DataFrame in / (DataFrame, report)
                                out. No I/O, so it's trivial to unit-test and
                                reuse anywhere.
  * run_file_silver(...)     -- the whole loop: discover one-folder-per-table
                                Parquet under a Bronze Lakehouse Files path,
                                cleanse each, overwrite into a (optionally
                                schema-foldered) Silver Lakehouse, skip empty
                                tables, prune Silver tables whose source is gone,
                                refresh the endpoint.

CASTING IS OFF BY DEFAULT, on purpose. A string that looks numeric is very often
a CODE, not a number -- a Navision "No." field, a postal code, a leading-zero id.
Casting it to a double loses the leading zeros and breaks every string join. Turn
casting on per source (cast=True) only where you know the string columns really
are numbers/dates, and still exempt individual columns with no_cast.

PERFORMANCE: column statistics are computed in single aggregation passes, not one
count() per column -- so a wide table does not fan out into hundreds of tiny
Spark jobs (the main cost in the hand-rolled version).
"""

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, DateType, DoubleType
from pyspark.sql.window import Window

from fabric_medallion_toolkit.utils.logging_utils import get_logger

logger = get_logger("silver.clean_tabular")

# A value is "numeric-shaped" if it's an optional sign, digits/commas, optional
# decimal part -- the thousands comma is stripped before the cast.
_NUMERIC_RE = r"^-?[\d,]+\.?\d*$"


def sanitise(name: str) -> str:
    """Column/table name -> a name safe for a Delta path and the SQL endpoint."""
    for ch in (" ", ".", "$", "/", "-", "(", ")"):
        name = name.replace(ch, "_")
    return name


def _string_cols(df, exclude):
    return [f.name for f in df.schema.fields
            if isinstance(f.dataType, StringType) and f.name not in exclude]


def clean_tabular(df, *, no_trim=(), no_cast=(), cast=False, pk=None):
    """Cleanse one already-tabular DataFrame. Returns (clean_df, report).

    Steps, in order:
      1. trim leading/trailing whitespace on string columns (except `no_trim`)
      2. normalise empty strings ("", "   ") to null
      3. drop columns that are 100% null (run after 1+2, so all-whitespace/all-
         empty-string columns are caught too)
      4. OPTIONAL casting (`cast=False` by default): a string column whose every
         non-null value is cleanly numeric- or ISO-date-shaped is cast to that
         type. See the module note on why this is off by default; exempt columns
         with `no_cast`.
      5. flag `_is_full_duplicate` (always) and `_is_pk_duplicate` (when `pk`
         given) -- rows are KEPT, never dropped
      6. add `_silver_processed_at`

    report keys: rows, dropped_empty_columns, cast_columns, full_duplicates,
    pk_duplicates, pk_used.
    """
    no_trim = set(no_trim or ())
    no_cast = set(no_cast or ())
    pk = list(pk or [])

    total = df.count()
    report = {"rows": total, "dropped_empty_columns": [], "cast_columns": [],
              "full_duplicates": 0, "pk_duplicates": 0, "pk_used": pk}
    if total == 0:
        return df, report

    # 1. trim
    for name in _string_cols(df, no_trim):
        df = df.withColumn(name, F.trim(F.col(name)))

    # 2. empty string -> null (all string columns, incl. no_trim ones)
    for name in _string_cols(df, set()):
        df = df.withColumn(name, F.when(F.col(name) == "", None).otherwise(F.col(name)))

    # 3. drop fully-empty columns -- ONE aggregation pass over all data columns
    data_cols = [c for c in df.columns if not c.startswith("_")]
    if data_cols:
        null_counts = df.agg(*[
            F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c) for c in data_cols
        ]).collect()[0].asDict()
        dropped = [c for c, n in null_counts.items() if (n or 0) == total]
        if dropped:
            df = df.drop(*dropped)
            report["dropped_empty_columns"] = dropped

    # 4. optional casting -- ONE aggregation pass computes eligibility for every
    #    candidate string column at once (not three count()s per column)
    if cast:
        candidates = _string_cols(df, no_cast)
        if candidates:
            exprs = []
            for c in candidates:
                nn = F.col(c).isNotNull()
                exprs += [
                    F.sum(F.when(nn, 1).otherwise(0)).alias(f"{c}\x00nn"),
                    F.sum(F.when(nn & ~F.col(c).rlike(_NUMERIC_RE), 1).otherwise(0)).alias(f"{c}\x00nonnum"),
                    F.sum(F.when(nn & F.col(c).cast(DateType()).isNull(), 1).otherwise(0)).alias(f"{c}\x00nondate"),
                ]
            stats = df.agg(*exprs).collect()[0].asDict()
            for c in candidates:
                nn = stats[f"{c}\x00nn"] or 0
                if nn == 0:
                    continue
                if (stats[f"{c}\x00nonnum"] or 0) == 0:
                    df = df.withColumn(c, F.regexp_replace(F.col(c), ",", "").cast(DoubleType()))
                    report["cast_columns"].append((c, "double"))
                elif (stats[f"{c}\x00nondate"] or 0) == 0:
                    df = df.withColumn(c, F.col(c).cast(DateType()))
                    report["cast_columns"].append((c, "date"))

    # 5. duplicate flags (full-row over the data columns; PK over pk if present)
    df = df.withColumn("_is_full_duplicate",
                       F.count("*").over(Window.partitionBy(*df.columns)) > 1)
    pk_in = [p for p in pk if p in df.columns]
    if pk_in:
        df = df.withColumn("_is_pk_duplicate",
                           F.count("*").over(Window.partitionBy(*pk_in)) > 1)
    else:
        df = df.withColumn("_is_pk_duplicate", F.lit(False))

    # 6. processed timestamp
    df = df.withColumn("_silver_processed_at", F.current_timestamp())

    # dup counts for the report -- ONE pass
    dups = df.agg(
        F.sum(F.col("_is_full_duplicate").cast("int")).alias("full"),
        F.sum(F.col("_is_pk_duplicate").cast("int")).alias("pk"),
    ).collect()[0]
    report["full_duplicates"] = int(dups["full"] or 0)
    report["pk_duplicates"] = int(dups["pk"] or 0)
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
    """Field access that works whether an SDK returns dicts or objects."""
    if isinstance(obj, dict):
        return obj.get(key)
    return getattr(obj, key, None)


def _is_dir(entry):
    v = _attr(entry, "isDir")
    return _attr(entry, "isDirectory") if v is None else v


def resolve_lakehouse_path(mssparkutils, lakehouse_name):
    """ABFS storage path of a Lakehouse, by its Workspace display name. The path
    lives under .properties.abfsPath in Fabric, not as a top-level field."""
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
    for col in df.columns:
        safe = sanitise(col)
        if safe != col:
            df = df.withColumnRenamed(col, safe)
    return df


def run_file_silver(bronze_lakehouse, bronze_subpath="", *, silver_lakehouse,
                    silver_schema="", tables=None, exclude_prefixes=("$",),
                    overrides=None, cast=False, file_format="parquet",
                    prune=True, refresh_endpoint=True, spark=None, mssparkutils=None):
    """Clean every one-folder-per-table dataset under a Bronze Lakehouse Files
    path into a Silver Lakehouse. Both Lakehouses are resolved by display name.

    bronze_subpath : folder under Files/ holding the per-table folders ("" = root).
    silver_schema  : schema folder under Silver Tables/ ("" = root). Needs the
                     Silver Lakehouse to be schema-enabled to register as schema.table.
    tables         : limit to these table names (None = all discovered).
    exclude_prefixes: skip table folders whose name starts with any of these
                     (case-insensitive), e.g. ("$", "dbo_") for NAV system tables
                     and SQL-mirror artifacts.
    overrides      : per-table dict: {"skip", "pk", "no_trim", "no_cast", "cast"}.
    cast           : default casting for all tables (off). Override per table.
    prune          : drop Silver tables whose Bronze source folder is gone.

    Empty tables are skipped (never written). Returns a list of per-table reports.
    """
    spark = _get_spark(spark)
    msu = _get_mssparkutils(mssparkutils)
    overrides = overrides or {}
    prefixes = tuple(p.lower() for p in (exclude_prefixes or ()))

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

    logger.info(f"Discovered {len(discovered)} table(s) under {files_root}")
    to_process = [t for t in (tables or discovered) if not overrides.get(t, {}).get("skip")]

    reports = []
    for t in to_process:
        ov = overrides.get(t, {})
        try:
            reader = spark.read.format(file_format)
            if file_format == "csv":
                reader = reader.option("header", "true")
            df = _sanitise_columns(reader.load(f"{files_root}/{t}"))
            clean, report = clean_tabular(
                df, no_trim=ov.get("no_trim", ()), no_cast=ov.get("no_cast", ()),
                cast=ov.get("cast", cast), pk=ov.get("pk", ()),
            )
            report["table"] = t
            if report["rows"] == 0:
                report["written"] = False
                logger.info(f"{t}: 0 rows -- skipped (not written to Silver)")
            else:
                out = f"{silver_tables_root}/{sanitise(t)}"
                clean.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(out)
                report["written"] = True
                report["silver_path"] = out
                logger.info(f"{t}: {report['rows']} rows -> {out} "
                            f"(dropped {len(report['dropped_empty_columns'])} empty col(s), "
                            f"full dup {report['full_duplicates']}, pk dup {report['pk_duplicates']})")
            reports.append(report)
        except Exception as e:
            logger.error(f"{t}: FAILED -- {e}")
            reports.append({"table": t, "written": False, "error": str(e)})

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
    """Drop Silver table folders not in `expected_names` (the current Bronze
    discovery). Guard: never prune when discovery is empty."""
    if not expected_names:
        logger.warning("Prune skipped: Bronze discovery is empty (guard against wiping Silver).")
        return []
    try:
        entries = mssparkutils.fs.ls(silver_tables_root)
    except Exception:
        return []  # schema folder doesn't exist yet -> nothing to prune
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
