"""
Generic cleansing for ALREADY-TABULAR Bronze -- mirrored / copied source tables
that arrive with real columns, NOT the JSON `raw_data` that auto_standardize and
explode_nested_array handle. This is the reusable version of the per-source
"Bronze -> Silver clean" notebooks (e.g. Navision), so adding a new tabular
source is a few lines rather than a copied notebook.

Two entry points:
  * clean_tabular(df, ...)   -- PURE transform, DataFrame in / (DataFrame, report)
                                out. No I/O, trivial to unit-test and reuse.
  * run_file_silver(...)     -- discover one-folder-per-table datasets under a
                                Bronze Lakehouse Files path, cleanse each, write
                                into a (optionally schema-foldered) Silver
                                Lakehouse, skip empties, prune removed tables.

BUILT FOR MANY TABLES. A source can have thousands of (mostly small/empty)
tables, so the defaults are tuned for that:
  * tables are processed CONCURRENTLY (max_workers) -- one-at-a-time leaves the
    cluster idle on small tables; concurrent job submission keeps it busy.
  * per table there are only ~2 Spark actions: ONE agg (row count + per-column
    null counts together) and the write. Trim + empty->null is a single select.
  * DUPLICATE FLAGGING IS OFF BY DEFAULT. The full-row duplicate check is a
    Window.partitionBy over EVERY column -- a full shuffle per table. Turn it on
    (flag_duplicates=True) only when you actually need the flags.

CASTING IS ALSO OFF BY DEFAULT. A string that looks numeric is usually a CODE
(Navision "No." field, postal code, leading-zero id); casting it to a double
loses leading zeros and breaks joins. Opt in per source/table with cast=True.
"""

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, DateType, DoubleType
from pyspark.sql.window import Window

from fabric_medallion_toolkit.utils.logging_utils import get_logger

logger = get_logger("silver.clean_tabular")

_NUMERIC_RE = r"^-?[\d,]+\.?\d*$"


def sanitise(name: str) -> str:
    """Column/table name -> a name safe for a Delta path and the SQL endpoint."""
    for ch in (" ", ".", "$", "/", "-", "(", ")"):
        name = name.replace(ch, "_")
    return name


def _string_cols(df, exclude):
    return [f.name for f in df.schema.fields
            if isinstance(f.dataType, StringType) and f.name not in exclude]


def clean_tabular(df, *, no_trim=(), no_cast=(), cast=False, pk=None,
                  flag_duplicates=False):
    """Cleanse one already-tabular DataFrame. Returns (clean_df, report).

    Steps:
      1. trim string columns (except `no_trim`) AND normalise ""/"   " to null --
         in a single projection
      2. drop columns that are 100% null -- using the same agg that counts rows
      3. OPTIONAL casting (cast=False): numeric-/ISO-date-shaped string columns
         (off by default; see module note). Exempt columns with `no_cast`.
      4. OPTIONAL duplicate flags (flag_duplicates=False): `_is_full_duplicate`
         and `_is_pk_duplicate` (full-row flag is a full shuffle -- opt in).
      5. add `_silver_processed_at`

    report keys: rows, dropped_empty_columns, cast_columns, full_duplicates,
    pk_duplicates (None when flagging off), pk_used.
    """
    no_trim = set(no_trim or ())
    no_cast = set(no_cast or ())
    pk = list(pk or [])
    report = {"rows": 0, "dropped_empty_columns": [], "cast_columns": [],
              "full_duplicates": None, "pk_duplicates": None, "pk_used": pk}

    # 1. trim + empty->null in ONE select (cheap to plan even on wide tables)
    proj = []
    for f in df.schema.fields:
        c = f.name
        if isinstance(f.dataType, StringType):
            col = F.col(c) if c in no_trim else F.trim(F.col(c))
            proj.append(F.when(col == "", None).otherwise(col).alias(c))
        else:
            proj.append(F.col(c))
    df = df.select(*proj)

    # 2. ONE agg: row count + per-column null count together -> empty-table check
    #    and fully-empty-column drop, in a single pass over the data.
    data_cols = [c for c in df.columns if not c.startswith("_")]
    agg = [F.count(F.lit(1)).alias("__rows__")]
    agg += [F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c) for c in data_cols]
    row = df.agg(*agg).collect()[0].asDict()
    total = int(row["__rows__"] or 0)
    report["rows"] = total
    if total == 0:
        return df, report

    dropped = [c for c in data_cols if (row[c] or 0) == total]
    if dropped:
        df = df.drop(*dropped)
        report["dropped_empty_columns"] = dropped

    # 3. optional casting -- ONE agg computes eligibility for all candidates
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
                if (stats[f"{c}\x00nn"] or 0) == 0:
                    continue
                if (stats[f"{c}\x00nonnum"] or 0) == 0:
                    df = df.withColumn(c, F.regexp_replace(F.col(c), ",", "").cast(DoubleType()))
                    report["cast_columns"].append((c, "double"))
                elif (stats[f"{c}\x00nondate"] or 0) == 0:
                    df = df.withColumn(c, F.col(c).cast(DateType()))
                    report["cast_columns"].append((c, "date"))

    # 4. optional duplicate flags (expensive -- a shuffle per table -- so opt-in)
    if flag_duplicates:
        df = df.withColumn("_is_full_duplicate",
                           F.count("*").over(Window.partitionBy(*df.columns)) > 1)
        pk_in = [p for p in pk if p in df.columns]
        df = df.withColumn("_is_pk_duplicate",
                           (F.count("*").over(Window.partitionBy(*pk_in)) > 1)
                           if pk_in else F.lit(False))
        d = df.agg(F.sum(F.col("_is_full_duplicate").cast("int")).alias("f"),
                   F.sum(F.col("_is_pk_duplicate").cast("int")).alias("p")).collect()[0]
        report["full_duplicates"] = int(d["f"] or 0)
        report["pk_duplicates"] = int(d["p"] or 0)

    # 5. processed timestamp
    df = df.withColumn("_silver_processed_at", F.current_timestamp())
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
    """Rename odd column names in ONE projection (not N withColumnRenamed)."""
    renames = {c: sanitise(c) for c in df.columns if sanitise(c) != c}
    if not renames:
        return df
    return df.select(*[F.col(c).alias(renames.get(c, c)) for c in df.columns])


def run_file_silver(bronze_lakehouse, bronze_subpath="", *, silver_lakehouse,
                    silver_schema="", tables=None, exclude_prefixes=("$",),
                    overrides=None, cast=False, flag_duplicates=False,
                    file_format="parquet", prune=True, refresh_endpoint=True,
                    max_workers=8, spark=None, mssparkutils=None):
    """Clean every one-folder-per-table dataset under a Bronze Lakehouse Files
    path into a Silver Lakehouse. Both Lakehouses resolved by display name.

    bronze_subpath  : folder under Files/ with the per-table folders ("" = root).
    silver_schema   : schema folder under Silver Tables/ ("" = root).
    tables          : limit to these (None = all discovered).
    exclude_prefixes: skip table folders starting with any of these (case-insensitive).
    overrides       : per-table {"skip","pk","no_trim","no_cast","cast","flag_duplicates"}.
    cast            : default casting (off). flag_duplicates: default dup flags (off).
    max_workers     : tables processed concurrently (1 = sequential). The main
                      speed lever when there are many small tables.
    prune           : drop Silver tables whose Bronze source folder is gone.

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

    to_process = [t for t in (tables or discovered) if not overrides.get(t, {}).get("skip")]
    logger.info(f"Discovered {len(discovered)} table(s); processing {len(to_process)} "
                f"with max_workers={max_workers}")

    def _one(t):
        ov = overrides.get(t, {})
        try:
            reader = spark.read.format(file_format)
            if file_format == "csv":
                reader = reader.option("header", "true")
            df = _sanitise_columns(reader.load(f"{files_root}/{t}"))
            clean, report = clean_tabular(
                df, no_trim=ov.get("no_trim", ()), no_cast=ov.get("no_cast", ()),
                cast=ov.get("cast", cast), pk=ov.get("pk", ()),
                flag_duplicates=ov.get("flag_duplicates", flag_duplicates),
            )
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
