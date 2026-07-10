"""
Standard calendar dimension — synthetic, not derived from Silver. Every
Power BI model with time-intelligence DAX (YTD, same-period-last-year,
running totals) needs one of these, and it's the same table shape
regardless of source, so it's built once from a date range, not from config
per source.

date_key is an int (yyyyMMdd) — the conventional surrogate key for a date
dimension, since it sorts correctly, joins fast, and is human-readable in
the fact table without a lookup.
"""

from pyspark.sql import functions as F

from fabric_medallion_toolkit.config import DateDimensionConfig
from fabric_medallion_toolkit.utils.logging_utils import get_logger

logger = get_logger("gold.date_dimension")


def build_date_dimension(spark, config: DateDimensionConfig) -> None:
    """
    Fully overwrites config.table_name every run — cheap and deterministic
    (a decade of dates is ~3,650 rows), so there's no merge logic needed
    here at all.
    """
    df = (
        spark.sql(f"SELECT explode(sequence(to_date('{config.start_date}'), "
                  f"to_date('{config.end_date}'), interval 1 day)) AS date")
        .withColumn("date_key", F.date_format("date", "yyyyMMdd").cast("int"))
        .withColumn("year", F.year("date"))
        .withColumn("quarter", F.quarter("date"))
        .withColumn("month", F.month("date"))
        .withColumn("month_name", F.date_format("date", "MMMM"))
        .withColumn("month_short_name", F.date_format("date", "MMM"))
        .withColumn("day_of_month", F.dayofmonth("date"))
        .withColumn("day_of_week", F.dayofweek("date"))  # 1=Sunday..7=Saturday
        .withColumn("day_name", F.date_format("date", "EEEE"))
        .withColumn("week_of_year", F.weekofyear("date"))
        .withColumn("is_weekend", F.dayofweek("date").isin(1, 7))
        .withColumn("year_month", F.date_format("date", "yyyy-MM"))
        .withColumn(
            "fiscal_year",
            F.when(
                F.lit(config.fiscal_year_start_month) == 1, F.year("date")
            ).otherwise(
                F.when(F.month("date") >= config.fiscal_year_start_month, F.year("date") + 1)
                 .otherwise(F.year("date"))
            ),
        )
        .withColumn(
            "fiscal_quarter",
            F.when(F.lit(config.fiscal_year_start_month) == 1, F.quarter("date"))
             .otherwise(((F.pmod(F.month("date") - config.fiscal_year_start_month, 12)) / 3).cast("int") + 1),
        )
    )

    logger.info(f"Building {config.table_name}: {config.start_date} to {config.end_date}")
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(config.table_name)

def add_date_dimension_sentinel(spark, table_name: str, sentinel_date: str = "1900-01-01") -> None:
    """
    Adds ONE placeholder row to an existing date dimension (built via
    build_date_dimension) for use as TableSchema.lookup_missing_from's
    fallback when a fact's own date is null. Deliberately NOT done by
    extending build_date_dimension's start_date back to this date --
    that would mean generating tens of thousands of unnecessary daily
    rows just to have this one date exist; this adds only the single row
    actually needed.

    Idempotent -- safe to call every run; does nothing if the sentinel
    row already exists.

    Only date/date_key are populated on this row; every other column
    (year, quarter, month_name, etc.) is left null, same as any other
    dimension's Unknown-row pattern -- this obviously-fake early date is
    not meant to be mistaken for a real calendar day.
    """
    from datetime import datetime
    from pyspark.sql.types import StructType, StructField

    existing = spark.table(table_name).filter(f"date = '{sentinel_date}'").limit(1).count()
    if existing > 0:
        logger.info(f"{table_name}: sentinel row {sentinel_date} already exists, nothing to do")
        return

    schema = spark.table(table_name).schema
    # Force every field nullable, same reasoning as add_unknown_member's
    # own fix elsewhere in the wheel: a field can be non-nullable purely
    # because Spark's inference never saw a null in the REAL calendar
    # data, which has nothing to do with whether this one placeholder row
    # is allowed to leave everything but date/date_key empty.
    nullable_schema = StructType([StructField(f.name, f.dataType, nullable=True) for f in schema.fields])
    cols = [f.name for f in schema.fields]

    sentinel_date_obj = datetime.strptime(sentinel_date, "%Y-%m-%d").date()
    sentinel_date_key = int(sentinel_date_obj.strftime("%Y%m%d"))
    sentinel_values = {"date": sentinel_date_obj, "date_key": sentinel_date_key}

    row = spark.createDataFrame([{c: sentinel_values.get(c) for c in cols}], schema=nullable_schema)
    row.write.format("delta").mode("append").saveAsTable(table_name)
    logger.info(f"{table_name}: added sentinel row for {sentinel_date}")
