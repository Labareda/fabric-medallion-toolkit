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
    df.write.format("delta").mode("overwrite").saveAsTable(config.table_name)
