# Fabric notebook source
# "Gold - Dim_Date" — standard calendar dimension. Synthetic (not derived
# from Silver), end date computed dynamically from the latest known issue
# due date so it never needs manual upkeep.
# Attach: Gold lakehouse + env_medallion_toolkit.

# CELL ********************
import fabric_medallion_toolkit as fmt
from fabric_medallion_toolkit.config import DateDimensionConfig

GOLD_SCHEMA = "Gold.gold"

# CELL ********************
end_date = spark.sql("""
    SELECT date_format(
        add_months(GREATEST(COALESCE(MAX(fields_duedate), current_date()), current_date()), 24),
        'yyyy-MM-dd'
    ) AS end_date
    FROM Silver.jira.issues
""").collect()[0]["end_date"]

fmt.build_date_dimension(spark, DateDimensionConfig(
    table_name=f"{GOLD_SCHEMA}.dim_date",
    start_date="2020-01-01",
    end_date=end_date,
))

print(f"Dim_Date built, end_date={end_date}")
