# Fabric notebook source

# MARKDOWN ********************

# ## Import environment and required packages

# CELL ********************
import fabric_medallion_toolkit as fmt
from fabric_medallion_toolkit.config import DateDimensionConfig

GOLD_SCHEMA = "Gold.gold"

# MARKDOWN ********************

# ## Compute a dynamic end date (latest known issue due date + 24 months buffer)

# CELL ********************
end_date = spark.sql("""
    SELECT date_format(
        add_months(GREATEST(COALESCE(MAX(fields_duedate), current_date()), current_date()), 24),
        'yyyy-MM-dd'
    ) AS end_date
    FROM Silver.jira.issues
""").collect()[0]["end_date"]

# MARKDOWN ********************

# ## Build the calendar dimension

# CELL ********************
calendar_schema = fmt.build_date_dimension(spark, DateDimensionConfig(
    table_name=f"{GOLD_SCHEMA}.dim_date",
    start_date="2020-01-01",
    end_date=end_date,
))

# MARKDOWN ********************

# ## Add a single sentinel row for missing/unknown dates

# CELL ********************
# Rather than extending the whole calendar back to 1900 (which would mean
# generating ~44,000 unnecessary daily rows just to have that one date
# exist), add ONLY that one row directly -- same idea as
# add_unknown_member for other dimensions, just done by hand here since
# build_date_dimension doesn't have that built in.
fmt.add_date_dimension_sentinel(spark, f"{GOLD_SCHEMA}.dim_date")

# MARKDOWN ********************

# ## Task complete

# CELL ********************
print(f"Dim_Date built successfully, end_date={end_date}")
