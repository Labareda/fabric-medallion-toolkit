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
fmt.build_date_dimension(spark, DateDimensionConfig(
    table_name=f"{GOLD_SCHEMA}.dim_date",
    start_date="2020-01-01",
    end_date=end_date,
))

# MARKDOWN ********************

# ## Task complete

# CELL ********************
print(f"Dim_Date built successfully, end_date={end_date}")
