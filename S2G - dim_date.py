# Fabric notebook source
# "S2G - dim_date" — the shared calendar dimension. Only needs the Gold
# lakehouse attached (nothing read from Silver). Run once, or every time --
# it's a full deterministic rebuild either way, cheap for a decade of dates.

# CELL ********************
%pip install /lakehouse/default/Files/libs/fabric_medallion_toolkit-0.2.1-py3-none-any.whl

# CELL ********************
from fabric_medallion_toolkit.config import DateDimensionConfig
from fabric_medallion_toolkit.gold import build_date_dimension

# CELL ********************
build_date_dimension(spark, DateDimensionConfig(
    table_name="gold.dim_date",
    start_date="2023-01-01",
    end_date="2035-12-31",
    fiscal_year_start_month=1,
))

# CELL ********************
print("S2G - dim_date complete.")
