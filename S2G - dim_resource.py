# Fabric notebook source
# "S2G - dim_resource" — SCD2 dimension. Attach Silver and Gold lakehouses.

# CELL ********************
%pip install /lakehouse/default/Files/libs/fabric_medallion_toolkit-0.2.1-py3-none-any.whl

# CELL ********************
from fabric_medallion_toolkit.gold import merge_scd2

# CELL ********************
# Currently just resource_name -- Jira alone doesn't expose attributes worth
# tracking history of. Once enriched (e.g. joined to Navision/HR data for
# department, role, etc.), list those columns in tracked_columns below so a
# change in them creates a new version instead of overwriting.
df_dim_resource = spark.sql("""
    SELECT DISTINCT assignee AS resource_name
    FROM jira.issues
    WHERE assignee IS NOT NULL
""")

# display(df_dim_resource)

merge_scd2(
    spark, df_dim_resource,
    merge_fields=["resource_name"],
    surrogate_key_column="resource_key",
    table_name="gold.dim_resource",
    tracked_columns=None,  # None = track every non-merge-field column
)

# CELL ********************
print("S2G - dim_resource complete.")
