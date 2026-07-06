# Fabric notebook source
# "S2G - dim_resource" — SCD2. Attach Silver + Gold lakehouses, plus env_medallion_toolkit.

# CELL ********************
import fabric_medallion_toolkit as fmt

# CELL ********************
# Currently just resource_name -- Jira alone doesn't expose attributes worth
# tracking history of. Once enriched (e.g. joined to Navision/HR data for
# department, role, etc.), list those columns in tracked_columns below.
df_dim_resource = spark.sql("""
    SELECT DISTINCT assignee AS resource_name
    FROM jira.issues
    WHERE assignee IS NOT NULL
""")

fmt.merge(spark, df_dim_resource, fmt.TableSchema(
    table_name="gold.dim_resource",
    table_type="scd2",
    merge_fields=["resource_name"],
    key_column="Resource_key",
    tracked_columns=None,  # None = track every non-merge-field column
    include_unknown_member=True,
))

# CELL ********************
print("S2G - dim_resource complete.")
