# Fabric notebook source
# "S2G - dim_project" — builds exactly one Gold table. Attach the Silver and
# Gold lakehouses to this notebook. Pattern for a new dim: copy this
# notebook, rename "S2G - dim_<name>", change the SQL and merge_fields.

# CELL ********************
%pip install /lakehouse/default/Files/libs/fabric_medallion_toolkit-0.2.1-py3-none-any.whl

# CELL ********************
from fabric_medallion_toolkit.gold import merge_dim

# CELL ********************
df_dim_project = spark.sql("""
    SELECT DISTINCT project_key
    FROM jira.issues
    WHERE project_key IS NOT NULL
""")

# display(df_dim_project)   # uncomment to inspect before merging

merge_dim(
    spark, df_dim_project,
    table_name="gold.dim_project",
    merge_fields=["project_key"],
    surrogate_key_column="project_key_sk",
)

# CELL ********************
print("S2G - dim_project complete.")
