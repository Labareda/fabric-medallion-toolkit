# Fabric notebook source

# MARKDOWN ********************

# ## Dim_Resolution
# Small but worth having as its own dimension: "how did this end" is a
# different question from "what state is it in", and Done-but-Cancelled
# vs Done-but-Delivered is a distinction the Programme Board cares about.

# CELL ********************
import fabric_medallion_toolkit as fmt

GOLD_SCHEMA = "Gold.gold"

# CELL ********************
schema = fmt.TableSchema(
    table_name=f"{GOLD_SCHEMA}.dim_resolution",
    table_type="dim",
    key_column="Resolution_Key",
    columns={
        "Resolution_Id":   {"type": "string", "merge_field": True, "missing": "Unknown"},
        "Resolution_Name": {"type": "string", "default": "Unresolved"},
        "Description":     {"type": "string", "default": ""},
    },
)

# CELL ********************
df = spark.sql("""
    SELECT r.id AS Resolution_Id, r.name AS Resolution_Name, r.description AS Description
    FROM Silver.jira.resolutions r
""")

# CELL ********************
fmt.merge(spark, df, schema)
print("Dim_Resolution built successfully")
