# Fabric notebook source

# MARKDOWN ********************

# ## Dim_Link_Type
# Drives the traceability reporting: "tests"/"is tested by",
# "blocks"/"is blocked by", "implements"/"is implemented by". Bridge_Issue_Link
# stores both directions, so this dimension carries both labels and the
# report picks whichever reads correctly from the user's starting point.

# CELL ********************
import fabric_medallion_toolkit as fmt

GOLD_SCHEMA = "Gold.gold"

# CELL ********************
schema = fmt.TableSchema(
    table_name=f"{GOLD_SCHEMA}.dim_link_type",
    table_type="dim",
    key_column="Link_Type_Key",
    columns={
        "Link_Type_Id":   {"type": "string", "merge_field": True, "missing": "Unknown"},
        "Link_Type_Name": {"type": "string", "default": "Unknown"},
        "Inward_Label":   {"type": "string", "default": ""},
        "Outward_Label":  {"type": "string", "default": ""},
    },
)

# CELL ********************
df = spark.sql("""
    SELECT lt.id AS Link_Type_Id, lt.name AS Link_Type_Name,
           lt.inward AS Inward_Label, lt.outward AS Outward_Label
    FROM Silver.jira.issue_link_types lt
""")

# CELL ********************
fmt.merge(spark, df, schema)
print("Dim_Link_Type built successfully")
