# Fabric notebook source

# MARKDOWN ********************

# ## Dim_Version -- Jira fix/affects versions
# Silver.jira.versions is a real extracted entity (it is in B2S's ENTITY_KEYS),
# so this is a proper dimension with real release dates -- not a scrape of
# distinct version names off the issue rows.
#
# NOTE this is NOT the same thing as the Release TIER in the issue hierarchy.
# Release-as-an-issue (hierarchylevel 4) is what the Gantt and the programme
# rollups use; Version is Jira's own release object. Both exist, they are not
# guaranteed to agree, and Dim_Issue's Release ancestor column is the source
# of truth for programme reporting. Keep this for defect "affects version"
# analysis and for reconciliation.

# CELL ********************
import fabric_medallion_toolkit as fmt

GOLD_SCHEMA = "Gold.gold"

# CELL ********************
schema = fmt.TableSchema(
    table_name=f"{GOLD_SCHEMA}.dim_version",
    table_type="dim",
    key_column="Version_Key",
    columns={
        "Version_Id":   {"type": "string", "merge_field": True, "missing": "Unknown"},
        "Version_Name": {"type": "string", "default": "Unknown"},
        "Description":  {"type": "string", "default": ""},
        "Start_Date":   {"type": "date"},
        "Release_Date": {"type": "date"},
        "Is_Released":  {"type": "boolean", "default": False},
        "Is_Archived":  {"type": "boolean", "default": False},
    },
)

# CELL ********************
df = spark.sql("""
    SELECT v.id AS Version_Id, v.name AS Version_Name, v.description AS Description,
           CAST(v.startDate AS date) AS Start_Date,
           CAST(v.releaseDate AS date) AS Release_Date,
           v.released AS Is_Released, v.archived AS Is_Archived
    FROM Silver.jira.versions v
""")

# CELL ********************
fmt.merge(spark, df, schema)
print("Dim_Version built successfully")
