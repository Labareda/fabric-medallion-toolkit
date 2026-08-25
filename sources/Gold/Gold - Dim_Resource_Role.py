# Fabric notebook source

# MARKDOWN ********************

# ## Dim_Resource_Role -- Lead vs Involved
# Table/key renamed from Dim_Role/Role_Key to Dim_Resource_Role/
# Resource_Role_Key to match Fact_Resource_Allocation and Fact_Worklog's FK
# column naming, and what the semantic model (Dim Resource Role) expects.
# The client's resourcing question is about ROLE, not about users, so this
# tiny dimension is the axis they actually slice on.
#
# Contributes_To_Effort is what stops double counting. A Lead owns the work;
# Involved people are attached to it. Summing story points across the
# allocation fact without this would multiply every issue's effort by the
# number of names on it. Change the flag here if the client later wants
# Involved to carry a share.
#
# Built from a literal -- there is no Silver source for this; it is the
# model's own vocabulary.

# CELL ********************
import fabric_medallion_toolkit as fmt

GOLD_SCHEMA = "Gold.gold"

# CELL ********************
schema = fmt.TableSchema(
    table_name=f"{GOLD_SCHEMA}.dim_resourcerole",
    table_type="dim",
    key_column="Resource_Role_Key",
    columns={
        "Role_Name":             {"type": "string", "merge_field": True, "missing": "Unknown"},
        "Role_Description":      {"type": "string", "default": "Unknown"},
        "Is_Lead":               {"type": "boolean", "default": False},
        "Contributes_To_Effort": {"type": "boolean", "default": False},
        "Sort_Order":            {"type": "int", "default": 9},
    },
)

# CELL ********************
df = spark.createDataFrame(
    [
        ("Lead",     "Owns and is accountable for the item (Jira assignee)", True,  True,  1),
        ("Involved", "Named on the item via the People Involved field",      False, False, 2),
        ("Reporter", "Raised the item",                                      False, False, 3),
    ],
    "Role_Name string, Role_Description string, Is_Lead boolean, "
    "Contributes_To_Effort boolean, Sort_Order int",
)

# CELL ********************
fmt.merge(spark, df, schema)
print("Dim_Role built successfully")
