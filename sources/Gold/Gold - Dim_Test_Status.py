# Fabric notebook source

# MARKDOWN ********************

# ## Dim_Test_Status -- Xray run statuses
# Coverage_Status (OK / NOK / NOTRUN) mirrors how Xray itself rolls test
# results up to coverage, so a Power BI coverage number matches what the
# test team sees in Jira. If those two disagree the testers stop trusting
# the dashboard, and that is hard to recover.

# CELL ********************
import fabric_medallion_toolkit as fmt

GOLD_SCHEMA = "Gold.gold"

# CELL ********************
schema = fmt.TableSchema(
    table_name=f"{GOLD_SCHEMA}.dim_test_status",
    table_type="dim",
    key_column="Test_Status_Key",
    columns={
        "Test_Status_Name": {"type": "string", "merge_field": True, "missing": "Unknown"},
        "Description":      {"type": "string", "default": ""},
        "Colour":           {"type": "string", "default": ""},
        "Is_Final":         {"type": "boolean", "default": False},
        "Coverage_Status":  {"type": "string", "default": "NOTRUN"},
        "Sort_Order":       {"type": "int", "default": 9},
    },
)

# CELL ********************
df = spark.sql("""
    SELECT
        s.name AS Test_Status_Name,
        s.description AS Description,
        s.color AS Colour,
        s.name IN ('PASSED','FAILED') AS Is_Final,
        CASE s.name WHEN 'PASSED' THEN 'OK' WHEN 'FAILED' THEN 'NOK' ELSE 'NOTRUN' END AS Coverage_Status,
        CASE s.name WHEN 'PASSED' THEN 1 WHEN 'FAILED' THEN 2 WHEN 'BLOCKED' THEN 3
                    WHEN 'EXECUTING' THEN 4 WHEN 'TO DO' THEN 5 ELSE 9 END AS Sort_Order
    FROM Silver.xray.statuses s
""")

# CELL ********************
fmt.merge(spark, df, schema)
print("Dim_Test_Status built successfully")
