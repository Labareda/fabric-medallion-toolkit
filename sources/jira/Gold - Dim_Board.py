# Fabric notebook source
# "Gold - Dim_Board" — project_key kept as a plain attribute rather than
# snowflaking to Dim_Project, since Board is a minor dimension here (mainly
# for resourcing-view context/filtering, not the primary Gantt hierarchy).
# Attach: Silver + Gold lakehouses + env_medallion_toolkit.

# CELL ********************
import fabric_medallion_toolkit as fmt

GOLD_SCHEMA = "Gold.gold"

# CELL ********************
df = spark.sql(f"""
    SELECT
        CAST(board_id AS STRING) AS Board_Id,
        COALESCE(board_name, 'Unknown') AS Board_Name,
        COALESCE(board_type, 'Unknown') AS Board_Type,
        project_key AS Project_Code
    FROM Silver.jira.boards
""")

fmt.merge(spark, df, fmt.TableSchema(
    table_name=f"{GOLD_SCHEMA}.dim_board",
    table_type="dim",
    merge_fields=["Board_Id"],
    key_column="Board_Key",
))

print("Dim_Board built")
