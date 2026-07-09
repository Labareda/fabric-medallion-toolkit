# Fabric notebook source
# "Gold - Dim_User"
# Attach: Silver + Gold lakehouses + env_medallion_toolkit.

# CELL ********************
import fabric_medallion_toolkit as fmt

GOLD_SCHEMA = "Gold.gold"

# CELL ********************
df = spark.sql(f"""
    SELECT
        CAST(accountId AS STRING) AS User_Account_Id,
        COALESCE(displayName, 'Unknown') AS User_Name,
        emailAddress AS Email,
        COALESCE(CAST(CAST(active AS boolean) AS STRING), 'false') AS Is_Active,
        COALESCE(accountType, 'Unknown') AS Account_Type,
        timeZone AS Time_Zone
    FROM Silver.jira.users
""")

fmt.merge(spark, df, fmt.TableSchema(
    table_name=f"{GOLD_SCHEMA}.dim_user",
    table_type="dim",
    merge_fields=["User_Account_Id"],
    key_column="User_Key",
))

print("Dim_User built")
