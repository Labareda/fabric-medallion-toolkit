# Fabric notebook source

# MARKDOWN ********************

# ## Dim_Account -- Navision G/L accounts mapped to JDE/SAP
#
# Rebuilds the dw_Navision Dim_Account as a medallion Gold dimension.
# Grain: one row per distinct (G_L_Account_No, Dimension_Set_ID) seen in G/L
# postings since 2015-01-01. For each, it pulls the account name, the four global
# dimension code/name pairs (pivoted from the dimension-set entries), and the JDE
# mapping (object/sub account by account, company/bus-unit by the 4 dim codes).
#
# The original Step 1 CREATE + Step 2 incremental INSERT become `TableSchema` +
# `fmt.merge` on the (G_L_Account_No, Dimension_Set_ID) key: new combinations are
# inserted, existing ones refreshed. (The old "AM ... IS NULL" self-join that made
# it insert-only, and the unused G/L Register join, are dropped -- merge handles
# the first and the register columns aren't in this dimension.)
#
# SOURCE TABLES are the ess_* Silver tables (dbo_ prefix stripped by B2S).

# CELL ********************

import fabric_medallion_toolkit as fmt

SILVER_SCHEMA = "silver.navision"   # cleaned ess_* tables
GOLD_SCHEMA   = "Gold.navision"     # set to your Navision Gold lakehouse/schema

# Only G/L accounts posted on/after this date are mapped (matches the original).
POSTED_SINCE = "2015-01-01"

# MARKDOWN ********************

# ##### Declare the table schema

# CELL ********************

schema = fmt.TableSchema(
    table_name=f"{GOLD_SCHEMA}.dim_account",
    table_type="dim",
    key_column="Account_Key",
    columns={
        # Grain: one row per account + dimension set. Both are the natural key.
        "G_L_Account_No":          {"type": "int",    "merge_field": True, "missing": -1},
        "Dimension_Set_ID":        {"type": "int",    "merge_field": True, "missing": -1},
        "GL_Account_Name":         {"type": "string", "default": "Unknown"},
        "Mapping_Company":         {"type": "string"},
        "Mapping_Bus_Unit":        {"type": "string"},
        "Mapping_Object_Account":  {"type": "string"},
        "Mapping_Sub_Account":     {"type": "string"},
        "Mapping_Account_Name":    {"type": "string"},
        "Business_Unit_Code":      {"type": "string"},
        "Business_Unit_Name":      {"type": "string"},
        "Department_Code":         {"type": "string"},
        "Department_Name":         {"type": "string"},
        "Fund_Code":               {"type": "string"},
        "Fund_Name":               {"type": "string"},
        "Project_Code":            {"type": "string"},
        "Project_Name":            {"type": "string"},
        "Effective_From_Date":     {"type": "date"},
        "Effective_To_Date":       {"type": "date"},
    },
)

# MARKDOWN ********************

# ##### Build the dimension (insert new + refresh existing on account + dimension set)

# CELL ********************

df = spark.sql(f"""
    WITH
    -- Pivot the dimension-set entries into the four global dimension code/name
    -- pairs, one row per dimension_set_id.
    dse AS (
        SELECT
            t.dimension_set_id,
            MAX(CASE WHEN t1.global_dimension_no_ = 1 THEN t1.code END) AS GlobalDim1_Code,
            MAX(CASE WHEN t1.global_dimension_no_ = 1 THEN t1.name END) AS GlobalDim1_Name,
            MAX(CASE WHEN t1.global_dimension_no_ = 2 THEN t1.code END) AS GlobalDim2_Code,
            MAX(CASE WHEN t1.global_dimension_no_ = 2 THEN t1.name END) AS GlobalDim2_Name,
            MAX(CASE WHEN t1.global_dimension_no_ = 3 THEN t1.code END) AS GlobalDim3_Code,
            MAX(CASE WHEN t1.global_dimension_no_ = 3 THEN t1.name END) AS GlobalDim3_Name,
            MAX(CASE WHEN t1.global_dimension_no_ = 4 THEN t1.code END) AS GlobalDim4_Code,
            MAX(CASE WHEN t1.global_dimension_no_ = 4 THEN t1.name END) AS GlobalDim4_Name
        FROM {SILVER_SCHEMA}.ess_dimension_set_entry t
        JOIN {SILVER_SCHEMA}.ess_dimension_value t1
          ON t1.dimension_value_id = t.dimension_value_id
         AND t1.code               = t.dimension_value_code
        GROUP BY t.dimension_set_id
    )
    SELECT DISTINCT
        gl_e.g_l_account_no_        AS G_L_Account_No,
        gl_e.dimension_set_id       AS Dimension_Set_ID,
        gl_a.name                   AS GL_Account_Name,

        jde2.jde_company            AS Mapping_Company,
        jde2.jde_bus_unit           AS Mapping_Bus_Unit,
        jde1.jde_object_account     AS Mapping_Object_Account,
        jde1.jde_sub_account        AS Mapping_Sub_Account,
        jde1.jdeaccount_name        AS Mapping_Account_Name,

        dse.GlobalDim1_Code         AS Business_Unit_Code,
        dse.GlobalDim1_Name         AS Business_Unit_Name,
        dse.GlobalDim2_Code         AS Department_Code,
        dse.GlobalDim2_Name         AS Department_Name,
        dse.GlobalDim3_Code         AS Fund_Code,
        dse.GlobalDim3_Name         AS Fund_Name,
        dse.GlobalDim4_Code         AS Project_Code,
        dse.GlobalDim4_Name         AS Project_Name,

        CAST(NULL AS DATE)          AS Effective_From_Date,
        CAST(NULL AS DATE)          AS Effective_To_Date

    FROM {SILVER_SCHEMA}.ess_g_l_entry gl_e

    LEFT JOIN {SILVER_SCHEMA}.ess_g_l_account gl_a
        ON CAST(gl_a.no_ AS STRING) = CAST(gl_e.g_l_account_no_ AS STRING)

    LEFT JOIN {SILVER_SCHEMA}.ess_fund_dep_bus_unit_project fdbup
        ON fdbup.dimension_set_id = gl_e.dimension_set_id

    LEFT JOIN dse
        ON dse.dimension_set_id = gl_e.dimension_set_id

    -- JDE mapping by account number
    LEFT JOIN (SELECT * FROM {SILVER_SCHEMA}.ess_jde_mapping WHERE nav_account_no_ <> '') jde1
        ON jde1.nav_account_no_ = gl_a.no_

    -- JDE mapping by the four global dimension codes
    LEFT JOIN (SELECT * FROM {SILVER_SCHEMA}.ess_jde_mapping WHERE nav_global_dim__1_code <> '') jde2
        ON  jde2.nav_global_dim__1_code = fdbup.global_dim__1_code
        AND jde2.nav_global_dim__2_code = fdbup.global_dim__2_code
        AND jde2.nav_global_dim__3_code = fdbup.global_dim__3_code
        AND jde2.nav_global_dim__4_code = fdbup.global_dim__4_code

    WHERE gl_e.posting_date >= '{POSTED_SINCE}'
""")

# MARKDOWN ********************

# ##### Merge into Gold

# CELL ********************

fmt.merge(spark, df, schema)
print("dim_account built successfully")
