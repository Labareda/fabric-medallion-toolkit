# Fabric notebook source

# MARKDOWN ********************

# ## dim_benefit_type -- Navision benefit types as a Gold dimension
#
# Benefit types from Silver ess_benefit_type as a standalone dimension. SCD-1
# upsert via fmt.merge on Benefit_Code (insert new + update changed in one -- the
# old Step 2 INSERT and Step 3 UPDATE).
#
# INDEPENDENT of the other dimensions. It carries the benefit's G/L account and
# dimension CODES as natural keys -- it does NOT read Dim_G_L_Account_Mapping to
# resolve a mapping id. A fact that needs the account-mapping key resolves it by
# joining these natural keys to dim_g_l_account_mapping (the medallion way: facts
# resolve FKs to dims; dims don't read other dims).
#
# NCDG (non-contributory death grants) don't map correctly in the source, so for
# Benefit_Code = 'NCDG' the G/L account natural keys are overridden to
# 4631 / STATES / BENEFITS / RETIREMENT / NCD GRANT and the row flagged manually
# adjusted.

# CELL ********************

import fabric_medallion_toolkit as fmt

SILVER_SCHEMA = "silver.navision"   # cleaned ess_* tables
GOLD_SCHEMA   = "Gold.navision"     # set to your Navision Gold lakehouse/schema

# MARKDOWN ********************

# ##### Declare the table schema

# CELL ********************

schema = fmt.TableSchema(
    table_name=f"{GOLD_SCHEMA}.dim_benefit_type",
    table_type="dim",
    key_column="Benefit_Type_Key",
    columns={
        "Benefit_Code":                 {"type": "string",  "merge_field": True, "missing": "Unknown"},
        "Benefit_Description":          {"type": "string",  "default": "Unknown"},
        "Current_Payment_Limit":        {"type": "double"},
        "Active_Ind":                   {"type": "boolean", "default": False},
        "Non_Contributory_Benefit_Ind": {"type": "boolean", "default": False},
        "Record_Manually_Adjusted_Ind": {"type": "boolean", "default": False},

        # Benefit G/L account as NATURAL KEYS (resolve to dim_g_l_account_mapping
        # in a fact, not here -- keeps this dim independent).
        "G_L_Account":         {"type": "string"},
        "Business_Unit_Code":  {"type": "string"},
        "Department_Code":     {"type": "string"},
        "Fund_Code":           {"type": "string"},
        "Project_Code":        {"type": "string"},

        # Balance G/L account natural keys.
        "Bal_G_L_Account":        {"type": "string"},
        "Bal_Business_Unit_Code": {"type": "string"},
        "Bal_Department_Code":    {"type": "string"},
        "Bal_Fund_Code":          {"type": "string"},
        "Bal_Project_Code":       {"type": "string"},
    },
)

# MARKDOWN ********************

# ##### Build the dimension (SCD-1 on Benefit_Code -- no cross-dim dependency)

# CELL ********************

df = spark.sql(f"""
    SELECT
        bt.benefit_code                     AS Benefit_Code,
        bt.description                      AS Benefit_Description,
        CAST(bt.payment_limit AS DOUBLE)    AS Current_Payment_Limit,
        CAST(bt.active AS BOOLEAN)         AS Active_Ind,
        CAST(bt.ncb    AS BOOLEAN)         AS Non_Contributory_Benefit_Ind,
        CASE WHEN bt.benefit_code = 'NCDG' THEN TRUE ELSE FALSE END AS Record_Manually_Adjusted_Ind,

        -- Benefit G/L account natural keys, with the NCDG override baked in.
        CASE WHEN bt.benefit_code = 'NCDG' THEN '4631'       ELSE bt.g_l_account        END AS G_L_Account,
        CASE WHEN bt.benefit_code = 'NCDG' THEN 'STATES'     ELSE bt.global_dim__1_code END AS Business_Unit_Code,
        CASE WHEN bt.benefit_code = 'NCDG' THEN 'BENEFITS'   ELSE bt.global_dim__2_code END AS Department_Code,
        CASE WHEN bt.benefit_code = 'NCDG' THEN 'RETIREMENT' ELSE bt.global_dim__3_code END AS Fund_Code,
        CASE WHEN bt.benefit_code = 'NCDG' THEN 'NCD GRANT'  ELSE bt.global_dim__4_code END AS Project_Code,

        bt.bal__g_l_account        AS Bal_G_L_Account,
        bt.bal__global_dim__1_code AS Bal_Business_Unit_Code,
        bt.bal__global_dim__2_code AS Bal_Department_Code,
        bt.bal__global_dim__3_code AS Bal_Fund_Code,
        bt.bal__global_dim__4_code AS Bal_Project_Code

    FROM {SILVER_SCHEMA}.ess_benefit_type bt
""")

# MARKDOWN ********************

# ##### Merge into Gold

# CELL ********************

fmt.merge(spark, df, schema)
print("dim_benefit_type built successfully")
