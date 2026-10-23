# Fabric notebook source

# MARKDOWN ********************

# ## dim_benefit_type -- Navision benefit types (pure dimension)
#
# A clean star-schema dimension: benefit ATTRIBUTES only, keyed on Benefit_Code.
# SCD-1 upsert via fmt.merge (insert new + update changed in one).
#
# It does NOT carry G/L account keys -- the benefit->account mapping that links a
# benefit to the GL fact lives in fact_gl, which resolves Benefit_Type_Key from
# Silver ess_benefit_type. That keeps this dimension independent and the model a
# proper star (facts hold the foreign keys; dimensions don't reference each other).
#
# Record_Manually_Adjusted_Ind flags NCDG (its account mapping is corrected in the
# fact's resolution).

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
    },
)

# MARKDOWN ********************

# ##### Build the dimension (SCD-1 on Benefit_Code -- pure, no account keys)

# CELL ********************

df = spark.sql(f"""
    SELECT
        bt.benefit_code                  AS Benefit_Code,
        bt.description                   AS Benefit_Description,
        CAST(bt.payment_limit AS DOUBLE) AS Current_Payment_Limit,
        CAST(bt.active AS BOOLEAN)       AS Active_Ind,
        CAST(bt.ncb    AS BOOLEAN)       AS Non_Contributory_Benefit_Ind,
        CASE WHEN bt.benefit_code = 'NCDG' THEN TRUE ELSE FALSE END AS Record_Manually_Adjusted_Ind
    FROM {SILVER_SCHEMA}.ess_benefit_type bt
""")

# MARKDOWN ********************

# ##### Merge into Gold

# CELL ********************

fmt.merge(spark, df, schema)
print("dim_benefit_type built successfully")
