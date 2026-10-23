# Fabric notebook source

# MARKDOWN ********************

# ## fact_gl -- Navision General Ledger entries as a Gold fact
#
# Grain: ONE ROW PER G/L ENTRY (entry_no_).
#
# The original dw_Navision load used a Delta_Control watermark + Landing_ staging
# tables + TRUNCATE/INSERT. The medallion doesn't need any of that: Silver already
# holds the clean, full copy of ess_g_l_entry each run, so this is simply
#   read Silver -> resolve FKs -> fmt.merge on Entry_No
# and merge is idempotent (new entries inserted, changed ones updated).
#
# FKs resolved here (facts resolve keys to dims):
#   * Account_Key       -> dim_account, on (account, dimension set)
#   * Benefit_Type_Key  -> dim_benefit_type, on account + the four dimension codes
#                          (reusing the account dim's resolved codes). Most entries
#                          aren't benefit postings, so this is 'Unknown' for them.
#   * Posting_Date      -> relate to dim_date in the model (date kept here)
#   * Register_No       -> ess_g_l_register, via the entry-range (BETWEEN)
#
# Build AFTER dim_account and dim_benefit_type (both are looked up here).
#
# NOTE: the source notebook was truncated at "Step 5 (transform)", so this is a
# best-effort fact following the Jira fact conventions. Verify the column list.

# CELL ********************

import fabric_medallion_toolkit as fmt

SILVER_SCHEMA = "silver.navision"   # cleaned ess_* tables
GOLD_SCHEMA   = "Gold.navision"     # set to your Navision Gold lakehouse/schema

# Only entries on/after this date (matches the account dim's scope). Widen for
# more history.
POSTED_SINCE = "2015-01-01"

# MARKDOWN ********************

# ##### Declare the fact schema

# CELL ********************

schema = fmt.TableSchema(
    table_name=f"{GOLD_SCHEMA}.fact_gl",
    table_type="fact",
    key_column="GL_Fact_Key",
    columns={
        "Entry_No": {"type": "int", "merge_field": True},   # the G/L entry no -- grain

        # --- Measures (additive) ---
        "Amount":                     {"type": "double"},
        "Debit_Amount":               {"type": "double"},
        "Credit_Amount":              {"type": "double"},
        "VAT_Amount":                 {"type": "double"},
        "Quantity":                   {"type": "double"},
        "Additional_Currency_Amount": {"type": "double"},
        "Add_Currency_Debit_Amount":  {"type": "double"},
        "Add_Currency_Credit_Amount": {"type": "double"},

        # --- Dates (Posting_Date relates to Dim_Date in the model) ---
        "Posting_Date":           {"type": "date"},
        "Document_Date":          {"type": "date"},
        "Last_Modified_DateTime": {"type": "timestamp"},

        # --- Foreign keys to dimensions ---
        "Account_Key":      {"type": "string", "default": "Unknown"},   # dim_account
        "Benefit_Type_Key": {"type": "string", "default": "Unknown"},   # dim_benefit_type

        # --- Degenerate dimensions / attributes ---
        "G_L_Account_No":          {"type": "int"},
        "Dimension_Set_ID":        {"type": "int"},
        "Register_No":             {"type": "int"},
        "Document_No":             {"type": "string"},
        "Document_Type":           {"type": "int"},
        "External_Document_No":    {"type": "string"},
        "Transaction_No":          {"type": "int"},
        "Source_Code":             {"type": "string"},
        "Source_No":               {"type": "string"},
        "Source_Type":             {"type": "int"},
        "Journal_Batch_Name":      {"type": "string"},
        "Reason_Code":             {"type": "string"},
        "Business_Unit_Code":      {"type": "string"},
        "Global_Dimension_1_Code": {"type": "string"},
        "Global_Dimension_2_Code": {"type": "string"},
        "Gen_Posting_Type":        {"type": "int"},
        "Gen_Bus_Posting_Group":   {"type": "string"},
        "Gen_Prod_Posting_Group":  {"type": "string"},
        "Bal_Account_No":          {"type": "string"},
        "Bal_Account_Type":        {"type": "int"},
        "User_ID":                 {"type": "string"},
        "Description":             {"type": "string"},

        # --- Flags ---
        "System_Created_Entry": {"type": "boolean", "default": False},
        "Prior_Year_Entry":     {"type": "boolean", "default": False},
        "Reversed":             {"type": "boolean", "default": False},
        "Tax_Liable":           {"type": "boolean", "default": False},
        "Use_Tax":              {"type": "boolean", "default": False},
    },
)

# MARKDOWN ********************

# ##### Build the fact (merge on Entry_No -- insert new, update changed)

# CELL ********************

df = spark.sql(f"""
    SELECT /*+ BROADCAST(gl_r) */
        gl_e.entry_no_                              AS Entry_No,

        CAST(gl_e.amount                      AS DOUBLE) AS Amount,
        CAST(gl_e.debit_amount                AS DOUBLE) AS Debit_Amount,
        CAST(gl_e.credit_amount               AS DOUBLE) AS Credit_Amount,
        CAST(gl_e.vat_amount                  AS DOUBLE) AS VAT_Amount,
        CAST(gl_e.quantity                    AS DOUBLE) AS Quantity,
        CAST(gl_e.additional_currency_amount  AS DOUBLE) AS Additional_Currency_Amount,
        CAST(gl_e.add__currency_debit_amount  AS DOUBLE) AS Add_Currency_Debit_Amount,
        CAST(gl_e.add__currency_credit_amount AS DOUBLE) AS Add_Currency_Credit_Amount,

        CAST(gl_e.posting_date           AS DATE)      AS Posting_Date,
        CAST(gl_e.document_date          AS DATE)      AS Document_Date,
        CAST(gl_e.last_modified_datetime AS TIMESTAMP) AS Last_Modified_DateTime,

        COALESCE(am.Account_Key,      'Unknown') AS Account_Key,
        COALESCE(bt.Benefit_Type_Key, 'Unknown') AS Benefit_Type_Key,

        gl_e.g_l_account_no_            AS G_L_Account_No,
        gl_e.dimension_set_id          AS Dimension_Set_ID,
        gl_r.no_                        AS Register_No,
        gl_e.document_no_              AS Document_No,
        gl_e.document_type             AS Document_Type,
        gl_e.external_document_no_     AS External_Document_No,
        gl_e.transaction_no_           AS Transaction_No,
        gl_e.source_code              AS Source_Code,
        gl_e.source_no_               AS Source_No,
        gl_e.source_type              AS Source_Type,
        gl_e.journal_batch_name       AS Journal_Batch_Name,
        gl_e.reason_code              AS Reason_Code,
        gl_e.business_unit_code       AS Business_Unit_Code,
        gl_e.global_dimension_1_code  AS Global_Dimension_1_Code,
        gl_e.global_dimension_2_code  AS Global_Dimension_2_Code,
        gl_e.gen__posting_type        AS Gen_Posting_Type,
        gl_e.gen__bus__posting_group  AS Gen_Bus_Posting_Group,
        gl_e.gen__prod__posting_group AS Gen_Prod_Posting_Group,
        gl_e.bal__account_no_         AS Bal_Account_No,
        gl_e.bal__account_type        AS Bal_Account_Type,
        gl_e.user_id                  AS User_ID,
        gl_e.description              AS Description,

        CAST(gl_e.system_created_entry AS BOOLEAN) AS System_Created_Entry,
        CAST(gl_e.prior_year_entry     AS BOOLEAN) AS Prior_Year_Entry,
        CAST(gl_e.reversed             AS BOOLEAN) AS Reversed,
        CAST(gl_e.tax_liable           AS BOOLEAN) AS Tax_Liable,
        CAST(gl_e.use_tax              AS BOOLEAN) AS Use_Tax

    FROM {SILVER_SCHEMA}.ess_g_l_entry gl_e

    -- Register number via the entry range. Registers are non-overlapping (1:1);
    -- any overlap would trip the merge's key-uniqueness check rather than
    -- silently duplicate the entry.
    LEFT JOIN {SILVER_SCHEMA}.ess_g_l_register gl_r
        ON gl_e.entry_no_ BETWEEN gl_r.from_entry_no_ AND gl_r.to_entry_no_

    -- Account mapping dimension, on account + dimension set.
    LEFT JOIN {GOLD_SCHEMA}.dim_account am
        ON  am.G_L_Account_No   = gl_e.g_l_account_no_
        AND am.Dimension_Set_ID = gl_e.dimension_set_id

    -- Benefit dimension: a benefit posts to a specific account + dimension combo,
    -- so match the entry's account and the account dim's four resolved codes to
    -- the benefit's natural keys. 'Unknown' for non-benefit entries.
    LEFT JOIN {GOLD_SCHEMA}.dim_benefit_type bt
        ON  bt.G_L_Account        = CAST(gl_e.g_l_account_no_ AS STRING)
        AND bt.Business_Unit_Code = am.Business_Unit_Code
        AND bt.Department_Code    = am.Department_Code
        AND bt.Fund_Code          = am.Fund_Code
        AND bt.Project_Code       = am.Project_Code

    WHERE gl_e.posting_date >= '{POSTED_SINCE}'
""")

# MARKDOWN ********************

# ##### Merge into Gold

# CELL ********************

fmt.merge(spark, df, schema)
print("fact_gl built successfully")
