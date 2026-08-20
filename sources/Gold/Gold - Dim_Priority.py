# Fabric notebook source

# MARKDOWN ********************

# ## Dim_Priority
# Sort_Order exists so a priority slicer reads Highest -> Lowest rather than
# alphabetically (High, Highest, Low, Lowest, Medium). Set it as the
# sort-by column for Priority_Name in the semantic model.
#
# MATCHED BY KEYWORD, NOT EXACT NAME -- this instance has (at least) two
# naming schemes live at once: the Jira default (Highest/High/Medium/Low/
# Lowest) AND a project-specific one (P1 - Critical/P2 - High/P3 - Medium/
# P4 - Low). An exact-string CASE only caught the first scheme; every P1-P4
# priority fell to the same ELSE 9 bucket, so the slicer couldn't order them
# at all. LIKE on a keyword catches any naming convention that uses the same
# words, and 'highest'/'lowest' are checked BEFORE the plain 'high'/'low'
# keyword (since both contain it as a substring) so "Highest"/"Lowest" don't
# get miscaught by the "High"/"Low" branch.

# CELL ********************
import fabric_medallion_toolkit as fmt

GOLD_SCHEMA = "Gold.gold"

# CELL ********************
schema = fmt.TableSchema(
    table_name=f"{GOLD_SCHEMA}.dim_priority",
    table_type="dim",
    key_column="Priority_Key",
    columns={
        "Priority_Id":    {"type": "string", "merge_field": True, "missing": "Unknown"},
        "Priority_Name":  {"type": "string", "default": "Unknown"},
        "Description":    {"type": "string", "default": ""},
        "Sort_Order":     {"type": "int", "default": 9},
    },
)

# CELL ********************
df = spark.sql("""
    SELECT
        p.id          AS Priority_Id,
        p.name        AS Priority_Name,
        p.description AS Description,
        CASE
            WHEN LOWER(p.name) LIKE '%highest%' OR LOWER(p.name) LIKE '%critical%' OR LOWER(p.name) LIKE 'p1%' THEN 1
            WHEN LOWER(p.name) LIKE '%high%'    OR LOWER(p.name) LIKE 'p2%'                                    THEN 2
            WHEN LOWER(p.name) LIKE '%medium%'  OR LOWER(p.name) LIKE 'p3%'                                    THEN 3
            WHEN LOWER(p.name) LIKE '%lowest%'  OR LOWER(p.name) LIKE 'p5%'                                    THEN 5
            WHEN LOWER(p.name) LIKE '%low%'     OR LOWER(p.name) LIKE 'p4%'                                    THEN 4
            ELSE 9
        END AS Sort_Order
    FROM Silver.jira.priorities p
""")

# CELL ********************
fmt.merge(spark, df, schema)
print("Dim_Priority built successfully")
