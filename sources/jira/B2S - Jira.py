# Fabric notebook source
# "B2S - Jira" — Bronze-to-Silver for the Jira source. No column_mappings
# maintained here at all: every entity's fields land in Silver automatically
# (auto_standardize), typed correctly via Spark's own JSON inference.
# Column NAMING and any real modeling happens later, in Gold -- this
# notebook's job is just "raw data, correct types, in Delta tables."
# NO API calls happen here. Run this AFTER "S2B - Jira".
# Attach Bronze, Silver, and Config lakehouses, plus env_medallion_toolkit.

# CELL ********************
from notebookutils import mssparkutils
from pyspark.sql import functions as F
import fabric_medallion_toolkit as fmt

# CELL ********************
BRONZE_SCHEMA = "Bronze.jira"
SILVER_SCHEMA = "Silver.jira"

# --- Every entity here, and the Bronze-side natural key column(s) it uses.
# These match S2B - Jira's config exactly -- if you add an entity to
# jira.json's "entities" list, add its natural key here too. (Nested-object
# keys, e.g. workflows' "id.entityId", flatten with underscores: "id_entityId".)
ENTITY_NATURAL_KEYS = {
    "issues": ["key"],
    "projects": ["key"],
    "users": ["accountId"],
    "issuetypes": ["id"],
    "statuses": ["id"],
    "priorities": ["id"],
    "resolutions": ["id"],
    "project_roles": ["id"],
    "audit_logs": ["id"],
    "boards": ["id"],
    "fields": ["id"],
    "issue_link_types": ["id"],
    "filters": ["id"],
    "groups": ["groupId"],
    "dashboards": ["id"],
    "screens": ["id"],
    "workflows": ["id_entityId"],
    "versions": ["id"],
    "components": ["id"],
    "sprints": ["id"],
}

# CELL ********************
failed_entities = []

for entity_name, natural_keys in ENTITY_NATURAL_KEYS.items():
    bronze_table = f"{BRONZE_SCHEMA}.{entity_name}"
    if not spark.catalog.tableExists(bronze_table):
        print(f"[{entity_name}] skipping -- {bronze_table} doesn't exist yet "
              f"(0 records landed in S2B, or that entity's extraction failed there)")
        failed_entities.append(entity_name)
        continue
    try:
        fmt.run_auto_silver_standardize(
            spark, entity_name=entity_name, natural_key_columns=natural_keys,
            bronze_schema=BRONZE_SCHEMA, silver_schema=SILVER_SCHEMA,
        )
        print(f"[{entity_name}] standardized -> {SILVER_SCHEMA}.{entity_name}")
    except Exception as exc:
        print(f"[{entity_name}] FAILED, skipping: {exc}")
        failed_entities.append(entity_name)

# CELL ********************
# --- Labels is the one exception: Bronze stores each label as a bare JSON
# string ("backend"), not an object, so auto_standardize's schema inference
# doesn't apply (Spark's JSON reader expects each record to be an object).
# Trivial one-column case anyway -- handled directly here instead.
if spark.catalog.tableExists(f"{BRONZE_SCHEMA}.labels"):
    labels_df = (
        spark.table(f"{BRONZE_SCHEMA}.labels")
        .withColumn("label_name", F.regexp_replace(F.col("raw_data"), '^"|"$', ""))  # strip the JSON quoting
        .select("label_name", "_extracted_at")
    )
    deduped = fmt.dedup_latest(labels_df, key_cols=["label_name"], order_by_col="_extracted_at").drop("_extracted_at")
    deduped.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{SILVER_SCHEMA}.labels")
    print(f"[labels] standardized -> {SILVER_SCHEMA}.labels")
else:
    print(f"[labels] skipping -- {BRONZE_SCHEMA}.labels doesn't exist yet")
    failed_entities.append("labels")

# CELL ********************
# --- Ad-hoc type corrections go here, per field, as you discover you need
# them -- no config to maintain, just Spark column expressions. Examples
# (edit/delete/add as needed once you've looked at real Silver data):
#
# df = spark.table(f"{SILVER_SCHEMA}.issues")
# df = df.withColumn(
#     "fields_duedate",
#     F.when(F.col("fields_duedate") == "", None).otherwise(F.to_date("fields_duedate"))
# )
# df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{SILVER_SCHEMA}.issues")

# CELL ********************
# --- Nested-inside-Issues tables: IssueLinks, IssueComponents,
# IssueFixVersion, IssueAffectsVersions, Histories, HistoryItems. These stay
# on explode_nested_array's explicit field list (not auto_standardize) --
# they're specific derived child tables you asked for by name, not "every
# field," so declaring exactly which fields make up each one is the right
# fit here, same as before.
if not spark.catalog.tableExists(f"{BRONZE_SCHEMA}.issues"):
    print("[issue_links/issue_components/issue_fix_version/issue_affects_version/histories/history_items] "
          "skipping -- no issues in Bronze yet")
else:
    bronze_issues_df = spark.table(f"{BRONZE_SCHEMA}.issues")

    issue_links_df = fmt.explode_nested_array(
        bronze_issues_df, array_json_path="fields.issuelinks",
        parent_key_column="issue_key", parent_key_json_path="key",
        column_mappings=[
            fmt.ColumnMapping("link_id", "id", "string"),
            fmt.ColumnMapping("link_type", "type.name", "string"),
            fmt.ColumnMapping("inward_label", "type.inward", "string"),
            fmt.ColumnMapping("outward_label", "type.outward", "string"),
            fmt.ColumnMapping("outward_issue_key", "outwardIssue.key", "string"),
            fmt.ColumnMapping("inward_issue_key", "inwardIssue.key", "string"),
        ],
    )
    issue_links_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{SILVER_SCHEMA}.issue_links")
    print(f"[issue_links] {issue_links_df.count()} rows -> {SILVER_SCHEMA}.issue_links")

    issue_components_df = fmt.explode_nested_array(
        bronze_issues_df, array_json_path="fields.components",
        parent_key_column="issue_key", parent_key_json_path="key",
        column_mappings=[
            fmt.ColumnMapping("component_id", "id", "string"),
            fmt.ColumnMapping("component_name", "name", "string"),
        ],
    )
    issue_components_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{SILVER_SCHEMA}.issue_components")
    print(f"[issue_components] {issue_components_df.count()} rows -> {SILVER_SCHEMA}.issue_components")

    issue_fix_version_df = fmt.explode_nested_array(
        bronze_issues_df, array_json_path="fields.fixVersions",
        parent_key_column="issue_key", parent_key_json_path="key",
        column_mappings=[
            fmt.ColumnMapping("version_id", "id", "string"),
            fmt.ColumnMapping("version_name", "name", "string"),
        ],
    )
    issue_fix_version_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{SILVER_SCHEMA}.issue_fix_version")
    print(f"[issue_fix_version] {issue_fix_version_df.count()} rows -> {SILVER_SCHEMA}.issue_fix_version")

    issue_affects_version_df = fmt.explode_nested_array(
        bronze_issues_df, array_json_path="fields.versions",
        parent_key_column="issue_key", parent_key_json_path="key",
        column_mappings=[
            fmt.ColumnMapping("version_id", "id", "string"),
            fmt.ColumnMapping("version_name", "name", "string"),
        ],
    )
    issue_affects_version_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{SILVER_SCHEMA}.issue_affects_version")
    print(f"[issue_affects_version] {issue_affects_version_df.count()} rows -> {SILVER_SCHEMA}.issue_affects_version")

    histories_df = fmt.explode_nested_array(
        bronze_issues_df, array_json_path="changelog.histories",
        parent_key_column="issue_key", parent_key_json_path="key",
        column_mappings=[
            fmt.ColumnMapping("history_id", "id", "string"),
            fmt.ColumnMapping("author_account_id", "author.accountId", "string"),
            fmt.ColumnMapping("author_name", "author.displayName", "string"),
            fmt.ColumnMapping("created", "created", "timestamp", date_format="yyyy-MM-dd'T'HH:mm:ss.SSSZ"),
            fmt.ColumnMapping("items", "items", "string"),
        ],
    )

    history_items_df = fmt.explode_nested_array(
        histories_df, array_json_path="",
        column_mappings=[
            fmt.ColumnMapping("field_name", "field", "string"),
            fmt.ColumnMapping("from_value", "fromString", "string"),
            fmt.ColumnMapping("to_value", "toString", "string"),
        ],
        source_column="items",
        carry_through_columns=["issue_key", "history_id"],
    )
    history_items_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{SILVER_SCHEMA}.history_items")
    print(f"[history_items] {history_items_df.count()} rows -> {SILVER_SCHEMA}.history_items")

    histories_df = histories_df.drop("items")
    histories_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{SILVER_SCHEMA}.histories")
    print(f"[histories] {histories_df.count()} rows -> {SILVER_SCHEMA}.histories")

# CELL ********************
if failed_entities:
    print(f"B2S - Jira complete, WITH SKIPS/FAILURES in: {failed_entities} -- see messages above for why.")
else:
    print("B2S - Jira complete, all entities standardized.")
