# Fabric notebook source
# "B2S - Jira" — Bronze-to-Silver for the Jira source. Standardization
# only: reads what "S2B - Jira" already landed in Bronze, types and cleans
# it into Silver. NO API calls happen here at all. Run this AFTER
# "S2B - Jira".
# Attach Bronze, Silver, and Config lakehouses, plus env_medallion_toolkit.

# CELL ********************
import json
from notebookutils import mssparkutils
import fabric_medallion_toolkit as fmt

# CELL ********************
SOURCE_NAME = "jira"
# Lakehouse-qualified -- see S2B - Jira for why a bare "jira" schema name
# isn't enough once Bronze and Silver are genuinely separate lakehouses.
# This notebook reads FROM Bronze and writes TO Silver, so both need to be
# explicit regardless of which one happens to be pinned default here.
BRONZE_SCHEMA = "Bronze.jira"
SILVER_SCHEMA = "Silver.jira"

# Same ABFS approach as S2B - Jira -- see that notebook for how to get this path.
CONFIG_ABFS_PATH = "abfss://<workspace>@onelake.dfs.fabric.microsoft.com/Config.Lakehouse/Files/jira.json"

def resolve_secret(akv_name: str, secret_name: str) -> str:
    return mssparkutils.credentials.getSecret(akv_name, secret_name)

# source_config isn't needed here (no API calls in this notebook) -- only
# the "silver" blocks from jira.json matter.
config_json_text = mssparkutils.fs.head(CONFIG_ABFS_PATH, 10 * 1024 * 1024)
config_dict = json.loads(config_json_text)
_source_config, silver_configs = fmt.load_source_config(config_dict, secret_resolver=resolve_secret)
silver_by_entity = {sc.entity_name: sc for sc in silver_configs}

# CELL ********************
# --- Main entities: everything with a "silver" block in jira.json ---
failed_entities = []

for entity_name, silver_cfg in silver_by_entity.items():
    bronze_table = f"{BRONZE_SCHEMA}.{entity_name}"
    if not spark.catalog.tableExists(bronze_table):
        # Two reasons this happens, both fine to just skip: the entity
        # genuinely returned 0 records in Bronze (land_records doesn't
        # create a table for zero rows), or Bronze extraction failed for
        # it entirely. Either way, nothing here to standardize yet.
        print(f"[{entity_name}] skipping -- {bronze_table} doesn't exist yet (0 records landed, or Bronze extraction hasn't succeeded for this entity)")
        failed_entities.append(entity_name)
        continue
    try:
        fmt.run_silver_standardize(spark, silver_cfg, bronze_schema=BRONZE_SCHEMA, silver_schema=SILVER_SCHEMA)
        print(f"[{entity_name}] standardized -> {SILVER_SCHEMA}.{entity_name} (Silver)")
    except Exception as exc:
        print(f"[{entity_name}] Silver standardization FAILED, skipping: {exc}")
        failed_entities.append(entity_name)

# CELL ********************
# --- Versions (per-project Bronze entity, standardized the same way as any other) ---
if not spark.catalog.tableExists(f"{BRONZE_SCHEMA}.versions"):
    print(f"[versions] skipping -- {BRONZE_SCHEMA}.versions doesn't exist yet (S2B - Jira hasn't landed it)")
else:
    versions_silver_cfg = fmt.SilverEntityConfig(
        source_name=SOURCE_NAME, entity_name="versions", natural_key_columns=["version_id"],
        column_mappings=[
            fmt.ColumnMapping("version_id", "id", "string"),
            fmt.ColumnMapping("version_name", "name", "string"),
            fmt.ColumnMapping("project_key", "_parent_id", "string"),
            fmt.ColumnMapping("is_released", "released", "boolean"),
            fmt.ColumnMapping("is_archived", "archived", "boolean"),
            fmt.ColumnMapping("release_date", "releaseDate", "date"),
            fmt.ColumnMapping("start_date", "startDate", "date"),
        ],
    )
    fmt.run_silver_standardize(spark, versions_silver_cfg, bronze_schema=BRONZE_SCHEMA, silver_schema=SILVER_SCHEMA)
    print("[versions] standardized -> jira.versions (Silver)")

# CELL ********************
# --- Components (per-project Bronze entity) ---
if not spark.catalog.tableExists(f"{BRONZE_SCHEMA}.components"):
    print(f"[components] skipping -- {BRONZE_SCHEMA}.components doesn't exist yet (S2B - Jira hasn't landed it)")
else:
    components_silver_cfg = fmt.SilverEntityConfig(
        source_name=SOURCE_NAME, entity_name="components", natural_key_columns=["component_id"],
        column_mappings=[
            fmt.ColumnMapping("component_id", "id", "string"),
            fmt.ColumnMapping("component_name", "name", "string"),
            fmt.ColumnMapping("description", "description", "string"),
            fmt.ColumnMapping("project_key", "_parent_id", "string"),
            fmt.ColumnMapping("lead_account_id", "lead.accountId", "string"),
            fmt.ColumnMapping("lead_name", "lead.displayName", "string"),
        ],
    )
    fmt.run_silver_standardize(spark, components_silver_cfg, bronze_schema=BRONZE_SCHEMA, silver_schema=SILVER_SCHEMA)
    print("[components] standardized -> jira.components (Silver)")

# CELL ********************
# --- Sprints (per-board Bronze entity) ---
if not spark.catalog.tableExists(f"{BRONZE_SCHEMA}.sprints"):
    print(f"[sprints] skipping -- {BRONZE_SCHEMA}.sprints doesn't exist yet (S2B - Jira hasn't landed it, "
          "likely because Boards access failed there)")
else:
    sprints_silver_cfg = fmt.SilverEntityConfig(
        source_name=SOURCE_NAME, entity_name="sprints", natural_key_columns=["sprint_id"],
        column_mappings=[
            fmt.ColumnMapping("sprint_id", "id", "string"),
            fmt.ColumnMapping("sprint_name", "name", "string"),
            fmt.ColumnMapping("board_id", "_parent_id", "string"),
            fmt.ColumnMapping("sprint_state", "state", "string"),
            fmt.ColumnMapping("sprint_goal", "goal", "string"),
            fmt.ColumnMapping("start_date", "startDate", "timestamp", date_format="yyyy-MM-dd'T'HH:mm:ss.SSSZ"),
            fmt.ColumnMapping("end_date", "endDate", "timestamp", date_format="yyyy-MM-dd'T'HH:mm:ss.SSSZ"),
            fmt.ColumnMapping("complete_date", "completeDate", "timestamp", date_format="yyyy-MM-dd'T'HH:mm:ss.SSSZ"),
        ],
    )
    fmt.run_silver_standardize(spark, sprints_silver_cfg, bronze_schema=BRONZE_SCHEMA, silver_schema=SILVER_SCHEMA)
    print("[sprints] standardized -> jira.sprints (Silver)")

# CELL ********************
# --- Nested-inside-Issues tables: NO new API calls -- already in Bronze via
# issues' raw_data (fields.* / changelog.*), just need exploding into their
# own tables.
if not spark.catalog.tableExists(f"{BRONZE_SCHEMA}.issues"):
    print("[issue_links/issue_components/issue_fix_version/issue_affects_version/histories] "
          "skipping -- jira.issues doesn't exist yet (0 issues landed, or S2B - Jira hasn't run)")
else:
    bronze_issues_df = spark.table(f"{BRONZE_SCHEMA}.issues")

    # IssueLinks
    issue_links_df = fmt.explode_nested_array(
        bronze_issues_df, array_json_path="fields.issuelinks",
        parent_key_column="issue_key", parent_key_json_path="key",
        column_mappings=[
            fmt.ColumnMapping("link_type", "type.name", "string"),
            fmt.ColumnMapping("outward_issue_key", "outwardIssue.key", "string"),
            fmt.ColumnMapping("inward_issue_key", "inwardIssue.key", "string"),
        ],
    )
    issue_links_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{SILVER_SCHEMA}.issue_links")
    print(f"[issue_links] {issue_links_df.count()} rows -> jira.issue_links (Silver)")

    # IssueComponents
    issue_components_df = fmt.explode_nested_array(
        bronze_issues_df, array_json_path="fields.components",
        parent_key_column="issue_key", parent_key_json_path="key",
        column_mappings=[
            fmt.ColumnMapping("component_id", "id", "string"),
            fmt.ColumnMapping("component_name", "name", "string"),
        ],
    )
    issue_components_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{SILVER_SCHEMA}.issue_components")
    print(f"[issue_components] {issue_components_df.count()} rows -> jira.issue_components (Silver)")

    # IssueFixVersion
    issue_fix_version_df = fmt.explode_nested_array(
        bronze_issues_df, array_json_path="fields.fixVersions",
        parent_key_column="issue_key", parent_key_json_path="key",
        column_mappings=[
            fmt.ColumnMapping("version_id", "id", "string"),
            fmt.ColumnMapping("version_name", "name", "string"),
        ],
    )
    issue_fix_version_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{SILVER_SCHEMA}.issue_fix_version")
    print(f"[issue_fix_version] {issue_fix_version_df.count()} rows -> jira.issue_fix_version (Silver)")

    # IssueAffectsVersions
    issue_affects_version_df = fmt.explode_nested_array(
        bronze_issues_df, array_json_path="fields.versions",
        parent_key_column="issue_key", parent_key_json_path="key",
        column_mappings=[
            fmt.ColumnMapping("version_id", "id", "string"),
            fmt.ColumnMapping("version_name", "name", "string"),
        ],
    )
    issue_affects_version_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{SILVER_SCHEMA}.issue_affects_version")
    print(f"[issue_affects_version] {issue_affects_version_df.count()} rows -> jira.issue_affects_version (Silver)")

    # Histories -- one row per changelog ENTRY (a single edit event, which can
    # itself contain several field changes). The individual field changes stay
    # as a JSON array in the "items" column rather than being exploded a second
    # level down -- ask if you want that broken out further, it's a small
    # extension of the same pattern.
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
    histories_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{SILVER_SCHEMA}.histories")
    print(f"[histories] {histories_df.count()} rows -> jira.histories (Silver)")

# CELL ********************
if failed_entities:
    print(f"B2S - Jira complete, WITH SKIPS/FAILURES in: {failed_entities} -- see messages above for why.")
else:
    print("B2S - Jira complete, all entities standardized.")
