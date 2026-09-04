# Fabric notebook source
# "S2B - Jira" — Source-to-Bronze for the Jira source. Extraction only:
# pulls from the Jira API, lands raw JSON into Bronze. NO standardization
# happens here -- that's "B2S - Jira"'s job, reading from what this
# notebook lands. Run this one FIRST.
# Attach Bronze and Config lakehouses, plus env_medallion_toolkit.

# CELL ********************
from datetime import datetime, timezone
import json
from notebookutils import mssparkutils
import notebookutils
import fabric_medallion_toolkit as fmt

# CELL ********************
SOURCE_NAME = "jira"
# Lakehouse-qualified, not just a schema name -- "jira.issues" alone is
# ambiguous (it resolves inside whichever ONE lakehouse is currently pinned
# "default" in this notebook; the schema name doesn't select a lakehouse).
# "Bronze.jira" is an explicit 3-part reference (LakehouseName.schema.table
# once the entity name is appended) that works correctly regardless of
# which lakehouse is pinned default. Change "Bronze" if you named your
# Bronze lakehouse something else.
SCHEMA = "Bronze.jira"  # raw entity data lands here, unchanged
# Watermark control table lives under Config, alongside Xray's, for one place
# to look for pipeline state -- NOT the same physical table as Xray's,
# deliberately: the wheel's watermark table is keyed on entity_name ALONE (no
# source_name column), so two sources sharing one physical table would
# silently collide if they ever had a same-named entity. Config.jira keeps
# them co-located under one lakehouse while staying in separate schemas.
WATERMARK_SCHEMA = "Config.jira"

# Same reasoning as Xray's notebook: upsert_delta creates the watermarks
# TABLE on first write but never creates the SCHEMA it lives in. Config.jira
# is new (previously Config only held jira.json as a loose file), so it must
# be created explicitly before the first save_watermark call. Idempotent --
# harmless once it exists.
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {WATERMARK_SCHEMA}")

# Config's Files aren't a Spark table, and relative paths like
# "/lakehouse/default/Files/..." only work for whichever lakehouse is
# pinned default (Bronze usually is here, not Config) -- so read via the
# real ABFS path instead. Get it from: Config lakehouse -> Files ->
# right-click jira.json -> "Copy ABFS path", and paste it below.
CONFIG_ABFS_PATH = "abfss://<workspace>@onelake.dfs.fabric.microsoft.com/Config.Lakehouse/Files/jira.json"

# --- Secret resolution: a Fabric CONNECTION, not Azure Key Vault and NOT a
# Variable library. This client has no Key Vault access, and a Variable
# library's values are plain, git-trackable configuration -- NOT an
# encrypted secret store (confirmed against Microsoft's own variable-types
# docs: there is no "sensitive"/secret variable type). A Fabric Connection
# (Workspace -> Manage connections -> New connection) IS built for this: the
# credential is encrypted by Fabric, independent of git, and reached at
# runtime via notebookutils.connections.getCredential(connection_id).
#
# jira.json still uses the field names secret_akv_name / secret_akv_secret_name
# (so the wheel's existing config_loader needs no changes -- it just hands
# these two strings to whatever resolver is passed below). They're repurposed
# here to mean "connection id" and "which field inside that connection's
# credential to use" -- NOT an AKV vault/secret pair. The token itself never
# appears in jira.json or in this notebook.
#
# One-time setup: Workspace -> Manage connections -> New connection -> Web V2
# -> Base Url = your Jira base_url -> Authentication method = Basic ->
# username = Jira account email, password = the Jira API token -> tick
# "Allow Code-First Artifacts like Notebooks to access this connection". Copy
# the connection's ID (a GUID) into jira.json's secret_akv_name.
def resolve_secret(connection_id: str, field_name: str) -> str:
    raw = notebookutils.connections.getCredential(connection_id)
    credential_data = json.loads(raw["credential"])["credentialData"]
    # credentialData is a list of {"name": ..., "value": ...} entries. The
    # exact name Fabric uses for a Basic-auth password isn't publicly
    # documented, so this matches case-insensitively against the requested
    # name AND its common aliases, rather than assuming one exact string.
    aliases = {field_name.lower(), "password", "secret", "key", "value"} if field_name.lower() in ("password", "secret") \
        else {field_name.lower(), "username", "user"}
    for item in credential_data:
        if item.get("name", "").lower() in aliases:
            return item["value"]
    # Fails loudly with the ACTUAL field names present (not their values --
    # notebook output auto-redacts secret values, but names are safe to
    # print) so a mismatch is a one-message fix, not a guessing game.
    available = [item.get("name") for item in credential_data]
    raise KeyError(
        f"Could not find a field matching '{field_name}' in connection {connection_id}. "
        f"Fields actually present: {available}"
    )

config_json_text = mssparkutils.fs.head(CONFIG_ABFS_PATH, 10 * 1024 * 1024)  # 10MB is plenty for a config file
config_dict = json.loads(config_json_text)
source_config, _silver_configs = fmt.load_source_config(config_dict, secret_resolver=resolve_secret)
extractor = fmt.RestExtractor(source_config)

# CELL ********************
# --- Main entities: everything listed in jira.json's "entities" array ---
failed_entities = []

for entity in source_config.entities:
    try:
        extraction_started_at = datetime.now(timezone.utc)

        use_watermark = bool(entity.incremental_column or entity.watermark_params_template or entity.watermark_body_template)
        watermark_value = fmt.get_watermark(spark, SOURCE_NAME, entity, bronze_schema=WATERMARK_SCHEMA) if use_watermark else None

        # Jira JQL only accepts 'yyyy-MM-dd HH:mm' -- it REJECTS the ISO-8601
        # form (e.g. "2026-09-03T08:41:16.958+0000") that Jira's own
        # fields.updated returns and that a previous run may have stored as
        # the watermark. An un-normalised value makes `updated >= '{watermark}'`
        # invalid, so the incremental pull returns 0 rows and updates silently
        # stop landing. Normalise here (identical to the Xray pull): drop the
        # 'T'/'Z' and trim to the minute. Flooring to the minute with '>='
        # also gives a safe overlap so a same-minute update isn't skipped, and
        # because the stuck watermark sits at the last good point, this run
        # re-pulls everything changed since -- recovering the missed updates.
        if watermark_value and "T" in watermark_value:
            watermark_value = watermark_value.replace("T", " ").replace("Z", "")[:16]

        print(f"[{entity.entity_name}] {'incremental, watermark >= ' + str(watermark_value) if use_watermark else 'full load'}")

        records = list(extractor.extract_entity(entity, watermark_value=watermark_value))
        count = fmt.land_records(spark, records, source_name=SOURCE_NAME, entity=entity, bronze_schema=SCHEMA)
        print(f"[{entity.entity_name}] landed {count} records")

        if use_watermark and count > 0:
            new_watermark = fmt.compute_new_watermark(records, entity, extraction_started_at)
            fmt.save_watermark(spark, SOURCE_NAME, entity, new_watermark, bronze_schema=WATERMARK_SCHEMA)
            print(f"[{entity.entity_name}] watermark advanced to {new_watermark}")

    except Exception as exc:
        # One entity failing (permissions, a bad JQL, a transient API issue)
        # shouldn't stop every other entity from landing.
        print(f"[{entity.entity_name}] FAILED, skipping: {exc}")
        failed_entities.append(entity.entity_name)

# CELL ********************
# --- Per-project entities: Versions (one call per project, not a single global list) ---
project_keys = [r["primary_key"] for r in spark.table(f"{SCHEMA}.projects").select("primary_key").distinct().collect()] \
    if spark.catalog.tableExists(f"{SCHEMA}.projects") else []

versions_template = fmt.EntityConfig(
    entity_name="versions", endpoint_path="/rest/api/3/project/{parent_id}/versions",
    http_method="GET", pagination_style="none", records_json_path="", natural_key_field="id",
)
if project_keys:
    version_records = fmt.extract_per_parent(extractor, versions_template, parent_ids=project_keys, parent_field_name="_project_key")
    count = fmt.land_records(spark, version_records, source_name=SOURCE_NAME, entity=versions_template, bronze_schema=SCHEMA)
    print(f"[versions] landed {count} records across {len(project_keys)} projects")
else:
    print("[versions] skipping -- jira.projects doesn't exist (Projects extraction failed or hasn't run yet)")

# CELL ********************
# --- Per-project entities: Components (same pattern as Versions) ---
components_template = fmt.EntityConfig(
    entity_name="components", endpoint_path="/rest/api/3/project/{parent_id}/components",
    http_method="GET", pagination_style="none", records_json_path="", natural_key_field="id",
)
if project_keys:
    component_records = fmt.extract_per_parent(extractor, components_template, parent_ids=project_keys, parent_field_name="_project_key")
    count = fmt.land_records(spark, component_records, source_name=SOURCE_NAME, entity=components_template, bronze_schema=SCHEMA)
    print(f"[components] landed {count} records across {len(project_keys)} projects")
else:
    print("[components] skipping -- jira.projects doesn't exist (Projects extraction failed or hasn't run yet)")

# CELL ********************
# --- Per-board entities: Sprints (Scrum boards only -- Kanban boards are skipped automatically) ---
if not spark.catalog.tableExists(f"{SCHEMA}.boards"):
    print("[sprints] skipping -- jira.boards doesn't exist (Boards extraction failed or was never run, "
          "likely a permissions issue: this Jira instance/account may not have Jira Software/Agile access)")
else:
    board_ids = [r["primary_key"] for r in spark.table(f"{SCHEMA}.boards").select("primary_key").distinct().collect()]

    sprints_template = fmt.EntityConfig(
        entity_name="sprints", endpoint_path="/rest/agile/1.0/board/{parent_id}/sprint",
        http_method="GET", pagination_style="page_number", page_size=50,
        records_json_path="values", natural_key_field="id",
    )
    sprint_records = fmt.extract_per_parent(extractor, sprints_template, parent_ids=board_ids, parent_field_name="_board_id")
    count = fmt.land_records(spark, sprint_records, source_name=SOURCE_NAME, entity=sprints_template, bronze_schema=SCHEMA)
    print(f"[sprints] landed {count} records across {len(board_ids)} boards")

# CELL ********************
if failed_entities:
    print(f"S2B - Jira complete, WITH FAILURES in: {failed_entities} -- see messages above for why.")
else:
    print("S2B - Jira complete, all entities succeeded.")
