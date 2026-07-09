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
from pyspark.sql.types import StringType
import fabric_medallion_toolkit as fmt

adf_to_text_udf = F.udf(fmt.extract_adf_text, StringType())

# CELL ********************
BRONZE_SCHEMA = "Bronze.jira"
SILVER_SCHEMA = "Silver.jira"

# --- Every entity here, and the Bronze-side natural key column(s) it uses.
# These match S2B - Jira's config exactly -- if you add an entity to
# jira.json's "entities" list, add its natural key here too. (Nested-object
# keys, e.g. workflows' "id.entityId", flatten with underscores: "id_entityId".)
ENTITY_KEYS = {
    # Alphabetical -- also, coincidentally but importantly, keeps "fields"
    # ahead of "issues" (needed since issues' friendly-rename post_process
    # depends on "fields" already being standardized). If a new entity is
    # ever added whose name alphabetically precedes something it actually
    # depends on, that dependency needs handling explicitly rather than
    # relying on alphabetical luck -- worth remembering before assuming
    # this ordering is automatically safe forever.
    "audit_logs": ["id"],
    "boards": ["id"],
    "components": ["id"],
    "dashboards": ["id"],
    "fields": ["id"],
    "filters": ["id"],
    "groups": ["groupId"],
    "issue_link_types": ["id"],
    "issues": ["key"],
    "issuetypes": ["id"],
    "priorities": ["id"],
    "project_roles": ["id"],
    "projects": ["key"],
    "resolutions": ["id"],
    "screens": ["id"],
    "sprints": ["id"],
    "statuses": ["id"],
    "users": ["accountId"],
    "versions": ["id"],
    "workflows": ["id_entityId"],
}

# --- Per-entity columns to drop as redundant, since they're already
# captured in their own dedicated child tables built further down (see
# the explode_nested_array section) -- keeping them here too would just be
# duplicated data in two shapes. Entities not listed here have nothing to
# exclude.
ENTITY_EXCLUDE_COLUMNS = {
    "issues": [
        "changelog_histories",        # -> histories / history_items
        "fields_issuelinks",          # -> issue_links
        "fields_components",          # -> issue_components
        "fields_fixVersions",         # -> issue_fix_version
        "fields_versions",            # -> issue_affects_version
        "fields_attachment",          # -> attachments
        "fields_labels",              # -> issue_labels
        "fields_comment_comments",    # -> comments
        "fields_worklog_worklogs",    # -> worklogs
        "fields_subtasks",            # -> issue_subtasks
        "fields_customfield_10020",   # Sprint -- a custom field in this instance, not built-in. -> issue_sprints. Excludes MUST use the pre-friendly-rename name, since exclusion runs before renaming.
        # Pagination-envelope metadata for the above (maxResults/startAt/
        # total) -- these describe the API's paging, not actual data, so
        # they're noise same as "expand" (see run_auto_silver_standardize).
        "changelog_maxResults", "changelog_startAt", "changelog_total",
        "fields_comment_maxResults", "fields_comment_startAt", "fields_comment_total",
        "fields_worklog_maxResults", "fields_worklog_startAt", "fields_worklog_total",
        # Jira embeds 4 avatar sizes for every nested user/project object --
        # keep just 48x48 (the highest-res "standard" one), drop the rest,
        # wherever they appear (assignee/reporter/creator/project/parent...).
        "*avatarUrls_16x16*", "*avatarUrls_24x24*", "*avatarUrls_32x32*",
    ],
    "audit_logs": [
        "changedValues",       # -> audit_log_changed_values
        "associatedItems",     # -> audit_log_associated_items
    ],
    "dashboards": [
        "editPermissions",     # -> dashboard_edit_permissions
        "sharePermissions",    # -> dashboard_share_permissions
    ],
    "project_roles": [
        "actors",              # -> project_role_actors
    ],
    "projects": [
        "issueTypes",           # -> project_issue_types
        # Appears twice here -- once for the project's own icon, once for
        # its lead's user avatar -- wildcard catches both regardless of prefix.
        "*avatarUrls_16x16*", "*avatarUrls_24x24*", "*avatarUrls_32x32*",
    ],
    "users": [
        "*avatarUrls_16x16*", "*avatarUrls_24x24*", "*avatarUrls_32x32*",
    ],
}

# CELL ********************
failed_entities = []

for entity_name, natural_keys in ENTITY_KEYS.items():
    bronze_table = f"{BRONZE_SCHEMA}.{entity_name}"
    if not spark.catalog.tableExists(bronze_table):
        print(f"[{entity_name}] skipping -- {bronze_table} doesn't exist yet "
              f"(0 records landed in S2B, or that entity's extraction failed there)")
        failed_entities.append(entity_name)
        continue
    try:
        # For "issues" specifically: friendly custom-field renaming (using
        # the "fields" table's own id->name mapping -- data-driven, no
        # manual dictionary to maintain), ADF rich-text cleanup (every
        # "X_content"/"X_type"/"X_version" triple -> plain text "X"), and
        # a manual type correction for "review" (a date field that didn't
        # infer as one). Requires "fields" itself already standardized --
        # if it's not yet in ENTITY_KEYS' processing order before "issues",
        # friendly renaming falls back to skipped rather than failing.
        post_process = None
        if entity_name == "issues":
            def _issues_post_process(df):
                if spark.catalog.tableExists(f"{SILVER_SCHEMA}.fields"):
                    field_id_to_name = fmt.build_field_id_to_name(spark, f"{SILVER_SCHEMA}.fields", id_column="id", name_column="name")
                    df = fmt.rename_customfield_columns(df, field_id_to_name)
                df = fmt.clean_adf_columns(df, adf_to_text_udf)
                if "fields_review" in df.columns:
                    df = df.withColumn("fields_review", F.col("fields_review").cast("date"))
                if "fields_duedate" in df.columns:
                    df = df.withColumn("fields_duedate", F.col("fields_duedate").cast("date"))
                return df
            post_process = _issues_post_process

        fmt.run_auto_silver_standardize(
            spark, entity_name=entity_name, natural_key_columns=natural_keys,
            bronze_schema=BRONZE_SCHEMA, silver_schema=SILVER_SCHEMA,
            exclude_columns=ENTITY_EXCLUDE_COLUMNS.get(entity_name),
            post_process=post_process,
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
        .withColumn("label_name", F.regexp_replace(F.col("label_name"), '^#', ""))  # some labels come back "#something" -- strip the leading #
        .select("label_name", "extracted_at")
    )
    deduped = fmt.dedup_latest(labels_df, key_cols=["label_name"], order_by_col="extracted_at").drop("extracted_at")
    deduped = deduped.select(*sorted(deduped.columns))
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
# IssueFixVersion, IssueAffectsVersions, Histories, HistoryItems, IssueLabels,
# Comments. These stay on explode_nested_array's explicit field list (not
# auto_standardize) -- they're specific derived child tables you asked for
# by name, not "every field," so declaring exactly which fields make up
# each one is the right fit here, same as before.
#
# Every one of these carries issue_id and issue_created alongside
# issue_key, matching the client's schema convention of always having all
# three on a child table. Pulled once here (get_json_object straight off
# raw_data, since these are single values, not arrays) and carried through
# every explode call below rather than re-extracted each time.
if not spark.catalog.tableExists(f"{BRONZE_SCHEMA}.issues"):
    print("[issue_links/issue_components/issue_fix_version/issue_affects_version/histories/history_items/issue_labels/comments] "
          "skipping -- no issues in Bronze yet")
else:
    bronze_issues_df = (
        spark.table(f"{BRONZE_SCHEMA}.issues")
        .withColumn("issue_id", F.get_json_object(F.col("raw_data"), "$.id"))
        .withColumn("issue_created", F.get_json_object(F.col("raw_data"), "$.fields.created")
                    .cast("timestamp"))
    )

    issue_links_df = fmt.explode_nested_array(
        bronze_issues_df, array_json_path="fields.issuelinks",
        parent_key_column="issue_key", parent_key_json_path="key",
        carry_through_columns=["issue_id", "issue_created"],
        column_mappings=[
            fmt.ColumnMapping("link_id", "id", "string"),
            fmt.ColumnMapping("link_type", "type.name", "string"),
            fmt.ColumnMapping("inward_label", "type.inward", "string"),
            fmt.ColumnMapping("outward_label", "type.outward", "string"),
            fmt.ColumnMapping("outward_issue_key", "outwardIssue.key", "string"),
            fmt.ColumnMapping("inward_issue_key", "inwardIssue.key", "string"),
        ],
    )
    issue_links_df = issue_links_df.select(*sorted(issue_links_df.columns))
    issue_links_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{SILVER_SCHEMA}.issue_links")
    print(f"[issue_links] {issue_links_df.count()} rows -> {SILVER_SCHEMA}.issue_links")

    issue_components_df = fmt.explode_nested_array(
        bronze_issues_df, array_json_path="fields.components",
        parent_key_column="issue_key", parent_key_json_path="key",
        carry_through_columns=["issue_id", "issue_created"],
        column_mappings=[
            fmt.ColumnMapping("component_id", "id", "string"),
            fmt.ColumnMapping("component_name", "name", "string"),
        ],
    )
    issue_components_df = issue_components_df.select(*sorted(issue_components_df.columns))
    issue_components_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{SILVER_SCHEMA}.issue_components")
    print(f"[issue_components] {issue_components_df.count()} rows -> {SILVER_SCHEMA}.issue_components")

    issue_fix_version_df = fmt.explode_nested_array(
        bronze_issues_df, array_json_path="fields.fixVersions",
        parent_key_column="issue_key", parent_key_json_path="key",
        carry_through_columns=["issue_id", "issue_created"],
        column_mappings=[
            fmt.ColumnMapping("version_id", "id", "string"),
            fmt.ColumnMapping("version_name", "name", "string"),
        ],
    )
    issue_fix_version_df = issue_fix_version_df.select(*sorted(issue_fix_version_df.columns))
    issue_fix_version_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{SILVER_SCHEMA}.issue_fix_version")
    print(f"[issue_fix_version] {issue_fix_version_df.count()} rows -> {SILVER_SCHEMA}.issue_fix_version")

    issue_affects_version_df = fmt.explode_nested_array(
        bronze_issues_df, array_json_path="fields.versions",
        parent_key_column="issue_key", parent_key_json_path="key",
        carry_through_columns=["issue_id", "issue_created"],
        column_mappings=[
            fmt.ColumnMapping("version_id", "id", "string"),
            fmt.ColumnMapping("version_name", "name", "string"),
        ],
    )
    issue_affects_version_df = issue_affects_version_df.select(*sorted(issue_affects_version_df.columns))
    issue_affects_version_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{SILVER_SCHEMA}.issue_affects_version")
    print(f"[issue_affects_version] {issue_affects_version_df.count()} rows -> {SILVER_SCHEMA}.issue_affects_version")

    # Labels per issue -- a bridge table (issue_key/issue_id -> label name),
    # distinct from the global "labels" reference table built earlier
    # (every label name used ANYWHERE, with no issue linkage at all).
    issue_labels_df = fmt.explode_nested_array(
        bronze_issues_df, array_json_path="fields.labels",
        parent_key_column="issue_key", parent_key_json_path="key",
        carry_through_columns=["issue_id", "issue_created"],
        column_mappings=[
            fmt.ColumnMapping("label_name", "", "string"),  # each array element IS the label string itself
        ],
    )
    issue_labels_df = issue_labels_df.withColumn("label_name", F.regexp_replace(F.col("label_name"), '^#', ""))
    issue_labels_df = issue_labels_df.select(*sorted(issue_labels_df.columns))
    issue_labels_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{SILVER_SCHEMA}.issue_labels")
    print(f"[issue_labels] {issue_labels_df.count()} rows -> {SILVER_SCHEMA}.issue_labels")

    # Comments -- embedded directly in the issue payload (fields.comment.comments)
    # up to Jira's default per-issue limit, so this needs NO extra API calls,
    # unlike what I'd flagged earlier as needing a per-issue fetch. If an issue
    # has more comments than that embedded limit, only the most recent ones up
    # to the limit land here -- worth knowing if completeness matters for a
    # heavily-commented issue; a dedicated per-issue Comments fetch would be
    # the fix for that specific gap, separate from this.
    comments_df = fmt.explode_nested_array(
        bronze_issues_df, array_json_path="fields.comment.comments",
        parent_key_column="issue_key", parent_key_json_path="key",
        carry_through_columns=["issue_id", "issue_created"],
        column_mappings=[
            fmt.ColumnMapping("comment_id", "id", "string"),
            fmt.ColumnMapping("author_account_id", "author.accountId", "string"),
            fmt.ColumnMapping("author_name", "author.displayName", "string"),
            fmt.ColumnMapping("update_author_account_id", "updateAuthor.accountId", "string"),
            fmt.ColumnMapping("update_author_name", "updateAuthor.displayName", "string"),
            fmt.ColumnMapping("comment_body", "body", "string"),  # Atlassian Document Format -- kept as JSON, not flattened
            fmt.ColumnMapping("created", "created", "timestamp", date_format="yyyy-MM-dd'T'HH:mm:ss.SSSZ"),
            fmt.ColumnMapping("updated", "updated", "timestamp", date_format="yyyy-MM-dd'T'HH:mm:ss.SSSZ"),
            fmt.ColumnMapping("is_public", "jsdPublic", "boolean"),
        ],
    )
    comments_df = comments_df.withColumn("comment_body", adf_to_text_udf(F.col("comment_body")))
    comments_df = comments_df.select(*sorted(comments_df.columns))
    comments_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{SILVER_SCHEMA}.comments")
    print(f"[comments] {comments_df.count()} rows -> {SILVER_SCHEMA}.comments")

    # Worklogs -- same free-embedded-data situation as comments:
    # fields.worklog.worklogs comes back with the issue payload itself, no
    # extra API calls needed.
    worklogs_df = fmt.explode_nested_array(
        bronze_issues_df, array_json_path="fields.worklog.worklogs",
        parent_key_column="issue_key", parent_key_json_path="key",
        carry_through_columns=["issue_id", "issue_created"],
        column_mappings=[
            fmt.ColumnMapping("worklog_id", "id", "string"),
            fmt.ColumnMapping("author_account_id", "author.accountId", "string"),
            fmt.ColumnMapping("author_name", "author.displayName", "string"),
            fmt.ColumnMapping("update_author_account_id", "updateAuthor.accountId", "string"),
            fmt.ColumnMapping("update_author_name", "updateAuthor.displayName", "string"),
            fmt.ColumnMapping("comment_body", "comment", "string"),  # ADF -- converted to text below
            fmt.ColumnMapping("started", "started", "timestamp", date_format="yyyy-MM-dd'T'HH:mm:ss.SSSZ"),
            fmt.ColumnMapping("time_spent", "timeSpent", "string"),
            fmt.ColumnMapping("time_spent_seconds", "timeSpentSeconds", "long"),
            fmt.ColumnMapping("created", "created", "timestamp", date_format="yyyy-MM-dd'T'HH:mm:ss.SSSZ"),
            fmt.ColumnMapping("updated", "updated", "timestamp", date_format="yyyy-MM-dd'T'HH:mm:ss.SSSZ"),
        ],
    )
    worklogs_df = worklogs_df.withColumn("comment_body", adf_to_text_udf(F.col("comment_body")))
    worklogs_df = worklogs_df.select(*sorted(worklogs_df.columns))
    worklogs_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{SILVER_SCHEMA}.worklogs")
    print(f"[worklogs] {worklogs_df.count()} rows -> {SILVER_SCHEMA}.worklogs")

    histories_df = fmt.explode_nested_array(
        bronze_issues_df, array_json_path="changelog.histories",
        parent_key_column="issue_key", parent_key_json_path="key",
        carry_through_columns=["issue_id", "issue_created"],
        column_mappings=[
            fmt.ColumnMapping("history_id", "id", "string"),
            fmt.ColumnMapping("author_account_id", "author.accountId", "string"),
            fmt.ColumnMapping("author_name", "author.displayName", "string"),
            fmt.ColumnMapping("created", "created", "timestamp", date_format="yyyy-MM-dd'T'HH:mm:ss.SSSZ"),
            fmt.ColumnMapping("items", "items", "string"),
        ],
    )

    history_items_df = fmt.explode_nested_array(
        histories_df, array_json_path="",  # "items" IS the array already -- nothing further to drill into
        column_mappings=[
            fmt.ColumnMapping("field_id", "fieldId", "string"),
            fmt.ColumnMapping("field_name", "field", "string"),
            fmt.ColumnMapping("old_value", "from", "string"),
            fmt.ColumnMapping("old_value_formatted", "fromString", "string"),
            fmt.ColumnMapping("new_value", "to", "string"),
            fmt.ColumnMapping("new_value_formatted", "toString", "string"),
        ],
        source_column="items",
        carry_through_columns=["issue_key", "issue_id", "issue_created", "history_id"],
    )
    history_items_df = history_items_df.select(*sorted(history_items_df.columns))
    history_items_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{SILVER_SCHEMA}.history_items")
    print(f"[history_items] {history_items_df.count()} rows -> {SILVER_SCHEMA}.history_items")

    histories_df = histories_df.drop("items")
    histories_df = histories_df.select(*sorted(histories_df.columns))
    histories_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{SILVER_SCHEMA}.histories")
    print(f"[histories] {histories_df.count()} rows -> {SILVER_SCHEMA}.histories")

# CELL ********************
# --- More nested-inside-Issues/AuditLogs tables: still zero new API calls,
# same explode_nested_array pattern as issue_links/histories above.

# Attachments -- nested in each issue's fields.attachment
attachments_df = fmt.explode_nested_array(
    bronze_issues_df, array_json_path="fields.attachment",
    parent_key_column="issue_key", parent_key_json_path="key",
    carry_through_columns=["issue_id", "issue_created"],
    column_mappings=[
        fmt.ColumnMapping("attachment_id", "id", "string"),
        fmt.ColumnMapping("filename", "filename", "string"),
        fmt.ColumnMapping("file_size", "size", "long"),
        fmt.ColumnMapping("mime_type", "mimeType", "string"),
        fmt.ColumnMapping("created", "created", "timestamp", date_format="yyyy-MM-dd'T'HH:mm:ss.SSSZ"),
        fmt.ColumnMapping("author_account_id", "author.accountId", "string"),
        fmt.ColumnMapping("author_name", "author.displayName", "string"),
        fmt.ColumnMapping("content_url", "content", "string"),
        fmt.ColumnMapping("thumbnail_url", "thumbnail", "string"),
    ],
)
attachments_df = attachments_df.select(*sorted(attachments_df.columns))
attachments_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{SILVER_SCHEMA}.attachments")
print(f"[attachments] {attachments_df.count()} rows -> {SILVER_SCHEMA}.attachments")


# CELL ********************
# AuditLogChangedValues / AuditLogAssociatedItems -- nested inside each audit
# record. Bronze's audit_logs table already has the full record in raw_data.
if not spark.catalog.tableExists(f"{BRONZE_SCHEMA}.audit_logs"):
    print("[audit_log_changed_values/audit_log_associated_items] skipping -- no audit_logs in Bronze yet")
else:
    bronze_audit_df = spark.table(f"{BRONZE_SCHEMA}.audit_logs")

    audit_changed_values_df = fmt.explode_nested_array(
        bronze_audit_df, array_json_path="changedValues",
        parent_key_column="audit_id", parent_key_json_path="id",
        column_mappings=[
            fmt.ColumnMapping("field_name", "fieldName", "string"),
            fmt.ColumnMapping("old_value", "changedFrom", "string"),
            fmt.ColumnMapping("new_value", "changedTo", "string"),
        ],
    )
    audit_changed_values_df = audit_changed_values_df.select(*sorted(audit_changed_values_df.columns))
    audit_changed_values_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{SILVER_SCHEMA}.audit_log_changed_values")
    print(f"[audit_log_changed_values] {audit_changed_values_df.count()} rows -> {SILVER_SCHEMA}.audit_log_changed_values")

    audit_associated_items_df = fmt.explode_nested_array(
        bronze_audit_df, array_json_path="associatedItems",
        parent_key_column="audit_id", parent_key_json_path="id",
        column_mappings=[
            fmt.ColumnMapping("item_id", "id", "string"),
            fmt.ColumnMapping("item_name", "name", "string"),
            fmt.ColumnMapping("item_type_name", "typeName", "string"),
            fmt.ColumnMapping("item_parent_id", "parentId", "string"),
            fmt.ColumnMapping("item_parent_name", "parentName", "string"),
        ],
    )
    audit_associated_items_df = audit_associated_items_df.select(*sorted(audit_associated_items_df.columns))
    audit_associated_items_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{SILVER_SCHEMA}.audit_log_associated_items")
    print(f"[audit_log_associated_items] {audit_associated_items_df.count()} rows -> {SILVER_SCHEMA}.audit_log_associated_items")

# CELL ********************
# --- Dashboard permissions (both editPermissions and sharePermissions).
# These are polymorphic -- the "type" field (loggedin/user/project/group/
# global) determines which nested object is actually present, so most
# columns below will be NULL depending on a given row's type. That's
# expected, not a bug: get_by_path returns None gracefully for whichever
# nested object doesn't apply to that specific permission's type.
if not spark.catalog.tableExists(f"{BRONZE_SCHEMA}.dashboards"):
    print("[dashboard_edit_permissions/dashboard_share_permissions] skipping -- no dashboards in Bronze yet")
else:
    bronze_dashboards_df = spark.table(f"{BRONZE_SCHEMA}.dashboards")

    _permission_column_mappings = [
        fmt.ColumnMapping("permission_id", "id", "string"),
        fmt.ColumnMapping("permission_type", "type", "string"),
        fmt.ColumnMapping("project_id", "project.id", "string"),
        fmt.ColumnMapping("project_key", "project.key", "string"),
        fmt.ColumnMapping("project_name", "project.name", "string"),
        fmt.ColumnMapping("user_account_id", "user.accountId", "string"),
        fmt.ColumnMapping("user_display_name", "user.displayName", "string"),
        fmt.ColumnMapping("group_id", "group.groupId", "string"),
        fmt.ColumnMapping("group_name", "group.name", "string"),
    ]

    dashboard_edit_permissions_df = fmt.explode_nested_array(
        bronze_dashboards_df, array_json_path="editPermissions",
        parent_key_column="dashboard_id", parent_key_json_path="id",
        column_mappings=_permission_column_mappings,
    )
    dashboard_edit_permissions_df = dashboard_edit_permissions_df.select(*sorted(dashboard_edit_permissions_df.columns))
    dashboard_edit_permissions_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{SILVER_SCHEMA}.dashboard_edit_permissions")
    print(f"[dashboard_edit_permissions] {dashboard_edit_permissions_df.count()} rows -> {SILVER_SCHEMA}.dashboard_edit_permissions")

    dashboard_share_permissions_df = fmt.explode_nested_array(
        bronze_dashboards_df, array_json_path="sharePermissions",
        parent_key_column="dashboard_id", parent_key_json_path="id",
        column_mappings=_permission_column_mappings,
    )
    dashboard_share_permissions_df = dashboard_share_permissions_df.select(*sorted(dashboard_share_permissions_df.columns))
    dashboard_share_permissions_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{SILVER_SCHEMA}.dashboard_share_permissions")
    print(f"[dashboard_share_permissions] {dashboard_share_permissions_df.count()} rows -> {SILVER_SCHEMA}.dashboard_share_permissions")

# CELL ********************
# --- Project role membership (project_roles.actors)
if not spark.catalog.tableExists(f"{BRONZE_SCHEMA}.project_roles"):
    print("[project_role_actors] skipping -- no project_roles in Bronze yet")
else:
    bronze_project_roles_df = spark.table(f"{BRONZE_SCHEMA}.project_roles")

    project_role_actors_df = fmt.explode_nested_array(
        bronze_project_roles_df, array_json_path="actors",
        parent_key_column="project_role_id", parent_key_json_path="id",
        column_mappings=[
            fmt.ColumnMapping("actor_id", "id", "string"),
            fmt.ColumnMapping("display_name", "displayName", "string"),
            fmt.ColumnMapping("actor_type", "type", "string"),
            fmt.ColumnMapping("actor_user_account_id", "actorUser.accountId", "string"),
            fmt.ColumnMapping("actor_group_name", "actorGroup.name", "string"),
        ],
    )
    project_role_actors_df = project_role_actors_df.select(*sorted(project_role_actors_df.columns))
    project_role_actors_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{SILVER_SCHEMA}.project_role_actors")
    print(f"[project_role_actors] {project_role_actors_df.count()} rows -> {SILVER_SCHEMA}.project_role_actors")

# CELL ********************
# --- Project issue types (projects.issueTypes) and issue subtasks
# (issues.fields.subtasks). Different parents, kept in the same cell since
# both need their respective Bronze tables checked first.
if not spark.catalog.tableExists(f"{BRONZE_SCHEMA}.projects"):
    print("[project_issue_types] skipping -- no projects in Bronze yet")
else:
    bronze_projects_df = spark.table(f"{BRONZE_SCHEMA}.projects")

    project_issue_types_df = fmt.explode_nested_array(
        bronze_projects_df, array_json_path="issueTypes",
        parent_key_column="project_key", parent_key_json_path="key",
        column_mappings=[
            fmt.ColumnMapping("issue_type_id", "id", "string"),
            fmt.ColumnMapping("issue_type_name", "name", "string"),
            fmt.ColumnMapping("description", "description", "string"),
            fmt.ColumnMapping("hierarchy_level", "hierarchyLevel", "int"),
            fmt.ColumnMapping("is_subtask", "subtask", "boolean"),
            fmt.ColumnMapping("icon_url", "iconUrl", "string"),
            fmt.ColumnMapping("avatar_id", "avatarId", "long"),
        ],
    )
    project_issue_types_df = project_issue_types_df.select(*sorted(project_issue_types_df.columns))
    project_issue_types_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{SILVER_SCHEMA}.project_issue_types")
    print(f"[project_issue_types] {project_issue_types_df.count()} rows -> {SILVER_SCHEMA}.project_issue_types")

if not spark.catalog.tableExists(f"{BRONZE_SCHEMA}.issues"):
    print("[issue_subtasks] skipping -- no issues in Bronze yet")
else:
    # Reuses bronze_issues_df (with issue_id/issue_created already added)
    # from the earlier issues-children cell above.
    issue_subtasks_df = fmt.explode_nested_array(
        bronze_issues_df, array_json_path="fields.subtasks",
        parent_key_column="issue_key", parent_key_json_path="key",
        carry_through_columns=["issue_id", "issue_created"],
        column_mappings=[
            fmt.ColumnMapping("subtask_issue_id", "id", "string"),
            fmt.ColumnMapping("subtask_issue_key", "key", "string"),
            fmt.ColumnMapping("summary", "fields.summary", "string"),
            fmt.ColumnMapping("status_name", "fields.status.name", "string"),
            fmt.ColumnMapping("priority_name", "fields.priority.name", "string"),
            fmt.ColumnMapping("issue_type_name", "fields.issuetype.name", "string"),
        ],
    )
    issue_subtasks_df = issue_subtasks_df.select(*sorted(issue_subtasks_df.columns))
    issue_subtasks_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{SILVER_SCHEMA}.issue_subtasks")
    print(f"[issue_subtasks] {issue_subtasks_df.count()} rows -> {SILVER_SCHEMA}.issue_subtasks")

    # Sprints an issue has been in -- Sprint is a CUSTOM field in this
    # instance (customfield_10020), not Jira's built-in "fields.sprint" --
    # confirmed from actual Bronze data. An issue can move through several
    # sprints over its life, so this is a real bridge table, not a single
    # value on the issue.
    issue_sprints_df = fmt.explode_nested_array(
        bronze_issues_df, array_json_path="fields.customfield_10020",
        parent_key_column="issue_key", parent_key_json_path="key",
        carry_through_columns=["issue_id", "issue_created"],
        column_mappings=[
            fmt.ColumnMapping("sprint_id", "id", "long"),
            fmt.ColumnMapping("sprint_name", "name", "string"),
            fmt.ColumnMapping("board_id", "boardId", "long"),
            fmt.ColumnMapping("state", "state", "string"),
            fmt.ColumnMapping("goal", "goal", "string"),
            fmt.ColumnMapping("start_date", "startDate", "timestamp", date_format="yyyy-MM-dd'T'HH:mm:ss.SSSX"),
            fmt.ColumnMapping("end_date", "endDate", "timestamp", date_format="yyyy-MM-dd'T'HH:mm:ss.SSSX"),
            fmt.ColumnMapping("complete_date", "completeDate", "timestamp", date_format="yyyy-MM-dd'T'HH:mm:ss.SSSX"),
        ],
    )
    issue_sprints_df = issue_sprints_df.select(*sorted(issue_sprints_df.columns))
    issue_sprints_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{SILVER_SCHEMA}.issue_sprints")
    print(f"[issue_sprints] {issue_sprints_df.count()} rows -> {SILVER_SCHEMA}.issue_sprints")

# CELL ********************
if failed_entities:
    print(f"B2S - Jira complete, WITH SKIPS/FAILURES in: {failed_entities} -- see messages above for why.")
else:
    print("B2S - Jira complete, all entities standardized.")
