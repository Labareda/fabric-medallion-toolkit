"""
Configuration objects for the whole toolkit. Everything is data, not code —
onboarding a new source is (in the common case) writing one SourceConfig +
EntityConfig + a SilverEntityConfig + some GoldTableConfigs, no new Python.

Keep credentials OUT of these objects at rest — build AuthConfig from a Key
Vault secret inside the notebook:

    from notebookutils import mssparkutils
    token = mssparkutils.credentials.getSecret("<akv-name>", "<secret-name>")
    auth = AuthConfig(kind="basic", username="...", secret=token)
"""

from dataclasses import dataclass, field
from typing import Optional, Dict, Any, List


@dataclass
class AuthConfig:
    kind: str  # "basic" | "bearer" | "api_key" | "none"
    username: Optional[str] = None
    secret: Optional[str] = None
    api_key_header: Optional[str] = None

    def as_requests_auth(self):
        if self.kind == "basic":
            return (self.username, self.secret)
        return None

    def as_headers(self) -> Dict[str, str]:
        if self.kind == "bearer":
            return {"Authorization": f"Bearer {self.secret}"}
        if self.kind == "api_key" and self.api_key_header:
            return {self.api_key_header: self.secret}
        return {}


@dataclass
class LakehouseConfig:
    bronze_schema: str = "bronze"   # Lakehouse schema/prefix for bronze tables
    silver_schema: str = "silver"
    gold_schema: str = "gold"


@dataclass
class EntityConfig:
    """
    One config per entity (endpoint) within a source. Written so most REST
    APIs need ONLY this config, no new extractor subclass — see
    GenericRestExtractor for how each field is used.
    """
    entity_name: str                          # short slug, e.g. "issues", "customers"
    endpoint_path: str                        # e.g. "/rest/api/3/search"
    http_method: str = "GET"                  # "GET" | "POST"
    pagination_style: str = "offset_limit"    # "offset_limit" | "page_number" | "cursor" | "none"
    page_size: int = 100
    cursor_param_name: str = "cursor"          # cursor-style only: request field name for the token, e.g. Jira's "nextPageToken"
    cursor_page_size_param_name: str = "limit" # cursor-style only: request field name for page size, e.g. Jira's "maxResults"
    records_json_path: str = ""               # dot path to the list of records in the response; "" = root is the list
    total_json_path: Optional[str] = None     # dot path to a total-count field, if the API returns one
    next_cursor_json_path: Optional[str] = None  # dot path to a next-page cursor/token, if cursor-paginated
    natural_key_field: str = "id"             # dot path WITHIN each record for its natural/business key
    extra_params: Dict[str, Any] = field(default_factory=dict)   # extra query params (GET) merged in every call
    extra_body: Dict[str, Any] = field(default_factory=dict)     # extra JSON body fields (POST) merged in every call

    # --- High-watermark incremental extraction (optional; leave defaults for a full-load entity) ---
    incremental_column: Optional[str] = None
        # dot path WITHIN each record to the source's own "last modified" field,
        # e.g. "fields.updated" for Jira. Used to compute the NEXT watermark
        # after a successful run (max of this field across the batch).
    watermark_params_template: Dict[str, str] = field(default_factory=dict)
        # For GET entities: extra_params entries containing a "{watermark}"
        # placeholder, merged in (and overriding extra_params) only when a
        # watermark value is supplied, e.g. {"modified_since": "{watermark}"}
    watermark_body_template: Dict[str, str] = field(default_factory=dict)
        # Same idea for POST entities, e.g.
        # {"jql": "updated >= '{watermark}' ORDER BY updated"}
    initial_watermark_value: str = "1900-01-01T00:00:00"
        # Used on the very first run for this entity, before any watermark
        # has been recorded.


@dataclass
class SourceConfig:
    source_name: str                # short slug, e.g. "jira", "navision", "salesforce"
    base_url: str
    auth: AuthConfig = None
    entities: List[EntityConfig] = field(default_factory=list)
    request_timeout_seconds: int = 30
    max_retries: int = 5


@dataclass
class ColumnMapping:
    """One Silver column: where it comes from in raw_data, and its target type."""
    target_column: str
    json_path: str                  # dot path into the parsed raw_data JSON, e.g. "fields.duedate"
    data_type: str = "string"       # "string" | "int" | "long" | "double" | "boolean" | "date" | "timestamp"
    date_format: Optional[str] = None  # e.g. "yyyy-MM-dd" or "yyyy-MM-dd'T'HH:mm:ss.SSSZ"; None = let Spark infer


@dataclass
class SilverEntityConfig:
    """Drives the generic Silver standardize step for one Bronze entity."""
    source_name: str
    entity_name: str
    natural_key_columns: List[str]         # standardized (target) column name(s) forming the business key
    column_mappings: List[ColumnMapping]
    dedup_order_column: str = "extracted_at"  # which standardized/meta column decides "latest" on dedup


@dataclass
class TableSchema:
    """
    Declares one Gold table: its name, type, merge fields, and what to call
    its key column, in one place. Used directly in notebooks -- build the
    DataFrame yourself (however you like), then call merge(spark, df, schema)
    as the last step.

    key_column is yours to name meaningfully per table (e.g. "Project_key"
    for dim_project, "Sales_key" for fact_sales) -- defining merge_fields
    (and key_column) in the schema is what triggers the toolkit to generate
    and merge that key; you never call the key-generation machinery directly.
    """
    table_name: str                        # schema-qualified, e.g. "gold.dim_project"
    table_type: str                        # "dim" | "scd2" | "fact"
    merge_fields: List[str]                # business/natural key column(s); also the key's hash input
    key_column: str = "key"                # what to call the generated key column, e.g. "Project_key"
    tracked_columns: Optional[List[str]] = None  # scd2 only; None = track every non-merge-field column
    include_unknown_member: bool = False   # dim/scd2 only; adds a placeholder row so fact lookups never go NULL
    unknown_value: str = "Unknown"         # the sentinel merge_fields get on that placeholder row


@dataclass
class GoldTableConfig:
    """
    Drives one Gold table build. `select_sql` is plain Spark SQL — reference
    Silver tables directly (e.g. "SELECT ... FROM jira.issues"), join across
    sources, whatever the model needs. The "schema" of a Gold table is
    whatever select_sql returns, defined once, not duplicated as separate DDL.

    This is the JSON-config-driven equivalent of TableSchema, for cases
    where you want a whole model defined in config rather than one notebook
    cell per table (see config_loader.load_gold_config). Both ultimately
    produce the same kind of table; use whichever fits how you're working.

    table_type: "dim" | "scd2" | "fact"
        dim  — stable key, current values only, overwritten in place.
        scd2 — full version history; a changed record gets a NEW key rather
               than being overwritten, so old fact rows keep pointing at the
               attribute values that were true when they happened.
        fact — grain-level data; MERGEs on merge_fields like a dim does, but
               keeps every column select_sql returns rather than treating
               anything as a "changing attribute".
    """
    table_name: str
    select_sql: str
    table_type: str = "fact"               # "dim" | "scd2" | "fact"
    merge_fields: List[str] = field(default_factory=list)
    key_column: str = "key"                # what to call the generated key column, e.g. "Project_key"
    tracked_columns: Optional[List[str]] = None  # scd2 only; None = track every non-merge-field column


@dataclass
class DateDimensionConfig:
    """A calendar dim is synthetic, not derived from Silver — its own tiny config."""
    table_name: str = "dim_date"
    start_date: str = "2020-01-01"
    end_date: str = "2035-12-31"
    fiscal_year_start_month: int = 1  # 1 = fiscal year == calendar year; e.g. 4 = FY starts in April
