"""
Handles the "array nested inside a parent record" shape -- Jira's
issuelinks, fixVersions, components, changelog.histories, etc. -- turning
each into a proper one-row-per-element table, with NO additional API calls,
since the data's already sitting in the parent's Bronze raw_data.

Not Jira-specific: any source where a record's raw_data contains a nested
array you want as its own child/bridge table fits this same function.
"""

import json
from typing import List, Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType

from fabric_medallion_toolkit.config import ColumnMapping
from fabric_medallion_toolkit.utils.json_path import get_by_path
from fabric_medallion_toolkit.silver.standardize import _json_extract_udf_factory, _SPARK_CAST


def _array_extract_udf_factory(array_json_path: str):
    def _extract(raw_json):
        if not raw_json:
            return []
        try:
            obj = json.loads(raw_json)
        except (TypeError, ValueError):
            return []
        arr = get_by_path(obj, array_json_path, default=None)
        if not isinstance(arr, list):
            return []
        return [json.dumps(el, default=str) for el in arr]
    return _extract


def _parent_key_udf_factory(parent_key_json_path: str):
    def _extract(raw_json):
        if not raw_json:
            return None
        try:
            obj = json.loads(raw_json)
        except (TypeError, ValueError):
            return None
        val = get_by_path(obj, parent_key_json_path, default=None)
        return str(val) if val is not None else None
    return _extract


def explode_nested_array(bronze_df: DataFrame, array_json_path: str,
                          column_mappings: List[ColumnMapping],
                          parent_key_column: Optional[str] = None,
                          parent_key_json_path: Optional[str] = None,
                          source_column: str = "raw_data",
                          carry_through_columns: Optional[List[str]] = None) -> DataFrame:
    """
    bronze_df: raw Bronze read of the PARENT entity (e.g. jira.issues) --
        has raw_data as usual, UNLESS chaining a second explosion level (see
        source_column below), in which case pass the FIRST explosion's
        output DataFrame here instead.
    array_json_path: dot path to the array field within each parent row's
        JSON, e.g. "fields.issuelinks". Pass "" if source_column's value IS
        ALREADY the array itself (the chained/two-level case below).
    parent_key_column / parent_key_json_path: BOTH optional -- set them
        when there's a genuine new key to derive from source_column's JSON
        (the normal single-level case, e.g. pulling "issue_key" from each
        issue's own "key" field). Leave both None when there's nothing new
        to derive -- e.g. a chained second-level call where the array
        elements don't carry an identifier for their own parent (Jira's
        changelog items don't repeat their history entry's id) -- and rely
        on carry_through_columns instead to keep whatever key already
        exists on the row.
    column_mappings: applied to EACH array element (not the parent record) --
        same ColumnMapping shape as a normal Silver entity.
    source_column: which column holds the JSON to explode -- "raw_data" for
        a normal single-level explosion straight off Bronze. For a SECOND
        level (e.g. each history entry's own "items" array of field
        changes), call this function once to get history-level rows first,
        then call it AGAIN on that result with source_column="items" and
        array_json_path="" (items is already the array -- nothing further
        to drill into).
    carry_through_columns: extra columns already present on bronze_df to
        keep as-is in the output -- e.g. on a chained second-level call,
        pass ["issue_key", "history_id"] to keep both keys from the first
        level untouched, rather than trying to re-derive them.
    """
    array_udf = F.udf(_array_extract_udf_factory(array_json_path), ArrayType(StringType()))

    with_array = bronze_df.withColumn("_elements", array_udf(F.col(source_column)))
    if parent_key_column is not None:
        parent_key_udf = F.udf(_parent_key_udf_factory(parent_key_json_path), StringType())
        with_array = with_array.withColumn(parent_key_column, parent_key_udf(F.col(source_column)))
    exploded = (
        with_array
        .filter(F.size("_elements") > 0)
        .withColumn("_element", F.explode("_elements"))
    )

    df = exploded
    for mapping in column_mappings:
        extract_udf = F.udf(_json_extract_udf_factory(mapping.json_path), StringType())
        raw_col = f"_raw__{mapping.target_column}"
        df = df.withColumn(raw_col, extract_udf(F.col("_element")))

        if mapping.data_type == "date":
            typed = F.to_date(F.col(raw_col), mapping.date_format) if mapping.date_format \
                else F.to_date(F.col(raw_col))
        elif mapping.data_type == "timestamp":
            typed = F.to_timestamp(F.col(raw_col), mapping.date_format) if mapping.date_format \
                else F.to_timestamp(F.col(raw_col))
        elif mapping.data_type in _SPARK_CAST:
            typed = F.col(raw_col).cast(_SPARK_CAST[mapping.data_type])
        else:
            raise ValueError(
                f"Unknown data_type '{mapping.data_type}' for column '{mapping.target_column}'"
            )
        df = df.withColumn(mapping.target_column, typed).drop(raw_col)

    keep_cols = ([parent_key_column] if parent_key_column is not None else []) + \
                (carry_through_columns or []) + [m.target_column for m in column_mappings]
    return df.select(*keep_cols)
