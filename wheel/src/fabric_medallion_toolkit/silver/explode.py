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
                          parent_key_column: str, parent_key_json_path: str,
                          column_mappings: List[ColumnMapping]) -> DataFrame:
    """
    bronze_df: raw Bronze read of the PARENT entity (e.g. jira.issues) --
        has raw_data as usual.
    array_json_path: dot path to the array field within each parent record's
        raw_data, e.g. "fields.issuelinks".
    parent_key_column / parent_key_json_path: what to call, and where to
        find, the parent record's own natural key (e.g. "issue_key" / "key")
        -- kept on every exploded row so you can join back to the parent.
        Parent rows whose array is empty/missing simply produce no output
        rows (nothing to explode).
    column_mappings: applied to EACH array element (not the parent record) --
        same ColumnMapping shape as a normal Silver entity.
    """
    array_udf = F.udf(_array_extract_udf_factory(array_json_path), ArrayType(StringType()))
    parent_key_udf = F.udf(_parent_key_udf_factory(parent_key_json_path), StringType())

    with_array = bronze_df.withColumn("_elements", array_udf(F.col("raw_data")))
    with_parent_key = with_array.withColumn(parent_key_column, parent_key_udf(F.col("raw_data")))
    exploded = (
        with_parent_key
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

    keep_cols = [parent_key_column] + [m.target_column for m in column_mappings]
    return df.select(*keep_cols)
