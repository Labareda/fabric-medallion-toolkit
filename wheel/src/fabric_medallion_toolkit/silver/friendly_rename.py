"""
Renames "customfield_10070"-style column name fragments to friendly names,
driven entirely by data (a field_id -> friendly_name mapping you already
have -- e.g. from the "fields" Silver table, itself pulled straight from
Jira's own /rest/api/3/field metadata) rather than a hand-maintained
dictionary that needs updating every time a custom field is added.

Generic Fabric plumbing in spirit -- any source with a similar "numeric ID
embedded in an auto-generated column name, with a separate lookup table
giving the real name" shape could reuse this, though the "customfield_"
prefix convention itself is Jira-specific.
"""

import re
from typing import Dict, Optional

from pyspark.sql import DataFrame


def _sanitize_friendly_name(name: str) -> str:
    """Same cleanup rule as auto_standardize's column aliasing, so renamed
    columns look consistent with everything else in the table."""
    cleaned = re.sub(r"[^0-9a-zA-Z_]", "_", name)
    return re.sub(r"_+", "_", cleaned).strip("_").lower()


def rename_customfield_columns(df: DataFrame, field_id_to_name: Dict[str, str],
                                pattern: str = r"customfield_\d+") -> DataFrame:
    """
    field_id_to_name: e.g. {"customfield_10070": "Reporting", ...} -- keys
        must match exactly what appears embedded in column names (usually
        "customfield_NNNNN", matching Jira's own field_id format).
    pattern: regex matching the ID FRAGMENT to replace within a column
        name -- default handles Jira's "customfield_NNNNN" convention.
        Only that matched fragment is replaced; any prefix (e.g.
        "fields_") or suffix (e.g. "_id", "_value", "_content") on the
        column name is left untouched.

    A customfield ID with no entry in field_id_to_name is left as-is --
    this is a safe, additive rename, never a required one.

    Collisions (two different field IDs whose friendly names sanitize to
    the same string) are disambiguated by appending the numeric ID, so no
    rename ever silently overwrites another column.
    """
    compiled = re.compile(pattern)

    # First pass: compute each column's PROPOSED new name, without yet
    # applying collision handling.
    proposed = {}  # old_col_name -> proposed_new_name
    for col_name in df.columns:
        match = compiled.search(col_name)
        if not match:
            continue
        field_id = match.group(0)
        if field_id not in field_id_to_name:
            continue
        friendly = _sanitize_friendly_name(field_id_to_name[field_id])
        proposed[col_name] = col_name[:match.start()] + friendly + col_name[match.end():]

    # Count how many source columns propose each target name -- a name
    # used by 2+ columns is a collision, and ALL of them (not just the
    # second one onward) need the ID suffix, so the result is symmetric
    # rather than one colliding column looking "normal" and the other
    # looking like an afterthought.
    from collections import Counter
    name_counts = Counter(proposed.values())

    renames = {}
    for col_name, new_name in proposed.items():
        if name_counts[new_name] > 1:
            match = compiled.search(col_name)
            digits = re.search(r"\d+", match.group(0)).group(0)
            new_name = col_name[:match.start()] + f"{_sanitize_friendly_name(field_id_to_name[match.group(0)])}_{digits}" + col_name[match.end():]
        renames[col_name] = new_name

    for old_name, new_name in renames.items():
        df = df.withColumnRenamed(old_name, new_name)
    return df


def build_field_id_to_name(spark, fields_table: str, id_column: str = "field_id",
                            name_column: str = "field_name") -> Dict[str, str]:
    """
    Convenience loader: reads the "fields" reference table (or equivalent)
    into the plain Python dict rename_customfield_columns expects.
    """
    rows = spark.table(fields_table).select(id_column, name_column).collect()
    return {row[id_column]: row[name_column] for row in rows if row[id_column] and row[name_column]}
