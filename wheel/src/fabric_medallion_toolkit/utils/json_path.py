"""
Tiny dot-path JSON accessor used throughout Bronze/Silver so no layer needs
source-specific parsing code — every source's nested JSON is walked the
same way, driven by config.
"""

from typing import Any


def get_by_path(obj: Any, path: str, default: Any = None) -> Any:
    """
    Walks a dotted path, e.g. "fields.assignee.displayName" or "status.name",
    through a nested dict. Returns `default` if any hop is missing or not a dict.
    Empty path ("") returns obj itself (useful when the whole record is the value).
    """
    if path == "":
        return obj
    cur = obj
    for part in path.split("."):
        if not isinstance(cur, dict):
            return default
        cur = cur.get(part)
        if cur is None:
            return default
    return cur
