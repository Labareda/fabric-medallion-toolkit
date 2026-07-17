
"""
Extracts plain, readable text from Atlassian Document Format (ADF) --
Jira's rich-text JSON structure used for description, comment bodies,
environment, and several custom field types. Generic Fabric plumbing, not
Jira-specific in principle (any source using ADF could reuse this), but
it lives here since ADF itself is an Atlassian-specific format.
"""

import json
from typing import Optional


def _walk_adf_node(node) -> str:
    if isinstance(node, dict):
        node_type = node.get("type")
        if node_type == "text":
            return node.get("text", "")
        if node_type == "hardBreak":
            return "\n"
        if node_type == "mention":
            return f"@{node.get('attrs', {}).get('text', '')}"
        if node_type == "emoji":
            return node.get("attrs", {}).get("text", "")
        # paragraph, doc, bulletList, listItem, etc. -- just recurse into
        # content and join; good enough for "readable text", not a full
        # ADF renderer
        children = node.get("content", [])
        return "".join(_walk_adf_node(c) for c in children)
    elif isinstance(node, list):
        return "".join(_walk_adf_node(n) for n in node)
    return ""


def extract_adf_text(adf_value) -> Optional[str]:
    """
    adf_value: any of --
      - the full ADF document dict, e.g. {"type": "doc", "version": 1, "content": [...]},
      - just its "content" array directly (this is what you'll actually
        get from auto_standardize's own output: it names a rich-text
        field's parts "X_content"/"X_type"/"X_version" separately, so
        "X_content" arrives as the bare block array, not the wrapper dict),
      - or a JSON string containing either of the above (as it typically
        arrives from a Bronze/Silver column already through to_json).
    Returns plain text with paragraph breaks preserved as newlines, or
    None if there's nothing extractable.
    """
    if adf_value is None:
        return None

    obj = adf_value
    if isinstance(adf_value, str):
        try:
            obj = json.loads(adf_value)
        except (ValueError, TypeError):
            return adf_value  # wasn't actually JSON -- return as-is rather than lose it

    if isinstance(obj, dict):
        blocks = obj.get("content", [])
    elif isinstance(obj, list):
        blocks = obj  # already just the content array
    else:
        return str(obj) if obj else None

    # Join top-level content blocks (paragraphs, etc.) with a blank line
    # between them, matching how the text visually reads in Jira itself
    paragraphs = [_walk_adf_node(block).strip() for block in blocks]
    paragraphs = [p for p in paragraphs if p]
    result = "\n\n".join(paragraphs)
    return result if result else None
