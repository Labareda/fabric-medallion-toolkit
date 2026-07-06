"""
Some entities aren't a single global list -- they're scoped to a parent
(Jira: versions/components per project, sprints per board; other APIs have
their own version of this). EntityConfig assumes one fixed endpoint, so
this handles the "call the same endpoint once per parent, with the parent's
ID substituted in, and tag each record with which parent it came from"
pattern generically -- reusable for any future source with the same shape,
not just Jira.
"""

from dataclasses import replace
from typing import Iterable, List, Dict, Any

from fabric_medallion_toolkit.config import EntityConfig
from fabric_medallion_toolkit.utils.logging_utils import get_logger

logger = get_logger("bronze.per_parent")


def extract_per_parent(extractor, entity_template: EntityConfig, parent_ids: Iterable[Any],
                        parent_placeholder: str = "{parent_id}",
                        parent_field_name: str = "_parent_id",
                        skip_errors: bool = True) -> List[Dict[str, Any]]:
    """
    Calls entity_template's endpoint once per ID in parent_ids, substituting
    parent_placeholder in endpoint_path with that ID each time. Injects
    parent_field_name into every returned record -- this is deliberate even
    if the API's own response happens to already include the parent's ID,
    since you can then always rely on parent_field_name being there
    regardless of what a specific endpoint does or doesn't return.

    entity_template.endpoint_path should contain the placeholder, e.g.
    "/rest/api/3/project/{parent_id}/versions". natural_key_field on the
    template applies per-record as usual; it does NOT need to include the
    parent, since parent_field_name is added separately.

    skip_errors=True (default): if a specific parent's call fails (e.g. a
    Kanban board doesn't support the sprint endpoint a Scrum board does),
    log a warning and continue with the remaining parents rather than
    aborting the whole batch. Set False if you'd rather fail loudly.

    Returns a plain list (not a generator) since the record count per
    parent is usually small (tens, not thousands) — simpler to reason about
    than lazily chaining generators across many parents.
    """
    all_records = []
    for parent_id in parent_ids:
        scoped_entity = replace(
            entity_template,
            endpoint_path=entity_template.endpoint_path.replace(parent_placeholder, str(parent_id)),
        )
        try:
            for record in extractor.extract_entity(scoped_entity):
                record[parent_field_name] = parent_id
                all_records.append(record)
        except Exception as exc:
            if skip_errors:
                logger.warning(f"{entity_template.entity_name} for parent '{parent_id}' failed, skipping: {exc}")
                continue
            raise
    return all_records
