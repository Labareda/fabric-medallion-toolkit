"""
Triggers a Fabric SQL Analytics Endpoint metadata refresh for a Lakehouse --
generic Fabric plumbing, not tied to Jira or any specific source. Needed
because the SQL Endpoint syncs from Delta on its own schedule/lag, so a
schema change (a column's type changing, a table added) isn't necessarily
visible there the instant a pipeline run finishes.

Uses the GA (not preview) "Refresh SQL Endpoint Metadata" REST API:
    POST /v1/workspaces/{workspaceId}/sqlEndpoints/{sqlEndpointId}/refreshMetadata

mssparkutils is passed in as a parameter rather than imported directly in
this module -- `from notebookutils import mssparkutils` only succeeds
inside an actual Fabric runtime, and importing it at module level here
would make the whole wheel unimportable (and untestable) outside Fabric.
Same reasoning as why extractor.py takes `spark` as a parameter rather
than assuming a global session.
"""

import time
from typing import Any, Dict


def refresh_sql_endpoint(mssparkutils, lakehouse_name: str, timeout_minutes: int = 5,
                          poll_interval_seconds: int = 5, max_poll_seconds: int = 300) -> Dict[str, Any]:
    """
    mssparkutils: pass the module itself from the calling notebook
        (`from notebookutils import mssparkutils`), not re-imported here.
    lakehouse_name: the Lakehouse item's display name (e.g. "Silver").
    timeout_minutes: passed through to the refresh API's own timeout.
    poll_interval_seconds / max_poll_seconds: only relevant if the API
        responds 202 (async) rather than resolving immediately -- controls
        how this function polls the long-running operation until it
        finishes or max_poll_seconds is exceeded.

    Raises on a genuine failure or timeout -- callers that want this to be
    best-effort (a sync lag shouldn't fail an entire pipeline run) should
    wrap the call in their own try/except, same as any other step that's
    allowed to fail without stopping everything else.
    """
    import requests  # a normal pip package, fine to import at call time or module level either way

    lh = mssparkutils.lakehouse.getWithProperties(name=lakehouse_name)
    workspace_id = lh.workspaceId
    sql_endpoint_id = lh.properties["sqlEndpointProperties"]["id"]

    token = mssparkutils.credentials.getToken("https://api.fabric.microsoft.com")
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    body = {"timeout": {"timeUnit": "Minutes", "value": timeout_minutes}}

    resp = requests.post(
        f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/sqlEndpoints/{sql_endpoint_id}/refreshMetadata",
        headers=headers, json=body,
    )

    if resp.status_code == 200:
        return resp.json()

    if resp.status_code == 202:
        operation_url = resp.headers.get("Location") or resp.headers.get("location")
        waited = 0
        while waited < max_poll_seconds:
            time.sleep(poll_interval_seconds)
            waited += poll_interval_seconds
            poll_resp = requests.get(operation_url, headers=headers)
            status = poll_resp.json().get("status")
            if status == "Succeeded":
                return requests.get(f"{operation_url}/result", headers=headers).json()
            elif status == "Failed":
                raise RuntimeError(f"SQL Endpoint refresh failed for {lakehouse_name}: {poll_resp.json()}")
        raise TimeoutError(f"SQL Endpoint refresh for {lakehouse_name} did not complete within {max_poll_seconds}s")

    raise RuntimeError(f"Unexpected status {resp.status_code} refreshing {lakehouse_name}: {resp.text}")
