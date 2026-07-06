"""
Config-driven REST extractor. Handles the three pagination styles almost
every REST API uses (offset/limit, page-number, cursor/token) purely from
EntityConfig — no subclassing needed for the common case.

Subclass ONLY when a source needs something genuinely non-generic: unusual
auth handshake (OAuth2 refresh flow), a request body that isn't a flat
merge of extra_body + pagination params, or a response shape too irregular
for a single json_path to describe. Override `fetch_page` in that case and
inherit everything else (retry/backoff, extract_records, is_last_page).
"""

import time
from typing import Dict, Any, List, Iterator, Optional

import requests

from fabric_medallion_toolkit.config import SourceConfig, EntityConfig
from fabric_medallion_toolkit.utils.json_path import get_by_path
from fabric_medallion_toolkit.utils.logging_utils import get_logger


class RestExtractor:
    """Pulls paginated records from any REST API entity described by EntityConfig."""

    def __init__(self, source_config: SourceConfig):
        self.source_config = source_config
        self.logger = get_logger(f"bronze.{source_config.source_name}")
        self.session = requests.Session()
        if source_config.auth:
            headers = source_config.auth.as_headers()
            if headers:
                self.session.headers.update(headers)

    def _request_with_retry(self, method: str, url: str, **kwargs) -> requests.Response:
        last_exc = None
        cfg = self.source_config
        for attempt in range(1, cfg.max_retries + 1):
            try:
                resp = self.session.request(
                    method, url,
                    auth=cfg.auth.as_requests_auth() if cfg.auth else None,
                    timeout=cfg.request_timeout_seconds,
                    **kwargs,
                )
                if resp.status_code == 429:
                    wait = int(resp.headers.get("Retry-After", 2 ** attempt))
                    self.logger.warning(f"Rate limited, sleeping {wait}s (attempt {attempt})")
                    time.sleep(wait)
                    continue

                if resp.status_code in (400, 401, 403, 404):
                    # Not transient -- retrying the identical malformed/unauthorized
                    # request won't ever succeed. Fail fast and surface the API's
                    # actual error message (usually far more specific than the
                    # generic status text), rather than burning through all
                    # retries first.
                    body_snippet = resp.text[:1000] if resp.text else "(no response body)"
                    raise RuntimeError(
                        f"{method} {url} returned {resp.status_code}, not retrying "
                        f"(non-transient client error). Response body: {body_snippet}"
                    )

                resp.raise_for_status()
                return resp
            except requests.RequestException as exc:
                last_exc = exc
                wait = min(2 ** attempt, 60)
                self.logger.warning(f"Request failed ({exc}), retrying in {wait}s "
                                     f"(attempt {attempt}/{cfg.max_retries})")
                time.sleep(wait)
        raise RuntimeError(f"Request to {url} failed after {cfg.max_retries} attempts") from last_exc

    def _fetch_page(self, entity: EntityConfig, offset: int, page_num: int,
                     cursor: Optional[str], watermark_value: Optional[str]) -> Dict[str, Any]:
        url = f"{self.source_config.base_url}{entity.endpoint_path}"

        params: Dict[str, Any] = dict(entity.extra_params)
        body: Dict[str, Any] = dict(entity.extra_body)

        # Apply watermark templates (only when a watermark value is supplied
        # AND the entity defines a template for it — full-load entities just
        # ignore this) -- values with a "{watermark}" placeholder get it
        # substituted; these override the matching key in extra_params/extra_body.
        if watermark_value is not None:
            for k, v in entity.watermark_params_template.items():
                params[k] = v.format(watermark=watermark_value)
            for k, v in entity.watermark_body_template.items():
                body[k] = v.format(watermark=watermark_value)

        if entity.pagination_style == "offset_limit":
            pag = {"offset": offset, "limit": entity.page_size}
        elif entity.pagination_style == "page_number":
            pag = {"page": page_num, "pageSize": entity.page_size}
        elif entity.pagination_style == "cursor":
            pag = {entity.cursor_param_name: cursor} if cursor else {}
            pag[entity.cursor_page_size_param_name] = entity.page_size
        elif entity.pagination_style == "none":
            pag = {}
        else:
            raise ValueError(f"Unknown pagination_style '{entity.pagination_style}'")

        if entity.http_method.upper() == "GET":
            params.update(pag)
            resp = self._request_with_retry("GET", url, params=params)
        else:
            body.update(pag)
            resp = self._request_with_retry("POST", url, json=body, params=params or None)

        return resp.json()

    def extract_records(self, page_response: Dict[str, Any], entity: EntityConfig) -> List[Dict[str, Any]]:
        records = get_by_path(page_response, entity.records_json_path, default=None)
        if records is None and entity.records_json_path == "" and isinstance(page_response, list):
            records = page_response
        return records or []

    def _is_last_page(self, page_response: Dict[str, Any], entity: EntityConfig,
                       offset: int, records_returned: int) -> bool:
        if records_returned == 0:
            return True
        if entity.pagination_style == "cursor":
            next_cursor = get_by_path(page_response, entity.next_cursor_json_path or "", default=None)
            return not next_cursor
        if entity.total_json_path:
            total = get_by_path(page_response, entity.total_json_path, default=None)
            if total is not None:
                return (offset + records_returned) >= total
        return records_returned < entity.page_size

    def _next_cursor(self, page_response: Dict[str, Any], entity: EntityConfig) -> Optional[str]:
        if entity.pagination_style != "cursor":
            return None
        return get_by_path(page_response, entity.next_cursor_json_path or "", default=None)

    def extract_entity(self, entity: EntityConfig, max_pages: Optional[int] = None,
                        watermark_value: Optional[str] = None) -> Iterator[Dict[str, Any]]:
        """
        watermark_value: pass the last-recorded watermark (see bronze/watermark.py)
        for incremental entities; leave None for a full load. Has no effect
        on entities that don't define a watermark template.
        """
        offset = 0
        page_num = 1
        cursor = None
        while True:
            self.logger.info(f"Fetching {entity.entity_name} page {page_num} (offset={offset})")
            page = self._fetch_page(entity, offset, page_num, cursor, watermark_value)
            records = self.extract_records(page, entity)
            for r in records:
                yield r

            offset += len(records)
            if self._is_last_page(page, entity, offset - len(records), len(records)):
                break
            cursor = self._next_cursor(page, entity)
            page_num += 1
            if max_pages and page_num > max_pages:
                self.logger.info(f"Hit max_pages={max_pages}, stopping early")
                break
