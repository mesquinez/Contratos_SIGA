from __future__ import annotations

from typing import Any, Optional

from src.core.base_scraper import BaseScraper


class CatalogScraper(BaseScraper):
    """Catalog-specific implementation using /Catalogo/paginate.action."""

    DEFAULT_QUERY_PARAMS: dict[str, Any] = {
        # Backend expects these DTO fields to be non-null.
        "orderColumn": 0,
        "orderDirection": "asc",
    }

    def __init__(
        self,
        *args: Any,
        static_query_params: Optional[dict[str, Any]] = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.static_query_params = static_query_params or {}

    def get_endpoint(self) -> str:
        return "/Catalogo/paginate.action"

    def get_http_method(self) -> str:
        return "GET"

    def build_request_payload(self, start: int, length: int) -> dict[str, Any]:
        """
        Build the GET query params for catalog pagination.

        start/length are mandatory for offset-based pagination.
        Additional fixed params can be injected from pipeline config.
        """
        page_number = (start // length) + 1 if length > 0 else 1

        params: dict[str, Any] = {
            "start": start,
            "length": length,
            # Some implementations also use draw as request sequence id.
            "draw": page_number,
        }
        params.update(self.DEFAULT_QUERY_PARAMS)
        params.update(self.static_query_params)
        return params

    def parse_response(self, json_data: Any) -> tuple[list[dict[str, Any]], Optional[int]]:
        """
        Expected response key:
        - listarCatalogo: list[dict]

        Fallback keys are accepted to stay resilient to minor API variations.
        """
        if isinstance(json_data, list):
            return self._normalize_records(json_data), len(json_data)

        if not isinstance(json_data, dict):
            return [], None

        records = (
            json_data.get("listarCatalogo")
            or json_data.get("data")
            or json_data.get("aaData")
            or json_data.get("items")
            or json_data.get("rows")
            or json_data.get("content")
            or json_data.get("results")
            or []
        )
        if not records:
            records = self._find_records_fallback(json_data)
        normalized = self._normalize_records(records)

        total = self._extract_total(json_data)
        return normalized, total

    @staticmethod
    def _normalize_records(records: Any) -> list[dict[str, Any]]:
        if not isinstance(records, list):
            return []
        normalized: list[dict[str, Any]] = []
        for row in records:
            if isinstance(row, dict):
                normalized.append(row)
            else:
                normalized.append({"value": row})
        return normalized

    @staticmethod
    def _extract_total(payload: dict[str, Any]) -> Optional[int]:
        total_candidates = (
            payload.get("recordsTotal"),
            payload.get("recordsFiltered"),
            payload.get("iTotalRecords"),
            payload.get("iTotalDisplayRecords"),
            payload.get("totalElements"),
            payload.get("total"),
            payload.get("qtdRegistros"),
            payload.get("quantidadeTotal"),
        )
        for value in total_candidates:
            if value is None:
                continue
            try:
                return int(value)
            except (TypeError, ValueError):
                continue
        return None

    @classmethod
    def _find_records_fallback(cls, payload: dict[str, Any]) -> list[dict[str, Any]]:
        """
        Best-effort recursive lookup for list-like record containers.
        Used only when primary keys are absent.
        """
        preferred_keys = {
            "listarCatalogo",
            "data",
            "aaData",
            "items",
            "rows",
            "content",
            "results",
        }

        def _normalize_candidate(value: Any) -> list[dict[str, Any]]:
            if not isinstance(value, list):
                return []
            if not value:
                return []
            if all(isinstance(x, dict) for x in value):
                return value
            return []

        for key in preferred_keys:
            candidate = _normalize_candidate(payload.get(key))
            if candidate:
                return candidate

        for value in payload.values():
            if isinstance(value, dict):
                nested = cls._find_records_fallback(value)
                if nested:
                    return nested
            else:
                candidate = _normalize_candidate(value)
                if candidate:
                    return candidate
        return []
