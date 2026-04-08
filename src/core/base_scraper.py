from __future__ import annotations

import asyncio
import inspect
import json
import logging
import random
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urljoin


logger = logging.getLogger(__name__)


@dataclass(slots=True)
class ScraperSettings:
    """Runtime settings shared by all scraper implementations."""

    base_url: str
    output_dir: str
    page_size: int = 100
    min_delay_seconds: float = 0.7
    max_delay_seconds: float = 1.8
    max_retries: int = 5
    retry_backoff_seconds: float = 1.5
    retry_max_backoff_seconds: float = 30.0
    retry_jitter_seconds: float = 0.35
    request_timeout_ms: int = 60_000
    test_max_batches: int = 1


class BaseScraper(ABC):
    """
    Generic orchestrator for XHR/Fetch JSON scrapers.

    Child classes are responsible for endpoint and response-specific logic.
    This base class centralizes:
    - Browser controller lifecycle management
    - Retry policy
    - StateManager persistence (offset uses state.downloaded)
    - JSON batch persistence
    - Ethical delays between requests
    """

    def __init__(
        self,
        browser_controller: Any,
        state_manager: Any,
        settings: ScraperSettings,
        *,
        test_mode: bool = False,
    ) -> None:
        self.browser_controller = browser_controller
        self.state_manager = state_manager
        self.settings = settings
        self.test_mode = test_mode

        self.output_dir = Path(settings.output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    @abstractmethod
    def get_endpoint(self) -> str:
        """Return endpoint path (absolute path or full URL)."""

    @abstractmethod
    def build_request_payload(self, start: int, length: int) -> dict[str, Any]:
        """Build request payload/query params for the current page."""

    @abstractmethod
    def parse_response(self, json_data: Any) -> tuple[list[dict[str, Any]], Optional[int]]:
        """
        Parse raw JSON and return:
        - records list
        - optional total records reported by API
        """

    def get_http_method(self) -> str:
        return "GET"

    def get_headers(self) -> dict[str, str]:
        return {}

    async def run(self) -> dict[str, Any]:
        """
        Main orchestration loop.

        Offset progression is strictly driven by state.downloaded.
        """

        state = self._ensure_state_shape(self._load_state())
        downloaded = int(self._state_get(state, "downloaded", 0))
        total_records = self._state_get(state, "total_records", None)
        batch_number = int(self._state_get(state, "batch_number", 0))
        completed = bool(self._state_get(state, "completed", False))

        logger.info(
            "Starting scraper %s | downloaded=%s | completed=%s | test_mode=%s",
            self.__class__.__name__,
            downloaded,
            completed,
            self.test_mode,
        )

        await self._open_browser()
        try:
            while not completed:
                if self.test_mode and batch_number >= int(self.settings.test_max_batches):
                    logger.info(
                        "Test mode limit reached (%s batches). Stopping scraper loop.",
                        self.settings.test_max_batches,
                    )
                    break

                payload = self.build_request_payload(start=downloaded, length=self.settings.page_size)
                json_data = await self._fetch_json_with_retry(payload)
                records, reported_total = self.parse_response(json_data)

                if reported_total is not None:
                    total_records = int(reported_total)
                    self._state_set(state, "total_records", total_records)

                if not records:
                    if self._should_raise_on_empty_first_page(
                        downloaded=downloaded,
                        reported_total=reported_total,
                        json_data=json_data,
                    ):
                        debug_path = self._save_debug_response(
                            json_data=json_data,
                            reason="empty_first_page_unmapped_payload",
                        )
                        raise RuntimeError(
                            "Empty first page with non-empty JSON payload. "
                            "Likely response-mapping mismatch. "
                            f"Debug saved to: {debug_path}"
                        )

                    logger.info("No records returned at offset=%s. Marking as completed.", downloaded)
                    completed = True
                    self._state_set(state, "completed", True)
                    self._state_set(state, "updated_at", self._utc_now())
                    self._save_state(state)
                    break

                batch_number += 1
                batch_path = self._save_batch_file(
                    batch_number=batch_number,
                    offset=downloaded,
                    records=records,
                )

                downloaded += len(records)
                self._state_set(state, "downloaded", downloaded)
                self._state_set(state, "batch_number", batch_number)
                self._state_set(state, "last_batch_size", len(records))
                self._state_set(state, "last_batch_file", str(batch_path))
                self._state_set(state, "updated_at", self._utc_now())

                if total_records is not None and downloaded >= total_records:
                    completed = True
                    self._state_set(state, "completed", True)

                if len(records) < self.settings.page_size:
                    # Defensive EOF heuristic for APIs that do not expose total records.
                    completed = True
                    self._state_set(state, "completed", True)

                self._save_state(state)

                if completed:
                    logger.info(
                        "Scraper completed | downloaded=%s | total=%s",
                        downloaded,
                        total_records,
                    )
                    break

                await self._sleep_ethical_delay()

            return state
        except Exception as exc:  # pragma: no cover - safety net
            self._state_set(state, "last_error", repr(exc))
            self._state_set(state, "updated_at", self._utc_now())
            self._save_state(state)
            logger.exception("Fatal error in %s", self.__class__.__name__)
            raise
        finally:
            await self._close_browser()

    async def _fetch_json_with_retry(self, payload: dict[str, Any]) -> Any:
        last_error: Exception | None = None
        for attempt in range(1, int(self.settings.max_retries) + 1):
            try:
                return await self._request_json(payload)
            except Exception as exc:  # pragma: no cover - network path
                last_error = exc
                if attempt >= int(self.settings.max_retries):
                    break

                delay = min(
                    float(self.settings.retry_backoff_seconds) * (2 ** (attempt - 1)),
                    float(self.settings.retry_max_backoff_seconds),
                )
                delay += random.uniform(0, float(self.settings.retry_jitter_seconds))
                logger.warning(
                    "Request failed (attempt %s/%s): %s. Retrying in %.2fs",
                    attempt,
                    self.settings.max_retries,
                    exc,
                    delay,
                )
                await asyncio.sleep(delay)

        assert last_error is not None
        raise last_error

    async def _request_json(self, payload: dict[str, Any]) -> Any:
        method = self.get_http_method().upper()
        url = self._build_url(self.get_endpoint())
        headers = self.get_headers() or {}

        params = payload if method == "GET" else None
        json_body = payload if method != "GET" else None

        controller = self.browser_controller

        if hasattr(controller, "request_json"):
            return await self._call_controller_json_method(
                controller.request_json,
                method=method,
                url=url,
                headers=headers,
                params=params,
                json_body=json_body,
            )

        if hasattr(controller, "fetch_json"):
            return await self._call_controller_json_method(
                controller.fetch_json,
                method=method,
                url=url,
                headers=headers,
                params=params,
                json_body=json_body,
            )

        if hasattr(controller, "request"):
            response = await self._call_controller_request_method(
                controller.request,
                method=method,
                url=url,
                headers=headers,
                params=params,
                json_body=json_body,
            )
            return await self._response_to_json(response)

        method_fn = getattr(controller, method.lower(), None)
        if method_fn is not None:
            kwargs = {"headers": headers, "timeout": self.settings.request_timeout_ms}
            if params is not None:
                kwargs["params"] = params
            if json_body is not None:
                kwargs["json"] = json_body
            response = await method_fn(url, **kwargs)
            return await self._response_to_json(response)

        raise RuntimeError(
            "BrowserController does not expose a supported request interface "
            "(expected request_json/fetch_json/request/get/post...)"
        )

    async def _call_controller_json_method(
        self,
        method_callable: Any,
        *,
        method: str,
        url: str,
        headers: dict[str, str],
        params: Optional[dict[str, Any]],
        json_body: Optional[dict[str, Any]],
    ) -> Any:
        attempts: list[dict[str, Any]] = [
            {
                "method": method,
                "url": url,
                "headers": headers,
                "params": params,
                "json": json_body,
                "timeout_ms": self.settings.request_timeout_ms,
            },
            {
                "method": method,
                "url": url,
                "headers": headers,
                "params": params,
                "json_data": json_body,
                "timeout_ms": self.settings.request_timeout_ms,
            },
            {
                "method": method,
                "url": url,
                "headers": headers,
                "params": params,
                "data": json_body,
                "timeout_ms": self.settings.request_timeout_ms,
            },
            {
                "method": method,
                "url": url,
                "headers": headers,
                "params": params,
                "json": json_body,
            },
        ]
        for kwargs in attempts:
            clean_kwargs = {k: v for k, v in kwargs.items() if v is not None}
            try:
                data = method_callable(**clean_kwargs)
                return await data if inspect.isawaitable(data) else data
            except TypeError:
                continue
        raise TypeError("Failed to call BrowserController JSON method with supported signatures.")

    async def _call_controller_request_method(
        self,
        method_callable: Any,
        *,
        method: str,
        url: str,
        headers: dict[str, str],
        params: Optional[dict[str, Any]],
        json_body: Optional[dict[str, Any]],
    ) -> Any:
        attempts: list[dict[str, Any]] = [
            {
                "method": method,
                "url": url,
                "headers": headers,
                "params": params,
                "json": json_body,
                "timeout": self.settings.request_timeout_ms,
            },
            {
                "method": method,
                "url": url,
                "headers": headers,
                "params": params,
                "data": json.dumps(json_body) if json_body is not None else None,
                "timeout": self.settings.request_timeout_ms,
            },
            {
                "method": method,
                "url": url,
                "headers": headers,
                "params": params,
                "json": json_body,
            },
        ]
        for kwargs in attempts:
            clean_kwargs = {k: v for k, v in kwargs.items() if v is not None}
            try:
                response = method_callable(**clean_kwargs)
                return await response if inspect.isawaitable(response) else response
            except TypeError:
                continue
        raise TypeError("Failed to call BrowserController request method with supported signatures.")

    async def _response_to_json(self, response: Any) -> Any:
        if isinstance(response, (dict, list)):
            return response

        if hasattr(response, "ok") and not bool(response.ok):
            status = getattr(response, "status", "unknown")
            status_text = getattr(response, "status_text", "")
            raise RuntimeError(f"HTTP error {status} {status_text}".strip())

        if hasattr(response, "json"):
            parsed = response.json()
            return await parsed if inspect.isawaitable(parsed) else parsed

        raise TypeError("Controller response does not expose JSON payload.")

    def _should_raise_on_empty_first_page(
        self,
        *,
        downloaded: int,
        reported_total: Optional[int],
        json_data: Any,
    ) -> bool:
        """
        Guardrail to avoid false-positive completion when parser mapping is wrong.
        """
        if downloaded != 0:
            return False
        if reported_total == 0:
            return False
        if json_data in (None, {}, []):
            return False
        return True

    async def _sleep_ethical_delay(self) -> None:
        min_delay = float(self.settings.min_delay_seconds)
        max_delay = float(self.settings.max_delay_seconds)
        if max_delay <= 0:
            return
        if max_delay < min_delay:
            min_delay, max_delay = max_delay, min_delay
        await asyncio.sleep(random.uniform(min_delay, max_delay))

    def _save_batch_file(
        self,
        *,
        batch_number: int,
        offset: int,
        records: list[dict[str, Any]],
    ) -> Path:
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        file_name = f"batch_{batch_number:06d}_offset_{offset:012d}_{timestamp}.json"
        file_path = self.output_dir / file_name
        payload = {
            "meta": {
                "batch_number": batch_number,
                "offset": offset,
                "count": len(records),
                "saved_at": self._utc_now(),
                "endpoint": self.get_endpoint(),
            },
            "records": records,
        }
        with file_path.open("w", encoding="utf-8") as fp:
            json.dump(payload, fp, ensure_ascii=False, indent=2)
        return file_path

    def _save_debug_response(self, *, json_data: Any, reason: str) -> Path:
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        file_name = f"debug_{reason}_{timestamp}.json"
        file_path = self.output_dir / file_name
        payload = {
            "meta": {
                "reason": reason,
                "saved_at": self._utc_now(),
                "endpoint": self.get_endpoint(),
            },
            "response": json_data,
        }
        with file_path.open("w", encoding="utf-8") as fp:
            json.dump(payload, fp, ensure_ascii=False, indent=2)
        return file_path

    async def _open_browser(self) -> None:
        opener = getattr(self.browser_controller, "open", None)
        if opener is None:
            return
        result = opener()
        if inspect.isawaitable(result):
            await result

    async def _close_browser(self) -> None:
        closer = getattr(self.browser_controller, "close", None)
        if closer is None:
            return
        result = closer()
        if inspect.isawaitable(result):
            await result

    def _build_url(self, endpoint: str) -> str:
        if endpoint.startswith("http://") or endpoint.startswith("https://"):
            return endpoint
        return urljoin(self.settings.base_url.rstrip("/") + "/", endpoint.lstrip("/"))

    def _ensure_state_shape(self, state: Any) -> Any:
        if state is None:
            state = {}
        if self._state_get(state, "downloaded", None) is None:
            self._state_set(state, "downloaded", 0)
        if self._state_get(state, "completed", None) is None:
            self._state_set(state, "completed", False)
        return state

    def _load_state(self) -> Any:
        if hasattr(self.state_manager, "load_state"):
            return self.state_manager.load_state()
        if hasattr(self.state_manager, "load"):
            return self.state_manager.load()
        raise RuntimeError("StateManager must expose load_state() or load().")

    def _save_state(self, state: Any) -> None:
        if hasattr(self.state_manager, "save_state"):
            self.state_manager.save_state(state)
            return
        if hasattr(self.state_manager, "save"):
            self.state_manager.save(state)
            return
        raise RuntimeError("StateManager must expose save_state() or save().")

    @staticmethod
    def _state_get(state: Any, key: str, default: Any = None) -> Any:
        if isinstance(state, dict):
            return state.get(key, default)
        return getattr(state, key, default)

    @staticmethod
    def _state_set(state: Any, key: str, value: Any) -> None:
        if isinstance(state, dict):
            state[key] = value
            return
        setattr(state, key, value)

    @staticmethod
    def _utc_now() -> str:
        return datetime.now(timezone.utc).isoformat()
