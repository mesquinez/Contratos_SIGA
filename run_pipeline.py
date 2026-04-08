from __future__ import annotations

import argparse
import asyncio
import importlib
import inspect
import json
import logging
import re
import unicodedata
from pathlib import Path
from typing import Any, Optional

from src.core.base_scraper import ScraperSettings
from src.scrapers.catalog_scraper import CatalogScraper
from src.scrapers.contracts_scraper import ContractsScraper


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("run_pipeline")


class JsonStateManager:
    """Fallback StateManager compatible with BaseScraper expectations."""

    def __init__(self, state_file: str) -> None:
        self.state_path = Path(state_file)
        self.state_path.parent.mkdir(parents=True, exist_ok=True)

    def load_state(self) -> dict[str, Any]:
        if not self.state_path.exists():
            return {"downloaded": 0, "completed": False}
        with self.state_path.open("r", encoding="utf-8") as fp:
            data = json.load(fp)
        if "downloaded" not in data:
            data["downloaded"] = 0
        if "completed" not in data:
            data["completed"] = False
        return data

    def save_state(self, state: dict[str, Any]) -> None:
        with self.state_path.open("w", encoding="utf-8") as fp:
            json.dump(state, fp, ensure_ascii=False, indent=2)

    def reset(self) -> None:
        self.save_state({"downloaded": 0, "completed": False})


class PlaywrightBrowserController:
    """
    Fallback BrowserController using Playwright APIRequestContext.
    If your project already has src.core.browser_controller.BrowserController,
    run_pipeline will use that class instead.
    """

    def __init__(self, base_url: str, timeout_ms: int) -> None:
        self.base_url = base_url
        self.timeout_ms = timeout_ms
        self._playwright = None
        self._request_context = None

    async def open(self) -> None:
        from playwright.async_api import async_playwright

        self._playwright = await async_playwright().start()
        self._request_context = await self._playwright.request.new_context(base_url=self.base_url)

    async def close(self) -> None:
        if self._request_context is not None:
            await self._request_context.dispose()
            self._request_context = None
        if self._playwright is not None:
            await self._playwright.stop()
            self._playwright = None

    async def request_json(
        self,
        *,
        method: str,
        url: str,
        headers: Optional[dict[str, str]] = None,
        params: Optional[dict[str, Any]] = None,
        json: Optional[dict[str, Any]] = None,
        json_data: Optional[dict[str, Any]] = None,
        timeout_ms: Optional[int] = None,
    ) -> Any:
        if self._request_context is None:
            raise RuntimeError("BrowserController is not opened. Call open() first.")

        body = json if json is not None else json_data
        normalized_headers = {str(k).lower(): str(v) for k, v in (headers or {}).items()}
        content_type = normalized_headers.get("content-type", "").lower()
        is_form_encoded = "application/x-www-form-urlencoded" in content_type

        fetch_kwargs: dict[str, Any] = {
            "method": method,
            "headers": headers,
            "params": params,
            "timeout": timeout_ms or self.timeout_ms,
        }
        if is_form_encoded:
            fetch_kwargs["form"] = body or {}
        else:
            fetch_kwargs["data"] = body

        response = await self._request_context.fetch(url, **fetch_kwargs)

        if not response.ok:
            status = response.status
            text = await response.text()
            raise RuntimeError(f"HTTP {status} error for {url}: {text[:500]}")

        return await response.json()


def _parse_static_params(values: list[str]) -> dict[str, Any]:
    parsed: dict[str, Any] = {}
    for item in values:
        if "=" not in item:
            raise ValueError(f"Invalid --catalog-param '{item}'. Use key=value.")
        key, value = item.split("=", 1)
        parsed[key.strip()] = _coerce_scalar(value.strip())
    return parsed


def _coerce_scalar(value: str) -> Any:
    lowered = value.lower()
    if lowered in {"true", "false"}:
        return lowered == "true"
    if lowered in {"null", "none"}:
        return None
    try:
        return int(value)
    except ValueError:
        pass
    try:
        return float(value)
    except ValueError:
        pass
    return value


def _resolve_state_manager(state_file: str) -> Any:
    try:
        module = importlib.import_module("src.core.state_manager")
        state_manager_cls = getattr(module, "StateManager")
        return state_manager_cls(state_file=state_file)
    except Exception:
        logger.info("Using fallback JsonStateManager.")
        return JsonStateManager(state_file=state_file)


def _resolve_browser_controller(base_url: str, timeout_ms: int) -> Any:
    try:
        module = importlib.import_module("src.core.browser_controller")
        controller_cls = getattr(module, "BrowserController")
        return controller_cls(base_url=base_url, timeout_ms=timeout_ms)
    except Exception:
        logger.info("Using fallback PlaywrightBrowserController.")
        return PlaywrightBrowserController(base_url=base_url, timeout_ms=timeout_ms)


def _slugify_situacao(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value)
    ascii_only = normalized.encode("ascii", "ignore").decode("ascii")
    slug = re.sub(r"[^a-zA-Z0-9]+", "_", ascii_only).strip("_").lower()
    return slug or "sem_situacao"


async def _run_optional_step(step_name: str, source: str) -> None:
    """
    Tries known function locations for backward compatibility.
    Missing optional steps are logged and skipped safely.
    """
    candidates_by_source = {
        "catalog": {
            "clean": [
                ("src.pipelines.clean_catalog", "run"),
                ("src.pipelines.cleaning", "run_catalog_cleaning"),
                ("src.pipelines.cleaning", "run"),
            ],
            "consolidate": [
                ("src.pipelines.consolidate_catalog", "run"),
                ("src.pipelines.consolidation", "run_catalog_consolidation"),
                ("src.pipelines.consolidation", "run"),
            ],
        },
        "contracts": {
            "clean": [
                ("src.pipelines.clean_contracts", "run"),
                ("src.pipelines.cleaning", "run_contracts_cleaning"),
                ("src.pipelines.cleaning", "run"),
            ],
            "consolidate": [
                ("src.pipelines.consolidate_contracts", "run"),
                ("src.pipelines.consolidation", "run_contracts_consolidation"),
                ("src.pipelines.consolidation", "run"),
            ],
        },
    }
    source_candidates = candidates_by_source.get(source, candidates_by_source["catalog"])
    candidates = source_candidates[step_name]

    for module_name, fn_name in candidates:
        try:
            module = importlib.import_module(module_name)
            fn = getattr(module, fn_name)
        except Exception:
            continue

        result = fn()
        if inspect.isawaitable(result):
            await result
        logger.info("Executed %s step via %s.%s", step_name, module_name, fn_name)
        return

    logger.info("No %s module found. Step skipped.", step_name)


async def run_pipeline(args: argparse.Namespace) -> None:
    default_paths = {
        "catalog": {
            "output_dir": "data/raw/catalog",
            "state_file": "state/catalog_state.json",
        },
        "contracts": {
            "output_dir": "data/raw/contracts",
            "state_file": "state/contracts_state.json",
        },
    }
    source_defaults = default_paths[args.source]

    if args.source == "catalog":
        output_dir = args.output_dir or source_defaults["output_dir"]
        state_file = args.state_file or source_defaults["state_file"]
        settings = ScraperSettings(
            base_url=args.base_url,
            output_dir=output_dir,
            page_size=args.page_size,
            min_delay_seconds=args.min_delay,
            max_delay_seconds=args.max_delay,
            max_retries=args.max_retries,
            retry_backoff_seconds=args.retry_backoff,
            retry_max_backoff_seconds=args.retry_max_backoff,
            retry_jitter_seconds=args.retry_jitter,
            request_timeout_ms=args.timeout_ms,
            test_max_batches=args.test_max_batches,
        )
        state_manager = _resolve_state_manager(state_file)
        if args.reset:
            logger.info("Reset flag detected. Clearing saved state.")
            if hasattr(state_manager, "reset"):
                state_manager.reset()
            else:
                state_manager.save_state({"downloaded": 0, "completed": False})

        browser_controller = _resolve_browser_controller(
            base_url=settings.base_url,
            timeout_ms=settings.request_timeout_ms,
        )
        scraper = CatalogScraper(
            browser_controller=browser_controller,
            state_manager=state_manager,
            settings=settings,
            test_mode=args.test_mode,
            static_query_params=_parse_static_params(args.catalog_param),
        )
        logger.info("Starting scrape stage (%s).", scraper.__class__.__name__)
        await scraper.run()
        logger.info("Scrape stage completed.")
    else:
        base_output_dir = Path(args.output_dir or source_defaults["output_dir"])
        state_base_dir = Path("state")
        logger.info(
            "Starting state-wide contracts sweep across all public agencies in RJ "
            "for %s status category(ies).",
            len(args.situacoes),
        )
        for situacao in args.situacoes:
            situacao_slug = _slugify_situacao(situacao)
            output_dir = str(base_output_dir / situacao_slug)

            if args.state_file:
                custom_state = Path(args.state_file)
                state_file = str(
                    custom_state.with_name(
                        f"{custom_state.stem}_{situacao_slug}{custom_state.suffix or '.json'}"
                    )
                )
            else:
                state_file = str(state_base_dir / f"contracts_{situacao_slug}_state.json")

            settings = ScraperSettings(
                base_url=args.base_url,
                output_dir=output_dir,
                page_size=args.page_size,
                min_delay_seconds=args.min_delay,
                max_delay_seconds=args.max_delay,
                max_retries=args.max_retries,
                retry_backoff_seconds=args.retry_backoff,
                retry_max_backoff_seconds=args.retry_max_backoff,
                retry_jitter_seconds=args.retry_jitter,
                request_timeout_ms=args.timeout_ms,
                test_max_batches=args.test_max_batches,
            )
            state_manager = _resolve_state_manager(state_file)
            if args.reset:
                logger.info("Reset flag detected for situacao='%s'. Clearing saved state.", situacao)
                if hasattr(state_manager, "reset"):
                    state_manager.reset()
                else:
                    state_manager.save_state({"downloaded": 0, "completed": False})

            browser_controller = _resolve_browser_controller(
                base_url=settings.base_url,
                timeout_ms=settings.request_timeout_ms,
            )
            scraper = ContractsScraper(
                browser_controller=browser_controller,
                state_manager=state_manager,
                settings=settings,
                test_mode=args.test_mode,
                situacao=situacao,
                unidade=None,
            )
            logger.info(
                "Starting state-wide scrape stage (%s) | situacao='%s' "
                "| output_dir='%s' | state_file='%s'",
                scraper.__class__.__name__,
                situacao,
                output_dir,
                state_file,
            )
            await scraper.run()
            logger.info("State-wide scrape stage completed for situacao='%s'.", situacao)

    if not args.skip_clean:
        await _run_optional_step("clean", args.source)
    else:
        logger.info("Clean step skipped by flag.")

    if not args.skip_consolidate:
        await _run_optional_step("consolidate", args.source)
    else:
        logger.info("Consolidate step skipped by flag.")


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Catalog pipeline entrypoint.")

    parser.add_argument("--source", choices=["catalog", "contracts"], default="catalog")
    parser.add_argument("--situacoes", nargs="+", default=["Ativo"])
    parser.add_argument("--base-url", default="https://compras.rj.gov.br")
    parser.add_argument("--output-dir", default=None)
    parser.add_argument("--state-file", default=None)
    parser.add_argument("--page-size", type=int, default=100)

    parser.add_argument("--min-delay", type=float, default=0.7)
    parser.add_argument("--max-delay", type=float, default=1.8)

    parser.add_argument("--max-retries", type=int, default=5)
    parser.add_argument("--retry-backoff", type=float, default=1.5)
    parser.add_argument("--retry-max-backoff", type=float, default=30.0)
    parser.add_argument("--retry-jitter", type=float, default=0.35)
    parser.add_argument("--timeout-ms", type=int, default=60_000)

    parser.add_argument("--test-mode", action="store_true")
    parser.add_argument("--test-max-batches", type=int, default=1)
    parser.add_argument("--reset", action="store_true")

    parser.add_argument(
        "--catalog-param",
        action="append",
        default=[],
        help="Static catalog query param in key=value format. Can be repeated.",
    )

    parser.add_argument("--skip-clean", action="store_true")
    parser.add_argument("--skip-consolidate", action="store_true")

    return parser


def main() -> None:
    parser = build_arg_parser()
    args = parser.parse_args()
    asyncio.run(run_pipeline(args))


if __name__ == "__main__":
    main()
