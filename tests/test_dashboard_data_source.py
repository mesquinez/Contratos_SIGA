from __future__ import annotations

import sqlite3

import pytest

from src.dashboard import app


def _make_connection() -> sqlite3.Connection:
    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row
    return connection


@pytest.fixture(autouse=True)
def clear_streamlit_caches() -> None:
    app.get_table_columns.clear()
    app.get_filter_options.clear()
    app.run_filtered_query.clear()
    yield
    app.get_table_columns.clear()
    app.get_filter_options.clear()
    app.run_filtered_query.clear()


def test_get_table_columns_accepts_source_cache_key(monkeypatch: pytest.MonkeyPatch) -> None:
    connection = _make_connection()
    connection.execute(
        """
        CREATE TABLE contratos_oficiais (
            numero_contrato TEXT,
            unidade TEXT,
            situacao TEXT,
            ano INTEGER,
            valor_total REAL
        )
        """
    )
    connection.commit()

    monkeypatch.setattr(app, "get_connection", lambda: (connection, "contratos_oficiais"))

    columns = app.get_table_columns(("historical", 1.0, 1))

    assert columns == ["numero_contrato", "unidade", "situacao", "ano", "valor_total"]


def test_get_filter_options_accepts_source_cache_key(monkeypatch: pytest.MonkeyPatch) -> None:
    connection = _make_connection()
    connection.execute(
        """
        CREATE TABLE contratos (
            unidade TEXT,
            situacao TEXT,
            ano INTEGER,
            valor_total REAL
        )
        """
    )
    connection.executemany(
        "INSERT INTO contratos (unidade, situacao, ano, valor_total) VALUES (?, ?, ?, ?)",
        [
            ("SEFAZ", "Ativo", 2024, 100.0),
            ("SEDUC", "Encerrado", 2025, 200.0),
        ],
    )
    connection.commit()

    monkeypatch.setattr(app, "get_connection", lambda: (connection, "contratos"))

    unidades, situacoes, min_year, max_year = app.get_filter_options(("legacy", 2.0, 2))

    assert unidades == ["SEDUC", "SEFAZ"]
    assert situacoes == ["Ativo", "Encerrado"]
    assert min_year == 2024
    assert max_year == 2025


def test_get_active_source_prefers_historical_database(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(app, "database_has_table", lambda database_path, table_name: True)

    def fail_if_called(*args, **kwargs):  # pragma: no cover - defensive guard
        raise AssertionError("legacy initializer should not be called when historical DB exists")

    monkeypatch.setattr(app, "initialize_legacy_database", fail_if_called)

    database_path, table_name = app.get_active_source()

    assert database_path == app.HISTORICAL_DATABASE_PATH
    assert table_name == app.HISTORICAL_TABLE_NAME


def test_get_active_source_falls_back_to_legacy_database(monkeypatch: pytest.MonkeyPatch) -> None:
    called = {}

    monkeypatch.setattr(app, "database_has_table", lambda database_path, table_name: False)

    def fake_initialize_legacy_database(parquet_signature):
        called["signature"] = parquet_signature
        return str(app.LEGACY_DATABASE_PATH)

    monkeypatch.setattr(app, "initialize_legacy_database", fake_initialize_legacy_database)
    monkeypatch.setattr(app, "get_parquet_signature", lambda: (123.0, 456))

    database_path, table_name = app.get_active_source()

    assert database_path == app.LEGACY_DATABASE_PATH
    assert table_name == app.LEGACY_TABLE_NAME
    assert called["signature"] == (123.0, 456)
