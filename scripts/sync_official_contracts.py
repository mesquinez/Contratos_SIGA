from __future__ import annotations

import argparse
import csv
import hashlib
import re
import sqlite3
import tempfile
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.request import Request, urlopen
from zipfile import ZipFile

import pandas as pd


DEFAULT_URL = "https://www.compras.rj.gov.br/siga/imagens/CONTRATOS.zip"
BASE_DIR = Path(__file__).resolve().parents[1]
RAW_DIR = BASE_DIR / "data" / "raw" / "official"
PROCESSED_DIR = BASE_DIR / "data" / "processed"
DB_PATH = BASE_DIR / "data" / "database" / "contratos_historico.db"
TABLE_NAME = "contratos_oficiais"
SNAPSHOT_PARQUET = PROCESSED_DIR / "contratos_rio_grande_oficial.parquet"
SNAPSHOT_CSV = PROCESSED_DIR / "contratos_rio_grande_oficial.csv"


def normalize_name(name: str) -> str:
    text = unicodedata.normalize("NFKD", str(name)).encode("ascii", "ignore").decode("ascii")
    return re.sub(r"[^a-zA-Z0-9]+", "_", text).strip("_").lower()


def parse_currency(value: Any) -> float | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    text = str(value).strip()
    if not text or text.lower() == "nan":
        return None
    text = re.sub(r"[^\d,.\-]", "", text)
    if not text:
        return None
    if "," in text and "." in text:
        text = text.replace(".", "").replace(",", ".")
    elif "," in text:
        text = text.replace(",", ".")
    try:
        return float(text)
    except ValueError:
        return None


def download_zip(url: str, destination: Path) -> Path:
    destination.parent.mkdir(parents=True, exist_ok=True)
    request = Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/zip,application/octet-stream,*/*",
            "Referer": "https://www.compras.rj.gov.br/Principal/extracaoTotal.action",
        },
    )
    with urlopen(request, timeout=120) as response, destination.open("wb") as target:
        target.write(response.read())
    return destination


def read_contracts_csv(zip_path: Path) -> pd.DataFrame:
    with ZipFile(zip_path) as archive:
        candidates = [name for name in archive.namelist() if name.upper().endswith("CONTRATOS.CSV")]
        if not candidates:
            raise FileNotFoundError("CONTRATOS.CSV not found inside the zip")
        with archive.open(candidates[0]) as file_obj:
            raw_df = pd.read_csv(file_obj, sep=";", encoding="latin-1", dtype=str)

    return raw_df.rename(columns={column: normalize_name(column) for column in raw_df.columns})


def build_snapshot(raw_df: pd.DataFrame, extraction_timestamp: str) -> pd.DataFrame:
    def _series_or_default(column_name: str, default: Any = None) -> pd.Series:
        if column_name in raw_df.columns:
            return raw_df[column_name]
        return pd.Series([default] * len(raw_df), index=raw_df.index)

    mapped = pd.DataFrame(
        {
            "numero_contrato": _series_or_default("contratacao"),
            "data": pd.to_datetime(
                _series_or_default("data_contratacao"),
                format="%d/%m/%Y",
                errors="coerce",
            ).dt.strftime("%Y-%m-%d"),
            "processo_sei": _series_or_default("processo"),
            "unidade": _series_or_default("unidade"),
            "valor_total": _series_or_default("valor_total_contrato_valor_estimado_para_contratacao_r").apply(parse_currency),
            "modalidade": _series_or_default("tipo_de_aquisicao"),
            "situacao": _series_or_default("status_contratacao"),
            "objeto": _series_or_default("objeto"),
            "fornecedor_nome": _series_or_default("fornecedor"),
            "fornecedor_cnpj": _series_or_default("cpf_cnpj"),
            "gestores": pd.Series([None] * len(raw_df), dtype="object"),
            "url_pncp": _series_or_default("url_pncp"),
            "id_interno": _series_or_default("codigo_do_contrato"),
            "categoria_original": _series_or_default("regimejuridico"),
        }
    )
    mapped["data"] = pd.to_datetime(mapped["data"], errors="coerce")
    mapped["ano"] = mapped["data"].dt.year.astype("Int64")
    mapped["data"] = mapped["data"].dt.strftime("%Y-%m-%d")
    mapped["valor_numerico"] = pd.to_numeric(mapped["valor_total"], errors="coerce")
    mapped["data_extracao"] = extraction_timestamp
    mapped = mapped.where(pd.notna(mapped), None)
    return mapped


def stable_key(row: pd.Series) -> str:
    id_interno = str(row.get("id_interno") or "").strip()
    if id_interno:
        return f"id::{id_interno}"
    parts = [
        str(row.get("numero_contrato") or "").strip(),
        str(row.get("processo_sei") or "").strip(),
        str(row.get("data") or "").strip(),
        str(row.get("fornecedor_cnpj") or "").strip(),
        str(row.get("valor_total") or "").strip(),
    ]
    token = "|".join(parts)
    return f"sha::{hashlib.sha1(token.encode('utf-8')).hexdigest()}"


def ensure_database(connection: sqlite3.Connection) -> None:
    connection.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            record_key TEXT PRIMARY KEY,
            numero_contrato TEXT,
            data TEXT,
            processo_sei TEXT,
            unidade TEXT,
            valor_total REAL,
            modalidade TEXT,
            situacao TEXT,
            objeto TEXT,
            fornecedor_nome TEXT,
            fornecedor_cnpj TEXT,
            gestores TEXT,
            url_pncp TEXT,
            id_interno TEXT,
            categoria_original TEXT,
            ano INTEGER,
            valor_numerico REAL,
            data_extracao TEXT,
            ingested_at TEXT
        )
        """
    )
    connection.execute(f"CREATE UNIQUE INDEX IF NOT EXISTS idx_{TABLE_NAME}_record_key ON {TABLE_NAME} (record_key)")
    connection.commit()


def insert_new_rows(connection: sqlite3.Connection, snapshot: pd.DataFrame) -> int:
    snapshot = snapshot.copy()
    snapshot["record_key"] = snapshot.apply(stable_key, axis=1)
    snapshot["ingested_at"] = datetime.now(timezone.utc).isoformat()

    rows = snapshot[
        [
            "record_key",
            "numero_contrato",
            "data",
            "processo_sei",
            "unidade",
            "valor_total",
            "modalidade",
            "situacao",
            "objeto",
            "fornecedor_nome",
            "fornecedor_cnpj",
            "gestores",
            "url_pncp",
            "id_interno",
            "categoria_original",
            "ano",
            "valor_numerico",
            "data_extracao",
            "ingested_at",
        ]
    ]

    before = connection.total_changes
    connection.executemany(
        f"""
        INSERT OR IGNORE INTO {TABLE_NAME} (
            record_key,
            numero_contrato,
            data,
            processo_sei,
            unidade,
            valor_total,
            modalidade,
            situacao,
            objeto,
            fornecedor_nome,
            fornecedor_cnpj,
            gestores,
            url_pncp,
            id_interno,
            categoria_original,
            ano,
            valor_numerico,
            data_extracao,
            ingested_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [tuple(row) for row in rows.itertuples(index=False, name=None)],
    )
    connection.commit()
    return connection.total_changes - before


def load_existing_total(connection: sqlite3.Connection) -> int:
    row = connection.execute(f"SELECT COUNT(*) AS total FROM {TABLE_NAME}").fetchone()
    return int(row[0]) if row else 0


def save_snapshot_files(snapshot: pd.DataFrame) -> list[str]:
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    warnings: list[str] = []
    try:
        snapshot.to_parquet(SNAPSHOT_PARQUET, index=False)
    except PermissionError as exc:
        warnings.append(f"Could not write parquet snapshot: {exc}")
    try:
        snapshot.drop(columns=["ano", "valor_numerico", "data_extracao"], errors="ignore").to_csv(
            SNAPSHOT_CSV,
            index=False,
            sep=";",
            encoding="utf-8-sig",
            quoting=csv.QUOTE_ALL,
        )
    except PermissionError as exc:
        warnings.append(f"Could not write CSV snapshot: {exc}")
    return warnings


def run(download_url: str, zip_path: Path | None = None) -> dict[str, Any]:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    extraction_timestamp = datetime.now(timezone.utc).date().isoformat()
    local_zip = zip_path or (RAW_DIR / "CONTRATOS.zip")
    if zip_path is None:
        download_zip(download_url, local_zip)

    raw_df = read_contracts_csv(local_zip)
    snapshot = build_snapshot(raw_df, extraction_timestamp)
    snapshot["record_key"] = snapshot.apply(stable_key, axis=1)

    with sqlite3.connect(DB_PATH) as connection:
        ensure_database(connection)
        existing_before = load_existing_total(connection)
        new_rows = insert_new_rows(connection, snapshot)
        existing_after = load_existing_total(connection)

    snapshot_warnings = save_snapshot_files(snapshot)

    return {
        "zip_path": str(local_zip),
        "database_path": str(DB_PATH),
        "snapshot_parquet": str(SNAPSHOT_PARQUET),
        "snapshot_csv": str(SNAPSHOT_CSV),
        "rows_in_snapshot": int(len(snapshot)),
        "rows_already_present": int(existing_before),
        "rows_inserted": int(new_rows),
        "rows_after": int(existing_after),
        "warnings": snapshot_warnings,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Download the official RJ contracts zip and sync new rows to SQLite.")
    parser.add_argument("--download-url", default=DEFAULT_URL)
    parser.add_argument("--zip-path", default=None, help="Use an existing CONTRATOS.zip instead of downloading it.")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    zip_path = Path(args.zip_path) if args.zip_path else None
    result = run(args.download_url, zip_path=zip_path)
    print(
        f"Snapshot={result['rows_in_snapshot']:,} | "
        f"Inserted={result['rows_inserted']:,} | "
        f"Total={result['rows_after']:,}"
    )
    print(f"DB: {result['database_path']}")
    print(f"Parquet: {result['snapshot_parquet']}")
    for warning in result["warnings"]:
        print(f"WARNING: {warning}")


if __name__ == "__main__":
    main()
