from __future__ import annotations

import hashlib
import json
import logging
import re
from pathlib import Path
from typing import Any

import pandas as pd


logger = logging.getLogger(__name__)


INPUT_ROOT = Path("data/raw/contracts")
OUTPUT_CSV = Path("data/processed/contratos_rj_consolidado.csv")
OUTPUT_PARQUET = Path("data/processed/contratos_rj_consolidado.parquet")

CORE_COLUMNS = [
    "numero_contrato",
    "processo",
    "objeto",
    "fornecedor",
    "fornecedor_cnpj",
    "valor_total",
    "valor_numerico",
    "unidade",
    "data",
    "modalidade",
    "situacao",
    "id_interno",
    "url_pncp",
    "gestores",
    "status_extracao",
]

# Consultancy Feature 1: keyword-based category for BI slicing.
OBJETO_TAG_RULES: dict[str, tuple[str, ...]] = {
    "TI": (
        "software",
        "sistema",
        "tecnologia",
        "informatica",
        "informática",
        "licenca",
        "licença",
        "nuvem",
        "cloud",
        "suporte tecnico",
        "suporte técnico",
        "dados",
    ),
    "SERVICOS": (
        "prestacao de servicos",
        "prestação de serviços",
        "servico",
        "serviço",
        "manutencao",
        "manutenção",
        "limpeza",
        "vigilancia",
        "vigilância",
        "consultoria",
    ),
    "OBRAS_ENGENHARIA": (
        "obra",
        "engenharia",
        "reforma",
        "construcao",
        "construção",
        "edificacao",
        "edificação",
        "pavimentacao",
        "pavimentação",
    ),
    "FORNECIMENTO_BENS": (
        "aquisição",
        "aquisicao",
        "compra",
        "fornecimento",
        "material",
        "equipamento",
        "medicamento",
        "mobiliario",
        "mobiliário",
        "uniforme",
    ),
}


def _to_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    return "" if text.lower() in {"nan", "none", "null"} else text


def _normalize_text_or_none(value: Any) -> str | None:
    text = _to_text(value)
    return text if text else None


def _normalize_gestores(raw_value: Any) -> str | None:
    if raw_value is None:
        return None
    if isinstance(raw_value, list):
        values = [_to_text(item) for item in raw_value]
        values = [v for v in values if v]
        return ", ".join(values) if values else None

    text = _to_text(raw_value)
    if not text:
        return None

    text = text.replace("<br \\/>", "<br/>")
    text = re.sub(r"<br\s*/?>", ", ", text, flags=re.IGNORECASE)
    text = re.sub(r"\s*,\s*", ", ", text)
    text = re.sub(r"\s{2,}", " ", text).strip(" ,")
    return text or None


def _pick(data: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        value = data.get(key)
        if _to_text(value):
            return value
    return None


def _parse_currency_to_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)

    text = _to_text(value)
    if not text:
        return None

    # Keep only currency-significant chars.
    text = re.sub(r"[^\d,.\-]", "", text)
    if not text:
        return None

    # BR pattern: 1.234,56 -> 1234.56
    if "," in text and "." in text:
        text = text.replace(".", "").replace(",", ".")
    elif "," in text:
        text = text.replace(",", ".")

    try:
        return float(text)
    except ValueError:
        return None


def _status_from_path(file_path: Path) -> str:
    try:
        rel = file_path.relative_to(INPUT_ROOT)
    except ValueError:
        return "desconhecido"
    return rel.parts[0] if rel.parts else "desconhecido"


def _map_rich_row(row: dict[str, Any], status_extracao: str) -> dict[str, Any]:
    numero_contrato = _pick(row, "nuChaveContrato", "numero_contrato", "numeroContrato", "numero")
    processo = _pick(row, "processo", "processo_sei", "processoSei", "nuProcesso")
    data = _pick(row, "dtContratacaoStr", "data", "dtContratacao")
    unidade = _pick(row, "unidade", "nmUnidade")
    valor_total = _pick(row, "vlTotalContratadoStr", "valor_total", "valorTotal", "vlContrato")
    modalidade = _pick(row, "modalidade", "modalidadeLicitacao")
    situacao = _pick(row, "situacaoContrato", "situacao")
    objeto = _pick(row, "objContrato", "objeto", "descricaoObjeto")
    fornecedor = _pick(row, "razaoFornecedor", "fornecedor_nome", "fornecedor", "nomeFornecedor")
    fornecedor_cnpj = _pick(row, "cnpjFornecedor", "fornecedor_cnpj", "cnpj", "nuCnpjFornecedor")
    url_pncp = _pick(row, "urlPncp", "url_pncp", "urlPNCP")
    id_interno = _pick(row, "idContrato", "id_interno", "id", "codigo")

    gestores_raw = row.get("listaGestores", row.get("gestores"))
    gestores = _normalize_gestores(gestores_raw)

    return {
        "numero_contrato": _normalize_text_or_none(numero_contrato),
        "processo": _normalize_text_or_none(processo),
        "objeto": _normalize_text_or_none(objeto),
        "fornecedor": _normalize_text_or_none(fornecedor),
        "fornecedor_cnpj": _normalize_text_or_none(fornecedor_cnpj),
        "valor_total": _normalize_text_or_none(valor_total),
        "valor_numerico": _parse_currency_to_float(valor_total),
        "unidade": _normalize_text_or_none(unidade),
        "data": _normalize_text_or_none(data),
        "modalidade": _normalize_text_or_none(modalidade),
        "situacao": _normalize_text_or_none(situacao),
        "id_interno": _normalize_text_or_none(id_interno),
        "url_pncp": _normalize_text_or_none(url_pncp),
        "gestores": gestores,
        "status_extracao": status_extracao,
    }


def _map_simple_row(row: list[Any], status_extracao: str) -> dict[str, Any] | None:
    if len(row) < 11:
        return None

    # Positional mapping from known DataTables payload.
    numero_contrato = row[0]
    data = row[1]
    processo = row[2]
    unidade = row[3]
    valor_total = row[4]
    modalidade = row[6]
    gestores = row[7]
    url_pncp = row[8]
    id_interno = row[10]

    return {
        "numero_contrato": _normalize_text_or_none(numero_contrato),
        "processo": _normalize_text_or_none(processo),
        "objeto": None,
        "fornecedor": None,
        "fornecedor_cnpj": None,
        "valor_total": _normalize_text_or_none(valor_total),
        "valor_numerico": _parse_currency_to_float(valor_total),
        "unidade": _normalize_text_or_none(unidade),
        "data": _normalize_text_or_none(data),
        "modalidade": _normalize_text_or_none(modalidade),
        "situacao": None,
        "id_interno": _normalize_text_or_none(id_interno),
        "url_pncp": _normalize_text_or_none(url_pncp),
        "gestores": _normalize_gestores(gestores),
        "status_extracao": status_extracao,
    }


def _extract_rows(payload: Any, status_extracao: str) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []

    if isinstance(payload, dict):
        # Support known wrappers and nested debug payloads.
        for key in ("records", "listarContratos", "data"):
            value = payload.get(key)
            if isinstance(value, list):
                for row in value:
                    if isinstance(row, dict):
                        records.append(_map_rich_row(row, status_extracao))
                    elif isinstance(row, list):
                        mapped = _map_simple_row(row, status_extracao)
                        if mapped is not None:
                            records.append(mapped)
            elif isinstance(value, dict):
                records.extend(_extract_rows(value, status_extracao))

        if "response" in payload:
            records.extend(_extract_rows(payload["response"], status_extracao))

    elif isinstance(payload, list):
        for row in payload:
            if isinstance(row, dict):
                records.append(_map_rich_row(row, status_extracao))
            elif isinstance(row, list):
                mapped = _map_simple_row(row, status_extracao)
                if mapped is not None:
                    records.append(mapped)

    return records


def _dedup_key(row: pd.Series) -> str | None:
    id_interno = _to_text(row.get("id_interno"))
    if id_interno:
        return f"id::{id_interno}"

    numero = _to_text(row.get("numero_contrato"))
    processo = _to_text(row.get("processo"))
    if numero or processo:
        token = f"{numero}|{processo}"
        return f"np::{hashlib.sha1(token.encode('utf-8')).hexdigest()}"
    return None


def _classify_objeto(objeto: Any) -> str:
    text = _to_text(objeto).lower()
    if not text:
        return "NAO_CLASSIFICADO"
    for tag, terms in OBJETO_TAG_RULES.items():
        if any(term in text for term in terms):
            return tag
    return "OUTROS"


def _apply_consultancy_features(df: pd.DataFrame) -> pd.DataFrame:
    # Feature 1: smart category from objeto keywords.
    df["categoria_objeto"] = df["objeto"].apply(_classify_objeto)

    # Feature 2: top 5 fornecedores by total contract value.
    fornecedor_rank_base = (
        df.fillna({"fornecedor": "SEM_FORNECEDOR"})
        .groupby("fornecedor", dropna=False)["valor_numerico"]
        .sum(min_count=1)
        .sort_values(ascending=False)
    )
    top5_fornecedores = set(fornecedor_rank_base.head(5).index.tolist())
    df["fornecedor_top5_valor"] = df["fornecedor"].fillna("SEM_FORNECEDOR").isin(top5_fornecedores)

    # Feature 3: supplier average ticket (helps spend concentration analysis).
    mean_ticket = (
        df.groupby("fornecedor", dropna=False)["valor_numerico"]
        .mean()
        .to_dict()
    )
    df["ticket_medio_fornecedor"] = df["fornecedor"].map(mean_ticket)
    return df


def _print_cheirinho(df: pd.DataFrame) -> None:
    total_contratos = len(df)
    total_volume = df["valor_numerico"].sum(min_count=1)
    total_volume = float(total_volume) if pd.notna(total_volume) else 0.0

    print("\n========== CHEIRINHO DOS DADOS ==========")
    print(f"Total de contratos unicos: {total_contratos:,}".replace(",", "."))
    print(f"Volume financeiro total (R$): {total_volume:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))

    print("\nQuebra por status_extracao:")
    status_counts = df["status_extracao"].fillna("desconhecido").value_counts(dropna=False)
    print(status_counts.to_string())

    print("\nTop 5 unidades por volume financeiro:")
    top_unidades = (
        df.fillna({"unidade": "SEM_UNIDADE"})
        .groupby("unidade", dropna=False)["valor_numerico"]
        .sum(min_count=1)
        .sort_values(ascending=False)
        .head(5)
    )
    print(top_unidades.to_string())

    print("\nAmostra final (head 5):")
    print(df.head(5).to_string(index=False))
    print("=========================================\n")


def run() -> pd.DataFrame:
    if not INPUT_ROOT.exists():
        logger.warning("Input folder does not exist: %s", INPUT_ROOT)
        empty_df = pd.DataFrame(columns=CORE_COLUMNS)
        OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
        empty_df.to_csv(OUTPUT_CSV, index=False, sep=";", encoding="utf-8-sig")
        empty_df.to_parquet(OUTPUT_PARQUET, index=False)
        return empty_df

    json_files = sorted(p for p in INPUT_ROOT.rglob("*.json") if p.is_file())
    if not json_files:
        logger.warning("No JSON files found under: %s", INPUT_ROOT)
        empty_df = pd.DataFrame(columns=CORE_COLUMNS)
        OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
        empty_df.to_csv(OUTPUT_CSV, index=False, sep=";", encoding="utf-8-sig")
        empty_df.to_parquet(OUTPUT_PARQUET, index=False)
        return empty_df

    all_rows: list[dict[str, Any]] = []
    for file_path in json_files:
        if file_path.name.startswith("debug_"):
            continue
        status_extracao = _status_from_path(file_path)
        try:
            payload = json.loads(file_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            logger.warning("Skipping invalid JSON: %s", file_path)
            continue
        all_rows.extend(_extract_rows(payload, status_extracao))

    if not all_rows:
        logger.warning("No valid contract rows extracted from %s", INPUT_ROOT)
        df = pd.DataFrame(columns=CORE_COLUMNS)
    else:
        df = pd.DataFrame(all_rows)

    for col in CORE_COLUMNS:
        if col not in df.columns:
            df[col] = None

    # Normalize key columns before dedup and analytics.
    for text_col in (
        "numero_contrato",
        "processo",
        "objeto",
        "fornecedor",
        "fornecedor_cnpj",
        "valor_total",
        "unidade",
        "data",
        "modalidade",
        "situacao",
        "id_interno",
        "url_pncp",
        "gestores",
        "status_extracao",
    ):
        df[text_col] = df[text_col].apply(_normalize_text_or_none)

    df["valor_numerico"] = df["valor_numerico"].apply(_parse_currency_to_float)
    df["data"] = pd.to_datetime(df["data"], dayfirst=True, errors="coerce").dt.strftime("%Y-%m-%d")

    df["_dedup_key"] = df.apply(_dedup_key, axis=1)
    # Keep rows with null dedup_key but drop duplicates when key exists.
    with_key = df[df["_dedup_key"].notna()].drop_duplicates(subset=["_dedup_key"], keep="first")
    without_key = df[df["_dedup_key"].isna()]
    df = pd.concat([with_key, without_key], ignore_index=True)
    df = df.drop(columns=["_dedup_key"])

    df = _apply_consultancy_features(df)

    ordered_columns = CORE_COLUMNS + [c for c in df.columns if c not in CORE_COLUMNS]
    df = df[ordered_columns]

    _print_cheirinho(df)

    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUTPUT_CSV, index=False, sep=";", encoding="utf-8-sig")
    df.to_parquet(OUTPUT_PARQUET, index=False)
    logger.info("Saved consolidated outputs to %s and %s", OUTPUT_CSV, OUTPUT_PARQUET)
    return df


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    run()
