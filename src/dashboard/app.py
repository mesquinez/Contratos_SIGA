from __future__ import annotations

import csv
import sqlite3
from pathlib import Path

import pandas as pd
import streamlit as st


BASE_DIR = Path(__file__).resolve().parents[2]
LEGACY_DATABASE_PATH = BASE_DIR / "data" / "database" / "contratos.db"
LEGACY_TABLE_NAME = "contratos"
HISTORICAL_DATABASE_PATH = BASE_DIR / "data" / "database" / "contratos_historico.db"
HISTORICAL_TABLE_NAME = "contratos_oficiais"
PARQUET_CANDIDATES = [
    BASE_DIR / "data" / "processed" / "contratos_rio_grande_oficial.parquet",
    BASE_DIR / "data" / "processed" / "contratos_rio_grande_consolidado.parquet",
]


DISPLAY_COLUMNS = [
    "numero_contrato",
    "data",
    "ano",
    "processo_sei",
    "unidade",
    "situacao",
    "modalidade",
    "fornecedor_nome",
    "fornecedor_cnpj",
    "valor_total",
    "gestores",
    "url_pncp",
    "id_interno",
    "categoria_original",
    "objeto",
]


def format_brl(value: float | int | None) -> str:
    if value is None or pd.isna(value):
        return "R$ 0,00"
    formatted = f"{float(value):,.2f}"
    return f"R$ {formatted.replace(',', 'X').replace('.', ',').replace('X', '.')}"


def quote_identifier(name: str) -> str:
    return f'"{name.replace(chr(34), chr(34) * 2)}"'


def get_parquet_path() -> Path:
    for candidate in PARQUET_CANDIDATES:
        if candidate.exists():
            return candidate
    return PARQUET_CANDIDATES[-1]


def database_has_table(database_path: Path, table_name: str) -> bool:
    if not database_path.exists():
        return False
    with sqlite3.connect(database_path) as connection:
        row = connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            (table_name,),
        ).fetchone()
    return row is not None


def pick_first_available(columns: list[str], *candidates: str) -> str | None:
    for candidate in candidates:
        if candidate in columns:
            return candidate
    return None


@st.cache_resource(show_spinner=False)
def initialize_legacy_database(parquet_signature: tuple[float, int] | None = None) -> str:
    parquet_path = get_parquet_path()
    if not parquet_path.exists():
        raise FileNotFoundError(f"Arquivo Parquet nao encontrado em {parquet_path}")

    LEGACY_DATABASE_PATH.parent.mkdir(parents=True, exist_ok=True)

    df = pd.read_parquet(parquet_path)
    df = df.dropna(axis=1, how="all").copy()

    if "data" in df.columns:
        df["data"] = pd.to_datetime(df["data"], errors="coerce")
        df["ano"] = df["data"].dt.year.astype("Int64")
        df["data"] = df["data"].dt.strftime("%Y-%m-%d")
        df.loc[df["data"] == "NaT", "data"] = None
    else:
        df["ano"] = pd.Series([pd.NA] * len(df), dtype="Int64")

    if "valor_total" in df.columns:
        df["valor_total"] = pd.to_numeric(df["valor_total"], errors="coerce")

    with sqlite3.connect(LEGACY_DATABASE_PATH) as connection:
        df.to_sql(LEGACY_TABLE_NAME, connection, if_exists="replace", index=False)

        indexed_columns = {"unidade", "situacao", "ano", "fornecedor_nome", "valor_total"}
        available_index_columns = indexed_columns.intersection(df.columns)
        for column in available_index_columns:
            connection.execute(
                f"CREATE INDEX IF NOT EXISTS idx_{LEGACY_TABLE_NAME}_{column} "
                f"ON {quote_identifier(LEGACY_TABLE_NAME)} ({quote_identifier(column)})"
            )
        connection.commit()

    return str(LEGACY_DATABASE_PATH)


def get_parquet_signature() -> tuple[float, int]:
    stat = get_parquet_path().stat()
    return (stat.st_mtime, stat.st_size)


def get_active_source() -> tuple[Path, str]:
    if database_has_table(HISTORICAL_DATABASE_PATH, HISTORICAL_TABLE_NAME):
        return HISTORICAL_DATABASE_PATH, HISTORICAL_TABLE_NAME
    initialize_legacy_database(get_parquet_signature())
    return LEGACY_DATABASE_PATH, LEGACY_TABLE_NAME


def get_active_source_cache_key() -> tuple[str, float, int]:
    database_path, _table_name = get_active_source()
    if database_path == HISTORICAL_DATABASE_PATH and database_path.exists():
        stat = database_path.stat()
        return ("historical", stat.st_mtime, stat.st_size)

    parquet_stat = get_parquet_path().stat()
    return ("legacy", parquet_stat.st_mtime, parquet_stat.st_size)


def get_connection() -> tuple[sqlite3.Connection, str]:
    database_path, table_name = get_active_source()
    connection = sqlite3.connect(database_path, check_same_thread=False)
    connection.row_factory = sqlite3.Row
    return connection, table_name


@st.cache_data(show_spinner=False)
def get_table_columns(source_cache_key: tuple[str, float, int]) -> list[str]:
    connection, table_name = get_connection()
    with connection:
        rows = connection.execute(f"PRAGMA table_info({quote_identifier(table_name)})").fetchall()
    return [row["name"] for row in rows]


@st.cache_data(show_spinner=False)
def get_filter_options(source_cache_key: tuple[str, float, int]) -> tuple[list[str], list[str], int, int]:
    connection, table_name = get_connection()
    with connection:
        unidades = [
            row["unidade"]
            for row in connection.execute(
                f"""
                SELECT DISTINCT unidade
                FROM {quote_identifier(table_name)}
                WHERE unidade IS NOT NULL AND TRIM(unidade) <> ''
                ORDER BY unidade
                """
            ).fetchall()
        ]
        situacoes = [
            row["situacao"]
            for row in connection.execute(
                f"""
                SELECT DISTINCT situacao
                FROM {quote_identifier(table_name)}
                WHERE situacao IS NOT NULL AND TRIM(situacao) <> ''
                ORDER BY situacao
                """
            ).fetchall()
        ]
        year_row = connection.execute(
            f"""
            SELECT MIN(ano) AS min_ano, MAX(ano) AS max_ano
            FROM {quote_identifier(table_name)}
            WHERE ano IS NOT NULL
            """
        ).fetchone()

    min_year = int(year_row["min_ano"]) if year_row["min_ano"] is not None else 0
    max_year = int(year_row["max_ano"]) if year_row["max_ano"] is not None else 0
    return unidades, situacoes, min_year, max_year


def build_where_clause(
    unidades: list[str],
    situacoes: list[str],
    year_range: tuple[int, int] | None,
) -> tuple[str, list[object]]:
    clauses: list[str] = []
    params: list[object] = []

    if unidades:
        placeholders = ", ".join(["?"] * len(unidades))
        clauses.append(f"unidade IN ({placeholders})")
        params.extend(unidades)

    if situacoes:
        placeholders = ", ".join(["?"] * len(situacoes))
        clauses.append(f"situacao IN ({placeholders})")
        params.extend(situacoes)

    if year_range:
        clauses.append("ano BETWEEN ? AND ?")
        params.extend([int(year_range[0]), int(year_range[1])])

    where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    return where_sql, params


@st.cache_data(show_spinner=False)
def run_filtered_query(
    unidades: tuple[str, ...],
    situacoes: tuple[str, ...],
    year_range: tuple[int, int] | None,
    source_cache_key: tuple[str, float, int],
) -> tuple[pd.DataFrame, int, float, pd.DataFrame, bool]:
    where_sql, params = build_where_clause(list(unidades), list(situacoes), year_range)

    available_columns = get_table_columns(source_cache_key)
    selected_columns = [column for column in DISPLAY_COLUMNS if column in available_columns]
    if not selected_columns:
        selected_columns = available_columns

    select_sql = ", ".join(quote_identifier(column) for column in selected_columns)
    vendor_column = pick_first_available(available_columns, "fornecedor_nome", "fornecedor")

    connection, table_name = get_connection()
    with connection:
        metrics = connection.execute(
            f"""
            SELECT
                COUNT(*) AS total_contratos,
                COALESCE(SUM(valor_total), 0) AS volume_total
            FROM {quote_identifier(table_name)}
            {where_sql}
            """,
            params,
        ).fetchone()

        table_df = pd.read_sql_query(
            f"""
            SELECT {select_sql}
            FROM {quote_identifier(table_name)}
            {where_sql}
            ORDER BY
                CASE WHEN data IS NULL THEN 1 ELSE 0 END,
                data DESC,
                valor_total DESC
            """,
            connection,
            params=params,
        )

        if vendor_column:
            vendor_df = pd.read_sql_query(
                f"""
                SELECT
                    COALESCE(
                        NULLIF(TRIM({quote_identifier(vendor_column)}), ''),
                        'Fornecedor nao informado'
                    ) AS fornecedor_nome,
                    COUNT(*) AS contratos,
                    COALESCE(SUM(valor_total), 0) AS volume_total
                FROM {quote_identifier(table_name)}
                {where_sql}
                GROUP BY 1
                ORDER BY volume_total DESC, contratos DESC
                LIMIT 5
                """,
                connection,
                params=params,
            )
        else:
            vendor_df = pd.DataFrame(columns=["fornecedor_nome", "contratos", "volume_total"])

    return (
        table_df,
        int(metrics["total_contratos"]),
        float(metrics["volume_total"] or 0),
        vendor_df,
        vendor_column is not None,
    )


def dataframe_to_csv_bytes(df: pd.DataFrame) -> bytes:
    export_df = df.copy()
    if "valor_total" in export_df.columns:
        export_df["valor_total"] = pd.to_numeric(export_df["valor_total"], errors="coerce")
    return export_df.to_csv(
        index=False,
        sep=";",
        encoding="utf-8-sig",
        quoting=csv.QUOTE_ALL,
        lineterminator="\n",
    ).encode("utf-8-sig")


def apply_page_style() -> None:
    st.markdown(
        """
        <style>
        .stApp {
            background:
                radial-gradient(circle at top left, rgba(20, 157, 221, 0.16), transparent 28%),
                radial-gradient(circle at top right, rgba(20, 184, 166, 0.12), transparent 22%),
                linear-gradient(180deg, #f6fbff 0%, #eef4f7 100%);
        }
        .block-container {
            padding-top: 2rem;
            padding-bottom: 2rem;
        }
        .metric-card {
            background: rgba(255, 255, 255, 0.88);
            border: 1px solid rgba(15, 23, 42, 0.08);
            border-radius: 18px;
            padding: 1.2rem 1rem;
            box-shadow: 0 12px 30px rgba(15, 23, 42, 0.06);
            backdrop-filter: blur(6px);
        }
        .metric-label {
            color: #486581;
            font-size: 0.9rem;
            margin-bottom: 0.35rem;
        }
        .metric-value {
            color: #102a43;
            font-size: 1.8rem;
            font-weight: 700;
            line-height: 1.1;
        }
        .section-card {
            background: rgba(255, 255, 255, 0.78);
            border: 1px solid rgba(15, 23, 42, 0.08);
            border-radius: 18px;
            padding: 1rem 1.1rem 0.6rem 1.1rem;
            box-shadow: 0 12px 30px rgba(15, 23, 42, 0.05);
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def render_metric_card(title: str, value: str) -> None:
    st.markdown(
        f"""
        <div class="metric-card">
            <div class="metric-label">{title}</div>
            <div class="metric-value">{value}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def main() -> None:
    st.set_page_config(
        page_title="Dashboard de Contratos do RJ",
        page_icon=":bar_chart:",
        layout="wide",
        initial_sidebar_state="expanded",
    )
    apply_page_style()

    st.title("Dashboard de Contratos do Estado do Rio de Janeiro")
    st.caption("Explore contratos consolidados, aplique filtros e exporte os resultados com seguranca.")

    try:
        get_active_source()
    except Exception as exc:
        st.error(f"Nao foi possivel inicializar a base local SQLite: {exc}")
        st.stop()

    source_cache_key = get_active_source_cache_key()
    unidades, situacoes, min_year, max_year = get_filter_options(source_cache_key)
    has_years = min_year != 0 and max_year != 0
    default_year_range = (min_year, max_year) if has_years else None

    st.sidebar.header("Filtros")
    selected_unidades = st.sidebar.multiselect("Unidade (Orgao)", unidades)
    selected_situacoes = st.sidebar.multiselect("Situacao (Status)", situacoes)
    selected_year_range = (
        st.sidebar.slider(
            "Ano",
            min_value=min_year,
            max_value=max_year,
            value=default_year_range,
        )
        if has_years
        else None
    )

    table_df, total_contracts, total_volume, top_vendors, has_vendor_data = run_filtered_query(
        tuple(selected_unidades),
        tuple(selected_situacoes),
        selected_year_range,
        source_cache_key,
    )

    kpi_col1, kpi_col2 = st.columns(2)
    with kpi_col1:
        render_metric_card("Total de Contratos", f"{total_contracts:,}".replace(",", "."))
    with kpi_col2:
        render_metric_card("Volume Financeiro", format_brl(total_volume))

    insights_col, export_col = st.columns([2, 1])
    with insights_col:
        st.markdown('<div class="section-card">', unsafe_allow_html=True)
        st.subheader("Consultancy Insights")
        if not has_vendor_data:
            st.info("A base atual nao possui coluna de fornecedor disponivel para montar o ranking.")
        elif top_vendors.empty:
            st.info("Nenhum fornecedor encontrado para os filtros selecionados.")
        else:
            top_vendors["volume_total"] = top_vendors["volume_total"].apply(format_brl)
            st.dataframe(
                top_vendors.rename(
                    columns={
                        "fornecedor_nome": "Fornecedor",
                        "contratos": "Qtd. Contratos",
                        "volume_total": "Volume Total",
                    }
                ),
                use_container_width=True,
                hide_index=True,
            )
        st.markdown("</div>", unsafe_allow_html=True)

    with export_col:
        st.markdown('<div class="section-card">', unsafe_allow_html=True)
        st.subheader("Exportacao")
        st.write("Baixe exatamente o recorte atual, pronto para abrir em Excel ou BI.")
        st.download_button(
            label="Download Filtered Data as CSV",
            data=dataframe_to_csv_bytes(table_df),
            file_name="contratos_filtrados.csv",
            mime="text/csv",
            use_container_width=True,
        )
        st.markdown("</div>", unsafe_allow_html=True)

    st.subheader("Resultados Filtrados")
    if table_df.empty:
        st.warning("Nenhum contrato encontrado com os filtros atuais.")
    else:
        table_view = table_df.copy()
        if "valor_total" in table_view.columns:
            table_view["valor_total"] = table_view["valor_total"].apply(format_brl)

        column_labels = {
            "numero_contrato": "Numero do Contrato",
            "data": "Data",
            "ano": "Ano",
            "processo_sei": "Processo SEI",
            "unidade": "Unidade",
            "situacao": "Situacao",
            "modalidade": "Modalidade",
            "fornecedor_nome": "Fornecedor",
            "fornecedor_cnpj": "CNPJ",
            "valor_total": "Valor Total",
            "gestores": "Gestores",
            "url_pncp": "URL PNCP",
            "id_interno": "ID Interno",
            "categoria_original": "Categoria Original",
            "objeto": "Objeto",
        }
        table_view = table_view.rename(columns=column_labels)

        st.dataframe(
            table_view,
            use_container_width=True,
            hide_index=True,
            column_config={
                "URL PNCP": st.column_config.LinkColumn("URL PNCP"),
            },
        )


if __name__ == "__main__":
    main()
