from __future__ import annotations

import re
from typing import Any, Optional

from src.core.base_scraper import BaseScraper


class ContractsScraper(BaseScraper):
    """Contracts scraper implementation using /Contrato/paginate.action."""

    DEFAULT_FORM_FIELDS: dict[str, Any] = {
        "campoPesquisa_1": "",
        "campoPesquisa_2": None,
        "campoPesquisa_3": None,
        "cnpjFornecedor": "",
        "condicao_2": None,
        "condicao_3": None,
        "dsFormaLicitacao": "",
        "dtContratacao": None,
        "dtContratacaoStr": None,
        "dtFimVigenciaContrato": None,
        "dtFimVigenciaContratoStr": "",
        "dtIniVigenciaContrato": None,
        "dtIniVigenciaContratoStr": "",
        "filtroDtContratacao": "",
        "filtroModalidade": "",
        "filtroNuChaveContrato": "",
        "filtroProcesso": "",
        "filtroUnidade": "",
        "filtroVlTotalContratado": "",
        "filtroVlTotalExecutado": "",
        "gestorContrato": None,
        "idArtigo": None,
        "idClasse": None,
        "idContrato": None,
        "idFamilia": None,
        "idTipo": None,
        "idTipoLicitacao": None,
        "length2": 0,
        "length3": 0,
        "modalidade": "",
        "naturezaDespesa": "",
        "nuChaveContrato": "",
        "objContrato": "",
        "orderColumn2": None,
        "orderColumn3": None,
        "orderDirection2": None,
        "orderDirection3": None,
        "processo": "",
        "qtdAditivada": None,
        "qtdOriginal": None,
        "razaoFornecedor": "",
        "sdf": None,
        "sdf2": None,
        "sdfUTC": None,
        "start2": 0,
        "start3": 0,
        "sustentavel": False,
        "termoPesquisaItem_1": "",
        "termoPesquisaItem_2": None,
        "termoPesquisaItem_3": None,
        "tipoPesquisa_1": "",
        "tipoPesquisa_2": None,
        "tipoPesquisa_3": None,
        "tipoRelatorio": None,
        "total": None,
        "totalRegistro": None,
        "vlEmpenho": None,
        "vlLiquidado": None,
        "vlPago": None,
        "vlTotalContratado": None,
        "vlTotalCorrenteMaximo": None,
        "vlTotalCorrenteMinimo": None,
        "vlTotalEmpenhado": None,
        "vlTotalExecutado": "",
        "vlTotalLiquidado": None,
        "vlTotalPago": None,
        "vlUnitAditivado": None,
        "vlUnitOriginal": None,
    }

    def __init__(
        self,
        *args: Any,
        situacao: str = "Ativo",
        unidade: Optional[str] = None,
        static_form_fields: Optional[dict[str, Any]] = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.situacao = situacao
        self.unidade = unidade
        self.static_form_fields = static_form_fields or {}

    def get_endpoint(self) -> str:
        return "/Contrato/paginate.action"

    def get_http_method(self) -> str:
        return "POST"

    def get_headers(self) -> dict[str, str]:
        return {
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json",
        }

    def build_request_payload(
        self,
        start: int,
        length: int,
        situacao: Optional[str] = None,
        unidade: Optional[str] = None,
    ) -> dict[str, Any]:
        situacao_value = situacao if situacao is not None else self.situacao
        unidade_value = unidade if unidade is not None else self.unidade

        payload: dict[str, Any] = {
            "draw": 1,
            "start": start,
            "length": length,
            "orderColumn": 0,
            "orderDirection": "desc",
            "columns[0][data]": "0",
            "search[value]": "",
            "situacaoContrato": situacao_value,
            "unidade": "" if unidade_value in (None, "") else unidade_value,
        }

        payload.update(self.DEFAULT_FORM_FIELDS)
        payload.update(self.static_form_fields)
        return payload

    def parse_response(self, json_data: Any) -> tuple[list[dict[str, Any]], Optional[int]]:
        if not isinstance(json_data, dict):
            return [], None

        rich_mapped = self._parse_rich_records(json_data.get("listarContratos"))
        simple_mapped = self._parse_simple_records(json_data.get("data"))
        records = self._merge_records(rich_mapped, simple_mapped)
        return records, self._extract_total(json_data)

    def _parse_rich_records(self, raw_records: Any) -> list[dict[str, Any]]:
        if not isinstance(raw_records, list):
            return []
        mapped: list[dict[str, Any]] = []
        for row in raw_records:
            item = self._map_rich_row(row)
            if item is not None:
                mapped.append(item)
        return mapped

    def _parse_simple_records(self, raw_records: Any) -> list[dict[str, Any]]:
        if not isinstance(raw_records, list):
            return []
        mapped: list[dict[str, Any]] = []
        for row in raw_records:
            item = self._map_simple_row(row)
            if item is not None:
                mapped.append(item)
        return mapped

    def _merge_records(
        self,
        rich_rows: list[dict[str, Any]],
        simple_rows: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        if not rich_rows:
            return simple_rows
        if not simple_rows:
            return rich_rows

        merged: list[dict[str, Any]] = []
        rich_by_key = {self._row_key(row): row for row in rich_rows if self._row_key(row) is not None}
        used_rich_keys: set[Any] = set()

        for simple_row in simple_rows:
            key = self._row_key(simple_row)
            rich_row = rich_by_key.get(key) if key is not None else None
            if rich_row is not None:
                used_rich_keys.add(key)
                merged.append(self._overlay_row(simple_row, rich_row))
            else:
                merged.append(simple_row)

        # Include rich-only rows that were not matched (prevents data loss).
        for rich_row in rich_rows:
            key = self._row_key(rich_row)
            if key is not None and key in used_rich_keys:
                continue
            merged.append(rich_row)

        return merged

    @staticmethod
    def _overlay_row(base_row: dict[str, Any], overlay_row: dict[str, Any]) -> dict[str, Any]:
        out = dict(base_row)
        for key, value in overlay_row.items():
            if value not in (None, ""):
                out[key] = value
        return out

    @staticmethod
    def _row_key(row: dict[str, Any]) -> Any:
        id_interno = ContractsScraper._normalize_key_part(row.get("id_interno"))
        numero_contrato = ContractsScraper._normalize_key_part(row.get("numero_contrato"))
        processo_sei = ContractsScraper._normalize_key_part(row.get("processo_sei"))

        if id_interno is not None:
            return ("id", id_interno)
        if numero_contrato is not None and processo_sei is not None:
            return ("np", numero_contrato, processo_sei)
        if numero_contrato is not None:
            return ("n", numero_contrato)
        return None

    @staticmethod
    def _normalize_key_part(value: Any) -> Optional[str]:
        if value in (None, ""):
            return None
        text = str(value).strip()
        return text or None

    def _map_rich_row(self, row: Any) -> Optional[dict[str, Any]]:
        if not isinstance(row, dict):
            return None

        numero_contrato = self._pick(row, "nuChaveContrato", "numeroContrato", "numero")
        processo_sei = self._pick(row, "processo", "processoSei", "nuProcesso")
        data_val = self._pick(row, "dtContratacaoStr", "data", "dtContratacao")
        unidade = self._pick(row, "unidade", "nmUnidade")
        valor_total = self._pick(row, "vlTotalContratadoStr", "valorTotal", "vlContrato")
        modalidade = self._pick(row, "modalidade", "modalidadeLicitacao")
        situacao = self._pick(row, "situacaoContrato", "situacao", default=self.situacao)
        objeto = self._pick(row, "objContrato", "objeto", "descricaoObjeto")
        fornecedor_cnpj = self._pick(row, "cnpjFornecedor", "cnpj", "nuCnpjFornecedor")
        fornecedor_nome = self._pick(row, "razaoFornecedor", "fornecedor", "nomeFornecedor")
        url_pncp = self._pick(row, "urlPncp", "urlPNCP")
        id_interno = self._pick(row, "idContrato", "id", "codigo")

        gestores_raw = row.get("listaGestores")
        if gestores_raw is None:
            gestores_raw = row.get("gestores")
        gestores = self._format_gestores(gestores_raw)

        return {
            "numero_contrato": numero_contrato,
            "data": data_val,
            "processo_sei": processo_sei,
            "unidade": unidade,
            "valor_total": valor_total,
            "modalidade": modalidade,
            "situacao": situacao,
            "objeto": objeto,
            "fornecedor_nome": fornecedor_nome,
            "fornecedor_cnpj": fornecedor_cnpj,
            "gestores": gestores,
            "url_pncp": url_pncp,
            "id_interno": id_interno,
        }

    def _map_simple_row(self, row: Any) -> Optional[dict[str, Any]]:
        if isinstance(row, dict):
            return self._map_rich_row(row)

        if not isinstance(row, list):
            return None
        if len(row) < 11:
            return None

        return {
            "numero_contrato": row[0],
            "data": row[1],
            "processo_sei": row[2],
            "unidade": row[3],
            "valor_total": row[4],
            "modalidade": row[6],
            "situacao": self.situacao,
            "objeto": None,
            "fornecedor_nome": None,
            "fornecedor_cnpj": None,
            "gestores": self._format_gestores(row[7]),
            "url_pncp": row[8],
            "id_interno": row[10],
        }

    @staticmethod
    def _pick(row: dict[str, Any], *keys: str, default: Any = None) -> Any:
        for key in keys:
            value = row.get(key)
            if value not in (None, ""):
                return value
        return default

    @staticmethod
    def _format_gestores(raw_value: Any) -> str:
        if raw_value is None:
            return ""

        if isinstance(raw_value, list):
            values = [str(item).strip() for item in raw_value if str(item).strip()]
            return ", ".join(values)

        text = str(raw_value).strip()
        if not text:
            return ""

        text = text.replace("<br \\/>", "<br/>")
        text = re.sub(r"<br\s*/?>", ", ", text, flags=re.IGNORECASE)
        text = re.sub(r"\s*,\s*", ", ", text)
        text = re.sub(r"\s{2,}", " ", text).strip(" ,")
        return text

    @staticmethod
    def _extract_total(payload: dict[str, Any]) -> Optional[int]:
        for key in (
            "recordsTotal",
            "recordsFiltered",
            "iTotalRecords",
            "iTotalDisplayRecords",
            "totalElements",
            "total",
            "qtdRegistros",
            "quantidadeTotal",
        ):
            value = payload.get(key)
            if value is None:
                continue
            try:
                return int(value)
            except (TypeError, ValueError):
                continue
        return None
