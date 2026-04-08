from __future__ import annotations

import pandas as pd

from scripts.sync_official_contracts import build_snapshot


def test_build_snapshot_handles_missing_optional_columns() -> None:
    raw_df = pd.DataFrame(
        {
            "contratacao": ["CT-001"],
            "unidade": ["SEFAZ"],
        }
    )

    snapshot = build_snapshot(raw_df, "2026-04-08")

    assert list(snapshot["numero_contrato"]) == ["CT-001"]
    assert list(snapshot["unidade"]) == ["SEFAZ"]
    assert "valor_total" in snapshot.columns
    assert "data" in snapshot.columns
    assert snapshot.loc[0, "valor_total"] is None
    assert pd.isna(snapshot.loc[0, "data"])
