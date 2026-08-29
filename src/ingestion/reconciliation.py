"""Validações de qualidade e reconciliação pós-carga (R8)."""
from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


@dataclass
class ReconciliationResult:
    qtd_origem: int
    qtd_destino: int
    pct_divergencia: float
    pct_nulos_chave: float
    duplicados_no_lote: int
    aprovado: bool


def reconcile(
    df_landing_batch: DataFrame,
    qtd_origem: int,
    qtd_gravada: int,
    max_divergence_pct: float,
) -> ReconciliationResult:
    """Contagem origem x destino, % de nulos em `_source_id` e duplicidade
    de `_source_id` dentro do mesmo lote. A execução é marcada PARTIAL
    quando a divergência ultrapassa o limiar configurado em
    `reconciliation.max_divergence_pct` (pipeline_config.yaml)."""
    total = df_landing_batch.count()

    nulos_chave = df_landing_batch.filter(
        F.col("_source_id").isNull() | (F.col("_source_id") == "")
    ).count()
    pct_nulos = (100.0 * nulos_chave / total) if total else 0.0

    duplicados = (
        df_landing_batch.groupBy("_source_id")
        .count()
        .filter("count > 1")
        .count()
    )

    divergencia = abs(qtd_origem - qtd_gravada)
    pct_divergencia = (100.0 * divergencia / qtd_origem) if qtd_origem else 0.0

    aprovado = pct_divergencia <= max_divergence_pct

    return ReconciliationResult(
        qtd_origem=qtd_origem,
        qtd_destino=qtd_gravada,
        pct_divergencia=pct_divergencia,
        pct_nulos_chave=pct_nulos,
        duplicados_no_lote=duplicados,
        aprovado=aprovado,
    )
