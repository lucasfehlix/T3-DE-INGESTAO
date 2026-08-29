"""Tabela de controle de execuções (R5) e persistência de watermark entre
execuções (R3)."""
from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from .config import PipelineConfig

CONTROL_LOG_SCHEMA = (
    "_ingestion_id string, collection string, stage string, load_type string, "
    "watermark_inicial string, watermark_final string, qtd_lida_origem long, "
    "qtd_gravada_destino long, start_time timestamp, end_time timestamp, "
    "duracao_seg double, status string, mensagem_erro string"
)

WATERMARK_SCHEMA = "collection string, watermark string, updated_at timestamp"


@dataclass
class ExecutionResult:
    ingestion_id: str
    collection: str
    stage: str  # "landing" | "bronze"
    load_type: str
    watermark_inicial: Optional[str]
    watermark_final: Optional[str]
    qtd_lida_origem: int
    qtd_gravada_destino: int
    start_time: datetime
    end_time: datetime
    status: str  # SUCCESS | FAILED | PARTIAL
    mensagem_erro: Optional[str] = None

    @property
    def duracao_seg(self) -> float:
        return (self.end_time - self.start_time).total_seconds()


def new_ingestion_id() -> str:
    return str(uuid.uuid4())


class ControlLogger:
    """Fonte de verdade de R5: 'o que foi carregado, quando, por qual
    execução e com qual resultado?'"""

    def __init__(self, spark: SparkSession, pipeline_cfg: PipelineConfig):
        self.spark = spark
        self.pipeline_cfg = pipeline_cfg

    def log(self, result: ExecutionResult) -> None:
        row = [(
            result.ingestion_id, result.collection, result.stage, result.load_type,
            result.watermark_inicial, result.watermark_final,
            result.qtd_lida_origem, result.qtd_gravada_destino,
            result.start_time, result.end_time, result.duracao_seg,
            result.status, result.mensagem_erro,
        )]
        df = self.spark.createDataFrame(row, schema=CONTROL_LOG_SCHEMA)
        (
            df.write.format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .saveAsTable(self.pipeline_cfg.control_log_full_name())
        )

    def get_last_watermark(self, collection: str) -> Optional[str]:
        """Watermark persistida (R3): lida do início de cada execução
        incremental, sobrevive entre execuções porque mora em uma tabela
        Delta, não em memória/variável do notebook."""
        table = self.pipeline_cfg.watermark_full_name()
        if not self.spark.catalog.tableExists(table):
            return None
        row = (
            self.spark.table(table)
            .filter(F.col("collection") == collection)
            .orderBy(F.col("updated_at").desc())
            .select("watermark")
            .first()
        )
        return row["watermark"] if row else None

    def persist_watermark(self, collection: str, watermark: Optional[str]) -> None:
        if watermark is None:
            return
        table = self.pipeline_cfg.watermark_full_name()
        df = self.spark.createDataFrame(
            [(collection, watermark, datetime.now(timezone.utc))], schema=WATERMARK_SCHEMA
        )
        (
            df.write.format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .saveAsTable(table)
        )
