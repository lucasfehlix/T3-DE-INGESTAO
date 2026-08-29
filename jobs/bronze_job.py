"""Job de consolidação: Landing -> Bronze.

Lê apenas o lote recém-gravado pelo `ingestion_job` (identificado por
`_ingestion_id`, recebido via widget ou via taskValues da task anterior),
aplica dedup, quarentena e MERGE idempotente, roda a reconciliação (R8) e
fecha o registro de controle da execução (R3, R5, R6, R7).
"""
from __future__ import annotations

import logging
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

sys.path.append(str(Path(__file__).resolve().parents[1] / "src"))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from ingestion.bronze_writer import merge_into_bronze, split_quarantine, write_quarantine
from ingestion.config import CollectionConfig, PipelineConfig, load_collections_config, load_pipeline_config
from ingestion.control import ControlLogger, ExecutionResult
from ingestion.reconciliation import reconcile

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
STAGE = "bronze"


@dataclass
class BronzeRunSummary:
    collection: str
    qtd_lida_landing: int
    qtd_gravada_bronze: int
    qtd_quarentena: int
    status: str
    mensagem_erro: Optional[str] = None


def run_collection(
    spark: SparkSession,
    pipeline_cfg: PipelineConfig,
    coll_cfg: CollectionConfig,
    control: ControlLogger,
    ingestion_id: str,
) -> BronzeRunSummary:
    start_time = datetime.now(timezone.utc)
    status = "SUCCESS"
    error_message = None
    qtd_landing = qtd_bronze = qtd_quarentena = 0

    try:
        landing_table = pipeline_cfg.landing_table(coll_cfg.name)
        if not spark.catalog.tableExists(landing_table):
            raise RuntimeError(f"Tabela landing inexistente: {landing_table}")

        df_batch = spark.table(landing_table).filter(F.col("_ingestion_id") == ingestion_id)
        qtd_landing = df_batch.count()

        if qtd_landing == 0:
            logger.info(
                "Nenhum registro novo em landing para %s (ingestion_id=%s).",
                coll_cfg.name, ingestion_id,
            )
        else:
            valid, invalid = split_quarantine(df_batch)
            qtd_quarentena = write_quarantine(invalid, pipeline_cfg)
            qtd_bronze = merge_into_bronze(spark, valid, pipeline_cfg, coll_cfg.name)

            reconciliation = reconcile(
                df_landing_batch=df_batch,
                qtd_origem=qtd_landing,
                qtd_gravada=qtd_bronze + qtd_quarentena,
                max_divergence_pct=pipeline_cfg.max_divergence_pct,
            )
            if not reconciliation.aprovado:
                status = "PARTIAL"
                error_message = (
                    f"Divergência {reconciliation.pct_divergencia:.2f}% acima do limiar "
                    f"{pipeline_cfg.max_divergence_pct}%. Nulos de chave: "
                    f"{reconciliation.pct_nulos_chave:.2f}%. Duplicados no lote: "
                    f"{reconciliation.duplicados_no_lote}."
                )
                logger.warning(error_message)

    except Exception as exc:
        logger.exception("Falha na consolidação bronze da coleção %s", coll_cfg.name)
        status = "FAILED"
        error_message = str(exc)[:2000]

    end_time = datetime.now(timezone.utc)

    control.log(ExecutionResult(
        ingestion_id=ingestion_id,
        collection=coll_cfg.name,
        stage=STAGE,
        load_type=coll_cfg.load_type,
        watermark_inicial=None,
        watermark_final=None,
        qtd_lida_origem=qtd_landing,
        qtd_gravada_destino=qtd_bronze,
        start_time=start_time,
        end_time=end_time,
        status=status,
        mensagem_erro=error_message,
    ))

    return BronzeRunSummary(
        collection=coll_cfg.name,
        qtd_lida_landing=qtd_landing,
        qtd_gravada_bronze=qtd_bronze,
        qtd_quarentena=qtd_quarentena,
        status=status,
        mensagem_erro=error_message,
    )


def run(
    spark: SparkSession,
    pipeline_config_path: str,
    collections_config_path: str,
    ingestion_id: str,
    only_collections: Optional[list] = None,
):
    pipeline_cfg = load_pipeline_config(pipeline_config_path)
    collections = load_collections_config(collections_config_path)
    if only_collections:
        collections = [c for c in collections if c.name in only_collections]

    control = ControlLogger(spark, pipeline_cfg)
    return [
        run_collection(spark, pipeline_cfg, coll_cfg, control, ingestion_id)
        for coll_cfg in collections
    ]


def _get_dbutils(spark: SparkSession):
    try:
        from pyspark.dbutils import DBUtils
        return DBUtils(spark)
    except ImportError:
        import IPython
        return IPython.get_ipython().user_ns["dbutils"]


def _widget_or_none(dbutils, name: str):
    try:
        value = dbutils.widgets.get(name)
        return value or None
    except Exception:
        return None


def _widget_or_task_value(dbutils, widget_name: str, upstream_task_key: str) -> str:
    value = _widget_or_none(dbutils, widget_name)
    if value:
        return value
    return dbutils.jobs.taskValues.get(taskKey=upstream_task_key, key=widget_name, debugValue="")


def main() -> None:
    """Entrypoint usado quando este arquivo roda como task de Databricks
    Job, encadeada após `ingestion_job` (ver config/databricks_job.yml)."""
    spark = SparkSession.builder.getOrCreate()
    dbutils = _get_dbutils(spark)

    pipeline_config_path = dbutils.widgets.get("pipeline_config_path")
    collections_config_path = dbutils.widgets.get("collections_config_path")
    ingestion_id = _widget_or_task_value(dbutils, "ingestion_id", "ingestion_job")
    only_collections_raw = _widget_or_none(dbutils, "collections")
    only_collections = only_collections_raw.split(",") if only_collections_raw else None

    summaries = run(spark, pipeline_config_path, collections_config_path, ingestion_id, only_collections)

    for s in summaries:
        print(
            f"[{s.status}] {s.collection:<20} landing={s.qtd_lida_landing:<8} "
            f"bronze={s.qtd_gravada_bronze:<8} quarentena={s.qtd_quarentena}"
        )


if __name__ == "__main__":
    main()
