"""Job de ingestão: MongoDB (sample_mflix) -> Landing.

Lê a configuração externa (config/pipeline_config.yaml e
config/collections.json), percorre TODAS as coleções configuradas com o
MESMO código (R1) e grava cada uma na camada Landing, em lotes, com
retry/backoff e projection pushdown (R2).

Pode ser executado de duas formas:
  1. Como uma task de Databricks Job, lendo parâmetros via widgets
     (ver `main()` / config/databricks_job.yml).
  2. Chamando `run(...)` diretamente a partir de um notebook de
     desenvolvimento (ver notebooks/01_run_pipeline.py).
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

from ingestion.config import CollectionConfig, PipelineConfig, load_collections_config, load_pipeline_config
from ingestion.control import ControlLogger, ExecutionResult, new_ingestion_id
from ingestion.extractor import documents_to_rows, extract_batches
from ingestion.landing_writer import rows_to_dataframe, with_audit_columns, write_landing
from ingestion.mongo_connector import MongoConnector

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
STAGE = "landing"


@dataclass
class CollectionRunSummary:
    collection: str
    qtd_lida_origem: int
    watermark_inicial: Optional[str]
    watermark_final: Optional[str]
    status: str
    mensagem_erro: Optional[str] = None


def run_collection(
    spark: SparkSession,
    connector: MongoConnector,
    pipeline_cfg: PipelineConfig,
    coll_cfg: CollectionConfig,
    control: ControlLogger,
    ingestion_id: str,
) -> CollectionRunSummary:
    start_time = datetime.now(timezone.utc)
    watermark_inicial = control.get_last_watermark(coll_cfg.name) if coll_cfg.is_incremental else None
    watermark_maximo = watermark_inicial

    total_lida = 0
    total_gravada = 0
    error_message = None
    status = "SUCCESS"

    try:
        for batch in extract_batches(
            connector=connector,
            cfg=coll_cfg,
            last_watermark=watermark_inicial,
            batch_size=pipeline_cfg.batch_size,
            max_retries=pipeline_cfg.max_retries,
            backoff_base_seconds=pipeline_cfg.backoff_base_seconds,
        ):
            rows = documents_to_rows(batch)
            total_lida += len(rows)

            if coll_cfg.is_incremental and batch.watermark_values:
                valores = [v for v in batch.watermark_values if v is not None]
                if valores:
                    lote_max = max(valores)
                    if watermark_maximo is None or str(lote_max) > str(watermark_maximo):
                        watermark_maximo = lote_max

            if not rows:
                continue

            df = rows_to_dataframe(spark, rows)
            df = with_audit_columns(
                df,
                ingestion_id=ingestion_id,
                load_type=coll_cfg.load_type,
                source_path=f"mongodb_atlas:{pipeline_cfg.database}.{coll_cfg.name}",
            )
            write_landing(df, pipeline_cfg, coll_cfg.name, len(rows))
            total_gravada += len(rows)

        # coleção vazia (ex.: sessions) é um resultado válido, não uma falha
        if total_lida == 0:
            logger.info("Coleção %s sem documentos novos.", coll_cfg.name)

    except Exception as exc:  # falha permanente após esgotar os retries
        logger.exception("Falha na ingestão da coleção %s", coll_cfg.name)
        status = "FAILED"
        error_message = str(exc)[:2000]

    end_time = datetime.now(timezone.utc)

    # watermark só avança em execução SUCCESS - garante reprocessamento
    # seguro em caso de falha parcial, sem "pular" documentos
    if coll_cfg.is_incremental and status == "SUCCESS" and watermark_maximo is not None:
        control.persist_watermark(coll_cfg.name, str(watermark_maximo))

    control.log(ExecutionResult(
        ingestion_id=ingestion_id,
        collection=coll_cfg.name,
        stage=STAGE,
        load_type=coll_cfg.load_type,
        watermark_inicial=str(watermark_inicial) if watermark_inicial is not None else None,
        watermark_final=str(watermark_maximo) if watermark_maximo is not None else None,
        qtd_lida_origem=total_lida,
        qtd_gravada_destino=total_gravada,
        start_time=start_time,
        end_time=end_time,
        status=status,
        mensagem_erro=error_message,
    ))

    return CollectionRunSummary(
        collection=coll_cfg.name,
        qtd_lida_origem=total_lida,
        watermark_inicial=str(watermark_inicial) if watermark_inicial is not None else None,
        watermark_final=str(watermark_maximo) if watermark_maximo is not None else None,
        status=status,
        mensagem_erro=error_message,
    )


def run(
    spark: SparkSession,
    mongo_uri: str,
    pipeline_config_path: str,
    collections_config_path: str,
    only_collections: Optional[list] = None,
):
    """Ponto de entrada reutilizável (por notebook ou por job). Retorna o
    `ingestion_id` gerado nesta execução (para o bronze_job consumir) e o
    resumo por coleção."""
    pipeline_cfg = load_pipeline_config(pipeline_config_path)
    collections = load_collections_config(collections_config_path)
    if only_collections:
        collections = [c for c in collections if c.name in only_collections]

    control = ControlLogger(spark, pipeline_cfg)
    ingestion_id = new_ingestion_id()

    summaries = []
    with MongoConnector(
        uri=mongo_uri,
        database=pipeline_cfg.database,
        server_selection_timeout_ms=pipeline_cfg.server_selection_timeout_ms,
        socket_timeout_ms=pipeline_cfg.socket_timeout_ms,
        app_name=pipeline_cfg.app_name,
    ) as connector:
        for coll_cfg in collections:
            summary = run_collection(spark, connector, pipeline_cfg, coll_cfg, control, ingestion_id)
            summaries.append(summary)

    return ingestion_id, summaries


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


def main() -> None:
    """Entrypoint usado quando este arquivo roda como task de Databricks
    Job (widgets injetados pelo orquestrador)."""
    spark = SparkSession.builder.getOrCreate()
    dbutils = _get_dbutils(spark)

    pipeline_config_path = dbutils.widgets.get("pipeline_config_path")
    collections_config_path = dbutils.widgets.get("collections_config_path")
    only_collections_raw = _widget_or_none(dbutils, "collections")
    only_collections = only_collections_raw.split(",") if only_collections_raw else None

    pipeline_cfg = load_pipeline_config(pipeline_config_path)
    mongo_uri = dbutils.secrets.get(scope=pipeline_cfg.secret_scope, key=pipeline_cfg.secret_key)

    ingestion_id, summaries = run(
        spark, mongo_uri, pipeline_config_path, collections_config_path, only_collections
    )

    # taskValues: a próxima task (bronze_job) recupera o ingestion_id sem
    # precisar de tabela de controle intermediária (Aula 5, seção 5.3)
    dbutils.jobs.taskValues.set(key="ingestion_id", value=ingestion_id)
    dbutils.jobs.taskValues.set(key="linhas_novas", value=sum(s.qtd_lida_origem for s in summaries))

    for s in summaries:
        print(f"[{s.status}] {s.collection:<20} lidos={s.qtd_lida_origem:<8} watermark_final={s.watermark_final}")


if __name__ == "__main__":
    main()
