"""Escrita da camada Landing: dado bruto (JSON), append-only, com metadados
mínimos de carga - fiel à origem, sem qualquer regra de negócio.
"""
from __future__ import annotations

import math

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from .config import PipelineConfig

LANDING_SCHEMA = StructType([
    StructField("_source_id", StringType(), True),
    StructField("body", StringType(), True),
])


def rows_to_dataframe(spark: SparkSession, rows: list) -> DataFrame:
    return spark.createDataFrame(rows, schema=LANDING_SCHEMA)


def with_audit_columns(
    df: DataFrame,
    ingestion_id: str,
    load_type: str,
    source_path: str,
) -> DataFrame:
    """Adiciona as colunas de rastreabilidade mínimas exigidas (R4):
    _ingestion_id, _ingestion_timestamp, _source_path, _load_type,
    _ingestion_date. Escritas em Landing e propagadas para a Bronze."""
    now = F.current_timestamp()
    return (
        df.withColumn("_ingestion_id", F.lit(ingestion_id))
        .withColumn("_ingestion_timestamp", now)
        .withColumn("_source_path", F.lit(source_path))
        .withColumn("_load_type", F.lit(load_type))
        .withColumn("_ingestion_date", F.to_date(now))
    )


def write_landing(df: DataFrame, pipeline_cfg: PipelineConfig, collection: str, row_count: int) -> None:
    """Controle de paralelismo/partições (R2): número de partições
    calculado a partir do volume do lote e do alvo de linhas por partição,
    para evitar tanto small files (poucas linhas por arquivo) quanto skew
    (um único arquivo gigante)."""
    n_partitions = max(1, math.ceil(row_count / pipeline_cfg.target_rows_per_partition))
    table = pipeline_cfg.landing_table(collection)
    (
        df.repartition(n_partitions)
        .write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .partitionBy("_ingestion_date")
        .saveAsTable(table)
    )
