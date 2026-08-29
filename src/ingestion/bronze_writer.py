"""Consolidação Landing -> Bronze: dedup, quarentena e MERGE idempotente
(R3, R6, R7).
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

from .config import PipelineConfig


def deduplicate(df: DataFrame) -> DataFrame:
    """Mantém apenas a versão mais recente de cada _source_id dentro do
    lote (row_number sobre janela ordenada por _ingestion_timestamp desc -
    Aula 1, seção 1.3)."""
    window = Window.partitionBy("_source_id").orderBy(F.col("_ingestion_timestamp").desc())
    return (
        df.withColumn("_rn", F.row_number().over(window))
        .where("_rn = 1")
        .drop("_rn")
    )


def split_quarantine(df: DataFrame):
    """Nenhum registro é descartado silenciosamente (R7): documentos sem
    `_source_id` ou sem `body` vão para quarentena com motivo; o restante
    segue para a Bronze."""
    is_valid = F.col("_source_id").isNotNull() & (F.col("_source_id") != "") & F.col("body").isNotNull()

    valid = df.filter(is_valid)
    invalid = (
        df.filter(~is_valid)
        .withColumn(
            "motivo",
            F.when(F.col("_source_id").isNull() | (F.col("_source_id") == ""), F.lit("_source_id ausente"))
            .otherwise(F.lit("body ausente")),
        )
        .withColumn("quarentena_em", F.current_timestamp())
    )
    return valid, invalid


def write_quarantine(df: DataFrame, pipeline_cfg: PipelineConfig) -> int:
    count = df.count()
    if count == 0:
        return 0
    (
        df.write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(pipeline_cfg.quarantine_full_name())
    )
    return count


def merge_into_bronze(spark: SparkSession, df: DataFrame, pipeline_cfg: PipelineConfig, collection: str) -> int:
    """MERGE idempotente por `_source_id`: reexecutar o mesmo lote não
    duplica dado (R3). A Bronze permanece append-only em espírito - nenhuma
    regra de negócio é aplicada aqui, apenas dedup técnico pela chave de
    origem (R6)."""
    deduped = deduplicate(df)
    row_count = deduped.count()
    if row_count == 0:
        return 0

    target = pipeline_cfg.bronze_table(collection)

    if spark.catalog.tableExists(target):
        (
            DeltaTable.forName(spark, target)
            .alias("t")
            .merge(deduped.alias("s"), "t._source_id = s._source_id")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        (
            deduped.write.format("delta")
            .option("mergeSchema", "true")
            .partitionBy("_ingestion_date")
            .saveAsTable(target)
        )
    return row_count
