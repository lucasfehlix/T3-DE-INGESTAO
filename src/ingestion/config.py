"""Carregamento e validação da configuração externa da pipeline (R1).

Nenhum parâmetro de coleção ou de conexão é hardcoded no corpo do código -
tudo vem de `config/pipeline_config.yaml` e `config/collections.json`.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import yaml


@dataclass(frozen=True)
class CollectionConfig:
    name: str
    load_type: str  # "full" | "incremental"
    watermark_field: Optional[str]
    watermark_type: Optional[str]  # "string" | "date"
    watermark_default: Optional[str]
    projection_exclude: list
    notes: str = ""

    @property
    def is_incremental(self) -> bool:
        return self.load_type == "incremental"

    def projection(self):
        """Projection pushdown (R2): campos sensíveis/largos nunca trafegam
        da origem - MongoDB filtra no servidor, não no cliente."""
        if not self.projection_exclude:
            return None
        return {field: 0 for field in self.projection_exclude}


@dataclass(frozen=True)
class PipelineConfig:
    catalog: str
    landing_schema: str
    bronze_schema: str
    database: str
    secret_scope: str
    secret_key: str
    server_selection_timeout_ms: int
    socket_timeout_ms: int
    app_name: str
    batch_size: int
    max_retries: int
    backoff_base_seconds: float
    target_rows_per_partition: int
    max_divergence_pct: float
    table_prefix: str
    control_log_table: str
    watermark_table: str
    quarantine_table: str

    def landing_table(self, collection: str) -> str:
        return f"{self.catalog}.{self.landing_schema}.{self.table_prefix}{collection}"

    def bronze_table(self, collection: str) -> str:
        return f"{self.catalog}.{self.bronze_schema}.{self.table_prefix}{collection}"

    def control_log_full_name(self) -> str:
        return f"{self.catalog}.{self.bronze_schema}.{self.control_log_table}"

    def watermark_full_name(self) -> str:
        return f"{self.catalog}.{self.bronze_schema}.{self.watermark_table}"

    def quarantine_full_name(self) -> str:
        return f"{self.catalog}.{self.bronze_schema}.{self.quarantine_table}"


def load_pipeline_config(path) -> PipelineConfig:
    raw = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    return PipelineConfig(
        catalog=raw["catalog"],
        landing_schema=raw["schemas"]["landing"],
        bronze_schema=raw["schemas"]["bronze"],
        database=raw["mongodb"]["database"],
        secret_scope=raw["mongodb"]["secret_scope"],
        secret_key=raw["mongodb"]["secret_key"],
        server_selection_timeout_ms=raw["mongodb"]["server_selection_timeout_ms"],
        socket_timeout_ms=raw["mongodb"]["socket_timeout_ms"],
        app_name=raw["mongodb"]["app_name"],
        batch_size=raw["extraction"]["batch_size"],
        max_retries=raw["extraction"]["max_retries"],
        backoff_base_seconds=raw["extraction"]["backoff_base_seconds"],
        target_rows_per_partition=raw["write"]["target_rows_per_partition"],
        max_divergence_pct=raw["reconciliation"]["max_divergence_pct"],
        table_prefix=raw["table_naming"]["prefix"],
        control_log_table=raw["table_naming"]["control_log_table"],
        watermark_table=raw["table_naming"]["watermark_table"],
        quarantine_table=raw["table_naming"]["quarantine_table"],
    )


def load_collections_config(path) -> list:
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    return [
        CollectionConfig(
            name=item["name"],
            load_type=item["load_type"],
            watermark_field=item.get("watermark_field"),
            watermark_type=item.get("watermark_type"),
            watermark_default=item.get("watermark_default"),
            projection_exclude=item.get("projection_exclude", []),
            notes=item.get("notes", ""),
        )
        for item in raw["collections"]
    ]
