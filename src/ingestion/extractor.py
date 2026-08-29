"""Extração paginada do MongoDB com projection pushdown e filtro incremental
(R2, R3).

O mesmo código atende TODAS as coleções (R1): o que muda por coleção é
apenas o `CollectionConfig` (vindo de config/collections.json), nunca um
bloco de código dedicado.
"""
from __future__ import annotations

import datetime as dt
import itertools
import json
import logging
from dataclasses import dataclass
from typing import Any, Iterator, Optional

import bson

from .config import CollectionConfig
from .mongo_connector import MongoConnector, with_retry

logger = logging.getLogger(__name__)


@dataclass
class Batch:
    documents: list
    watermark_values: list


def _json_encode(value: Any) -> Any:
    if isinstance(value, bson.ObjectId):
        return str(value)
    if isinstance(value, (dt.datetime, dt.date)):
        return value.isoformat()
    if isinstance(value, bson.Decimal128):
        return str(value)
    if isinstance(value, bytes):
        return value.hex()
    return str(value)


def build_filter(cfg: CollectionConfig, last_watermark: Optional[str]) -> dict:
    """Full load não filtra nada. Incremental filtra watermark_field > o
    último valor persistido (ou o default da config, na primeira execução)."""
    if not cfg.is_incremental:
        return {}

    watermark = last_watermark if last_watermark is not None else cfg.watermark_default
    if cfg.watermark_type == "date":
        value = dt.datetime.fromisoformat(str(watermark).replace("Z", "+00:00"))
    else:
        value = watermark  # comparação lexicográfica de string (ex.: movies.lastupdated)
    return {cfg.watermark_field: {"$gt": value}}


def extract_batches(
    connector: MongoConnector,
    cfg: CollectionConfig,
    last_watermark: Optional[str],
    batch_size: int,
    max_retries: int,
    backoff_base_seconds: float,
) -> Iterator[Batch]:
    """Lê a coleção em lotes de `batch_size` documentos, nunca
    materializando a coleção inteira em memória de uma vez - é o que evita
    `list(cursor)` sobre uma coleção grande (R2). Cada chamada de rede
    (abertura do cursor e cada lote consumido) passa pelo `with_retry`."""
    mongo_filter = build_filter(cfg, last_watermark)
    projection = cfg.projection()

    def _open_cursor():
        return connector.collection(cfg.name).find(
            filter=mongo_filter, projection=projection, batch_size=batch_size
        )

    cursor = with_retry(_open_cursor, max_retries, backoff_base_seconds)

    def _pull_next_chunk():
        return list(itertools.islice(cursor, batch_size))

    while True:
        chunk = with_retry(_pull_next_chunk, max_retries, backoff_base_seconds)
        if not chunk:
            break
        yield _to_batch(chunk, cfg)


def _to_batch(documents: list, cfg: CollectionConfig) -> Batch:
    watermark_values = []
    if cfg.is_incremental:
        watermark_values = [doc.get(cfg.watermark_field) for doc in documents]
    return Batch(documents=documents, watermark_values=watermark_values)


def documents_to_rows(batch: Batch):
    """Converte cada documento em (_source_id, body_json) - representação
    genérica usada para TODAS as coleções, sem struct dedicado por coleção
    (R1). O schema heterogêneo do MongoDB (R7) fica resolvido por
    construção: nada precisa ser tipado para caber na Bronze."""
    rows = []
    for doc in batch.documents:
        source_id = str(doc.get("_id", ""))
        body = json.dumps(doc, default=_json_encode, ensure_ascii=False)
        rows.append((source_id, body))
    return rows
