"""Conexão MongoDB compartilhada e retry/backoff em falhas de rede (R2).

O `MongoConnector` é pensado para ser instanciado UMA vez por execução do
job e reutilizado em todas as coleções, em vez de abrir/fechar uma conexão
por coleção (reuso de conexão / connection pooling - o `MongoClient` já
mantém um pool interno; o ganho aqui é não pagar o custo de handshake a
cada coleção).
"""
from __future__ import annotations

import logging
import time

from pymongo import MongoClient
from pymongo.errors import AutoReconnect, NetworkTimeout

logger = logging.getLogger(__name__)

RETRIABLE_EXCEPTIONS = (AutoReconnect, NetworkTimeout)


class MongoConnector:
    def __init__(
        self,
        uri: str,
        database: str,
        server_selection_timeout_ms: int = 15_000,
        socket_timeout_ms: int = 300_000,
        app_name: str = "databricks-mongodb-ingestion",
    ) -> None:
        self._client = MongoClient(
            uri,
            serverSelectionTimeoutMS=server_selection_timeout_ms,
            socketTimeoutMS=socket_timeout_ms,
            appName=app_name,
        )
        self.database_name = database

    @property
    def database(self):
        return self._client[self.database_name]

    def collection(self, name: str):
        return self.database[name]

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> "MongoConnector":
        return self

    def __exit__(self, *exc) -> None:
        self.close()


def with_retry(
    func,
    max_retries: int = 3,
    backoff_base_seconds: float = 2.0,
    retriable=RETRIABLE_EXCEPTIONS,
):
    """Executa `func` com retry e backoff exponencial (2s, 4s, 8s, ...).

    Falhas não classificadas como transitórias (erro de sintaxe, auth,
    permissão) propagam imediatamente - retry cego em falha permanente só
    multiplica custo e tempo sem resolver nada (Aula 5, 5.4).
    """
    attempt = 0
    while True:
        try:
            return func()
        except retriable as exc:
            attempt += 1
            if attempt > max_retries:
                logger.error("Falha após %s tentativas: %s", attempt - 1, exc)
                raise
            wait = backoff_base_seconds * (2 ** (attempt - 1))
            logger.warning(
                "Tentativa %s/%s falhou (%s). Aguardando %.1fs antes de tentar novamente.",
                attempt, max_retries, exc, wait,
            )
            time.sleep(wait)
