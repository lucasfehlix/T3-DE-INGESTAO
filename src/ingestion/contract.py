"""Data contracts (bônus - desafio de +3pts).

Um YAML descreve o contrato de uma coleção (schema + semântica + SLA) e
gera tanto os checks de qualidade em Spark quanto o validator $jsonSchema
do MongoDB - a regra existe em um único lugar (Aula 4, seção 4.4).
"""
from __future__ import annotations

from pathlib import Path

import yaml
from pyspark.sql import DataFrame

_MAP_BSON = {"string": "string", "integer": "int", "double": "double", "array": "array"}


def load_contract(path) -> dict:
    return yaml.safe_load(Path(path).read_text(encoding="utf-8"))


def generate_spark_checks(contract: dict) -> list:
    checks = []
    for campo in contract["campos"]:
        nome, alias = campo["nome"], campo["nome"].replace(".", "_")
        if campo.get("obrigatorio"):
            checks.append((f"{alias}_nao_nulo", f"{nome} IS NOT NULL"))
        if "min" in campo and "max" in campo:
            checks.append((f"{alias}_range", f"{nome} BETWEEN {campo['min']} AND {campo['max']}"))
        elif "min" in campo:
            checks.append((f"{alias}_min", f"{nome} >= {campo['min']}"))
        if "min_itens" in campo:
            checks.append((f"{alias}_nao_vazio", f"size({nome}) >= {campo['min_itens']}"))
    return checks


def generate_mongo_validator(contract: dict) -> dict:
    props: dict = {}
    required: list = []
    for campo in contract["campos"]:
        if "." in campo["nome"]:
            continue  # subdocumentos ficam fora desta versão do contrato
        regra: dict = {"bsonType": _MAP_BSON[campo["tipo"]]}
        if "min" in campo:
            regra["minimum"] = campo["min"]
        if "max" in campo:
            regra["maximum"] = campo["max"]
        if "min_itens" in campo:
            regra["minItems"] = campo["min_itens"]
        props[campo["nome"]] = regra
        if campo.get("obrigatorio"):
            required.append(campo["nome"])
    return {"$jsonSchema": {"bsonType": "object", "required": required, "properties": props}}


def apply_checks(df: DataFrame, checks: list) -> list:
    total = df.count()
    resultados = []
    for nome, expr in checks:
        ok = df.filter(expr).count()
        resultados.append({
            "check": nome,
            "total": total,
            "ok": ok,
            "falhas": total - ok,
            "pct_ok": round(100.0 * ok / total, 2) if total else 0.0,
            "veredito": "APROVADO" if ok == total else "REPROVADO",
        })
    return resultados
