# Registro de Contribuições

## Grupo: Trabalho individual

| Membro | Matrícula | Contribuições principais |
|--------|-----------|--------------------------|
| Lucas Araujo Felix Portela | 2651722 | Pipeline completa (extract/load/control), config/pipeline_config.yaml, config/collections.json, jobs/ingestion_job.py, jobs/bronze_job.py, src/ingestion/*, notebooks/00-03, docs/ARQUITETURA.md, README.md, execução no Databricks e evidências das 3 cargas obrigatórias + prova de idempotência |

## Detalhamento por commit

> `git log --oneline --author="lucasfehlix@gmail.com"`

```
8f28ae2 Implementa pipeline de ingestao sample_mflix -> Bronze (Databricks)
174031f Remove %run problematico do create-secret em 01_run_pipeline
074fe45 Adiciona evidencia da Execucao 1 (carga full inicial)
a4f3998 Adiciona evidencia da Execucao 2 (incremental sem novidades)
94df763 Adiciona evidencia da Execucao 3 (incremental com dados novos)
ee6e2fc Preenche CONTRIBUICOES.md com nome, matricula e historico de commits
```
