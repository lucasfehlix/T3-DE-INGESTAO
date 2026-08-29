# Ingestão sample_mflix → Bronze (Databricks + Unity Catalog)

Pipeline genérica e parametrizada que ingere as 6 coleções do banco
`sample_mflix` (MongoDB) em duas camadas Delta — **Landing** (dado bruto) e
**Bronze** (deduplicado, idempotente, rastreável) — com carga full ou
incremental por coleção, watermark persistida, tabela de controle de
execuções e reconciliação de qualidade.

Arquitetura completa e decisões técnicas: [`docs/ARQUITETURA.md`](docs/ARQUITETURA.md).

## Estrutura

```
create-secret.ipynb          # cria o secret scope conn-db (URI via widget, nunca hardcoded)
config/
  pipeline_config.yaml     # configuração global (catálogo, schemas, retry, limiares) — sem credenciais
  collections.json         # parâmetros por coleção (modo de carga, watermark, projection)
  databricks_job.yml       # definição declarativa do Job (bônus - orquestração)
  contracts/
    movies_contract.yaml   # data contract da coleção movies (bônus)
src/ingestion/              # biblioteca reutilizável (OOP), sem nada hardcoded
  config.py                # carrega os YAML/JSON acima em dataclasses tipadas
  mongo_connector.py       # conexão reutilizável + retry/backoff
  extractor.py             # extração paginada + projection pushdown + filtro incremental
  landing_writer.py        # escrita Landing (append-only) + colunas de auditoria (R4)
  bronze_writer.py         # dedup + quarentena + MERGE idempotente (R3, R6, R7)
  control.py                # control_ingestion_log (R5) + watermark persistida (R3)
  reconciliation.py         # contagem origem x destino, nulos, duplicidade (R8)
  contract.py                # data contract -> checks Spark + validator MongoDB (bônus)
jobs/
  ingestion_job.py          # MongoDB -> Landing (entrypoint de Job / chamável de notebook)
  bronze_job.py              # Landing -> Bronze (entrypoint de Job / chamável de notebook)
notebooks/                    # notebooks .ipynb reais (formato Databricks)
  00_setup_catalog.ipynb      # cria catálogo/schemas (uma vez)
  01_run_pipeline.ipynb       # roda as 3 execuções de evidência exigidas
  02_reconciliation_and_evidence.ipynb  # queries de apoio para os prints
  03_data_contract_bonus.ipynb   # demonstração do data contract
docs/
  ARQUITETURA.md
  SAMPLE_MFLIX.md            # referência do dataset (já existia no repositório)
  evidencias/                 # prints das 3 execuções obrigatórias
CONTRIBUICOES.md
```

## Como executar

1. **Credenciais**: rode o notebook `create-secret.ipynb` (raiz do projeto)
   para criar o secret scope `conn-db` com a URI do MongoDB. Ele é
   deliberadamente separado do restante do projeto — nenhuma credencial
   vive em `config/`, `src/` ou `jobs/`. A URI real é digitada no widget
   `mongodb_uri` do próprio notebook em tempo de execução, nunca fica
   escrita no código-fonte nem é versionada no Git (ver `.gitignore`).
2. **Setup**: rode `notebooks/00_setup_catalog.ipynb` para criar
   `meu_catalog.landing` e `meu_catalog.bronze` (ajuste o nome do catálogo
   em `config/pipeline_config.yaml` se necessário).
3. **Pipeline**: rode `notebooks/01_run_pipeline.ipynb` de ponta a ponta. Ele
   executa, em sequência, as 3 execuções pedidas em `SEND_WORK.md`:
   - Execução 1 — carga full inicial das 6 coleções.
   - Execução 2 — incremental (movies/comments) sem novidades.
   - Execução 3 — incremental (comments) após inserir 1–3 documentos novos
     manualmente no Mongo (célula com o comando `mongosh` de exemplo).
4. **Evidências**: capture prints das células de `display(...)` sobre
   `control_ingestion_log` e salve em `docs/evidencias/`, com os nomes
   já usados no notebook.
5. **Reconciliação**: `notebooks/02_reconciliation_and_evidence.ipynb` traz as
   queries de apoio (contagem origem x destino, % nulos de chave,
   duplicidade, motivos de quarentena).
6. *(Bônus)* `notebooks/03_data_contract_bonus.ipynb` demonstra o data
   contract da coleção `movies`.

Em produção, os mesmos dois scripts (`jobs/ingestion_job.py` e
`jobs/bronze_job.py`) rodam como tasks encadeadas de um Databricks Job —
ver `config/databricks_job.yml` (cron diário + retry/backoff + timeout +
notificação de falha, seguindo o checklist de resiliência da Aula 5).

## Requisitos atendidos (mapa rápido)

| Requisito | Onde |
|---|---|
| R1 — Pipeline genérica e parametrizada | `config/collections.json` + `src/ingestion/*` + `jobs/*` — mesmo código para as 6 coleções, nada hardcoded |
| R2 — Boas práticas de recursos | ver tabela em `docs/ARQUITETURA.md` (6 das 6 técnicas listadas no enunciado) |
| R3 — Full/incremental + idempotência | `collections.json` (`load_type`), `control.py` (watermark persistida), `bronze_writer.merge_into_bronze` (MERGE) |
| R4 — Rastreabilidade | `landing_writer.with_audit_columns` — 5 colunas propagadas até a Bronze |
| R5 — Tabela de controle | `control.py::ControlLogger` → `meu_catalog.bronze.control_ingestion_log` |
| R6 — Bronze fiel à origem | Delta, append-only em espírito, corpo do documento nunca alterado |
| R7 — Schema drift / quarentena | corpo como JSON bruto (nunca quebra por tipo) + `bronze_writer.split_quarantine` |
| R8 — Reconciliação | `reconciliation.py` — chamado a cada execução do `bronze_job` |

## Segurança — credenciais e arquivos que não vão para o Git

- `create-secret.ipynb` lê a URI do MongoDB de um widget Databricks
  (`mongodb_uri`) em vez de tê-la escrita no código. Rode a célula, cole a
  URI real no campo do widget, execute — o valor nunca é salvo no notebook.
- `.gitignore` (raiz) exclui explicitamente `.env`, arquivos de chave/segredo
  e as pastas `pdfs/` e `code-samples/` — materiais de referência do
  professor que **não fazem parte da entrega** e que, no caso de
  `code-samples/create-secret.py` e `code-samples/create_catalog.ipynb`,
  ainda contêm credenciais de exemplo em texto puro. Eles continuam no seu
  disco para consulta, mas não serão versionados.
- Antes do PR final, confirme com o checklist do `SEND_WORK.md`:
  `git log -p | grep -i "password\|uri\|secret\|token"` deve retornar vazio.

## Limitações conhecidas

- O código foi desenvolvido e revisado nesta sessão **sem acesso a um
  workspace Databricks nem a um cluster MongoDB real** — não foi executado
  de ponta a ponta por mim. As 3 evidências obrigatórias (`docs/evidencias/`)
  precisam ser geradas rodando `notebooks/01_run_pipeline.ipynb` no seu
  ambiente.
- A camada Silver, CDC via Change Streams e testes automatizados (bônus)
  não foram implementados nesta entrega; o data contract e a definição de
  Job (`databricks_job.yml`) foram incluídos como bônus de baixo custo.
