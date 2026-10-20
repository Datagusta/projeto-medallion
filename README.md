# Medallion no Databricks: preços de combustíveis (ANP)

Pipeline de engenharia de dados ponta a ponta sobre os dados abertos de preços de combustíveis da ANP, usando a arquitetura Medallion no Databricks Free Edition.

> **Status do projeto:** Fases 0 a 3 concluídas (ambiente, landing, bronze e silver). Pendentes: ingestão incremental (Fase 4), gold com dbt (Fase 5), Job + Asset Bundle (Fase 6) e dashboard (Fase 7). Veja o [roadmap](#roadmap).

## Arquitetura

```
CSVs locais ──► Volume (gas_medallion.landing.raw/anp/)
            ──► Bronze  (Auto Loader, tudo string + metadados)     [Lakeflow Pipeline]
            ──► Silver  (tipagem, limpeza, dedup, expectations)    [Lakeflow Pipeline]
            ──► Gold    (agregações e rankings)                    [dbt-databricks]   (pendente)
            ──► Dashboard (AI/BI)                                                      (pendente)

Orquestração: Databricks Job (pipeline → dbt)  |  IaC: Databricks Asset Bundle          (pendente)
```

| Camada | Objeto | Papel |
|---|---|---|
| Landing | `/Volumes/gas_medallion/landing/raw/anp/` | Arquivos brutos, imutáveis |
| Bronze | `gas_medallion.bronze.bronze_combustiveis` | Dado cru em `string`, colunas em snake_case, auditoria |
| Silver | `gas_medallion.silver.silver_combustiveis` | Dado tipado, limpo, deduplicado e validado |
| Gold | `gas_medallion.gold.*` | Modelos analíticos (dbt) |

## Stack

Databricks Free Edition (serverless), Unity Catalog, Volumes, Auto Loader, Lakeflow Declarative Pipelines (`pyspark.pipelines`), dbt-databricks 1.12.7 (dbt-core 1.12.5), Databricks CLI 1.21.0, Git. Custo: US$ 0.

## Dados

- **Fonte:** ANP, "Série histórica do levantamento de preços de combustíveis" (arquivos semestrais "Combustíveis Automotivos"), via dados abertos do governo.
- **Carga inicial:** 2023-S1, 2023-S2, 2024-S1, 2024-S2 (~300 MB, **1.802.536 linhas**).
- **Reservados para a carga incremental:** 2025-S1 e 2025-S2 (`data/incremental/`, fora do Git e fora do Volume até a Fase 4).
- **Layout:** 16 colunas, separador `;`, decimal com vírgula, data `dd/MM/yyyy`, **UTF-8**. O layout de 2025 é idêntico ao de 2023/2024 (não há schema evolution).

| Produto | Linhas (bronze) | Unidade |
|---|---|---|
| GASOLINA | 465.105 | R$ / litro |
| ETANOL | 395.155 | R$ / litro |
| GASOLINA ADITIVADA | 361.396 | R$ / litro |
| DIESEL S10 | 334.729 | R$ / litro |
| DIESEL (comum) | 208.717 | R$ / litro |
| GNV | 37.434 | R$ / m³ |

## Estrutura do repositório

```
projeto3-medallion/
├── databricks.yml        # (Fase 6)
├── resources/            # pipeline.yml, job.yml (Fase 6)
├── src/pipeline/         # bronze.py, silver.py
├── dbt_combustiveis/     # modelos gold (Fase 5)
├── data/                 # CSVs locais (no .gitignore)
│   ├── anp/              # carga inicial
│   └── incremental/      # 2025-S1 e 2025-S2
├── docs/                 # prints e explicação de Delta
└── README.md
```

## Decisões técnicas, prós e contras

### 1. Escolha do dataset: arquivos semestrais "Combustíveis Automotivos"

| Prós | Contras |
|---|---|
| Um arquivo cobre todos os combustíveis automotivos, com layout consistente | Arquivos grandes (~70-80 MB), exigem upload via CLI |
| Cada semestre novo vira um arquivo, ideal para demonstrar o incremental | Os recortes mensais por produto foram descartados; misturá-los geraria duplicatas |
| Dados reais, com problemas reais de qualidade | GLP fica fora do escopo |

Arquivos de 2004 foram descartados: layout antigo e pouco valor analítico.

### 2. Catálogo `gas_medallion` (em vez de `workspace`)

Os schemas `landing`, `bronze`, `silver`, `gold` e o Volume `raw` foram criados em um catálogo próprio.

| Prós | Contras |
|---|---|
| Isola o projeto do catálogo padrão do workspace | Todo código e configuração deve usar `gas_medallion.` (o plano original usava `workspace.`) |
| Nome descritivo, facilita governança e linhagem | Precisa constar em `profiles.yml` do dbt e no `databricks.yml` |

### 3. Renomear os arquivos para um padrão limpo

`Preços semestrais - AUTOMOTIVOS_2023.01.csv` → `anp_automotivos_2023_s01.csv`.

| Prós | Contras |
|---|---|
| Evita problemas com acentos e espaços em CLI, Volume e Auto Loader | Perde-se o nome original da fonte (anotado aqui para rastreabilidade) |
| Nome ordenável e autoexplicativo | |

### 4. Upload pela CLI (`databricks fs cp`) e não pela UI

| Prós | Contras |
|---|---|
| Suporta arquivos grandes e é reprodutível por script | Requer CLI instalado e autenticado (no Windows, exigiu ajuste de PATH) |
| Tamanhos conferidos byte a byte após o upload | O prefixo correto é `dbfs:`, não `dbc:` |

### 5. Encoding: UTF-8 (suposição inicial corrigida)

Partimos da hipótese de ISO-8859-1, típica de dados da ANP. O teste mostrou que `encoding => 'ISO-8859-1'` falhava no `read_files` (`FAILED_READ_FILE.NO_HINT`), e a leitura **sem** a opção trouxe os acentos corretos. Logo, os arquivos são UTF-8 e a opção foi removida da bronze.

| Prós | Contras |
|---|---|
| Configuração mais simples e validada empiricamente | Se a ANP mudar o encoding em arquivos futuros, a leitura pode corromper acentos silenciosamente; vale checar em cada novo semestre |

### 6. Bronze: tudo `string`, colunas em snake_case, `rescue` para colunas novas

Opções do Auto Loader: `cloudFiles.inferColumnTypes=false` e `cloudFiles.schemaEvolutionMode=rescue`. Colunas normalizadas por função própria (remove acento e espaço), com `select` + `alias` em vez de `withColumnRenamed` em loop. Colunas de auditoria: `_ingested_at` e `_source_file`.

| Prós | Contras |
|---|---|
| Bronze nunca falha por tipo inválido; erros de dado aparecem na silver, onde são tratados | Consultas diretas na bronze exigem `cast` |
| `rescue` captura colunas inesperadas em `_rescued_data` sem derrubar o pipeline | Colunas novas ficam "escondidas" em `_rescued_data` até alguém olhar (hoje: 0 linhas) |
| Rastreabilidade por arquivo (`_source_file`) e por momento (`_ingested_at`) | Tabela com colunas em `string` ocupa mais espaço do que tipos nativos |

**Validação da bronze:** 1.802.536 linhas (igual à soma dos CSVs), 4 arquivos distintos (431.576; 472.424; 477.154; 421.382) e `_rescued_data` com 0 linhas.

### 7. Silver: transformações

| Coluna | Transformação | Motivo |
|---|---|---|
| `data_coleta` | `to_date(..., 'dd/MM/yyyy')` | Tipagem |
| `valor_venda` | `,` → `.` e `cast(decimal(10,3))` | Tipagem numérica exata, sem erro de ponto flutuante |
| `cnpj_revenda` | `trim` e remoção de não-dígitos | Em 2023 vem como ` 00.003.188/0001-21` e em 2025 como `04431113000100`; sem isso o mesmo posto teria dois CNPJs |
| `unidade_medida` | colapsar espaços | `R$ / litro` e `R$ /litro` coexistem |
| Textos (município, revenda, produto, bairro, bandeira, UF) | `trim` + `upper` | Padronização |

`valor_de_compra` (100% vazio no layout observado) **não foi carregado** na silver.

| Prós | Contras |
|---|---|
| Silver pronta para consumo analítico, com tipos corretos | `upper` perde a capitalização original (irrelevante para análise) |
| Normalização do CNPJ garante deduplicação e análises por revenda consistentes entre anos | Descartar `valor_de_compra` exige reavaliar se a ANP passar a preenchê-lo |

### 8. Deduplicação por (`cnpj_revenda`, `produto`, `data_coleta`)

Resultado: **14 linhas removidas** (1.802.536 → 1.802.522), em 14 grupos de 2 linhas.

| Tipo | Grupos | Observação |
|---|---|---|
| Mesmo preço | 10 | Duplicata exata, remoção inofensiva |
| Preços diferentes | 4 | Todos no CNPJ `000082260001...`; o `dropDuplicates` manteve uma linha arbitrária |

Todos os casos ocorrem em 10 ou 12/06/2024 e envolvem só 4 CNPJs, o que sugere problema pontual de coleta ou publicação, não erro aleatório. A hipótese de filiais com o mesmo CNPJ **não foi confirmada**.

| Prós | Contras |
|---|---|
| Simples e suficiente: impacto de 14 em 1,8 milhão (≈0,0008%) | Em streaming, `dropDuplicates` mantém estado que cresce com o volume |
| Chave natural do negócio (posto × produto × dia) | Nos 4 casos de preço divergente, a linha mantida é arbitrária (não determinística) |
| | Roda **antes** das expectations, então elas não enxergam as duplicatas |

**Alternativas avaliadas e não adotadas:** incluir `nome_rua`/`numero_rua` na chave (poderia preservar filiais; falta confirmar com os dados) e `create_auto_cdc_flow` com ordenação por `_ingested_at` (determinístico, mais complexo; indicado em produção).

### 9. Expectations

| Expectation | Regra | Comportamento |
|---|---|---|
| `estado_preenchido` | `estado_sigla IS NOT NULL` | registra |
| `unidade_preenchida` | `unidade_medida IS NOT NULL` | registra |
| `preco_valido` | `valor_venda > 0 AND valor_venda < 20` | descarta |
| `data_valida` | `data_coleta IS NOT NULL` | descarta |
| `cnpj_valido` | `LENGTH(cnpj_revenda) = 14` | descarta |

**Resultado na carga inicial:** 0 reprovações em todas (1.802.522 aprovados em cada).

| Prós | Contras |
|---|---|
| Qualidade declarativa e auditável no event log | Zero reprovações na carga inicial: o relatório ainda não demonstra descartes |
| Mistura registrar e descartar, conforme a gravidade | `expect_or_fail` não foi usado (decisão consciente: derrubar o pipeline por 1 linha ruim seria desproporcional) |
| Limite de preço (`< 20`) serve para todos os produtos, inclusive GNV (R$ 2,39 a 6,89) | Limite fixo pode deixar passar preços absurdos mas menores que 20 |
| | Expectations só veem dados pós-dedup |

Os dados chegaram limpos e isso é um resultado legítimo; nenhuma falha foi forçada artificialmente. A carga de 2025 será reavaliada.

### 10. Tratamento do GNV e dos diesels

| Decisão | Prós | Contras |
|---|---|---|
| **GNV mantido na silver** com unidade `R$ / m³` | Silver completa; a comparabilidade é decisão analítica, tratada no gold | Quem consumir a silver sem filtrar por unidade mistura litro com m³ |
| **DIESEL e DIESEL S10 separados** | Fiel à fonte (são produtos distintos) | O gold precisa criar `familia_produto` para comparações agregadas |

### 11. Pipeline criado pela UI nesta etapa, Asset Bundle depois

A UI atual cria o pipeline direto (com `workspace.default` e nome automático) e as configurações são ajustadas depois. O código foi validado na UI e **versionado localmente**; na Fase 6 o Asset Bundle fará o deploy a partir do repositório.

| Prós | Contras |
|---|---|
| Feedback rápido para validar a lógica | Pipeline da UI e pipeline do bundle serão objetos distintos; o da UI deve ser removido depois |
| Código já organizado na estrutura final | Risco de divergência entre o que está no workspace e no Git até a Fase 6 |

### 12. Ferramentas e ambiente (Windows)

| Decisão | Motivo e custo |
|---|---|
| CLI novo (binário, v1.21.0), não `pip install databricks-cli` | O legado não suporta Asset Bundles. Custo: o `winget` não registrou o PATH, adicionado manualmente |
| `.venv` dedicado para o dbt | Isola dependências do Anaconda `base`. Custo: dois ambientes ativos no prompt |
| `.gitignore` antes do primeiro commit | Evita vazar dados, `profiles.yml` e tokens |
| Comandos em PowerShell | `mkdir -p`, `touch` e heredocs de bash não funcionam |

## Reconciliação de linhas

| Etapa | Linhas |
|---|---|
| Bronze | 1.802.536 |
| Removidas por dedup | 14 |
| Descartadas por `expect_or_drop` | 0 |
| **Silver** | **1.802.522** |

Silver por produto: DIESEL 208.717; DIESEL S10 334.729; ETANOL 395.155; GASOLINA 465.099; GASOLINA ADITIVADA 361.390; GNV 37.432. Datas de 2023-01-02 a 2024-12-31.

## Como reproduzir (até a Fase 3)

1. Criar o catálogo `gas_medallion` e rodar:
   ```sql
   CREATE SCHEMA IF NOT EXISTS gas_medallion.landing;
   CREATE SCHEMA IF NOT EXISTS gas_medallion.bronze;
   CREATE SCHEMA IF NOT EXISTS gas_medallion.silver;
   CREATE SCHEMA IF NOT EXISTS gas_medallion.gold;
   CREATE VOLUME IF NOT EXISTS gas_medallion.landing.raw;
   ```
2. Baixar os CSVs semestrais da ANP, renomear para `anp_automotivos_AAAA_sNN.csv` e subir:
   ```powershell
   databricks auth login --host https://SEU-WORKSPACE.cloud.databricks.com
   databricks fs mkdir dbfs:/Volumes/gas_medallion/landing/raw/anp
   databricks fs cp .\data\anp\ dbfs:/Volumes/gas_medallion/landing/raw/anp/ --recursive
   ```
3. Criar um ETL pipeline (Jobs & Pipelines), adicionar `src/pipeline/bronze.py` e `src/pipeline/silver.py`, e executar com **Run pipeline**.
4. Validar com as queries da seção de reconciliação.

## Limitações da Free Edition e riscos

| Risco | Mitigação |
|---|---|
| Cota de computação | Dataset limitado a ~3 anos; pipeline sob demanda, sem loops |
| Serverless-only | Verificar na Fase 6 se a `dbt_task` roda; plano B: dbt local ou via GitHub Actions |
| Dependência da API `pyspark.pipelines` (antes `dlt`) | Versão e sintaxe a fixar neste README ao final |
| SQL Warehouse "dormindo" | Primeira query demora; aquecer antes do `dbt build` |
| Token vazando | `.gitignore` e variável de ambiente `DATABRICKS_TOKEN` |

## Roadmap

- [x] **Fase 0:** CLI, Git, `.venv` com dbt, autenticação
- [x] **Fase 1:** schemas, Volume e carga inicial (tamanhos conferidos)
- [x] **Fase 2:** bronze com Auto Loader (contagens validadas)
- [x] **Fase 3:** silver com expectations (grafo e relatório obtidos)
- [ ] **Fase 4:** ingestão incremental (subir 2025-S1, rodar sem full refresh, provar com `_source_file` e `DESCRIBE HISTORY`; repetir com 2025-S2)
- [ ] **Fase 5:** gold com dbt (`preco_medio_estado_mes`, `ranking_municipios`, `variacao_semanal`; testes e docs)
- [ ] **Fase 6:** Job (pipeline → dbt) e Asset Bundle
- [ ] **Fase 7:** dashboard AI/BI e documentação final

## Entregáveis

- [x] Grafo do pipeline (bronze → silver, 5 expectations) em `docs/`
- [x] Relatório de expectations (query + resultado)
- [ ] Grafo com a carga incremental
- [ ] `databricks.yml` e `resources/`
- [ ] Projeto dbt com testes e docs
- [ ] Print do job com as duas tasks e do dashboard
- [ ] Documento de Delta (`DESCRIBE HISTORY`, time travel, `OPTIMIZE`, `VACUUM`, `RESTORE`, liquid clustering)

## Relatório de expectations (query)

```sql
SELECT
  d.name AS expectation,
  SUM(d.passed_records) AS aprovados,
  SUM(d.failed_records) AS reprovados
FROM (
  SELECT explode(
    from_json(
      details:flow_progress.data_quality.expectations,
      'array<struct<name:string,dataset:string,passed_records:int,failed_records:int>>'
    )
  ) AS d
  FROM event_log(TABLE(gas_medallion.silver.silver_combustiveis))
  WHERE event_type = 'flow_progress'
    AND details:flow_progress.data_quality.expectations IS NOT NULL
)
GROUP BY d.name
ORDER BY reprovados DESC;
```