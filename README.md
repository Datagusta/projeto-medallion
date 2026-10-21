# Medallion no Databricks: preços de combustíveis (ANP)

Pipeline de engenharia de dados ponta a ponta sobre os dados abertos de preços de combustíveis da ANP, usando a arquitetura Medallion no Databricks Free Edition.

> **Status do projeto:** Fases 0 a 5 concluídas (ambiente, landing, bronze, silver, ingestão incremental com 2025-S1 e 2025-S2, e gold com dbt: 3 modelos e 25/25 itens do `dbt build` passando). Pendentes: Job + Asset Bundle (Fase 6) e dashboard (Fase 7). Veja o [roadmap](#roadmap).

## Arquitetura

```
CSVs locais ──► Volume (gas_medallion.landing.raw/anp/)
            ──► Bronze  (Auto Loader, tudo string + metadados)     [Lakeflow Pipeline]
            ──► Silver  (tipagem, limpeza, dedup, expectations)    [Lakeflow Pipeline]
            ──► Gold    (agregações e rankings)                    [dbt-databricks]
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
- **Cargas incrementais** (`data/incremental/`, enviadas ao Volume uma por vez e processadas com *Run pipeline* normal, sem full refresh):

| Carga | Linhas na bronze | Observação |
|---|---|---|
| 2025-S1 | 429.523 | 9.114 linhas sem dados (ver decisão 9.1) |
| 2025-S2 | 384.208 | Sem linhas vazias nem reprovações |
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
| `unidade_medida` | colapsar espaços e padronizar `m3` → `m³` | `R$ / litro` e `R$ /litro` coexistem; a ANP trocou `m³` por `m3` no GNV em 01/07/2025 (decisão 14) |
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
| | Roda **depois** das expectations (ver 9.1); na versão inicial rodava antes e mascarava descartes |

**Alternativas avaliadas e não adotadas:** incluir `nome_rua`/`numero_rua` na chave (poderia preservar filiais; falta confirmar com os dados) e `create_auto_cdc_flow` com ordenação por `_ingested_at` (determinístico, mais complexo; indicado em produção).

### 9. Expectations

| Expectation | Regra | Comportamento |
|---|---|---|
| `estado_preenchido` | `estado_sigla IS NOT NULL` | registra |
| `unidade_preenchida` | `unidade_medida IS NOT NULL` | registra |
| `preco_valido` | `valor_venda > 0 AND valor_venda < 20` | descarta |
| `data_valida` | `data_coleta IS NOT NULL` | descarta |
| `cnpj_valido` | `LENGTH(cnpj_revenda) = 14` | descarta |
| `unidade_conhecida` | `unidade_medida IN ('R$ / litro', 'R$ / m³')` | registra (adicionada após o caso do GNV, decisão 14) |

**Resultado na carga inicial:** 0 reprovações em todas (1.802.522 aprovados em cada).

| Prós | Contras |
|---|---|
| Qualidade declarativa e auditável no event log | Zero reprovações na carga inicial: o relatório ainda não demonstra descartes |
| Mistura registrar e descartar, conforme a gravidade | `expect_or_fail` não foi usado (decisão consciente: derrubar o pipeline por 1 linha ruim seria desproporcional) |
| Limite de preço (`< 20`) serve para todos os produtos, inclusive GNV (R$ 2,39 a 6,89) | Limite fixo pode deixar passar preços absurdos mas menores que 20 |
| | Uma linha vazia reprova em todas as expectations ao mesmo tempo; cada uma conta as mesmas linhas, não problemas distintos |

Na carga inicial os dados chegaram limpos, resultado legítimo, sem falhas forçadas artificialmente. O 2025-S1 trouxe o primeiro caso real (9.114 linhas vazias), descrito em 9.1.

### 9.1 Expectations antes da dedup (silver em dois passos)

**Problema encontrado:** o arquivo 2025-S1 traz 9.114 linhas sem dados. Na primeira versão da silver, o `dropDuplicates` rodava antes das expectations e colapsou as 9.114 em 1 (campos nulos contam como iguais). O relatório mostrou 1 reprovação, subestimando o problema em 9.113 linhas.

**Correção:** separar a silver em dois passos.

```
bronze_combustiveis ──► silver_tipada (view: tipagem + 5 expectations)
                    ──► silver_combustiveis (tabela: dropDuplicates)
```

O reprocessamento foi um **full refresh só da silver** (a bronze ficou como "Omitted"), preservando checkpoints e o histórico incremental da bronze.

**Resultado:** o relatório passou a mostrar 9.114 reprovações em cada expectation (as mesmas linhas, não 45 mil problemas distintos), com 2.222.945 aprovadas sobre 2.232.059 avaliadas. A silver final não mudou (2.222.931 naquele momento), o que confirma que a refatoração só melhorou a observabilidade. No 2025-S2 (update incremental seguinte): 384.208 aprovadas e 0 reprovadas.

| Prós | Contras |
|---|---|
| Relatório de qualidade fiel ao que entrou | Mais um objeto no grafo |
| Dedup não mascara problemas de qualidade | Exigiu reprocessar a silver |
| Separa validar e deduplicar | O relatório histórico anterior à refatoração subestima descartes e não deve ser usado |

A causa das linhas vazias (provavelmente linhas só com separadores) **não foi verificada** no arquivo bruto.

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
| Token do dbt em variável de ambiente (`DATABRICKS_TOKEN`), validade de 30 dias, escopo `all-apis` | Nada de segredo no repositório. Custo: redefinir a variável a cada terminal novo; `all-apis` é o escopo mais amplo (aceito pelo prazo curto e por ser projeto de estudo) |
| `pip-system-certs` no `.venv` | O dbt falhou com `CERTIFICATE_VERIFY_FAILED` (certificado autoassinado na cadeia); o pacote resolveu. A causa provável é inspeção de HTTPS na rede, mas isso não foi confirmado |

### 13. Gold com dbt

Três modelos em `gas_medallion.gold`, todos `table`, lendo a silver declarada como `source`.

| Modelo | Grão | Decisões |
|---|---|---|
| `preco_medio_estado_mes` | estado × produto × mês | `familia_produto` agrupa gasolina + aditivada e diesel + S10; GNV tem linhas próprias, sem mistura com litro |
| `ranking_municipios` | estado × município × produto (período todo) | Estado na chave (nomes de município se repetem); mínimo de 30 coletas (`var min_coletas_ranking`), **valor arbitrário** a revisar; `rank_mais_caro` e `rank_mais_barato` |
| `variacao_semanal` | produto × semana | Variação só se a semana anterior é exatamente 7 dias antes (evita `LAG` sobre lacunas); primeira e última semana podem ser parciais |

Testes: 22 testes de dados (not_null, accepted_values, `dbt_utils.unique_combination_of_columns` e um singular de preço médio entre R$ 2 e 12), incluindo testes **na source** (unicidade da chave natural e unidade). Resultado: `PASS=25 ERROR=0` (3 modelos + 22 testes).

| Prós | Contras |
|---|---|
| Testes, documentação e linhagem declarativos | Duas ferramentas para orquestrar (pipeline e dbt) |
| Testes na source verificam a silver de fora do pipeline | Modelos `table` recriados por inteiro a cada execução (sem incremental) |
| Separação clara: engenharia (Lakeflow) e modelagem analítica (dbt) | Piso de 30 coletas no ranking é uma escolha não calibrada |
| Dá a segunda linha de defesa de qualidade (decisão 14) | Depende de token pessoal e de SQL Warehouse |

### 14. Mudança de unidade do GNV em 01/07/2025

**O que aconteceu:** o primeiro `dbt build` teve 2 testes de unicidade falhando (1 grupo em `variacao_semanal` e 72 em `ranking_municipios`). Investigação: a ANP trocou a unidade do GNV de `R$ / m³` (bytes `C2 B3`) para `R$ / m3` (byte `33`) em 01/07/2025, exatamente na fronteira entre os arquivos 2025-S1 e 2025-S2. Como `unidade_medida` estava no `GROUP BY` dos modelos, o mesmo GNV aparecia em duas linhas. Em 46.940 linhas a unidade antiga e em 8.732 a nova.

**Por que a silver não pegou:** a única expectation sobre a unidade (`unidade_preenchida`) testava só nulo, e a afirmação "uma unidade por produto" foi verificada apenas na carga inicial, não após os incrementais. Quem detectou foi o teste do gold, que funcionou como segunda linha de defesa.

**Correção:**
- `regexp_replace` na silver (`m3` → `m³`); a bronze mantém o valor original da fonte;
- expectation `unidade_conhecida` (só registra, pois uma unidade nova é sinal para investigar, não motivo para perder linhas);
- `accepted_values` na source do dbt;
- full refresh seletivo só da silver (bronze "Omitted").

**Resultado:** GNV com uma única unidade e 55.672 linhas (46.940 + 8.732); total da silver inalterado em 2.607.139; série mensal do GNV com 36 meses, todos em `R$ / m³`; a semana de 30/06/2025 passou a ter uma linha (347 coletas, variação de −0,17%). A `unidade_conhecida` reprovou 9.114 linhas, número idêntico ao das linhas vazias; a leitura provável é que linhas vazias têm unidade nula e `NULL IN (...)` não é verdadeiro (inferido pela coincidência dos números, não consultado linha a linha).

| Prós | Contras |
|---|---|
| Correção na camada responsável pela padronização | Exigiu reprocessar a silver |
| Bronze preserva o dado cru | A lista de unidades aceitas é fixa; uma unidade legítima nova falha o build até ser incluída |
| Falha futura fica explícita (expectation + teste na source) | |

## Reconciliação de linhas

**Carga inicial (2023-2024):**

| Etapa | Linhas |
|---|---|
| Bronze | 1.802.536 |
| Removidas por dedup | 14 |
| Descartadas por `expect_or_drop` | 0 |
| **Silver** | **1.802.522** |

**Após as duas cargas incrementais (estado atual):**

| Etapa | Linhas |
|---|---|
| Bronze (6 arquivos: 1.802.536 + 429.523 + 384.208) | 2.616.267 |
| Reprovadas nas expectations (linhas vazias do 2025-S1) | −9.114 |
| Aprovadas em `silver_tipada` | 2.607.153 |
| Removidas por dedup | −14 |
| **Silver** | **2.607.139** |

A silver medida (2.607.139) coincide com o cálculo. Por unidade: GNV com 55.672 linhas em `R$ / m³` e os demais produtos em `R$ / litro`. O incremento do 2025-S2 foi de 384.208 linhas na bronze e na silver, sem descartes.

Silver por produto (só a carga inicial 2023-2024): DIESEL 208.717; DIESEL S10 334.729; ETANOL 395.155; GASOLINA 465.099; GASOLINA ADITIVADA 361.390; GNV 37.432. Datas de 2023-01-02 a 2024-12-31.

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
5. Gold: definir `DATABRICKS_TOKEN`, criar `dbt_combustiveis/profiles.yml` a partir do `profiles.yml.example`, e rodar `dbt deps` e `dbt build --profiles-dir .` dentro de `dbt_combustiveis/` (instalar `requirements.txt`, que inclui `pip-system-certs` se a rede inspecionar HTTPS).

## Limitações da Free Edition e riscos

| Risco | Mitigação |
|---|---|
| Cota de computação | Dataset limitado a ~3 anos; pipeline sob demanda, sem loops |
| Serverless-only | Verificar na Fase 6 se a `dbt_task` roda; plano B: dbt local ou via GitHub Actions |
| Dependência da API `pyspark.pipelines` (antes `dlt`) | Versão e sintaxe a fixar neste README ao final |
| SQL Warehouse "dormindo" | Primeira query demora; aquecer antes do `dbt build` |
| Token vazando | `.gitignore` e variável de ambiente `DATABRICKS_TOKEN` |
| Time travel por `VERSION AS OF` falhou na streaming table da bronze (`assertion failed: The reconciliation query was not resolved...`); causa não confirmada | Demonstrar time travel nas tabelas gold (Delta comuns) |

## Roadmap

- [x] **Fase 0:** CLI, Git, `.venv` com dbt, autenticação
- [x] **Fase 1:** schemas, Volume e carga inicial (tamanhos conferidos)
- [x] **Fase 2:** bronze com Auto Loader (contagens validadas)
- [x] **Fase 3:** silver com expectations (grafo e relatório obtidos)
- [x] **Fase 4:** ingestão incremental (2025-S1 e 2025-S2 sem full refresh na bronze; provada por `_source_file`, grafo e `DESCRIBE HISTORY`: versões 4 e 5 são `STREAMING UPDATE` em modo `Append`)
- [x] **Fase 5:** gold com dbt (3 modelos, 22 testes, `PASS=25 ERROR=0`); falta o print do `dbt docs`
- [ ] **Fase 6:** Job (pipeline → dbt) e Asset Bundle
- [ ] **Fase 7:** dashboard AI/BI e documentação final

## Entregáveis

- [x] Grafo do pipeline (bronze → silver, 5 expectations) em `docs/`
- [x] Relatório de expectations (query + resultado)
- [x] Grafo com a carga incremental (bronze com ~430K de saída, não o reprocessamento total); salvar o print em `docs/`
- [ ] `databricks.yml` e `resources/`
- [x] Projeto dbt com testes passando (25/25); falta o print do grafo do `dbt docs`
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
WHERE d.dataset = 'silver_tipada'   -- ignora o histórico anterior à refatoração
GROUP BY d.name
ORDER BY reprovados DESC;
```

### Versão por update (recomendada)

A query acima soma todos os updates do event log, então updates reprocessados contam em dobro (o mesmo conjunto de 9.114 linhas vazias apareceu como 18.228). Use esta, que separa por update:

```sql
SELECT origin.update_id,
       MAX(timestamp) AS ultimo_evento,
       d.name AS expectation,
       SUM(d.passed_records) AS aprovados,
       SUM(d.failed_records) AS reprovados
FROM (
  SELECT origin, timestamp, explode(
    from_json(
      details:flow_progress.data_quality.expectations,
      'array<struct<name:string,dataset:string,passed_records:int,failed_records:int>>'
    )
  ) AS d
  FROM event_log(TABLE(gas_medallion.silver.silver_combustiveis))
  WHERE event_type = 'flow_progress'
    AND details:flow_progress.data_quality.expectations IS NOT NULL
)
WHERE d.dataset = 'silver_tipada'
GROUP BY origin.update_id, d.name
ORDER BY ultimo_evento DESC, d.name;
```

No update do último full refresh (cobre as 6 cargas): 2.607.153 aprovadas e 9.114 reprovadas por expectation.