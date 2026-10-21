# Delta Lake no projeto: o que foi observado

Tabela usada nas demonstrações: `gas_medallion.gold.preco_medio_estado_mes`, tabela Delta gerenciada, criada pelo dbt (`CREATE OR REPLACE TABLE AS SELECT`).

Convenção deste documento: **observado** = apareceu em saída de consulta no workspace; **inferido** = explicação provável que não foi verificada diretamente.

## 1. `_delta_log` e transações ACID

Uma tabela Delta é um conjunto de arquivos Parquet mais um log de transações (`_delta_log`). Cada escrita é um *commit* atômico no log, que gera uma nova **versão**. Quem lê enxerga sempre uma versão completa, nunca uma escrita pela metade, e é isso que permite consultar versões antigas (time travel) e auditar o que mudou.

Neste projeto, cada `dbt build` da tabela gold é um commit.

## 2. `DESCRIBE HISTORY`

Observado: 5 versões, todas `CREATE OR REPLACE TABLE AS SELECT`.

| Versão | Horário (UTC) | Origem |
|---|---|---|
| 0 | 2026-10-21 17:46:07 | 1º `dbt build` (log do dbt: 17:46:09) |
| 1 | 17:50:13 | não identificada |
| 2 | 18:04:40 | build após a correção do GNV (log do dbt: 18:04:40) |
| 3 | 19:56:18 | não identificada |
| 4 | 19:59:46 | `dbt_task` do job (o job terminou às 19:59:58) |

O dbt recria a tabela a cada execução, mas o **histórico foi preservado**: as versões antigas continuam consultáveis.

## 3. Time travel com um caso real

Query:

```sql
SELECT DISTINCT unidade_medida FROM gas_medallion.gold.preco_medio_estado_mes VERSION AS OF 0 WHERE produto = 'GNV';
SELECT DISTINCT unidade_medida FROM gas_medallion.gold.preco_medio_estado_mes VERSION AS OF 4 WHERE produto = 'GNV';
```

Observado:

| Versão | Unidades do GNV |
|---|---|
| 0 | `R$ / m³` **e** `R$ / m3` |
| 4 | `R$ / m³` |

A versão 0 foi construída antes da correção na silver e preserva o estado com o defeito (a ANP trocou `m³` por `m3` em 01/07/2025; ver decisão 14 do README). A versão 4 é o estado corrigido. Sem time travel, a evidência do problema teria desaparecido com o `CREATE OR REPLACE`.

**Limitação encontrada:** `VERSION AS OF` falhou na streaming table da bronze com `assertion failed: The reconciliation query was not resolved for the StreamingTable or MaterializedView`. A causa não foi confirmada; por isso a demonstração usa a tabela gold, que é Delta comum. Há uma pista na seção 10: a streaming table é apoiada por uma tabela de materialização interna, com uma propriedade de "reconciliation query".

## 4. `OPTIMIZE`

```sql
OPTIMIZE gas_medallion.gold.preco_medio_estado_mes;
```

Observado: `numFilesAdded: 0`, `numFilesRemoved: 0`. Nada foi compactado.

Explicação: a tabela tem 1 arquivo de ~60 KiB (seção 8), então não havia o que juntar. Isso é consistente com o resultado.

Por que isso importa: ingestão incremental (como a da bronze, uma carga por arquivo) tende a gerar muitos arquivos pequenos, e cada consulta paga o custo de abrir todos. `OPTIMIZE` os reescreve em arquivos maiores. Na gold o problema não aparece, porque cada `dbt build` escreve a tabela inteira de uma vez.

## 5. `VACUUM`

```sql
VACUUM gas_medallion.gold.preco_medio_estado_mes DRY RUN;
```

Observado: nenhuma linha retornada (nada seria removido).

Inferido: a retenção padrão é de 7 dias e todas as versões superadas têm poucas horas, então nenhum arquivo ainda é elegível. Não é ausência de arquivos antigos: as versões 0 a 3 continuam existindo.

**Trade-off com o time travel:** `VACUUM` apaga os arquivos que versões antigas ainda referenciam. Depois que ele roda além da retenção, `VERSION AS OF` para essas versões deixa de funcionar. Não use `RETAIN 0 HOURS` aqui: destruiria justamente a evidência da seção 3.

## 6. `RESTORE`

Não foi executado. Pelo que sei, `RESTORE TABLE ... TO VERSION AS OF n` não reescreve o histórico: grava uma **nova versão** com o conteúdo da versão `n`. Seria o caminho de recuperação se um build errado sobrescrevesse a gold, enquanto os arquivos antigos ainda existissem (antes do `VACUUM`).

## 7. Liquid clustering e manutenção automática (não aplicados)

- **Liquid clustering** (`CLUSTER BY`) substitui particionamento e `ZORDER` por uma organização de dados que se adapta às colunas de filtro. Não foi aplicado: as tabelas gold são pequenas. Seria candidato na silver (milhões de linhas, consultas por data e produto). Não foi testado neste projeto.
- **Manutenção automática:** pelo que sei, pipelines declarativos executam `OPTIMIZE` e `VACUUM` nas suas tabelas automaticamente, o que dispensaria rodá-los à mão na bronze e na silver. Não foi verificado aqui.

## 8. Medições ainda não capturadas

Os prints de `DESCRIBE DETAIL` da gold mostram só as primeiras colunas; `numFiles` e `sizeInBytes` ficaram fora da tela. Os valores da gold foram informados depois (uma única medição; como o `OPTIMIZE` não adicionou nem removeu arquivos, o valor se aplica a antes e depois, o que é inferido). Os da silver estão na seção 10.

| Tabela | numFiles | sizeInBytes |
|---|---|---|
| `gold.preco_medio_estado_mes` | 1 | 61.873 (~60 KiB) |
| `silver.silver_combustiveis` (tabela de materialização, seção 10) | 1 | 38.831.959 (~37 MiB) |

## 9. Observação sem explicação

O `createdAt` do `DESCRIBE DETAIL` (2026-10-21 19:59:43) é praticamente igual ao da versão mais recente, e não ao da versão 0 (17:46). O `id` da tabela e o histórico continuam os mesmos. Não verifiquei por que o `CREATE OR REPLACE` atualiza esse campo.

## 10. A silver (streaming table) vista por dentro

`DESCRIBE DETAIL gas_medallion.silver.silver_combustiveis`, observado:

| Campo | Valor |
|---|---|
| `name` | `gas_medallion.silver.__materialization_mat_<id do pipeline>_silver_combustiveis_1` |
| `numFiles` | 1 |
| `sizeInBytes` | 38.831.959 (~37 MiB), para 2.607.139 linhas |
| `createdAt` / `lastModified` (UTC) | 2026-10-21 19:48:47 / 19:49:38 |
| Partição / clusterização | nenhuma |
| `minReaderVersion` / `minWriterVersion` | 3 / 7 |
| Table features | `appendOnly`, `changeDataFeed`, `deletionVectors`, `domainMetadata`, `invariants`, `rowTracking` |
| Propriedades relevantes | compressão `zstd`; `delta.enableChangeDataFeed`, `delta.enableDeletionVectors` e `delta.enableRowTracking` ligados; `pipeline_internal.enzymeMode: Advanced` |

O último campo da linha retornada (`false`) não foi identificado.

Leituras:

- **A silver é apoiada por uma tabela de materialização interna.** O `name` retornado não é `silver_combustiveis`: a propriedade `spark.internal.streaming_table.parentTable` aponta para ela. Também existe a propriedade `spark.internal.pipelines.reconciliation_query_without_timetravel`. Isso é uma **pista, não uma confirmação**, para o erro de `VERSION AS OF` da seção 3: o time travel numa streaming table parece passar por essa camada intermediária.
- **Um único arquivo.** A carga completa foi escrita numa só passada (as datas coincidem com a janela do update do pipeline, 19:48:52 a 19:49:39), então não há problema de arquivos pequenos aqui. Esse problema surge quando muitos updates incrementais pequenos se acumulam. Se o pipeline compacta isso sozinho nas tabelas dele, não foi verificado.
- **Compressão.** Os 6 CSVs somam 443.727.135 bytes (~423 MiB), contra ~37 MiB da silver, cerca de 11 vezes menos. A comparação é aproximada: a silver descarta a coluna `valor_de_compra` (vazia) e 9.128 linhas. O ganho vem do formato colunar (Parquet) com compressão `zstd`.
- **Funcionalidades ligadas pelo pipeline:** Change Data Feed, deletion vectors e row tracking. O projeto não usa nenhuma delas diretamente.
