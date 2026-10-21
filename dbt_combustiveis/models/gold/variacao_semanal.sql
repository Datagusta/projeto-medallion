-- Grão: produto x semana (semana começa na segunda-feira, padrão do Spark).
-- A variação só é calculada se a semana anterior existe e é exatamente 7 dias antes;
-- com lacunas na série, o LAG compararia semanas não consecutivas e distorceria o resultado.
-- A primeira e a última semana da série podem ser parciais.
with semanal as (
    select
        produto,
        unidade_medida,
        cast(date_trunc('week', data_coleta) as date) as semana,
        round(avg(valor_venda), 3) as preco_medio,
        count(*) as qtd_coletas
    from {{ source('silver', 'silver_combustiveis') }}
    group by 1, 2, 3
),
com_anterior as (
    select
        *,
        lag(semana) over (partition by produto order by semana) as semana_anterior,
        lag(preco_medio) over (partition by produto order by semana) as preco_anterior
    from semanal
)
select
    produto,
    unidade_medida,
    semana,
    preco_medio,
    qtd_coletas,
    case when semana_anterior = date_sub(semana, 7) then preco_anterior end as preco_semana_anterior,
    case
        when semana_anterior = date_sub(semana, 7)
        then round((preco_medio - preco_anterior) / preco_anterior * 100, 2)
    end as variacao_pct
from com_anterior