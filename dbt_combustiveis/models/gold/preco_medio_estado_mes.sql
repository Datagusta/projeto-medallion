-- Grão: estado x produto x mês.
-- GNV (R$ / m³) fica em linhas próprias por produto e unidade; não é misturado com os produtos em litro.
select
    regiao_sigla,
    estado_sigla,
    produto,
    case
        when produto in ('GASOLINA', 'GASOLINA ADITIVADA') then 'GASOLINA'
        when produto in ('DIESEL', 'DIESEL S10') then 'DIESEL'
        else produto
    end as familia_produto,
    unidade_medida,
    cast(date_trunc('month', data_coleta) as date) as mes,
    round(avg(valor_venda), 3) as preco_medio,
    min(valor_venda) as preco_minimo,
    max(valor_venda) as preco_maximo,
    count(*) as qtd_coletas,
    count(distinct cnpj_revenda) as qtd_postos
from {{ source('silver', 'silver_combustiveis') }}
group by 1, 2, 3, 4, 5, 6