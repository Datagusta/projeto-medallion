-- Grão: estado x município x produto (todo o período disponível).
-- O estado entra na chave porque nomes de município se repetem entre UFs.
-- Municípios com poucas coletas ficam fora (var min_coletas_ranking), para o ranking não refletir ruído.
with base as (
    select
        estado_sigla,
        municipio,
        produto,
        unidade_medida,
        round(avg(valor_venda), 3) as preco_medio,
        count(*) as qtd_coletas,
        count(distinct cnpj_revenda) as qtd_postos,
        min(data_coleta) as primeira_coleta,
        max(data_coleta) as ultima_coleta
    from {{ source('silver', 'silver_combustiveis') }}
    group by 1, 2, 3, 4
    having count(*) >= {{ var('min_coletas_ranking') }}
)

select
    *,
    rank() over (partition by produto order by preco_medio desc) as rank_mais_caro,
    rank() over (partition by produto order by preco_medio asc) as rank_mais_barato
from base