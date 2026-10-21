-- Falha se existir preço médio mensal fora da faixa plausível (R$ 2 a R$ 12).
-- O teste retorna as linhas violadoras; zero linhas = passou.
select *
from {{ ref('preco_medio_estado_mes') }}
where preco_medio < 2
   or preco_medio > 12