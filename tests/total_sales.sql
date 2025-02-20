-- Testes de dados
-- No desafio foi solicitado para fazer o teste de dados para o total de vendas bruto em 2011 com o valor de $12.646.112,16

select sum(order_qty * unit_price) as total_sales
from {{ ref('fct_sales') }}
where year(order_date) = 2011
having total_sales != 12646104.41