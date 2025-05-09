--query sales camocim
select
	cast(dt_venda as date) as dt_venda,
	hora_venda,
	qtd_clientes,
	valor_vendas,
	maior_venda
from
	portfolio-408419.pescados.vendas
where 
	loja = 'Camocim'

--query sales mac
select
	cast(dt_venda as date) as dt_venda,
	produto,
	qtd_produtos_unid,
	valor_vendas,
	tipo_corte
from
	portfolio-408419.pescados.vendas
where 
	loja = 'Mac'

--query camocim inventory
select
	cast(dt_estoque as date) as dt_estoque,
	id_fornecedor,
	fornecedor,
	id_produto,
	produto,
	qtd_estoque_kg,
	qtd_saida_kg,
	tipo_corte
from
	portfolio-408419.pescados.estoque
where 
	loja = 'Camocim' 
	and (qtd_estoque_kg > 0 or qtd_saida_kg > 0)

--query mac inventory
select
	cast(dt_estoque as date) as dt_estoque,
	id_fornecedor,
	produto,
	valor_compra,
	qtd_estoque_kg,
	tipo_corte
from
	portfolio-408419.pescados.estoque
where 
	loja = 'Mac'
