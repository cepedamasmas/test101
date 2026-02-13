select
    c.nombre as cliente, c.ciudad, c.email,
    count(distinct p.pedido_id) as compras,
    sum(p.total) as total_gastado,
    round(avg(p.total), 0) as ticket_promedio,
    min(p.fecha) as primera_compra,
    max(p.fecha) as ultima_compra
from {{ ref('stg_pedidos') }} p
join {{ ref('stg_clientes') }} c on p.cliente_id = c.cliente_id
where p.estado != 'cancelado'
group by c.nombre, c.ciudad, c.email
order by total_gastado desc
limit 20
