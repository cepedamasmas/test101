select
    date_trunc('month', p.fecha) as mes,
    count(distinct p.pedido_id) as pedidos,
    sum(p.total) as revenue,
    round(avg(p.total), 0) as ticket_promedio,
    count(distinct p.cliente_id) as clientes_unicos
from {{ ref('stg_pedidos') }} p
where p.estado not in ('cancelado')
group by date_trunc('month', p.fecha)
order by mes
