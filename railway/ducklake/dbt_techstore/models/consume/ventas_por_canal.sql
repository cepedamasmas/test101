select
    date_trunc('month', p.fecha_creacion)       as mes,
    p.canal,
    p.canal_id,
    count(distinct p.pedido_id)                 as total_pedidos,
    count(distinct cp.cliente_id)               as clientes_unicos,
    sum(p.monto_total)                          as revenue,
    round(avg(p.monto_total), 0)                as ticket_promedio,
    min(p.monto_total)                          as ticket_minimo,
    max(p.monto_total)                          as ticket_maximo
from {{ ref('stg_pedido') }} p
left join {{ ref('stg_cliente_pedido') }} cp on cp.pedido_id = p.pedido_id
where p.status not in ('cancelled', 'canceled', 'cancelado')
  and p.fecha_creacion is not null
group by date_trunc('month', p.fecha_creacion), p.canal, p.canal_id
order by mes, p.canal
