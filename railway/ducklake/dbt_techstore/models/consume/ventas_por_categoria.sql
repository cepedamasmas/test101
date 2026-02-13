select
    prod.categoria,
    count(distinct d.pedido_id) as pedidos,
    sum(d.cantidad) as unidades,
    sum(d.subtotal) as revenue,
    round(avg(prod.margen_pct), 1) as margen_promedio_pct
from {{ ref('stg_detalle_pedidos') }} d
join {{ ref('stg_productos') }} prod on d.producto_id = prod.producto_id
group by prod.categoria
order by revenue desc
