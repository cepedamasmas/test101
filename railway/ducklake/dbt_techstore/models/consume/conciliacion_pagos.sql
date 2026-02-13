select
    p.metodo_pago,
    count(*) as transacciones,
    sum(p.total) as monto_pedidos,
    coalesce(sum(pb.monto), 0) as monto_banco,
    coalesce(sum(mp.monto_bruto), 0) as monto_mp,
    coalesce(sum(mp.comision_mp), 0) as comisiones_mp,
    coalesce(sum(pb.comision_bancaria), 0) as comisiones_banco,
    round(
        case when sum(p.total) > 0
        then (coalesce(sum(pb.monto), 0) + coalesce(sum(mp.monto_bruto), 0)) * 100.0 / sum(p.total)
        else 0 end, 1
    ) as pct_conciliado
from {{ ref('stg_pedidos') }} p
left join {{ ref('stg_pagos_banco') }} pb on p.pedido_id = pb.pedido_id
left join {{ ref('stg_liquidacion_mp') }} mp on p.pedido_id = mp.pedido_id
where p.estado != 'cancelado'
group by p.metodo_pago
order by transacciones desc
