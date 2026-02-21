-- Tiempo promedio entre creación del pedido y cierre/última actualización, por canal y status.
-- Proxy de velocidad de entrega: a menor días, mejor performance operativa.

select
    p.canal,
    p.status,
    count(distinct p.pedido_id)                             as total_pedidos,
    round(avg(
        date_diff(
            'day',
            p.fecha_creacion,
            coalesce(p.fecha_cierre, p.fecha_actualizacion)
        )
    ), 1)                                                   as dias_promedio,
    round(min(
        date_diff(
            'day',
            p.fecha_creacion,
            coalesce(p.fecha_cierre, p.fecha_actualizacion)
        )
    ), 1)                                                   as dias_minimo,
    round(max(
        date_diff(
            'day',
            p.fecha_creacion,
            coalesce(p.fecha_cierre, p.fecha_actualizacion)
        )
    ), 1)                                                   as dias_maximo
from {{ ref('stg_pedido') }} p
where p.status not in ('cancelled', 'canceled', 'cancelado')
  and p.fecha_creacion is not null
  and coalesce(p.fecha_cierre, p.fecha_actualizacion) is not null
group by p.canal, p.status
order by p.canal, dias_promedio
