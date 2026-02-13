select
    e.courier,
    count(*) as envios,
    round(avg(e.costo_envio), 0) as costo_promedio,
    round(avg(e.peso_kg), 2) as peso_promedio_kg,
    sum(case when e.estado_actual = 'entregado' then 1 else 0 end) as entregados,
    sum(case when e.estado_actual = 'devuelto' then 1 else 0 end) as devueltos,
    round(sum(case when e.estado_actual = 'devuelto' then 1 else 0 end) * 100.0 / count(*), 1) as pct_devolucion
from {{ ref('stg_envios') }} e
group by e.courier
order by envios desc
