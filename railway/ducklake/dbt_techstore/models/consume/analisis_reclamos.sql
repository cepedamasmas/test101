select
    r.tipo,
    r.prioridad,
    count(*) as cantidad,
    sum(case when r.estado = 'resuelto' then 1 else 0 end) as resueltos,
    sum(case when r.estado = 'abierto' then 1 else 0 end) as abiertos,
    round(sum(case when r.estado = 'resuelto' then 1 else 0 end) * 100.0 / count(*), 1) as pct_resolucion
from {{ ref('stg_reclamos') }} r
group by r.tipo, r.prioridad
order by cantidad desc
