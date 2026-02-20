-- TEST: Reclamos con fecha anterior a la del pedido al que pertenecen
--
-- Un reclamo no puede existir antes que el pedido que lo originó.
-- Si falla: hay problema con zonas horarias al castear fechas,
-- o datos corruptos en la fuente (reclamo asignado al pedido equivocado).

select
    r.reclamo_id,
    r.pedido_id,
    r.fecha_reclamo,
    p.fecha as fecha_pedido,
    p.fecha - r.fecha_reclamo as dias_de_diferencia
from {{ ref('stg_reclamos') }} r
join {{ ref('stg_pedidos') }} p
    on r.pedido_id = p.pedido_id
where r.fecha_reclamo < p.fecha
