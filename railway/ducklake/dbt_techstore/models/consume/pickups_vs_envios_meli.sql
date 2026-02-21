-- Comparativa: retiros en sucursal (meli_pickup) vs envíos a domicilio (type_6).
-- Responde: ¿qué porcentaje de operaciones son pickups vs envíos?

with pickups as (
    select
        'pickup'                                        as tipo,
        count(*)                                        as cantidad,
        count(distinct order_id)                        as ordenes_unicas,
        min(TRY_CAST(date_created as timestamp))        as primera_operacion,
        max(TRY_CAST(date_created as timestamp))        as ultima_operacion
    from {{ source('raw', 'meli_pickup') }}
    where status != 'cancelled'
),

envios as (
    select
        'envio'                                         as tipo,
        count(*)                                        as cantidad,
        count(distinct order_id)                        as ordenes_unicas,
        min(TRY_CAST(date_created as timestamp))        as primera_operacion,
        max(TRY_CAST(last_updated as timestamp))        as ultima_operacion
    from {{ source('raw', 'type_6') }}
    where status != 'cancelled'
),

unificado as (
    select * from pickups
    union all
    select * from envios
)

select
    tipo,
    cantidad,
    ordenes_unicas,
    primera_operacion,
    ultima_operacion,
    round(100.0 * cantidad / sum(cantidad) over (), 2)       as pct_del_total,
    round(100.0 * ordenes_unicas / sum(ordenes_unicas) over (), 2) as pct_ordenes
from unificado
order by cantidad desc
