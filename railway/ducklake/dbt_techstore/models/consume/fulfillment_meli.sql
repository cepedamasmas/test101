-- Para cada pedido MeLi, estado del envío asociado (type_6).
-- Permite detectar pedidos pagados sin envío, envíos demorados, etc.

select
    p.pedido_id,
    p.orden_id_origen                               as order_id,
    p.status                                        as status_pedido,
    p.monto_total,
    p.fecha_creacion,
    e.id                                            as envio_id,
    e.status                                        as status_envio,
    e.substatus                                     as substatus_envio,
    e.logistic_type,
    e.tracking_number,
    e.tracking_method,
    TRY_CAST(e.date_created  as timestamp)          as fecha_envio_creado,
    TRY_CAST(e.last_updated  as timestamp)          as fecha_envio_actualizado,
    case
        when e.id is null               then 'sin_envio'
        when e.status = 'delivered'     then 'entregado'
        when e.status = 'cancelled'     then 'cancelado'
        when e.status = 'ready_to_ship' then 'listo_para_enviar'
        when e.status = 'shipped'       then 'en_transito'
        else e.status
    end                                             as estado_fulfillment
from {{ ref('stg_pedido') }} p
left join {{ source('raw', 'type_6') }} e
    on e.order_id::varchar = p.orden_id_origen
where p.canal = 'mercadolibre'
