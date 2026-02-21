-- Conciliación: cuánto se cobró en el pedido vs cuánto llegó neto a la cuenta.
-- Cruza stg_pedido (meli) con type_8 (MercadoPago) via order_id.

select
    p.pedido_id,
    p.orden_id_origen                                       as order_id,
    p.status                                                as status_pedido,
    p.monto_total                                           as monto_pedido,
    p.fecha_creacion,
    pg.id                                                   as pago_id,
    pg.status                                               as status_pago,
    pg.status_detail,
    pg.transaction_amount                                   as monto_cobrado,
    pg.net_received_amount                                  as monto_neto_recibido,
    pg.mercadopago_fee                                      as fee_mp,
    pg.marketplace_fee                                      as fee_marketplace,
    (coalesce(pg.mercadopago_fee, 0)
        + coalesce(pg.marketplace_fee, 0))                  as fees_totales,
    pg.payment_type,
    pg.payment_method_id,
    pg.installments                                         as cuotas,
    pg.currency_id                                          as moneda,
    TRY_CAST(pg.date_approved as timestamp)                 as fecha_aprobacion,
    p.monto_total - coalesce(pg.net_received_amount, 0)     as diferencia_neto
from {{ ref('stg_pedido') }} p
left join {{ source('raw', 'type_8') }} pg
    on pg.order_id::varchar = p.orden_id_origen
where p.canal = 'mercadolibre'
