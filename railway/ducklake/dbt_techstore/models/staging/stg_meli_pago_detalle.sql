{{ config(materialized='table') }}

-- Pagos MercadoPago (type_8 → meli_pago).
-- Una fila por transacción de pago con montos, comisiones, método y datos del pagador.
-- Se vincula a stg_pedido via orden_id_origen.
-- Dedup por id, versión más reciente según date_created.

SELECT
    id::VARCHAR                                                             AS pago_id,
    order_id::VARCHAR                                                       AS orden_id_origen,
    status,
    status_detail,
    TRY_CAST(date_created   AS TIMESTAMP)                                   AS fecha_creacion,
    TRY_CAST(date_approved  AS TIMESTAMP)                                   AS fecha_aprobacion,

    -- Montos
    TRY_CAST(transaction_amount  AS DOUBLE)                                 AS monto_transaccion,
    TRY_CAST(total_paid_amount   AS DOUBLE)                                 AS monto_pagado_total,
    TRY_CAST(net_received_amount AS DOUBLE)                                 AS monto_neto_recibido,
    TRY_CAST(mercadopago_fee     AS DOUBLE)                                 AS comision_mercadopago,
    TRY_CAST(marketplace_fee     AS DOUBLE)                                 AS comision_marketplace,

    -- Método de pago
    payment_type                                                            AS tipo_pago,
    payment_method_id                                                       AS metodo_pago_id,
    TRY_CAST(installments AS INTEGER)                                       AS cuotas,
    currency_id                                                             AS moneda,

    -- Pagador (desde JSON payer)
    json_extract_string(payer, '$.id')                                      AS pagador_id,
    json_extract_string(payer, '$.email')                                   AS pagador_email,
    TRIM(
        COALESCE(json_extract_string(payer, '$.first_name'), '') || ' ' ||
        COALESCE(json_extract_string(payer, '$.last_name'),  '')
    )                                                                       AS pagador_nombre

FROM {{ source('raw', 'type_8') }}
QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY date_created DESC NULLS LAST) = 1
