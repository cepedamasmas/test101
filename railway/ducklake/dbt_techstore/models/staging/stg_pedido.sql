{{ config(materialized='table') }}

-- Fact table central: pedidos unificados de las 3 plataformas.
-- pedido_id: surrogate key md5(canal|orden_id_origen)
-- canal_id: obtenido via join a stg_canal
-- direccion_id: FK a stg_direccion (NULL si address es token anonimizado o no hay datos)
--   MeLi: la dirección está en type_6.receiver_address (JSON real, join por order_id)
--   VTEX: embebida en vtex_pedido.shippingData (puede ser token anonimizado)
--   Garbarino: embebida en garbarino_pedido.billing_address (puede ser token anonimizado)
-- Guard LIKE check: evita json_extract_string en tokens anonimizados tipo "direccion_000016".
-- Dedup: se queda con el registro más reciente por orden_id_origen.
-- Nota: monto_total de VTEX viene en centavos → se divide por 100.

WITH canal AS (
    SELECT canal_id, canal FROM {{ ref('stg_canal') }}
),

-- Direcciones MeLi desde type_6 (JSON real), dedup por order_id
meli_dir AS (
    SELECT
        order_id::VARCHAR AS orden_id_origen,
        md5(
            coalesce(json_extract_string(receiver_address, '$.street_name'),  '') || '|' ||
            coalesce(json_extract_string(receiver_address, '$.street_number'),'') || '|' ||
            coalesce(json_extract_string(receiver_address, '$.city.name'),    '') || '|' ||
            coalesce(json_extract_string(receiver_address, '$.zip_code'),     '') || '|' ||
            coalesce(json_extract_string(receiver_address, '$.country.id'),  'AR')
        ) AS direccion_id
    FROM {{ source('raw', 'type_6') }}
    WHERE receiver_address IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY last_updated DESC NULLS LAST) = 1
),

vtex AS (
    SELECT
        md5('vtex|' || orderId)                                                 AS pedido_id,
        orderId                                                                 AS orden_id_origen,
        'vtex'                                                                  AS canal,
        status,
        TRY_CAST(value AS DOUBLE) / 100.0                                       AS monto_total,
        TRY_CAST(creationDate AS TIMESTAMP)                                     AS fecha_creacion,
        TRY_CAST(lastChange   AS TIMESTAMP)                                     AS fecha_actualizacion,
        NULL::TIMESTAMP                                                         AS fecha_cierre,
        CASE WHEN shippingData LIKE '{' || '%' THEN
            md5(
                coalesce(json_extract_string(shippingData, '$.address.street'),     '') || '|' ||
                coalesce(json_extract_string(shippingData, '$.address.number'),     '') || '|' ||
                coalesce(json_extract_string(shippingData, '$.address.city'),       '') || '|' ||
                coalesce(json_extract_string(shippingData, '$.address.postalCode'), '') || '|' ||
                coalesce(json_extract_string(shippingData, '$.address.country'),   'AR')
            )
        ELSE NULL END                                                           AS direccion_id
    FROM {{ source('raw', 'vtex_pedido') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY orderId ORDER BY lastChange DESC) = 1
),

meli AS (
    SELECT
        md5('mercadolibre|' || id::VARCHAR)                                     AS pedido_id,
        id::VARCHAR                                                             AS orden_id_origen,
        'mercadolibre'                                                          AS canal,
        status,
        TRY_CAST(total_amount AS DOUBLE)                                        AS monto_total,
        TRY_CAST(date_created  AS TIMESTAMP)                                    AS fecha_creacion,
        TRY_CAST(last_updated  AS TIMESTAMP)                                    AS fecha_actualizacion,
        TRY_CAST(date_closed   AS TIMESTAMP)                                    AS fecha_cierre,
        NULL::VARCHAR                                                           AS direccion_id  -- se resuelve via LEFT JOIN con meli_dir
    FROM {{ source('raw', 'meli_pedido') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY last_updated DESC) = 1
),

garbarino AS (
    SELECT
        md5('garbarino|' || id::VARCHAR)                                        AS pedido_id,
        id::VARCHAR                                                             AS orden_id_origen,
        'garbarino'                                                             AS canal,
        status,
        TRY_CAST(json_extract_string(totals_sale, '$.total') AS DOUBLE)         AS monto_total,
        TRY_CAST(created AS TIMESTAMP)                                          AS fecha_creacion,
        NULL::TIMESTAMP                                                         AS fecha_actualizacion,
        NULL::TIMESTAMP                                                         AS fecha_cierre,
        CASE WHEN billing_address LIKE '{' || '%' THEN
            md5(
                coalesce(json_extract_string(billing_address, '$.street'),  '') || '|' ||
                coalesce(json_extract_string(billing_address, '$.number'),  '') || '|' ||
                coalesce(json_extract_string(billing_address, '$.city'),    '') || '|' ||
                coalesce(json_extract_string(billing_address, '$.zip'),     '') || '|' ||
                coalesce(json_extract_string(billing_address, '$.country'), 'AR')
            )
        ELSE NULL END                                                           AS direccion_id
    FROM {{ source('raw', 'garbarino_pedido') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY created DESC) = 1
),

unificado AS (
    SELECT * FROM vtex
    UNION ALL SELECT * FROM meli
    UNION ALL SELECT * FROM garbarino
)

SELECT
    u.pedido_id,
    u.orden_id_origen,
    c.canal_id,
    u.canal,
    u.status,
    u.monto_total,
    u.fecha_creacion,
    u.fecha_actualizacion,
    u.fecha_cierre,
    COALESCE(u.direccion_id, d.direccion_id)                                    AS direccion_id
FROM unificado u
JOIN canal c ON c.canal = u.canal
LEFT JOIN meli_dir d ON u.canal = 'mercadolibre' AND u.orden_id_origen = d.orden_id_origen
