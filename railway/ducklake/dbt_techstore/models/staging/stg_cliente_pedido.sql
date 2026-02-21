{{ config(materialized='table') }}

-- Bridge: un registro por cada (cliente, pedido).
-- Permite analizar comportamiento de compra por cliente.
-- cliente_id y pedido_id son surrogate keys consistentes con stg_cliente y stg_pedido.

WITH vtex AS (
    SELECT
        md5('vtex|' || coalesce(json_extract_string(clientProfileData, '$.userProfileId'), '')) AS cliente_id,
        md5('vtex|' || orderId)                                                                  AS pedido_id,
        'vtex'                                                                                   AS canal,
        TRY_CAST(value AS DOUBLE) / 100.0                                                        AS monto_total,
        TRY_CAST(creationDate AS TIMESTAMP)                                                      AS fecha_pedido,
        status
    FROM {{ source('raw', 'vtex_pedido') }}
    WHERE clientProfileData IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY orderId ORDER BY lastChange DESC) = 1
),

meli AS (
    SELECT
        md5('mercadolibre|' || coalesce(json_extract_string(buyer, '$.id'), '')) AS cliente_id,
        md5('mercadolibre|' || id::VARCHAR)                                       AS pedido_id,
        'mercadolibre'                                                            AS canal,
        TRY_CAST(total_amount AS DOUBLE)                                          AS monto_total,
        TRY_CAST(date_created AS TIMESTAMP)                                       AS fecha_pedido,
        status
    FROM {{ source('raw', 'meli_pedido') }}
    WHERE buyer IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY last_updated DESC) = 1
),

garbarino AS (
    SELECT
        md5('garbarino|' || coalesce(json_extract_string(customer, '$.id'), '')) AS cliente_id,
        md5('garbarino|' || id::VARCHAR)                                          AS pedido_id,
        'garbarino'                                                               AS canal,
        TRY_CAST(json_extract_string(totals_sale, '$.total') AS DOUBLE)          AS monto_total,
        TRY_CAST(created AS TIMESTAMP)                                            AS fecha_pedido,
        status
    FROM {{ source('raw', 'garbarino_pedido') }}
    WHERE customer IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY created DESC) = 1
)

SELECT * FROM vtex
UNION ALL SELECT * FROM meli
UNION ALL SELECT * FROM garbarino
