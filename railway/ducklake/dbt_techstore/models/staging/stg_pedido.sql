{{ config(materialized='table') }}

-- Fact table central: pedidos unificados de las 3 plataformas.
-- pedido_id: surrogate key md5(canal|orden_id_origen)
-- canal_id: obtenido via join a stg_canal
-- Dedup: se queda con el registro más reciente por orden_id_origen.
-- Nota: monto_total de VTEX viene en centavos → se divide por 100.

WITH canal AS (
    SELECT canal_id, canal FROM {{ ref('stg_canal') }}
),

vtex AS (
    SELECT
        md5('vtex|' || orderId)                                         AS pedido_id,
        orderId                                                         AS orden_id_origen,
        'vtex'                                                          AS canal,
        status,
        TRY_CAST(value AS DOUBLE) / 100.0                               AS monto_total,
        TRY_CAST(creationDate AS TIMESTAMP)                             AS fecha_creacion,
        TRY_CAST(lastChange   AS TIMESTAMP)                             AS fecha_actualizacion,
        NULL::TIMESTAMP                                                  AS fecha_cierre
    FROM {{ source('raw', 'vtex_pedido') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY orderId ORDER BY lastChange DESC) = 1
),

meli AS (
    SELECT
        md5('mercadolibre|' || id::VARCHAR)                             AS pedido_id,
        id::VARCHAR                                                     AS orden_id_origen,
        'mercadolibre'                                                  AS canal,
        status,
        TRY_CAST(total_amount AS DOUBLE)                                AS monto_total,
        TRY_CAST(date_created  AS TIMESTAMP)                            AS fecha_creacion,
        TRY_CAST(last_updated  AS TIMESTAMP)                            AS fecha_actualizacion,
        TRY_CAST(date_closed   AS TIMESTAMP)                            AS fecha_cierre
    FROM {{ source('raw', 'meli_pedido') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY last_updated DESC) = 1
),

garbarino AS (
    SELECT
        md5('garbarino|' || id::VARCHAR)                                AS pedido_id,
        id::VARCHAR                                                     AS orden_id_origen,
        'garbarino'                                                     AS canal,
        status,
        TRY_CAST(json_extract_string(totals_sale, '$.total') AS DOUBLE) AS monto_total,
        TRY_CAST(created AS TIMESTAMP)                                  AS fecha_creacion,
        NULL::TIMESTAMP                                                  AS fecha_actualizacion,
        NULL::TIMESTAMP                                                  AS fecha_cierre
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
    u.fecha_cierre
FROM unificado u
JOIN canal c ON c.canal = u.canal
