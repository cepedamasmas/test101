{{ config(materialized='table') }}

-- Líneas de pedido: un registro por producto dentro de cada pedido.
-- item_pedido_id: surrogate key md5(pedido_id|producto_id_origen)
-- Nota: precio VTEX en centavos → se divide por 100.

WITH meli_items AS (
    SELECT
        md5('mercadolibre|' || id::VARCHAR)                             AS pedido_id,
        id::VARCHAR                                                     AS orden_id_origen,
        'mercadolibre'                                                  AS canal,
        json_extract_string(item_row, '$.item.id')                     AS producto_id_origen,
        json_extract_string(item_row, '$.item.title')                  AS nombre_producto,
        TRY_CAST(json_extract_string(item_row, '$.quantity')   AS INTEGER) AS cantidad,
        TRY_CAST(json_extract_string(item_row, '$.unit_price') AS DOUBLE)  AS precio_unitario,
        TRY_CAST(json_extract_string(item_row, '$.quantity')   AS INTEGER)
            * TRY_CAST(json_extract_string(item_row, '$.unit_price') AS DOUBLE) AS subtotal,
        json_extract_string(item_row, '$.currency_id')                 AS moneda
    FROM (
        SELECT id, UNNEST(from_json(order_items, '["JSON"]')) AS item_row
        FROM {{ source('raw', 'meli_pedido') }}
        WHERE order_items IS NOT NULL AND order_items <> '[]'
    )
),

vtex_items AS (
    SELECT
        md5('vtex|' || orderId)                                         AS pedido_id,
        orderId                                                         AS orden_id_origen,
        'vtex'                                                          AS canal,
        json_extract_string(item_row, '$.productId')                   AS producto_id_origen,
        json_extract_string(item_row, '$.name')                        AS nombre_producto,
        TRY_CAST(json_extract_string(item_row, '$.quantity')      AS INTEGER) AS cantidad,
        TRY_CAST(json_extract_string(item_row, '$.sellingPrice')   AS DOUBLE)
            / 100.0                                                     AS precio_unitario,
        TRY_CAST(json_extract_string(item_row, '$.quantity')      AS INTEGER)
            * TRY_CAST(json_extract_string(item_row, '$.sellingPrice') AS DOUBLE)
            / 100.0                                                     AS subtotal,
        'ARS'                                                           AS moneda
    FROM (
        SELECT orderId, UNNEST(from_json(items, '["JSON"]')) AS item_row
        FROM {{ source('raw', 'vtex_pedido') }}
        WHERE items IS NOT NULL AND items <> '[]'
    )
),

garbarino_items AS (
    SELECT
        md5('garbarino|' || id::VARCHAR)                                AS pedido_id,
        id::VARCHAR                                                     AS orden_id_origen,
        'garbarino'                                                     AS canal,
        json_extract_string(item_row, '$.product_id')                  AS producto_id_origen,
        json_extract_string(item_row, '$.title')                       AS nombre_producto,
        TRY_CAST(json_extract_string(item_row, '$.quantity') AS INTEGER)  AS cantidad,
        TRY_CAST(json_extract_string(item_row, '$.price')    AS DOUBLE)   AS precio_unitario,
        TRY_CAST(json_extract_string(item_row, '$.quantity') AS INTEGER)
            * TRY_CAST(json_extract_string(item_row, '$.price') AS DOUBLE) AS subtotal,
        'ARS'                                                           AS moneda
    FROM (
        SELECT id, UNNEST(from_json(sold_items, '["JSON"]')) AS item_row
        FROM {{ source('raw', 'garbarino_pedido') }}
        WHERE sold_items IS NOT NULL AND sold_items <> '[]'
    )
),

unificado AS (
    SELECT * FROM meli_items
    UNION ALL SELECT * FROM vtex_items
    UNION ALL SELECT * FROM garbarino_items
)

SELECT
    md5(pedido_id || '|' || coalesce(producto_id_origen, nombre_producto)) AS item_pedido_id,
    pedido_id,
    orden_id_origen,
    canal,
    producto_id_origen,
    nombre_producto,
    cantidad,
    precio_unitario,
    subtotal,
    moneda
FROM unificado
