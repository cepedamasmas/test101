{{ config(materialized='table') }}

-- Productos únicos por canal, extraídos de los arrays de ítems en cada pedido.
-- producto_id: surrogate key md5(canal|producto_id_origen)
-- Nota: precio_unitario de VTEX viene en centavos → se divide por 100.

WITH meli_items AS (
    SELECT DISTINCT
        'mercadolibre'                                                          AS canal,
        json_extract_string(item_row, '$.item.id')                             AS producto_id_origen,
        json_extract_string(item_row, '$.item.title')                          AS nombre,
        json_extract_string(item_row, '$.item.category_id')                    AS categoria_id,
        TRY_CAST(json_extract_string(item_row, '$.unit_price') AS DOUBLE)      AS precio_unitario,
        json_extract_string(item_row, '$.currency_id')                         AS moneda
    FROM (
        SELECT UNNEST(from_json(order_items, '["JSON"]')) AS item_row
        FROM {{ source('raw', 'meli_pedido') }}
        WHERE order_items IS NOT NULL AND order_items <> '[]'
    )
),

vtex_items AS (
    SELECT DISTINCT
        'vtex'                                                                  AS canal,
        json_extract_string(item_row, '$.productId')                           AS producto_id_origen,
        json_extract_string(item_row, '$.name')                                AS nombre,
        NULL                                                                    AS categoria_id,
        TRY_CAST(json_extract_string(item_row, '$.sellingPrice') AS DOUBLE)
            / 100.0                                                             AS precio_unitario,
        'ARS'                                                                   AS moneda
    FROM (
        SELECT UNNEST(from_json(items, '["JSON"]')) AS item_row
        FROM {{ source('raw', 'vtex_pedido') }}
        WHERE items IS NOT NULL AND items <> '[]'
    )
),

garbarino_items AS (
    SELECT DISTINCT
        'garbarino'                                                             AS canal,
        json_extract_string(item_row, '$.product_id')                          AS producto_id_origen,
        json_extract_string(item_row, '$.title')                               AS nombre,
        NULL                                                                    AS categoria_id,
        TRY_CAST(json_extract_string(item_row, '$.price') AS DOUBLE)           AS precio_unitario,
        'ARS'                                                                   AS moneda
    FROM (
        SELECT UNNEST(from_json(sold_items, '["JSON"]')) AS item_row
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
    md5(canal || '|' || coalesce(producto_id_origen, '')) AS producto_id,
    canal,
    producto_id_origen,
    nombre,
    categoria_id,
    precio_unitario,
    moneda
FROM unificado
WHERE producto_id_origen IS NOT NULL
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY canal, producto_id_origen
    ORDER BY precio_unitario NULLS LAST
) = 1
