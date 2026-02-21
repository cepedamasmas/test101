{{ config(materialized='table') }}

-- Clientes unificados desde las 3 plataformas.
-- cliente_id: surrogate key md5(canal|cliente_id_origen)

WITH vtex AS (
    SELECT DISTINCT
        'vtex'                                                          AS canal,
        json_extract_string(clientProfileData, '$.userProfileId')      AS cliente_id_origen,
        json_extract_string(clientProfileData, '$.email')              AS email,
        json_extract_string(clientProfileData, '$.firstName')          AS nombre,
        json_extract_string(clientProfileData, '$.lastName')           AS apellido,
        json_extract_string(clientProfileData, '$.phone')              AS telefono
    FROM {{ source('raw', 'vtex_pedido') }}
    WHERE clientProfileData IS NOT NULL
),

meli AS (
    SELECT DISTINCT
        'mercadolibre'                                                  AS canal,
        json_extract_string(buyer, '$.id')                             AS cliente_id_origen,
        json_extract_string(buyer, '$.email')                          AS email,
        json_extract_string(buyer, '$.nickname')                       AS nombre,
        NULL                                                            AS apellido,
        NULL                                                            AS telefono
    FROM {{ source('raw', 'meli_pedido') }}
    WHERE buyer IS NOT NULL
),

garbarino AS (
    SELECT DISTINCT
        'garbarino'                                                     AS canal,
        json_extract_string(customer, '$.id')                          AS cliente_id_origen,
        json_extract_string(customer, '$.email')                       AS email,
        json_extract_string(customer, '$.name')                        AS nombre,
        NULL                                                            AS apellido,
        NULL                                                            AS telefono
    FROM {{ source('raw', 'garbarino_pedido') }}
    WHERE customer IS NOT NULL
),

unificado AS (
    SELECT * FROM vtex
    UNION ALL SELECT * FROM meli
    UNION ALL SELECT * FROM garbarino
)

SELECT
    md5(canal || '|' || coalesce(cliente_id_origen, '')) AS cliente_id,
    canal,
    cliente_id_origen,
    email,
    nombre,
    apellido,
    telefono
FROM unificado
WHERE cliente_id_origen IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY canal, cliente_id_origen ORDER BY email NULLS LAST) = 1
