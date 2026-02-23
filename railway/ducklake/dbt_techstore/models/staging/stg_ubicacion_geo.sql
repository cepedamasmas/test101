{{ config(materialized='table') }}

-- Ubicaciones geográficas únicas: dimensión de lugares a nivel ciudad/CP/país.
-- Fuentes:
--   type_6 receiver_address → destinos MeLi (lat/lon disponibles)
--   type_6 sender_address   → orígenes vendedor MeLi (lat/lon disponibles)
--   vtex_pedido shippingData → destinos VTEX (sin lat/lon)
--   meli_pickup store_info   → tiendas de retiro MeLi (lat/lon disponibles)
-- ubicacion_id: surrogate key md5(ciudad|codigo_postal|pais_codigo)
-- QUALIFY: si hay duplicados, prioriza el registro que tenga lat/lon.

WITH meli_destino AS (
    SELECT DISTINCT
        json_extract_string(receiver_address, '$.city.name')                    AS ciudad,
        json_extract_string(receiver_address, '$.state.name')                   AS provincia,
        json_extract_string(receiver_address, '$.country.id')                   AS pais_codigo,
        json_extract_string(receiver_address, '$.zip_code')                     AS codigo_postal,
        TRY_CAST(json_extract_string(receiver_address, '$.latitude')  AS DOUBLE) AS latitud,
        TRY_CAST(json_extract_string(receiver_address, '$.longitude') AS DOUBLE) AS longitud
    FROM {{ source('raw', 'type_6') }}
    WHERE receiver_address IS NOT NULL
),

meli_origen AS (
    SELECT DISTINCT
        json_extract_string(sender_address, '$.city.name')                      AS ciudad,
        json_extract_string(sender_address, '$.state.name')                     AS provincia,
        json_extract_string(sender_address, '$.country.id')                     AS pais_codigo,
        json_extract_string(sender_address, '$.zip_code')                       AS codigo_postal,
        TRY_CAST(json_extract_string(sender_address, '$.latitude')  AS DOUBLE)  AS latitud,
        TRY_CAST(json_extract_string(sender_address, '$.longitude') AS DOUBLE)  AS longitud
    FROM {{ source('raw', 'type_6') }}
    WHERE sender_address IS NOT NULL
),

vtex_geo AS (
    SELECT DISTINCT
        json_extract_string(shippingData, '$.address.city')                     AS ciudad,
        json_extract_string(shippingData, '$.address.state')                    AS provincia,
        json_extract_string(shippingData, '$.address.country')                  AS pais_codigo,
        json_extract_string(shippingData, '$.address.postalCode')               AS codigo_postal,
        NULL::DOUBLE                                                            AS latitud,
        NULL::DOUBLE                                                            AS longitud
    FROM {{ source('raw', 'vtex_pedido') }}
    WHERE shippingData IS NOT NULL
),

pickup_tienda AS (
    SELECT DISTINCT
        json_extract_string(store_info, '$.location.address.city')              AS ciudad,
        json_extract_string(store_info, '$.location.address.state')             AS provincia,
        'AR'::VARCHAR                                                           AS pais_codigo,
        json_extract_string(store_info, '$.location.address.zip_code')          AS codigo_postal,
        TRY_CAST(json_extract_string(store_info, '$.location.latitude')  AS DOUBLE) AS latitud,
        TRY_CAST(json_extract_string(store_info, '$.location.longitude') AS DOUBLE) AS longitud
    FROM {{ source('raw', 'meli_pickup') }}
    WHERE store_info IS NOT NULL
),

unificado AS (
    SELECT * FROM meli_destino
    UNION ALL SELECT * FROM meli_origen
    UNION ALL SELECT * FROM vtex_geo
    UNION ALL SELECT * FROM pickup_tienda
)

SELECT
    md5(
        coalesce(ciudad, '')        || '|' ||
        coalesce(codigo_postal, '') || '|' ||
        coalesce(pais_codigo, 'AR')
    )               AS ubicacion_id,
    ciudad,
    provincia,
    pais_codigo,
    codigo_postal,
    latitud,
    longitud
FROM unificado
WHERE ciudad IS NOT NULL OR codigo_postal IS NOT NULL
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY ciudad, codigo_postal, pais_codigo
    ORDER BY latitud NULLS LAST
) = 1
