{{ config(materialized='table') }}

-- Ubicaciones geográficas únicas: dimensión de lugares a nivel ciudad/CP/país.
-- Fuentes:
--   type_6 receiver_address → destinos MeLi (lat/lon disponibles)
--   type_6 sender_address   → orígenes vendedor MeLi (lat/lon disponibles)
--   vtex_pedido shippingData → destinos VTEX (sin lat/lon)
--   meli_pickup store_info   → tiendas de retiro MeLi (lat/lon disponibles)
-- ubicacion_id: surrogate key md5(ciudad|codigo_postal|pais_codigo)
-- Coordenadas: lat/lon del JSON cuando disponibles (MeLi/pickup);
--   fallback a stg_direccion_geocodificada (promedio geocodificado por ciudad).
--   Fuente canónica de coordenadas para todos los modelos consume.

WITH meli_destino AS (
    SELECT DISTINCT
        json_extract_string(receiver_address, '$.city.name')                    AS ciudad,
        json_extract_string(receiver_address, '$.state.name')                   AS provincia,
        json_extract_string(receiver_address, '$.country.id')                   AS pais_codigo,
        json_extract_string(receiver_address, '$.zip_code')                     AS codigo_postal,
        NULLIF(TRY_CAST(json_extract_string(receiver_address, '$.latitude')  AS DOUBLE), 0) AS latitud,
        NULLIF(TRY_CAST(json_extract_string(receiver_address, '$.longitude') AS DOUBLE), 0) AS longitud
    FROM {{ source('raw', 'type_6') }}
    WHERE receiver_address IS NOT NULL
),

meli_origen AS (
    SELECT DISTINCT
        json_extract_string(sender_address, '$.city.name')                      AS ciudad,
        json_extract_string(sender_address, '$.state.name')                     AS provincia,
        json_extract_string(sender_address, '$.country.id')                     AS pais_codigo,
        json_extract_string(sender_address, '$.zip_code')                       AS codigo_postal,
        NULLIF(TRY_CAST(json_extract_string(sender_address, '$.latitude')  AS DOUBLE), 0) AS latitud,
        NULLIF(TRY_CAST(json_extract_string(sender_address, '$.longitude') AS DOUBLE), 0) AS longitud
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
        NULLIF(TRY_CAST(json_extract_string(store_info, '$.location.latitude')  AS DOUBLE), 0) AS latitud,
        NULLIF(TRY_CAST(json_extract_string(store_info, '$.location.longitude') AS DOUBLE), 0) AS longitud
    FROM {{ source('raw', 'meli_pickup') }}
    WHERE store_info IS NOT NULL
),

unificado AS (
    SELECT * FROM meli_destino
    UNION ALL SELECT * FROM meli_origen
    UNION ALL SELECT * FROM vtex_geo
    UNION ALL SELECT * FROM pickup_tienda
),

-- Dedup: una fila por (ciudad, codigo_postal, pais_codigo).
-- Prioriza registros con lat/lon del JSON (MeLi/pickup) sobre los sin coordenadas (VTEX).
deduped AS (
    SELECT
        md5(
            coalesce(ciudad, '')        || '|' ||
            coalesce(codigo_postal, '') || '|' ||
            coalesce(pais_codigo, 'AR')
        )               AS ubicacion_id,
        ciudad,
        provincia,
        -- Homologar: ISO 3166-1 alpha-3 → alpha-2
        CASE pais_codigo WHEN 'ARG' THEN 'AR' ELSE pais_codigo END AS pais_codigo,
        codigo_postal,
        latitud,
        longitud
    FROM unificado
    WHERE ciudad IS NOT NULL OR codigo_postal IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY ciudad, codigo_postal, pais_codigo
        ORDER BY latitud NULLS LAST
    ) = 1
),

-- Coordenadas geocodificadas por ciudad desde stg_direccion_geocodificada.
-- Actúa como fallback cuando el JSON no trae lat/lon (VTEX, Garbarino).
coord_ciudad AS (
    SELECT
        LOWER(ciudad)   AS ciudad_key,
        AVG(latitud)    AS latitud_geo,
        AVG(longitud)   AS longitud_geo
    FROM {{ ref('stg_direccion_geocodificada') }}
    WHERE ciudad IS NOT NULL
    GROUP BY LOWER(ciudad)
)

SELECT
    d.ubicacion_id,
    d.ciudad,
    replace(replace(replace(replace(replace(
        UPPER(d.provincia),
        'Á', 'A'), 'É', 'E'), 'Í', 'I'), 'Ó', 'O'), 'Ú', 'U')
                                                             AS provincia,
    d.pais_codigo,
    d.codigo_postal,
    COALESCE(d.latitud,  c.latitud_geo)  AS latitud,
    COALESCE(d.longitud, c.longitud_geo) AS longitud
FROM deduped d
LEFT JOIN coord_ciudad c ON LOWER(d.ciudad) = c.ciudad_key
