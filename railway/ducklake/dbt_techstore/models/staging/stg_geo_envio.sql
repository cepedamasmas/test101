{{ config(materialized='table') }}

-- Fact geográfico de envíos: origen y destino de cada pedido con coordenadas.
-- Diseñado para visualización en mapas (heat maps, flow maps, coropleths por provincia).
-- Fuentes:
--   type_6       → MeLi: origen vendedor + destino comprador (lat/lon disponibles)
--   vtex_pedido  → VTEX: destino comprador (sin lat/lon, tiene calle+número)
--   meli_pickup  → MeLi: sucursal de retiro (lat/lon disponibles)
-- geo_envio_id: surrogate md5(tipo|canal|orden_id_origen)

WITH meli_origen AS (
    SELECT
        md5('origen|mercadolibre|' || order_id::VARCHAR)                        AS geo_envio_id,
        'origen_vendedor'                                                       AS tipo,
        'mercadolibre'                                                          AS canal,
        order_id::VARCHAR                                                       AS orden_id_origen,
        json_extract_string(sender_address, '$.city.name')                      AS ciudad,
        json_extract_string(sender_address, '$.state.name')                     AS provincia,
        json_extract_string(sender_address, '$.country.id')                     AS pais_codigo,
        json_extract_string(sender_address, '$.zip_code')                       AS codigo_postal,
        TRY_CAST(json_extract_string(sender_address, '$.latitude')  AS DOUBLE)  AS latitud,
        TRY_CAST(json_extract_string(sender_address, '$.longitude') AS DOUBLE)  AS longitud,
        json_extract_string(sender_address, '$.street_name')                    AS calle,
        json_extract_string(sender_address, '$.street_number')                  AS numero
    FROM {{ source('raw', 'type_6') }}
    WHERE sender_address IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY last_updated DESC NULLS LAST) = 1
),

meli_destino AS (
    SELECT
        md5('destino|mercadolibre|' || order_id::VARCHAR)                       AS geo_envio_id,
        'destino_comprador'                                                     AS tipo,
        'mercadolibre'                                                          AS canal,
        order_id::VARCHAR                                                       AS orden_id_origen,
        json_extract_string(receiver_address, '$.city.name')                    AS ciudad,
        json_extract_string(receiver_address, '$.state.name')                   AS provincia,
        json_extract_string(receiver_address, '$.country.id')                   AS pais_codigo,
        json_extract_string(receiver_address, '$.zip_code')                     AS codigo_postal,
        TRY_CAST(json_extract_string(receiver_address, '$.latitude')  AS DOUBLE) AS latitud,
        TRY_CAST(json_extract_string(receiver_address, '$.longitude') AS DOUBLE) AS longitud,
        json_extract_string(receiver_address, '$.street_name')                  AS calle,
        json_extract_string(receiver_address, '$.street_number')                AS numero
    FROM {{ source('raw', 'type_6') }}
    WHERE receiver_address IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY last_updated DESC NULLS LAST) = 1
),

vtex_destino AS (
    SELECT
        md5('destino|vtex|' || orderId)                                         AS geo_envio_id,
        'destino_comprador'                                                     AS tipo,
        'vtex'                                                                  AS canal,
        orderId                                                                 AS orden_id_origen,
        json_extract_string(shippingData, '$.address.city')                     AS ciudad,
        json_extract_string(shippingData, '$.address.state')                    AS provincia,
        json_extract_string(shippingData, '$.address.country')                  AS pais_codigo,
        json_extract_string(shippingData, '$.address.postalCode')               AS codigo_postal,
        NULL::DOUBLE                                                            AS latitud,
        NULL::DOUBLE                                                            AS longitud,
        json_extract_string(shippingData, '$.address.street')                   AS calle,
        json_extract_string(shippingData, '$.address.number')                   AS numero
    FROM {{ source('raw', 'vtex_pedido') }}
    WHERE shippingData IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY orderId ORDER BY lastChange DESC) = 1
),

meli_pickup_tienda AS (
    SELECT
        md5('retiro|mercadolibre|' || order_id::VARCHAR)                        AS geo_envio_id,
        'retiro_sucursal'                                                       AS tipo,
        'mercadolibre'                                                          AS canal,
        order_id::VARCHAR                                                       AS orden_id_origen,
        json_extract_string(store_info, '$.location.address.city')              AS ciudad,
        json_extract_string(store_info, '$.location.address.state')             AS provincia,
        NULL::VARCHAR                                                           AS pais_codigo,
        json_extract_string(store_info, '$.location.address.zip_code')          AS codigo_postal,
        TRY_CAST(json_extract_string(store_info, '$.location.latitude')  AS DOUBLE) AS latitud,
        TRY_CAST(json_extract_string(store_info, '$.location.longitude') AS DOUBLE) AS longitud,
        json_extract_string(store_info, '$.location.address.address_line')      AS calle,
        NULL::VARCHAR                                                           AS numero
    FROM {{ source('raw', 'meli_pickup') }}
    WHERE store_info IS NOT NULL
)

SELECT * FROM meli_origen
UNION ALL SELECT * FROM meli_destino
UNION ALL SELECT * FROM vtex_destino
UNION ALL SELECT * FROM meli_pickup_tienda
