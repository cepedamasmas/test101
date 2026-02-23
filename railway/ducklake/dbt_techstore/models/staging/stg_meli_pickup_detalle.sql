{{ config(materialized='table') }}

-- Retiros en sucursal MercadoLibre (meli_pickup).
-- Dataset acotado: ~1,980 filas, abr 2019 - mar 2020.
-- Incluye datos de la tienda con geolocalización (lat/lon) y persona que retira.
-- Se vincula a stg_pedido via orden_id_origen.

SELECT
    id::VARCHAR                                                                     AS pickup_id,
    order_id::VARCHAR                                                               AS orden_id_origen,
    status,
    TRY_CAST(date_created AS TIMESTAMP)                                             AS fecha_creacion,
    TRY_CAST(date_ready   AS TIMESTAMP)                                             AS fecha_lista,
    buyer_id::VARCHAR                                                               AS comprador_id,

    -- Tienda (desde JSON store_info)
    json_extract_string(store_info, '$.id')                                         AS tienda_id,
    json_extract_string(store_info, '$.name')                                       AS tienda_nombre,
    json_extract_string(store_info, '$.location.address.city')                      AS tienda_ciudad,
    json_extract_string(store_info, '$.location.address.state')                     AS tienda_provincia,
    json_extract_string(store_info, '$.location.address.zip_code')                  AS tienda_codigo_postal,
    json_extract_string(store_info, '$.location.address.address_line')              AS tienda_direccion,
    TRY_CAST(json_extract_string(store_info, '$.location.latitude')  AS DOUBLE)     AS tienda_latitud,
    TRY_CAST(json_extract_string(store_info, '$.location.longitude') AS DOUBLE)     AS tienda_longitud,

    -- Persona que retira (desde JSON pickup_person)
    json_extract_string(pickup_person, '$.name')                                    AS persona_nombre,
    json_extract_string(pickup_person, '$.id_type')                                 AS persona_doc_tipo,
    json_extract_string(pickup_person, '$.id_number')                               AS persona_doc_numero

FROM {{ source('raw', 'meli_pickup') }}
