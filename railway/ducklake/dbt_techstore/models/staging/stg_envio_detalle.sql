{{ config(materialized='table') }}

-- Detalle logístico de envíos MercadoLibre (type_6 → meli_envio_detalle).
-- Una fila por envío: tracking, status, tipo de logística y
-- direcciones completas de origen (vendedor) y destino (comprador) con lat/lon.
-- Dedup por id, versión más reciente según last_updated.

SELECT
    id::VARCHAR                                                                 AS envio_id,
    order_id::VARCHAR                                                           AS orden_id_origen,
    status,
    substatus,
    logistic_type,
    tracking_number,
    tracking_method,
    TRY_CAST(date_created  AS TIMESTAMP)                                        AS fecha_creacion,
    TRY_CAST(last_updated  AS TIMESTAMP)                                        AS fecha_actualizacion,

    -- Origen (vendedor)
    json_extract_string(sender_address, '$.city.name')                          AS origen_ciudad,
    json_extract_string(sender_address, '$.state.name')                         AS origen_provincia,
    json_extract_string(sender_address, '$.country.id')                         AS origen_pais,
    json_extract_string(sender_address, '$.zip_code')                           AS origen_codigo_postal,
    TRY_CAST(json_extract_string(sender_address, '$.latitude')  AS DOUBLE)      AS origen_latitud,
    TRY_CAST(json_extract_string(sender_address, '$.longitude') AS DOUBLE)      AS origen_longitud,
    json_extract_string(sender_address, '$.street_name')                        AS origen_calle,
    json_extract_string(sender_address, '$.street_number')                      AS origen_numero,

    -- Destino (comprador)
    json_extract_string(receiver_address, '$.city.name')                        AS destino_ciudad,
    json_extract_string(receiver_address, '$.state.name')                       AS destino_provincia,
    json_extract_string(receiver_address, '$.country.id')                       AS destino_pais,
    json_extract_string(receiver_address, '$.zip_code')                         AS destino_codigo_postal,
    TRY_CAST(json_extract_string(receiver_address, '$.latitude')  AS DOUBLE)    AS destino_latitud,
    TRY_CAST(json_extract_string(receiver_address, '$.longitude') AS DOUBLE)    AS destino_longitud,
    json_extract_string(receiver_address, '$.street_name')                      AS destino_calle,
    json_extract_string(receiver_address, '$.street_number')                    AS destino_numero,
    json_extract_string(receiver_address, '$.comment')                          AS destino_complemento

FROM {{ source('raw', 'type_6') }}
QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY last_updated DESC NULLS LAST) = 1
