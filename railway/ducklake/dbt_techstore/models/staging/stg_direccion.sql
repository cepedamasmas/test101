{{ config(materialized='table') }}

-- Dimensión de direcciones de entrega únicas.
-- direccion_id: md5(calle|altura|ciudad|cp|pais) — misma lógica que en stg_pedido.
-- Fuentes:
--   type_6 receiver_address → MeLi (más completa: calle, altura, complemento, lat/lon)
--   vtex_pedido shippingData → VTEX (ciudad/provincia/cp, calle si la tiene)
--   garbarino_pedido billing_address → Garbarino (43 filas, datos de facturación)
-- QUALIFY: si la misma dirección aparece en varias fuentes, prioriza la que tiene lat/lon.

WITH meli AS (
    SELECT DISTINCT
        json_extract_string(receiver_address, '$.street_name')                   AS calle,
        json_extract_string(receiver_address, '$.street_number')                 AS altura,
        json_extract_string(receiver_address, '$.comment')                       AS complemento,
        json_extract_string(receiver_address, '$.city.name')                     AS ciudad,
        json_extract_string(receiver_address, '$.state.name')                    AS provincia,
        json_extract_string(receiver_address, '$.zip_code')                      AS cp,
        json_extract_string(receiver_address, '$.country.id')                    AS pais,
        TRY_CAST(json_extract_string(receiver_address, '$.latitude')  AS DOUBLE) AS latitud,
        TRY_CAST(json_extract_string(receiver_address, '$.longitude') AS DOUBLE) AS longitud,
        'mercadolibre'::VARCHAR                                                  AS fuente
    FROM {{ source('raw', 'type_6') }}
    WHERE receiver_address IS NOT NULL
),

vtex AS (
    SELECT DISTINCT
        json_extract_string(shippingData, '$.address.street')                    AS calle,
        json_extract_string(shippingData, '$.address.number')                    AS altura,
        json_extract_string(shippingData, '$.address.complement')                AS complemento,
        json_extract_string(shippingData, '$.address.city')                      AS ciudad,
        json_extract_string(shippingData, '$.address.state')                     AS provincia,
        json_extract_string(shippingData, '$.address.postalCode')                AS cp,
        json_extract_string(shippingData, '$.address.country')                   AS pais,
        NULL::DOUBLE                                                             AS latitud,
        NULL::DOUBLE                                                             AS longitud,
        'vtex'::VARCHAR                                                          AS fuente
    FROM {{ source('raw', 'vtex_pedido') }}
    WHERE shippingData IS NOT NULL
),

garbarino AS (
    SELECT DISTINCT
        json_extract_string(billing_address, '$.street')                         AS calle,
        json_extract_string(billing_address, '$.number')                         AS altura,
        json_extract_string(billing_address, '$.complement')                     AS complemento,
        json_extract_string(billing_address, '$.city')                           AS ciudad,
        json_extract_string(billing_address, '$.state')                          AS provincia,
        json_extract_string(billing_address, '$.zip')                            AS cp,
        json_extract_string(billing_address, '$.country')                        AS pais,
        NULL::DOUBLE                                                             AS latitud,
        NULL::DOUBLE                                                             AS longitud,
        'garbarino'::VARCHAR                                                     AS fuente
    FROM {{ source('raw', 'garbarino_pedido') }}
    WHERE billing_address IS NOT NULL
),

unificado AS (
    SELECT * FROM meli
    UNION ALL SELECT * FROM vtex
    UNION ALL SELECT * FROM garbarino
)

SELECT
    md5(
        coalesce(calle,  '') || '|' ||
        coalesce(altura, '') || '|' ||
        coalesce(ciudad, '') || '|' ||
        coalesce(cp,     '') || '|' ||
        coalesce(pais, 'AR')
    )               AS direccion_id,
    calle,
    altura,
    complemento,
    ciudad,
    provincia,
    cp,
    pais,
    latitud,
    longitud,
    fuente
FROM unificado
WHERE ciudad IS NOT NULL OR cp IS NOT NULL
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY calle, altura, ciudad, cp, pais
    ORDER BY latitud NULLS LAST
) = 1
