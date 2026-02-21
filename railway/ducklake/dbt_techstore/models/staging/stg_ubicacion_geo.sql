{{ config(materialized='table') }}

-- Ubicaciones geográficas únicas extraídas de direcciones de entrega.
-- Fuentes: type_6 (receiver_address MeLi) + vtex_pedido (shippingData).
-- ubicacion_id: surrogate key md5(ciudad|codigo_postal|pais_codigo)

WITH meli_geo AS (
    SELECT DISTINCT
        json_extract_string(receiver_address, '$.city.name')        AS ciudad,
        json_extract_string(receiver_address, '$.state.name')       AS provincia,
        json_extract_string(receiver_address, '$.country.id')       AS pais_codigo,
        json_extract_string(receiver_address, '$.zip_code')         AS codigo_postal,
        TRY_CAST(json_extract_string(receiver_address, '$.latitude')  AS DOUBLE) AS latitud,
        TRY_CAST(json_extract_string(receiver_address, '$.longitude') AS DOUBLE) AS longitud
    FROM {{ source('raw', 'type_6') }}
    WHERE receiver_address IS NOT NULL
),

vtex_geo AS (
    SELECT DISTINCT
        json_extract_string(shippingData, '$.address.city')         AS ciudad,
        json_extract_string(shippingData, '$.address.state')        AS provincia,
        json_extract_string(shippingData, '$.address.country')      AS pais_codigo,
        json_extract_string(shippingData, '$.address.postalCode')   AS codigo_postal,
        NULL::DOUBLE                                                 AS latitud,
        NULL::DOUBLE                                                 AS longitud
    FROM {{ source('raw', 'vtex_pedido') }}
    WHERE shippingData IS NOT NULL
),

unificado AS (
    SELECT * FROM meli_geo
    UNION ALL
    SELECT * FROM vtex_geo
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
