{{ config(materialized='table') }}

-- Bridge: clientes y las ubicaciones geográficas a las que compraron.
-- MeLi: type_6 (receiver_address) + meli_pedido (buyer) vinculados por order_id.
-- VTEX: shippingData + clientProfileData del mismo registro.
-- ubicacion_id consistente con stg_ubicacion_geo.

WITH meli_link AS (
    SELECT DISTINCT
        md5('mercadolibre|' || coalesce(json_extract_string(p.buyer, '$.id'), ''))  AS cliente_id,
        md5(
            coalesce(json_extract_string(e.receiver_address, '$.city.name'), '')    || '|' ||
            coalesce(json_extract_string(e.receiver_address, '$.zip_code'), '')     || '|' ||
            coalesce(json_extract_string(e.receiver_address, '$.country.id'), 'AR')
        )                                                                            AS ubicacion_id,
        'destino'                                                                    AS tipo
    FROM {{ source('raw', 'type_6') }} e
    JOIN {{ source('raw', 'meli_pedido') }} p
        ON p.id::VARCHAR = e.order_id::VARCHAR
    WHERE e.receiver_address IS NOT NULL
      AND p.buyer IS NOT NULL
),

vtex_link AS (
    SELECT DISTINCT
        md5('vtex|' || coalesce(json_extract_string(clientProfileData, '$.userProfileId'), '')) AS cliente_id,
        md5(
            coalesce(json_extract_string(shippingData, '$.address.city'), '')       || '|' ||
            coalesce(json_extract_string(shippingData, '$.address.postalCode'), '') || '|' ||
            coalesce(json_extract_string(shippingData, '$.address.country'), 'AR')
        )                                                                                        AS ubicacion_id,
        'destino'                                                                                AS tipo
    FROM {{ source('raw', 'vtex_pedido') }}
    WHERE shippingData IS NOT NULL
      AND clientProfileData IS NOT NULL
)

SELECT DISTINCT cliente_id, ubicacion_id, tipo FROM meli_link
WHERE cliente_id IS NOT NULL AND ubicacion_id IS NOT NULL
UNION
SELECT DISTINCT cliente_id, ubicacion_id, tipo FROM vtex_link
WHERE cliente_id IS NOT NULL AND ubicacion_id IS NOT NULL
