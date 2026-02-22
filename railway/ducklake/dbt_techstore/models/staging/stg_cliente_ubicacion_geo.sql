{{ config(materialized='table') }}

-- Bridge: clientes y las ubicaciones geográficas a las que compraron.
-- MeLi: type_6 (receiver_address) + meli_pedido (buyer) vinculados por order_id.
-- VTEX: shippingData + clientProfileData del mismo registro.
-- ubicacion_id consistente con stg_ubicacion_geo.
-- RN-005/RN-010: fecha_ultimo_pedido permite resolver la ubicación principal en consume
--   usando: ROW_NUMBER() OVER (PARTITION BY cliente_id ORDER BY fecha_ultimo_pedido DESC) = 1

WITH meli_link AS (
    SELECT
        md5('mercadolibre|' || coalesce(json_extract_string(p.buyer, '$.id'), ''))  AS cliente_id,
        md5(
            coalesce(json_extract_string(e.receiver_address, '$.city.name'), '')    || '|' ||
            coalesce(json_extract_string(e.receiver_address, '$.zip_code'), '')     || '|' ||
            coalesce(json_extract_string(e.receiver_address, '$.country.id'), 'AR')
        )                                                                            AS ubicacion_id,
        'destino'                                                                    AS tipo,
        MAX(TRY_CAST(p.date_created AS TIMESTAMP))                                  AS fecha_ultimo_pedido
    FROM {{ source('raw', 'type_6') }} e
    JOIN {{ source('raw', 'meli_pedido') }} p
        ON p.id::VARCHAR = e.order_id::VARCHAR
    WHERE e.receiver_address IS NOT NULL
      AND p.buyer IS NOT NULL
    GROUP BY 1, 2, 3
),

vtex_link AS (
    SELECT
        md5('vtex|' || coalesce(json_extract_string(clientProfileData, '$.userProfileId'), '')) AS cliente_id,
        md5(
            coalesce(json_extract_string(shippingData, '$.address.city'), '')       || '|' ||
            coalesce(json_extract_string(shippingData, '$.address.postalCode'), '') || '|' ||
            coalesce(json_extract_string(shippingData, '$.address.country'), 'AR')
        )                                                                                        AS ubicacion_id,
        'destino'                                                                                AS tipo,
        MAX(TRY_CAST(creationDate AS TIMESTAMP))                                                 AS fecha_ultimo_pedido
    FROM {{ source('raw', 'vtex_pedido') }}
    WHERE shippingData IS NOT NULL
      AND clientProfileData IS NOT NULL
    GROUP BY 1, 2, 3
),

unificado AS (
    SELECT cliente_id, ubicacion_id, tipo, fecha_ultimo_pedido FROM meli_link
    WHERE cliente_id IS NOT NULL AND ubicacion_id IS NOT NULL
    UNION ALL
    SELECT cliente_id, ubicacion_id, tipo, fecha_ultimo_pedido FROM vtex_link
    WHERE cliente_id IS NOT NULL AND ubicacion_id IS NOT NULL
)

-- Consolidar: si el mismo par (cliente, ubicacion) aparece en ambas fuentes,
-- conservar la fecha más reciente
SELECT
    cliente_id,
    ubicacion_id,
    tipo,
    MAX(fecha_ultimo_pedido) AS fecha_ultimo_pedido
FROM unificado
GROUP BY cliente_id, ubicacion_id, tipo
