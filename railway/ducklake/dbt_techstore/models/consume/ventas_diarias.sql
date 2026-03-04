{{ config(materialized='table') }}

-- Serie temporal diaria de ventas por canal y provincia.
-- Útil para dashboards de tendencia y detección de anomalías.

WITH pedidos_con_items AS (
    -- RN-002: set de pedidos que tienen al menos 1 ítem (hash join, O(n))
    SELECT DISTINCT pedido_id FROM {{ ref('stg_item_pedido') }}
),

ubicacion_principal AS (
    SELECT cliente_id, ubicacion_id
    FROM {{ ref('stg_cliente_ubicacion_geo') }}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY cliente_id
        ORDER BY fecha_ultimo_pedido DESC NULLS LAST
    ) = 1
),

base AS (
    SELECT
        DATE_TRUNC('day', p.fecha_creacion) AS fecha,
        p.canal,
        UPPER(replace(replace(replace(replace(replace(
               replace(replace(replace(replace(replace(
               replace(replace(replace(replace(replace(
               replace(replace(replace(replace(replace(
                   ug.provincia,
                   '\U00C1','a'),'\U00C9','e'),'\U00CD','i'),'\U00D3','o'),'\U00DA','u'),
                   '\U00E1','a'),'\U00E9','e'),'\U00ED','i'),'\U00F3','o'),'\U00FA','u'),
                   '\u00c1','a'),'\u00c9','e'),'\u00cd','i'),'\u00d3','o'),'\u00da','u'),
                   '\u00e1','a'),'\u00e9','e'),'\u00ed','i'),'\u00f3','o'),'\u00fa','u'))
                                                         AS provincia,
        p.pedido_id,
        p.monto_total,
        cp.cliente_id
    FROM {{ ref('stg_pedido') }} p
    INNER JOIN pedidos_con_items pci ON pci.pedido_id = p.pedido_id
    LEFT JOIN {{ ref('stg_cliente_pedido') }} cp
        ON cp.pedido_id = p.pedido_id
    LEFT JOIN ubicacion_principal up
        ON up.cliente_id = cp.cliente_id
    LEFT JOIN {{ ref('stg_ubicacion_geo') }} ug
        ON ug.ubicacion_id = up.ubicacion_id
    WHERE p.fecha_creacion IS NOT NULL
      AND p.monto_total > 0
)

SELECT
    fecha,
    canal,
    provincia,
    COUNT(DISTINCT pedido_id)   AS pedidos,
    COUNT(DISTINCT cliente_id)  AS clientes_unicos,
    SUM(monto_total)            AS revenue,
    AVG(monto_total)            AS ticket_promedio
FROM base
GROUP BY fecha, canal, provincia
ORDER BY fecha DESC, canal, provincia
