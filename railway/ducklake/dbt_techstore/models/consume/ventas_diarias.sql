{{ config(materialized='table') }}

-- Serie temporal diaria de ventas por canal.
-- Útil para dashboards de tendencia y detección de anomalías.

WITH pedidos_con_items AS (
    -- RN-002: set de pedidos que tienen al menos 1 ítem (hash join, O(n))
    SELECT DISTINCT pedido_id FROM {{ ref('stg_item_pedido') }}
),

base AS (
    SELECT
        DATE_TRUNC('day', p.fecha_creacion) AS fecha,
        p.canal,
        p.pedido_id,
        p.monto_total,
        cp.cliente_id
    FROM {{ ref('stg_pedido') }} p
    INNER JOIN pedidos_con_items pci ON pci.pedido_id = p.pedido_id
    LEFT JOIN {{ ref('stg_cliente_pedido') }} cp
        ON cp.pedido_id = p.pedido_id
    WHERE p.fecha_creacion IS NOT NULL
      AND p.monto_total > 0
)

SELECT
    fecha,
    canal,
    COUNT(DISTINCT pedido_id)   AS pedidos,
    COUNT(DISTINCT cliente_id)  AS clientes_unicos,
    SUM(monto_total)            AS revenue,
    AVG(monto_total)            AS ticket_promedio
FROM base
GROUP BY fecha, canal
ORDER BY fecha DESC, canal
