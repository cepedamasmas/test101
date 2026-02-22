{{ config(materialized='table') }}

-- Serie temporal diaria de ventas por canal.
-- Útil para dashboards de tendencia y detección de anomalías.

WITH base AS (
    SELECT
        DATE_TRUNC('day', p.fecha_creacion) AS fecha,
        p.canal,
        p.pedido_id,
        p.monto_total,
        cp.cliente_id
    FROM {{ ref('stg_pedido') }} p
    LEFT JOIN {{ ref('stg_cliente_pedido') }} cp
        ON cp.pedido_id = p.pedido_id
    WHERE p.fecha_creacion IS NOT NULL
      AND p.monto_total > 0
      -- RN-002: excluir pedidos sin ítems asociados
      AND EXISTS (
          SELECT 1 FROM {{ ref('stg_item_pedido') }} ip
          WHERE ip.pedido_id = p.pedido_id
      )
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
