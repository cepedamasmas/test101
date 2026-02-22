{{ config(materialized='table') }}

-- KPIs de ventas agregados por canal.
-- Métricas: pedidos, revenue, ticket promedio, clientes únicos.

SELECT
    p.canal,
    COUNT(DISTINCT p.pedido_id)         AS total_pedidos,
    COUNT(DISTINCT cp.cliente_id)       AS clientes_unicos,
    SUM(p.monto_total)                  AS revenue_total,
    AVG(p.monto_total)                  AS ticket_promedio,
    MIN(p.fecha_creacion)               AS primer_pedido,
    MAX(p.fecha_creacion)               AS ultimo_pedido
FROM {{ ref('stg_pedido') }} p
LEFT JOIN {{ ref('stg_cliente_pedido') }} cp
    ON cp.pedido_id = p.pedido_id
WHERE p.monto_total > 0
GROUP BY p.canal
ORDER BY revenue_total DESC
