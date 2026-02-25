{{ config(materialized='table') }}

-- KPIs de ventas agregados por canal.
-- Métricas: pedidos, revenue, ticket promedio, clientes únicos, unidades vendidas (KPI-004).

WITH pedidos_validos AS (
    SELECT
        p.canal,
        p.pedido_id,
        p.monto_total,
        p.fecha_creacion,
        cp.cliente_id
    FROM {{ ref('stg_pedido') }} p
    LEFT JOIN {{ ref('stg_cliente_pedido') }} cp ON cp.pedido_id = p.pedido_id
    WHERE p.monto_total > 0
      -- RN-002: excluir pedidos sin ítems asociados
      AND EXISTS (
          SELECT 1 FROM {{ ref('stg_item_pedido') }} ip
          WHERE ip.pedido_id = p.pedido_id
      )
),

por_canal AS (
    SELECT
        canal,
        COUNT(DISTINCT pedido_id)   AS total_pedidos,
        COUNT(DISTINCT cliente_id)  AS clientes_unicos,
        SUM(monto_total)            AS revenue_total,
        AVG(monto_total)            AS ticket_promedio,
        MIN(fecha_creacion)         AS primer_pedido,
        MAX(fecha_creacion)         AS ultimo_pedido
    FROM pedidos_validos
    GROUP BY canal
),

-- KPI-004: unidades vendidas por canal (suma de cantidades de ítems)
unidades AS (
    SELECT pv.canal, SUM(ip.cantidad) AS total_unidades
    FROM pedidos_validos pv
    JOIN {{ ref('stg_item_pedido') }} ip ON ip.pedido_id = pv.pedido_id
    GROUP BY pv.canal
)

SELECT
    pc.canal,
    pc.total_pedidos,
    pc.clientes_unicos,
    pc.revenue_total,
    pc.ticket_promedio,
    u.total_unidades,
    pc.primer_pedido,
    pc.ultimo_pedido
FROM por_canal pc
LEFT JOIN unidades u ON u.canal = pc.canal
ORDER BY pc.revenue_total DESC
