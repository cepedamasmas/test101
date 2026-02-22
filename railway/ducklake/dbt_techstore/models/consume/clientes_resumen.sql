{{ config(materialized='table') }}

-- Resumen de actividad por cliente: LTV, frecuencia, recencia y canal preferido.
-- Útil para segmentación RFM y campañas de retención.

WITH canal_preferido AS (
    -- RN-004: canal con más pedidos; empate se rompe por mayor monto acumulado
    SELECT
        cliente_id,
        canal
    FROM {{ ref('stg_cliente_canal') }}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY cliente_id
        ORDER BY total_pedidos DESC, monto_total_acumulado DESC
    ) = 1
)

SELECT
    c.cliente_id,
    c.email,
    c.nombre,
    c.apellido,
    c.canal                             AS canal_origen,
    COUNT(DISTINCT cp.pedido_id)        AS total_pedidos,
    SUM(cp.monto_total)                 AS ltv,
    AVG(cp.monto_total)                 AS ticket_promedio,
    MIN(cp.fecha_pedido)                AS primer_pedido,
    MAX(cp.fecha_pedido)                AS ultimo_pedido,
    COUNT(DISTINCT cc.canal_id)         AS canales_usados,
    -- RN-001: cliente activo si tiene al menos 1 pedido en los últimos 12 meses
    CASE
        WHEN MAX(cp.fecha_pedido) >= CURRENT_DATE - INTERVAL '12 months'
        THEN 'Activo'
        ELSE 'Inactivo'
    END                                 AS estado_cliente,
    -- RN-004: canal preferido del cliente
    COALESCE(cpref.canal, 'Sin canal asignado') AS canal_preferido
FROM {{ ref('stg_cliente') }} c
LEFT JOIN {{ ref('stg_cliente_pedido') }} cp
    ON cp.cliente_id = c.cliente_id
    -- RN-002: solo contabilizar pedidos con ítems asociados
    AND EXISTS (
        SELECT 1 FROM {{ ref('stg_item_pedido') }} ip
        WHERE ip.pedido_id = cp.pedido_id
    )
LEFT JOIN {{ ref('stg_cliente_canal') }} cc
    ON cc.cliente_id = c.cliente_id
LEFT JOIN canal_preferido cpref
    ON cpref.cliente_id = c.cliente_id
GROUP BY
    c.cliente_id,
    c.email,
    c.nombre,
    c.apellido,
    c.canal,
    cpref.canal
ORDER BY ltv DESC NULLS LAST
