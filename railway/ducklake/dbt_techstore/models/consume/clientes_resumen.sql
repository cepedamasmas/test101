{{ config(materialized='table') }}

-- Resumen de actividad por cliente: LTV, frecuencia y recencia.
-- Útil para segmentación RFM y campañas de retención.

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
    COUNT(DISTINCT cc.canal_id)         AS canales_usados
FROM {{ ref('stg_cliente') }} c
LEFT JOIN {{ ref('stg_cliente_pedido') }} cp
    ON cp.cliente_id = c.cliente_id
LEFT JOIN {{ ref('stg_cliente_canal') }} cc
    ON cc.cliente_id = c.cliente_id
GROUP BY
    c.cliente_id,
    c.email,
    c.nombre,
    c.apellido,
    c.canal
ORDER BY ltv DESC NULLS LAST
