{{ config(materialized='table') }}

-- Bridge con métricas: productos vendidos por canal.
-- Agrega total de pedidos, unidades y revenue por (canal, producto).
-- Depende de stg_producto, stg_canal y stg_item_pedido.

SELECT
    c.canal_id,
    p.canal,
    p.producto_id,
    p.producto_id_origen,
    p.nombre,
    p.categoria_id,
    COUNT(DISTINCT i.pedido_id)     AS total_pedidos,
    SUM(i.cantidad)                 AS total_unidades,
    SUM(i.subtotal)                 AS revenue_total,
    MIN(i.precio_unitario)          AS precio_min,
    MAX(i.precio_unitario)          AS precio_max,
    AVG(i.precio_unitario)          AS precio_promedio
FROM {{ ref('stg_producto') }} p
JOIN {{ ref('stg_canal') }} c
    ON c.canal = p.canal
LEFT JOIN {{ ref('stg_item_pedido') }} i
    ON i.producto_id_origen = p.producto_id_origen
    AND i.canal = p.canal
GROUP BY
    c.canal_id,
    p.canal,
    p.producto_id,
    p.producto_id_origen,
    p.nombre,
    p.categoria_id
