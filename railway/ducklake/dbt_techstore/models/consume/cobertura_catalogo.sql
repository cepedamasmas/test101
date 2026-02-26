{{ config(materialized='table') }}

-- CU-004: Cobertura de catálogo por canal (KPI-008).
-- Una fila por (canal, producto) con métricas de venta.
-- Permite analizar:
--   · Qué % del catálogo total está presente en cada canal (cobertura_canal_pct).
--   · Cuántos canales venden cada producto (canales_con_presencia).
--   · Productos exclusivos de un único canal (exclusivo_canal).
-- Nota RN-008: stg_canal_producto se deriva de ventas reales, por lo que
--   en_catalogo = vendido por construcción. No hay anomalías posibles en este modelo.

WITH catalogo AS (
    SELECT *
    FROM {{ ref('stg_canal_producto') }}
    WHERE total_unidades > 0
),

-- KPI-008: productos únicos por canal
cobertura_por_canal AS (
    SELECT
        canal_id,
        canal,
        COUNT(DISTINCT producto_id) AS productos_en_canal
    FROM catalogo
    GROUP BY canal_id, canal
),

-- Total de productos únicos en todo el catálogo (todos los canales)
total_catalogo AS (
    SELECT COUNT(DISTINCT producto_id) AS total FROM catalogo
)

SELECT
    c.canal,
    c.nombre                                                            AS nombre_producto,
    c.categoria_id,
    c.producto_id_origen,
    c.total_pedidos,
    c.total_unidades,
    c.revenue_total,
    c.precio_min,
    c.precio_max,
    c.precio_promedio,
    -- En cuántos canales se vende este producto
    COUNT(*) OVER (PARTITION BY c.producto_id)                         AS canales_con_presencia,
    -- TRUE si el producto solo existe en este canal
    COUNT(*) OVER (PARTITION BY c.producto_id) = 1                    AS exclusivo_canal,
    -- KPI-008: % del catálogo total cubierto por este canal
    ROUND(
        cob.productos_en_canal * 100.0 / NULLIF(tc.total, 0),
        2
    )                                                                  AS cobertura_canal_pct
FROM catalogo c
JOIN cobertura_por_canal cob ON cob.canal_id = c.canal_id
CROSS JOIN total_catalogo tc
ORDER BY c.canal, c.revenue_total DESC NULLS LAST
