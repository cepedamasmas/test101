{{ config(materialized='table') }}

-- Ranking de productos por revenue y unidades vendidas, cruzado por canal.

SELECT
    cp.canal,
    cp.nombre,
    cp.producto_id_origen,
    cp.categoria_id,
    cp.total_pedidos,
    cp.total_unidades,
    cp.revenue_total,
    cp.precio_min,
    cp.precio_max,
    cp.precio_promedio,
    RANK() OVER (PARTITION BY cp.canal ORDER BY cp.revenue_total DESC)    AS rank_revenue,
    RANK() OVER (PARTITION BY cp.canal ORDER BY cp.total_unidades DESC)   AS rank_unidades
FROM {{ ref('stg_canal_producto') }} cp
WHERE cp.total_unidades > 0
ORDER BY cp.canal, rank_revenue
