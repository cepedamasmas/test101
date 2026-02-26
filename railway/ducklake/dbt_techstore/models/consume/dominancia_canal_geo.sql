{{ config(materialized='table') }}

-- CU-008: Dominancia de canal por geografía (KPI-011).
-- Una fila por (ciudad, canal).
-- share_pedidos_pct: % de pedidos de ese canal sobre el total de la ciudad.
-- share_revenue_pct: % de revenue de ese canal sobre el total de la ciudad.
-- canal_dominante:   TRUE para el canal con mayor share_pedidos en cada ciudad.
-- RN-005/010: cada pedido se atribuye a la ubicación principal del cliente.
-- RN-002/007: solo pedidos válidos (con ítems, con canal).

WITH ubicacion_principal AS (
    -- RN-005/010: una ubicación principal por cliente
    SELECT cliente_id, ubicacion_id
    FROM {{ ref('stg_cliente_ubicacion_geo') }}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY cliente_id
        ORDER BY fecha_ultimo_pedido DESC NULLS LAST
    ) = 1
),

pedidos_validos AS (
    SELECT
        cp.cliente_id,
        cp.pedido_id,
        cp.monto_total,
        p.canal
    FROM {{ ref('stg_cliente_pedido') }} cp
    JOIN {{ ref('stg_pedido') }} p ON p.pedido_id = cp.pedido_id
    WHERE p.monto_total > 0
      -- RN-002: solo pedidos con ítems
      AND EXISTS (
          SELECT 1 FROM {{ ref('stg_item_pedido') }} ip
          WHERE ip.pedido_id = cp.pedido_id
      )
),

pedidos_ciudad_canal AS (
    SELECT
        ug.ciudad,
        ug.provincia,
        ug.pais_codigo,
        pv.canal,
        COUNT(DISTINCT pv.pedido_id)    AS total_pedidos,
        COUNT(DISTINCT pv.cliente_id)   AS clientes_unicos,
        SUM(pv.monto_total)             AS revenue_total
    FROM pedidos_validos pv
    JOIN ubicacion_principal up ON up.cliente_id = pv.cliente_id
    JOIN {{ ref('stg_ubicacion_geo') }} ug ON ug.ubicacion_id = up.ubicacion_id
    WHERE ug.ciudad IS NOT NULL
    GROUP BY ug.ciudad, ug.provincia, ug.pais_codigo, pv.canal
),

totales_ciudad AS (
    SELECT
        ciudad,
        SUM(total_pedidos)  AS total_pedidos_ciudad,
        SUM(revenue_total)  AS total_revenue_ciudad
    FROM pedidos_ciudad_canal
    GROUP BY ciudad
)

SELECT
    pcc.ciudad,
    pcc.provincia,
    pcc.pais_codigo,
    pcc.canal,
    pcc.total_pedidos,
    pcc.clientes_unicos,
    pcc.revenue_total,
    tc.total_pedidos_ciudad,
    tc.total_revenue_ciudad,

    -- KPI-011: share de pedidos de este canal en la ciudad
    ROUND(pcc.total_pedidos * 100.0 / NULLIF(tc.total_pedidos_ciudad, 0), 2)    AS share_pedidos_pct,

    -- Share de revenue de este canal en la ciudad
    ROUND(pcc.revenue_total * 100.0 / NULLIF(tc.total_revenue_ciudad, 0), 2)    AS share_revenue_pct,

    -- Canal dominante en esta ciudad (mayor volumen de pedidos)
    RANK() OVER (PARTITION BY pcc.ciudad ORDER BY pcc.total_pedidos DESC) = 1   AS canal_dominante

FROM pedidos_ciudad_canal pcc
JOIN totales_ciudad tc ON tc.ciudad = pcc.ciudad
ORDER BY pcc.ciudad, pcc.total_pedidos DESC
