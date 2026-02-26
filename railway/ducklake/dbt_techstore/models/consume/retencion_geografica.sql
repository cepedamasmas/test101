{{ config(materialized='table') }}

-- CU-009: Retención geográfica y análisis logístico.
-- KPI-012: tasa_retencion_pct = clientes con 2+ pedidos / total clientes con pedidos × 100.
-- KPI-013: distancia_cd_km    = haversine(centroide ciudad → CD principal) en km.
-- KPI-014: correlacion_distancia_penetracion = CORR(distancia_cd_km, tasa_penetracion).
--           Valor nacional único (−1 a 1). Negativo alto → cuello de botella logístico.
-- RN-001/005/010 aplicados: clientes activos, ubicación principal por cliente.
-- RN-002: solo pedidos con ítems en el conteo de retención.
--
-- *** PARÁMETRO: Coordenadas del Centro de Distribución principal ***
-- Modificar lat_cd / lon_cd en la CTE `cd_principal` para actualizar el cálculo.
-- Default: CABA (-34.6037, -58.3816) — reemplazar con la coord real del CD.

WITH cd_principal AS (
    SELECT
        -34.6037 AS lat_cd,   -- ← reemplazar con latitud real del CD
        -58.3816 AS lon_cd    -- ← reemplazar con longitud real del CD
),

ubicacion_principal AS (
    -- RN-005/010: una ubicación principal por cliente
    SELECT cliente_id, ubicacion_id
    FROM {{ ref('stg_cliente_ubicacion_geo') }}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY cliente_id
        ORDER BY fecha_ultimo_pedido DESC NULLS LAST
    ) = 1
),

pedidos_validos AS (
    SELECT cp.cliente_id, cp.pedido_id
    FROM {{ ref('stg_cliente_pedido') }} cp
    -- RN-002: solo pedidos con ítems
    WHERE EXISTS (
        SELECT 1 FROM {{ ref('stg_item_pedido') }} ip
        WHERE ip.pedido_id = cp.pedido_id
    )
),

-- Pedidos por (cliente, ciudad): base del cálculo de retención
pedidos_cliente_ciudad AS (
    SELECT
        pv.cliente_id,
        ug.ciudad,
        ug.provincia,
        ug.pais_codigo,
        ug.latitud,
        ug.longitud,
        COUNT(pv.pedido_id) AS pedidos_del_cliente
    FROM pedidos_validos pv
    JOIN ubicacion_principal up ON up.cliente_id = pv.cliente_id
    JOIN {{ ref('stg_ubicacion_geo') }} ug ON ug.ubicacion_id = up.ubicacion_id
    WHERE ug.ciudad IS NOT NULL
    GROUP BY
        pv.cliente_id,
        ug.ciudad, ug.provincia, ug.pais_codigo, ug.latitud, ug.longitud
),

-- KPI-012: retención por ciudad
retencion_ciudad AS (
    SELECT
        ciudad,
        provincia,
        pais_codigo,
        AVG(latitud)                                                            AS latitud,
        AVG(longitud)                                                           AS longitud,
        COUNT(DISTINCT cliente_id)                                              AS total_clientes,
        COUNT(DISTINCT CASE WHEN pedidos_del_cliente >= 2 THEN cliente_id END)  AS clientes_recurrentes,
        ROUND(
            COUNT(DISTINCT CASE WHEN pedidos_del_cliente >= 2 THEN cliente_id END) * 100.0
            / NULLIF(COUNT(DISTINCT cliente_id), 0),
            2
        )                                                                       AS tasa_retencion_pct
    FROM pedidos_cliente_ciudad
    GROUP BY ciudad, provincia, pais_codigo
),

-- Tasa de penetración desde penetracion_mercado (para KPI-014)
penetracion AS (
    SELECT ciudad, tasa_penetracion, total_pedidos, revenue_total, population
    FROM {{ ref('penetracion_mercado') }}
    WHERE tasa_penetracion IS NOT NULL
),

-- KPI-013: distancia haversine ciudad → CD
base_con_distancia AS (
    SELECT
        rc.ciudad,
        rc.provincia,
        rc.pais_codigo,
        rc.latitud,
        rc.longitud,
        rc.total_clientes,
        rc.clientes_recurrentes,
        rc.tasa_retencion_pct,
        pm.tasa_penetracion,
        pm.total_pedidos,
        pm.revenue_total,
        pm.population,
        -- KPI-013: fórmula haversine (R = 6371 km)
        ROUND(
            2 * 6371 * ASIN(SQRT(
                POWER(SIN(RADIANS(rc.latitud  - cd.lat_cd) / 2), 2) +
                COS(RADIANS(cd.lat_cd)) * COS(RADIANS(rc.latitud)) *
                POWER(SIN(RADIANS(rc.longitud - cd.lon_cd) / 2), 2)
            )),
            1
        )                                                                   AS distancia_cd_km
    FROM retencion_ciudad rc
    LEFT JOIN penetracion pm ON LOWER(pm.ciudad) = LOWER(rc.ciudad)
    CROSS JOIN cd_principal cd
    WHERE rc.latitud IS NOT NULL
      AND rc.longitud IS NOT NULL
)

SELECT
    b.ciudad,
    b.provincia,
    b.pais_codigo,
    b.latitud,
    b.longitud,
    b.population,
    b.total_clientes,
    b.clientes_recurrentes,
    b.tasa_retencion_pct,
    b.total_pedidos,
    b.revenue_total,
    b.tasa_penetracion,
    b.distancia_cd_km,
    -- KPI-014: correlación Pearson nacional distancia ↔ penetración (valor único por fila)
    ROUND(CORR(b.distancia_cd_km, b.tasa_penetracion) OVER (), 4)          AS correlacion_distancia_penetracion
FROM base_con_distancia b
ORDER BY b.tasa_retencion_pct DESC NULLS LAST
