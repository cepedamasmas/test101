{{ config(materialized='table') }}

-- CU-007: Evolución y alertas de penetración geográfica.
-- Una fila por (ciudad, mes).
-- KPI-009 mensual: tasa_penetracion = total_pedidos / population.
-- variacion_pct:        variación porcentual vs. mes anterior (LAG).
-- alerta_caida:         TRUE si variacion_pct <= -20% (umbral ajustable en este modelo).
-- indice_estacionalidad: pedidos_mes / promedio_mensual_anual de esa ciudad.
-- Requiere al menos 2 meses históricos para calcular variaciones.
-- RN-005/010: cada pedido se atribuye a la ubicación principal del cliente.

-- UMBRAL_ALERTA_CAIDA: caída porcentual que dispara alerta (default -20%)
-- Para ajustar, modificar el literal -0.20 en el campo alerta_caida.

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
    SELECT cp.cliente_id, cp.pedido_id, cp.fecha_pedido, cp.monto_total
    FROM {{ ref('stg_cliente_pedido') }} cp
    -- RN-002: solo pedidos con ítems
    WHERE EXISTS (
        SELECT 1 FROM {{ ref('stg_item_pedido') }} ip
        WHERE ip.pedido_id = cp.pedido_id
    )
),

pedidos_ciudad_mes AS (
    SELECT
        ug.ciudad,
        ug.provincia,
        ug.pais_codigo,
        DATE_TRUNC('month', pv.fecha_pedido)::DATE      AS mes,
        YEAR(pv.fecha_pedido)                           AS anio,
        MONTH(pv.fecha_pedido)                          AS mes_num,
        COUNT(DISTINCT pv.pedido_id)                    AS total_pedidos,
        COUNT(DISTINCT pv.cliente_id)                   AS clientes_unicos,
        SUM(pv.monto_total)                             AS revenue_total
    FROM pedidos_validos pv
    JOIN ubicacion_principal up ON up.cliente_id = pv.cliente_id
    JOIN {{ ref('stg_ubicacion_geo') }} ug ON ug.ubicacion_id = up.ubicacion_id
    WHERE pv.fecha_pedido IS NOT NULL
      AND ug.ciudad IS NOT NULL
    GROUP BY
        ug.ciudad, ug.provincia, ug.pais_codigo,
        DATE_TRUNC('month', pv.fecha_pedido)::DATE,
        YEAR(pv.fecha_pedido), MONTH(pv.fecha_pedido)
),

-- Population desde ventas_por_ubicacion (join ciudad → population ya resuelto)
population_ciudad AS (
    SELECT ciudad, population
    FROM {{ ref('ventas_por_ubicacion') }}
    WHERE population IS NOT NULL AND population > 0
),

serie_con_tasa AS (
    SELECT
        pcm.*,
        pc.population,
        ROUND(pcm.total_pedidos * 1.0 / NULLIF(pc.population, 0), 6)   AS tasa_penetracion,
        -- Promedio mensual de esta ciudad en el mismo año (base del índice estacional)
        AVG(pcm.total_pedidos) OVER (
            PARTITION BY pcm.ciudad, pcm.anio
        )                                                               AS promedio_mensual_anual
    FROM pedidos_ciudad_mes pcm
    LEFT JOIN population_ciudad pc ON LOWER(pc.ciudad) = LOWER(pcm.ciudad)
),

con_lag AS (
    SELECT
        *,
        LAG(tasa_penetracion) OVER (
            PARTITION BY ciudad ORDER BY mes
        ) AS tasa_penetracion_mes_anterior
    FROM serie_con_tasa
)

SELECT
    cl.ciudad,
    cl.provincia,
    cl.pais_codigo,
    cl.mes,
    cl.anio,
    cl.mes_num,
    cl.population,
    cl.total_pedidos,
    cl.clientes_unicos,
    cl.revenue_total,
    cl.tasa_penetracion,
    cl.tasa_penetracion_mes_anterior,

    -- Variación absoluta vs. mes anterior
    ROUND(
        cl.tasa_penetracion - cl.tasa_penetracion_mes_anterior, 6
    )                                                               AS variacion_abs,

    -- Variación porcentual vs. mes anterior
    CASE
        WHEN cl.tasa_penetracion_mes_anterior > 0
        THEN ROUND(
            (cl.tasa_penetracion - cl.tasa_penetracion_mes_anterior)
            / cl.tasa_penetracion_mes_anterior * 100,
            2
        )
    END                                                             AS variacion_pct,

    -- Alerta: caída >= 20% vs. mes anterior (ajustar -0.20 para cambiar umbral)
    CASE
        WHEN cl.tasa_penetracion_mes_anterior > 0
         AND (cl.tasa_penetracion - cl.tasa_penetracion_mes_anterior)
             / cl.tasa_penetracion_mes_anterior <= -0.20
        THEN TRUE
        ELSE FALSE
    END                                                             AS alerta_caida,

    -- Índice de estacionalidad: pedidos_mes / promedio_mensual_anual de la ciudad
    ROUND(
        cl.total_pedidos * 1.0 / NULLIF(cl.promedio_mensual_anual, 0), 2
    )                                                               AS indice_estacionalidad

FROM con_lag cl
ORDER BY cl.ciudad, cl.mes
