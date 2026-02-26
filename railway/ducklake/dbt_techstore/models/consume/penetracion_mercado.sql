{{ config(materialized='table') }}

-- CU-006: Penetración de mercado por localidad.
-- KPI-009: tasa_penetracion    = total_pedidos / population  (pedidos / habitante)
-- KPI-010: revenue_per_capita  = revenue_total / population  ($ / habitante)
-- KPI-015: revenue_potencial_incremental — revenue adicional si la ciudad alcanzara
--           la tasa de penetración promedio nacional. Solo ciudades bajo la media.
-- Cuadrante estratégico: cruza penetración vs. tamaño de mercado.
--   · Vaca lechera:    pop >= mediana  AND penetración >= p75
--   · Gigante dormido: pop >= mediana  AND penetración <  p25  ← máxima oportunidad
--   · Nicho fiel:      pop <  mediana  AND penetración >= p75
--   · Sin foco:        resto
-- Requiere population en ventas_por_ubicacion (desde stg_direccion_geocodificada).
-- Ciudades sin population quedan excluidas.

WITH base AS (
    SELECT *
    FROM {{ ref('ventas_por_ubicacion') }}
    WHERE population IS NOT NULL
      AND population > 0
      AND ciudad IS NOT NULL
),

-- Métricas nacionales como baseline para KPI-015
nacional AS (
    SELECT
        SUM(total_pedidos) * 1.0 / NULLIF(SUM(population), 0) AS tasa_penetracion_nacional,
        SUM(revenue_total) / NULLIF(SUM(total_pedidos), 0)     AS ticket_promedio_nacional
    FROM base
),

-- Cortes de penetración y población para el cuadrante estratégico
percentiles AS (
    SELECT
        PERCENTILE_CONT(0.25) WITHIN GROUP (
            ORDER BY total_pedidos * 1.0 / population
        )                           AS p25_penetracion,
        PERCENTILE_CONT(0.75) WITHIN GROUP (
            ORDER BY total_pedidos * 1.0 / population
        )                           AS p75_penetracion,
        MEDIAN(population)          AS mediana_poblacion
    FROM base
)

SELECT
    b.ciudad,
    b.provincia,
    b.pais_codigo,
    b.latitud,
    b.longitud,
    b.population,
    b.total_pedidos,
    b.clientes_unicos,
    b.revenue_total,
    b.ticket_promedio,

    -- KPI-009: tasa de penetración por ciudad
    ROUND(b.total_pedidos * 1.0 / b.population, 6)             AS tasa_penetracion,

    -- KPI-010: revenue per cápita
    ROUND(b.revenue_total / b.population, 2)                   AS revenue_per_capita,

    -- KPI-015: revenue potencial incremental (solo ciudades bajo la media nacional)
    CASE
        WHEN (b.total_pedidos * 1.0 / b.population) < n.tasa_penetracion_nacional
        THEN ROUND(
            (n.tasa_penetracion_nacional - b.total_pedidos * 1.0 / b.population)
            * b.population
            * n.ticket_promedio_nacional,
            2
        )
        ELSE 0
    END                                                         AS revenue_potencial_incremental,

    -- Baseline nacional (para referencia en dashboards)
    ROUND(n.tasa_penetracion_nacional, 6)                      AS tasa_penetracion_nacional,
    ROUND(n.ticket_promedio_nacional, 2)                       AS ticket_promedio_nacional,

    -- Cuadrante estratégico
    CASE
        WHEN b.population >= p.mediana_poblacion
             AND (b.total_pedidos * 1.0 / b.population) >= p.p75_penetracion
             THEN 'Vaca lechera'
        WHEN b.population >= p.mediana_poblacion
             AND (b.total_pedidos * 1.0 / b.population) <  p.p25_penetracion
             THEN 'Gigante dormido'
        WHEN b.population <  p.mediana_poblacion
             AND (b.total_pedidos * 1.0 / b.population) >= p.p75_penetracion
             THEN 'Nicho fiel'
        ELSE 'Sin foco'
    END                                                         AS cuadrante_estrategico

FROM base b
CROSS JOIN nacional n
CROSS JOIN percentiles p
ORDER BY revenue_potencial_incremental DESC
