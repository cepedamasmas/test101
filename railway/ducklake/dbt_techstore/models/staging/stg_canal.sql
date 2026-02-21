{{ config(materialized='table') }}

SELECT 1 AS canal_id, 'vtex'          AS canal, 'vtex_pedido'       AS tabla_fuente
UNION ALL
SELECT 2,             'mercadolibre',            'meli_pedido'
UNION ALL
SELECT 3,             'garbarino',               'garbarino_pedido'
