-- TEST: Productos donde el precio de venta es menor al costo (margen negativo)
--
-- Un margen negativo puede ser un error de carga en el maestro de productos,
-- o una liquidación puntual (en ese caso se puede ignorar con severity: warn).
--
-- Si falla: revisar en MySQL si el precio o el costo están mal ingresados.

select
    producto_id,
    nombre,
    categoria,
    precio_unitario,
    costo,
    margen_pct
from {{ ref('stg_productos') }}
where precio_unitario < costo
