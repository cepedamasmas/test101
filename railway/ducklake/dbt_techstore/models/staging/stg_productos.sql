select
    producto_id, nombre, categoria,
    precio_unitario, stock, activo,
    costo, codigo_proveedor,
    round((precio_unitario - costo) * 100.0 / precio_unitario, 1) as margen_pct
from {{ source('raw', 'productos') }}
where categoria is not null
