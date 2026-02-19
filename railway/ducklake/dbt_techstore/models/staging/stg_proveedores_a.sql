-- Productos de proveedores cuyo nombre empieza con 'A'
select
    codigo,
    nombre,
    marca as proveedor,
    categoria,
    precio_lista,
    precio_costo,
    (precio_lista - precio_costo) as margen,
    round((precio_lista - precio_costo) / precio_lista * 100, 2) as margen_pct,
    stock_proveedor,
    disponible
from {{ ref('stg_catalogo_proveedor') }}
where nombre like 'A%'
order by nombre
