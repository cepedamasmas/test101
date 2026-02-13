select
    codigo, nombre, categoria,
    cast(precio_lista as decimal(12,2)) as precio_lista,
    cast(precio_costo as decimal(12,2)) as precio_costo,
    stock_proveedor, disponible, marca
from {{ source('raw', 'catalogo_proveedor') }}
