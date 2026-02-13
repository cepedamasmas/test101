select
    p.producto_id, p.nombre, p.categoria,
    p.stock as stock_interno,
    coalesce(c.stock_proveedor, 0) as stock_proveedor,
    p.precio_unitario,
    coalesce(c.precio_costo, p.costo) as precio_costo_actual,
    case
        when p.stock = 0 then 'SIN_STOCK'
        when p.stock < 10 then 'CRITICO'
        when p.stock < 30 then 'BAJO'
        else 'OK'
    end as alerta_stock
from {{ ref('stg_productos') }} p
left join {{ ref('stg_catalogo_proveedor') }} c on p.codigo_proveedor = c.codigo
where p.activo = true and p.stock < 30
order by p.stock asc
