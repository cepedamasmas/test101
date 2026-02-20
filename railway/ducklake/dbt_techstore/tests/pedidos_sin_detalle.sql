-- TEST: Pedidos sin ninguna línea en detalle_pedidos
--
-- Un pedido siempre debe tener al menos un ítem.
-- Si este test devuelve filas, hay pedidos "vacíos" en la fuente MySQL
-- que no se ingresaron completos o se borraron las líneas por error.

select
    p.pedido_id
from {{ ref('stg_pedidos') }} p
left join {{ ref('stg_detalle_pedidos') }} d
    on p.pedido_id = d.pedido_id
where d.detalle_id is null
