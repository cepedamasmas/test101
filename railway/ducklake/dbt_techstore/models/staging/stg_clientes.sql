with base as (
    select
        cliente_id, nombre,
        coalesce(email, 'sin_email@placeholder.com') as email,
        ciudad,
        cast(fecha_alta as date) as fecha_alta,
        estado, telefono,
        row_number() over (partition by email order by cliente_id) as email_rank
    from {{ source('raw', 'clientes') }}
    where estado != 'suspendido'
)

select
    cliente_id, nombre, email, ciudad, fecha_alta, estado, telefono,
    email_rank > 1 as email_duplicado
from base
