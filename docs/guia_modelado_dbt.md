# Guia de Modelado - DuckLake

## Que es dbt y por que lo usamos

dbt (data build tool) te permite escribir SQL puro para transformar datos. Vos escribis un archivo `.sql`, dbt lo ejecuta y crea una tabla en la base de datos. Nada mas.

No necesitas saber Python, ni instalar cosas raras. Tu trabajo es escribir SQL como siempre, con dos diferencias:
- En vez de `FROM tabla`, usas `FROM {{ source('raw', 'tabla') }}` o `FROM {{ ref('otra_tabla') }}`
- El archivo lo guardas en una carpeta especifica del proyecto

dbt se encarga del resto: orden de ejecucion, creacion de tablas, todo.

---

## Arquitectura en 3 capas

```
RAW (Bronze)          STAGING (Silver)         CONSUME (Gold)
datos crudos    -->   datos limpios      -->   metricas de negocio
tal cual llegan       dedup, casteos           joins, agregaciones
no se tocan           1 modelo por fuente      tablas para BI/reportes
```

**Tu trabajo esta en STAGING y CONSUME.** RAW ya esta resuelto por ingesta.

---

## Paso 1: Saber que datos tenes disponibles

Las tablas RAW disponibles estan listadas en `models/sources.yml`. Ahi vas a ver algo como:

```yaml
sources:
  - name: raw
    schema: raw
    tables:
      - name: clientes
      - name: productos
      - name: pedidos
      - name: detalle_pedidos
      # ... mas tablas
```

Cada `name` es una tabla que podes usar en tus queries. Si necesitas una tabla nueva, pedila al equipo de ingesta.

---

## Paso 2: Crear un modelo STAGING

Un modelo staging limpia una tabla RAW. Se crea UN archivo `.sql` por cada fuente.

### Donde va el archivo

```
dbt_techstore/models/staging/stg_NOMBRE.sql
```

La convencion es `stg_` + nombre de la tabla raw.

### Como referencia la tabla RAW

```sql
SELECT * FROM {{ source('raw', 'pedidos') }}
```

Eso es lo unico "nuevo". `{{ source('raw', 'pedidos') }}` es la forma de dbt de decir "traeme la tabla `pedidos` del schema `raw`". Cuando dbt ejecuta, reemplaza eso por la tabla real.

### Ejemplo real: stg_pedidos

```sql
-- archivo: models/staging/stg_pedidos.sql

select
    pedido_id, cliente_id,
    cast(fecha as date) as fecha,
    estado, metodo_pago, total
from {{ source('raw', 'pedidos') }}
```

Que hace:
- Selecciona las columnas que necesitamos
- Castea `fecha` de string a date
- Nada mas. Simple.

### Ejemplo con mas logica: stg_clientes

```sql
-- archivo: models/staging/stg_clientes.sql

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
```

Que hace:
- Reemplaza emails null con un placeholder
- Castea fecha
- Filtra suspendidos
- Detecta emails duplicados con `row_number()`

### Ejemplo con filtro de calidad: stg_detalle_pedidos

```sql
-- archivo: models/staging/stg_detalle_pedidos.sql

select
    detalle_id, pedido_id, producto_id,
    cantidad, precio_unitario, descuento, subtotal
from {{ source('raw', 'detalle_pedidos') }}
where cantidad > 0 and subtotal > 0
```

Que hace:
- Filtra registros basura (cantidad 0, subtotales negativos)

### Resumen: que se hace en staging

| Accion | Ejemplo |
|---|---|
| Castear tipos | `cast(fecha as date)` |
| Renombrar columnas | `order_id as pedido_id` |
| Filtrar basura | `where cantidad > 0` |
| Rellenar nulls | `coalesce(email, 'placeholder')` |
| Deduplicar | `row_number() over (partition by ...)` |
| Parsear campos | Extraer datos de strings JSON |

---

## Paso 3 (opcional): Agregar tests de calidad

En la misma carpeta staging existe un archivo `_schema.yml`. Ahi se declaran reglas que dbt valida automaticamente.

### Para que sirve

Pensalo como un contrato: "esta columna NUNCA deberia ser null" o "estos IDs deberian ser unicos". Si algo se rompe en la data, dbt te avisa antes de que llegue a los reportes.

### Como se ve

```yaml
# archivo: models/staging/_schema.yml

version: 2

models:
  - name: stg_pedidos
    description: "Pedidos con fecha casteada"
    columns:
      - name: pedido_id
        tests: [not_null, unique]
      - name: cliente_id
        tests: [not_null]
```

Traduccion: "pedido_id nunca puede ser null y tiene que ser unico. cliente_id nunca puede ser null."

### Tests disponibles

| Test | Que valida |
|---|---|
| `not_null` | La columna no tiene valores null |
| `unique` | No hay valores repetidos |
| `accepted_values` | Solo permite ciertos valores (ej: estados) |
| `relationships` | Valida FK contra otra tabla |

### Es obligatorio?

**No.** Todo funciona sin `_schema.yml`. Pero es muy recomendable al menos poner `not_null` y `unique` en las columnas clave. Es una red de seguridad gratis.

---

## Paso 4: Crear un modelo CONSUME

Los modelos consume cruzan tablas de staging entre si para generar metricas de negocio. Son las tablas que van a usar los dashboards y reportes.

### Donde va el archivo

```
dbt_techstore/models/consume/NOMBRE.sql
```

### Como referenciar tablas staging

```sql
SELECT * FROM {{ ref('stg_pedidos') }}
```

`{{ ref('stg_pedidos') }}` le dice a dbt "usame la tabla staging de pedidos". dbt se encarga de que staging se ejecute ANTES que consume automaticamente.

### Ejemplo real: revenue_mensual

```sql
-- archivo: models/consume/revenue_mensual.sql

select
    date_trunc('month', p.fecha) as mes,
    count(distinct p.pedido_id) as pedidos,
    sum(p.total) as revenue,
    round(avg(p.total), 0) as ticket_promedio,
    count(distinct p.cliente_id) as clientes_unicos
from {{ ref('stg_pedidos') }} p
where p.estado not in ('cancelado')
group by date_trunc('month', p.fecha)
order by mes
```

### Ejemplo real: top_clientes

```sql
-- archivo: models/consume/top_clientes.sql

select
    c.nombre as cliente, c.ciudad, c.email,
    count(distinct p.pedido_id) as compras,
    sum(p.total) as total_gastado,
    round(avg(p.total), 0) as ticket_promedio,
    min(p.fecha) as primera_compra,
    max(p.fecha) as ultima_compra
from {{ ref('stg_pedidos') }} p
join {{ ref('stg_clientes') }} c on p.cliente_id = c.cliente_id
where p.estado != 'cancelado'
group by c.nombre, c.ciudad, c.email
order by total_gastado desc
limit 20
```

Que hace:
- Cruza pedidos con clientes (ambos de staging)
- Calcula metricas por cliente
- Ordena por gasto total

---

## Resumen: que archivo va donde

```
models/
├── sources.yml                    <-- NO TOCAR (lo maneja ingesta)
├── staging/
│   ├── _schema.yml                <-- Tests de calidad (opcional)
│   ├── stg_clientes.sql           <-- 1 archivo por tabla RAW
│   ├── stg_pedidos.sql
│   └── stg_detalle_pedidos.sql
└── consume/
    ├── revenue_mensual.sql        <-- Metricas de negocio
    └── top_clientes.sql
```

## Cheat sheet

| Quiero... | Escribo... |
|---|---|
| Leer una tabla RAW | `{{ source('raw', 'nombre_tabla') }}` |
| Leer una tabla staging | `{{ ref('stg_nombre') }}` |
| Leer otra tabla consume | `{{ ref('nombre_consume') }}` |
| Crear modelo staging | Archivo en `models/staging/stg_nombre.sql` |
| Crear modelo consume | Archivo en `models/consume/nombre.sql` |

## Flujo de trabajo

1. Escribis tu `.sql` en la carpeta correcta
2. Lo pusheas a git
3. El pipeline se ejecuta solo y crea la tabla
4. La tabla queda disponible en DuckDB y PostgreSQL

No hay que correr nada manual. Push y listo.
