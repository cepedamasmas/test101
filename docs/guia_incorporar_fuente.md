# Guía: Cómo incorporar una nueva fuente de datos

Proceso completo para agregar una fuente nueva al pipeline DuckLake, desde el requerimiento del cliente hasta producción.

---

## Antes de empezar: relevamiento con el cliente

Antes de tocar código, definir estas preguntas:

| Pregunta | Por qué importa |
|----------|-----------------|
| ¿Qué servidor? (host, port, tipo de BD) | Define qué conector hay que crear |
| ¿Qué tablas? ¿Tiene permisos de lectura? | Scope del trabajo |
| ¿Qué volumen? ¿Filas aprox, frecuencia de cambio? | Afecta diseño de ingesta |
| ¿Qué caso de uso quiere? | Define el modelo consume |
| ¿Hay campo clave único? ¿Timestamp de actualización? | Necesario para dedup en staging |
| ¿Qué granularidad? (1 fila = 1 pedido? ¿1 línea?) | Afecta el modelo staging |

Sin estas respuestas no se puede diseñar bien ni staging ni consume.

---

## Resumen del flujo completo

```
REQUERIMIENTO CLIENTE
        ↓
[0] Relevamiento: tablas, volumen, caso de uso
        ↓
[1] Python: Conector nuevo → config.py → assets.py
        ↓
[2] dbt sources.yml  (registrar tablas RAW)
        ↓
[3] dbt staging/     (1 modelo por tabla: limpieza + tests)
        ↓
[4] dbt consume/     (1+ modelo con el caso de uso de negocio)
        ↓
[5] Verificar local: dbt run + test + dagster dev
        ↓
[6] git push → Railway autodeploya → pipeline corre
        ↓
PostgreSQL actualizado → disponible en BI tools / Dashboard
```

---

## Paso 1 — Crear el conector Python

Crear `railway/ducklake/connectors/{tipo}_connector.py` heredando `BaseConnector`:

```python
# ejemplo: mssql_connector.py
from .base import BaseConnector

class MSSQLConnector(BaseConnector):
    def extract(self) -> dict[str, pd.DataFrame]:
        # conectar con pyodbc o pymssql
        # una query por tabla
        # devolver {nombre_tabla: dataframe}
```

Registrar en `connectors/__init__.py`:
```python
from .mssql_connector import MSSQLConnector
```

Agregar configuración en `config.py`:
```python
MSSQL_CONFIG = {
    "host":     os.getenv("MSSQL_HOST"),
    "port":     int(os.getenv("MSSQL_PORT", 1433)),
    "database": os.getenv("MSSQL_DATABASE"),
    "username": os.getenv("MSSQL_USER"),
    "password": os.getenv("MSSQL_PASSWORD"),
}
MSSQL_TABLES = ["pedidos_canal_x", "clientes_canal_x", ...]
```

Agregar las tablas en `BASE_RAW_TABLES`:
```python
BASE_RAW_TABLES = {
    ...existentes...,
    "mssql": ["pedidos_canal_x", "clientes_canal_x", ...],
}
```

Invocar el conector en `dagster_pipeline/assets.py`, dentro del asset `raw_ingestion`:
```python
mssql = MSSQLConnector(MSSQL_CONFIG, MSSQL_TABLES)
try:
    for table, df in mssql.extract().items():
        n = raw.save(df, "mssql", table)
        total_rows[f"mssql.{table}"] = n
finally:
    mssql.close()
```

> **Variables de entorno**: agregar `MSSQL_HOST`, `MSSQL_USER`, etc. en Railway antes de deployar.

---

## Paso 2 — Registrar en dbt como source

En `dbt_techstore/models/sources.yml`, agregar un bloque por fuente nueva:

```yaml
- name: mssql_canal_x
  description: "Pedidos del canal X desde SQL Server"
  schema: raw
  tables:
    - name: pedidos_canal_x
      description: "Pedidos crudos del canal X"
    - name: clientes_canal_x
      description: "Clientes registrados en el canal X"
    - name: ...
```

Esto define cómo se llaman las tablas RAW dentro de dbt. Se usan con `{{ source('mssql_canal_x', 'pedidos_canal_x') }}` en los modelos.

---

## Paso 3 — Modelos staging (1 por tabla fuente)

Crear un archivo `.sql` por cada tabla en `dbt_techstore/models/staging/`:

```sql
-- stg_pedidos_canal_x.sql
with source as (
    select * from {{ source('mssql_canal_x', 'pedidos_canal_x') }}
),

cleaned as (
    select
        -- normalizar nombres (snake_case)
        -- castear tipos de datos
        -- filtrar nulls en PK
        -- deduplicar si la fuente trae duplicados
        cast(id_pedido as varchar)  as pedido_id,
        cast(fecha as date)         as fecha_pedido,
        upper(trim(estado))         as estado,
        cast(monto as decimal(18,2)) as monto,
        cliente_id
    from source
    where id_pedido is not null
)

select * from cleaned
```

> **Regla de staging**: solo limpieza. Sin joins, sin lógica de negocio, sin agregaciones. Un modelo por tabla fuente.

Agregar tests en `dbt_techstore/models/staging/_schema.yml`:

```yaml
- name: stg_pedidos_canal_x
  description: "Pedidos limpios del canal X"
  columns:
    - name: pedido_id
      description: "Identificador único del pedido"
      tests: [not_null, unique]
    - name: fecha_pedido
      tests: [not_null]
    - name: estado
      tests:
        - accepted_values:
            values: ['pendiente', 'confirmado', 'cancelado', 'devuelto']
    - name: monto
      tests: [not_null]
```

---

## Paso 4 — Modelo consume (el caso de uso de negocio)

Crear el modelo que responde la pregunta de negocio del cliente, en `dbt_techstore/models/consume/`:

```sql
-- pedidos_canal_x_kpis.sql
-- Ejemplo: KPIs mensuales de pedidos del canal X,
-- cruzado con la tabla de clientes unificada del lake

with pedidos as (
    select * from {{ ref('stg_pedidos_canal_x') }}
),

-- se puede cruzar con cualquier otra tabla staging del lake
clientes as (
    select * from {{ ref('stg_clientes') }}
),

final as (
    select
        date_trunc('month', p.fecha_pedido) as mes,
        p.estado,
        count(*)                             as total_pedidos,
        sum(p.monto)                         as revenue_total,
        count(distinct p.cliente_id)         as clientes_unicos,
        avg(p.monto)                         as ticket_promedio
    from pedidos p
    left join clientes c on p.cliente_id = c.cliente_id
    group by 1, 2
)

select * from final
```

> **Regla de consume**: aquí sí van joins, agregaciones, KPIs. Siempre usa `ref()` para referenciar staging, nunca `source()` directamente.

Agregar tests en `dbt_techstore/models/consume/_schema.yml`:

```yaml
- name: pedidos_canal_x_kpis
  description: "KPIs mensuales de pedidos del canal X"
  columns:
    - name: mes
      tests: [not_null]
    - name: total_pedidos
      tests: [not_null]
    - name: revenue_total
      tests: [not_null]
```

---

## Paso 5 — Verificar antes de deployar

```bash
# Desde railway/ducklake/

# 1. Parsear manifest para que Dagster vea los assets nuevos
dbt parse --project-dir dbt_techstore --profiles-dir dbt_techstore

# 2. Correr solo los modelos nuevos y los que dependen de ellos
dbt run --select stg_pedidos_canal_x+

# 3. Correr los tests de los modelos nuevos
dbt test --select stg_pedidos_canal_x+

# 4. Verificar que aparecen como assets en Dagster
dagster dev -m dagster_pipeline
# abrir http://localhost:3000 y ver el grafo
```

Si todo pasa, se puede deployar.

---

## Paso 6 — Deploy a Railway

```bash
git add .
git commit -m "feat: add mssql canal_x source (5 tables) + staging + kpis consume model"
git push
```

Railway redeploya automáticamente. El `Dockerfile` ya tiene `dbt parse`, así que el manifest se regenera y los assets nuevos aparecen en Dagster sin cambios adicionales.

---

## Qué NO hay que tocar

Una vez que el pipeline está armado, estas partes corren solas al agregar una fuente nueva:

| Componente | Por qué no necesita cambios |
|------------|----------------------------|
| `pipeline.py` | Llama a `materialize()` con todos los assets, detecta los nuevos automáticamente |
| `duckdb_catalog` | Registra todas las tablas RAW como vistas, incluyendo las nuevas |
| `postgres_export` | Exporta todos los schemas de DuckDB, incluyendo tablas nuevas |
| Dagster | Lee el `manifest.json` de dbt, detecta assets nuevos automáticamente |

El 90% del trabajo es el **conector Python** (Paso 1) y los **modelos dbt** (Pasos 3 y 4). La plomería del pipeline ya está hecha.

---

## Checklist rápido

- [ ] Relevamiento completo con el cliente (tablas, volumen, caso de uso, PK)
- [ ] Conector Python creado y registrado en `config.py` + `assets.py`
- [ ] Variables de entorno agregadas en Railway
- [ ] Source registrado en `sources.yml`
- [ ] Modelo staging por cada tabla (`stg_*.sql` + tests en `_schema.yml`)
- [ ] Modelo consume con el caso de uso (`consume/*.sql` + tests)
- [ ] `dbt run` + `dbt test` pasan sin errores localmente
- [ ] Dagster muestra los nuevos assets en el grafo
- [ ] `git push` → deploy exitoso en Railway
