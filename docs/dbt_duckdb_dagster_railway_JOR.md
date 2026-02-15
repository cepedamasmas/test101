# Stack: dbt + DuckDB + Dagster + Railway
### Pipeline de ventas farmacéuticas LATAM — guía de punta a punta

---

## 1. Arquitectura general

```
Fuentes          Orquestación     Transformación    Motor         Consumo
─────────        ────────────     ──────────────    ─────         ───────
CSV / Parquet ──► Dagster ───────► dbt models ─────► DuckDB ─────► QlikSense
                  (schedule,       (staging +        (almacena     Reportes
                   linaje,          consume,          y ejecuta)
                   retry,           ref() y
                   alertas)         tests)
```

### Rol de cada herramienta

| Herramienta | Rol |
|---|---|
| **DuckDB** | Motor de ejecución y almacenamiento (OLAP embebido) |
| **dbt** | Transformación y modelado SQL (staging → consume) |
| **Dagster** | Orquestación, scheduling, monitoreo y linaje visual |
| **Railway** | Plataforma de deploy del stack completo |

---

## 2. Estructura del proyecto

```
pharma_latam/
├── Dockerfile
├── dagster_home/
│   └── dagster.yaml
├── dbt_project.yml
├── profiles.yml
├── data/
│   ├── ventas_arg.csv
│   ├── ventas_bra.csv
│   └── productos.csv
├── models/
│   ├── staging/
│   │   ├── stg_ventas_arg.sql
│   │   ├── stg_ventas_bra.sql
│   │   └── schema.yml
│   └── consume/
│       ├── fact_ventas_latam.sql
│       └── schema.yml
├── orchestration/
│   └── definitions.py
└── requirements.txt
```

---

## 3. Datos fuente

**`data/ventas_arg.csv`**
```
invoice_id,fecha,producto_cod,cantidad,precio_unit,moneda
ARG-001,2024-01-15,AMOX500,100,850.50,ARS
ARG-002,2024-01-15,IBUP400,200,320.00,ARS
ARG-003,2024-02-01,AMOX500,50,870.00,ARS
```

**`data/ventas_bra.csv`**
```
invoice_id,fecha,produto_cod,quantidade,preco_unit,moeda
BRA-001,2024-01-20,AMOX500,80,45.90,BRL
BRA-002,2024-01-22,IBUP400,150,18.50,BRL
```

**`data/productos.csv`**
```
producto_cod,nombre,laboratorio,categoria_terapeutica
AMOX500,Amoxicilina 500mg,Laboratorio Bago,Antibióticos
IBUP400,Ibuprofeno 400mg,Roemmers,Antiinflamatorios
```

---

## 4. Configuración dbt

**`profiles.yml`** (en `~/.dbt/profiles.yml` o en la raíz del proyecto)

> Define **cómo dbt se conecta al motor de base de datos**. Puede tener múltiples targets (`dev`, `prod`) y vos elegís cuál usar al correr dbt. En este caso apunta a DuckDB local, pero si el destino fuera otro motor (BigQuery, Redshift, etc.) iría el host, puerto y credenciales acá. Es el único archivo que contiene información sensible y **no debe subirse al repositorio**.

```yaml
pharma_latam:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: pharma_latam.duckdb   # se crea automáticamente
      threads: 4
```

**`dbt_project.yml`**

> Es el **archivo raíz del proyecto dbt**. Define el nombre del proyecto, qué perfil de conexión usar (apunta al `profiles.yml`), dónde están los modelos y seeds, y las configuraciones por carpeta como la materialización. Sin este archivo, dbt no reconoce la carpeta como un proyecto válido.

```yaml
name: pharma_latam
version: '1.0.0'
profile: pharma_latam

model-paths: ["models"]
seed-paths: ["data"]

models:
  pharma_latam:
    staging:
      materialized: view      # staging como vistas
    consume:
      materialized: table     # consume como tablas físicas
```

---

## 5. Modelos dbt

### Capa Staging

Objetivo: normalizar cada fuente (nombres de columnas, tipos, metadata de país).

**`models/staging/stg_ventas_arg.sql`**

```sql
with source as (
    select * from read_csv_auto('data/ventas_arg.csv')
),

renamed as (
    select
        invoice_id,
        cast(fecha as date)                   as fecha,
        producto_cod,
        cast(cantidad as integer)             as cantidad,
        cast(precio_unit as decimal(18,2))    as precio_unit,
        moneda,
        'ARG'                                 as pais,
        current_timestamp                     as _loaded_at
    from source
)

select * from renamed
```

**`models/staging/stg_ventas_bra.sql`**

```sql
with source as (
    select * from read_csv_auto('data/ventas_bra.csv')
),

renamed as (
    select
        invoice_id,
        cast(fecha as date)                   as fecha,
        produto_cod                            as producto_cod,   -- normalización portugués → español
        cast(quantidade as integer)            as cantidad,
        cast(preco_unit as decimal(18,2))      as precio_unit,
        moeda                                  as moneda,
        'BRA'                                  as pais,
        current_timestamp                      as _loaded_at
    from source
)

select * from renamed
```

**`models/staging/schema.yml`**

> Define la **documentación y los tests de calidad** de los modelos de staging. Cada columna puede tener tests nativos de dbt (`unique`, `not_null`, `accepted_values`, `relationships`) que se ejecutan con `dbt test`. Este archivo también alimenta la documentación automática que genera `dbt docs`. No contiene lógica SQL, solo metadata y reglas de validación.

```yaml
version: 2

models:
  - name: stg_ventas_arg
    description: "Ventas Argentina normalizadas"
    columns:
      - name: invoice_id
        tests:
          - unique
          - not_null
      - name: fecha
        tests:
          - not_null
      - name: precio_unit
        tests:
          - not_null

  - name: stg_ventas_bra
    description: "Ventas Brasil normalizadas"
    columns:
      - name: invoice_id
        tests:
          - unique
          - not_null
```

### Capa Consume

Objetivo: consolidar, enriquecer con productos y convertir monedas.

**`models/consume/fact_ventas_latam.sql`**

```sql
with ventas_arg as (
    select * from {{ ref('stg_ventas_arg') }}
),

ventas_bra as (
    select * from {{ ref('stg_ventas_bra') }}
),

ventas_union as (
    select * from ventas_arg
    union all
    select * from ventas_bra
),

productos as (
    select * from read_csv_auto('data/productos.csv')
),

tipo_cambio as (
    select 'ARS' as moneda, 0.0011 as a_usd
    union all
    select 'BRL' as moneda, 0.20   as a_usd
),

final as (
    select
        v.invoice_id,
        v.fecha,
        v.pais,
        v.producto_cod,
        p.nombre                                          as producto_nombre,
        p.laboratorio,
        p.categoria_terapeutica,
        v.cantidad,
        v.precio_unit,
        v.moneda,
        round(v.precio_unit * tc.a_usd, 4)               as precio_unit_usd,
        round(v.cantidad * v.precio_unit, 2)              as venta_total_local,
        round(v.cantidad * v.precio_unit * tc.a_usd, 2)  as venta_total_usd
    from ventas_union v
    left join productos p
        on v.producto_cod = p.producto_cod
    left join tipo_cambio tc
        on v.moneda = tc.moneda
)

select * from final
```

**`models/consume/schema.yml`**

> Mismo rol que el `schema.yml` de staging pero aplicado a la capa de consume. Acá los tests son especialmente importantes porque validan la integridad del dato ya consolidado y transformado, que es lo que finalmente consume el negocio. Un fallo en estos tests es señal de un problema en algún modelo upstream.

```yaml
version: 2

models:
  - name: fact_ventas_latam
    description: "Fact table consolidada LATAM en USD"
    columns:
      - name: invoice_id
        tests:
          - unique
          - not_null
      - name: venta_total_usd
        tests:
          - not_null
```

---

## 6. Orquestación con Dagster

**`orchestration/definitions.py`**

```python
from dagster import (
    Definitions,
    AssetSelection,
    define_asset_job,
    ScheduleDefinition,
)
from dagster_dbt import DbtProject, DbtCliResource, dbt_assets

# Apunta al proyecto dbt
pharma_project = DbtProject(project_dir="../")

# Dagster lee todos los modelos dbt y los convierte en Assets automáticamente
@dbt_assets(manifest=pharma_project.manifest_path)
def pharma_dbt_assets(context, dbt: DbtCliResource):
    yield from dbt.cli(["run"], context=context).stream()

# Job que corre todo el pipeline
pharma_job = define_asset_job(
    name="pharma_pipeline",
    selection=AssetSelection.all()
)

# Schedule: todos los días a las 6am UTC
pharma_schedule = ScheduleDefinition(
    job=pharma_job,
    cron_schedule="0 6 * * *"
)

defs = Definitions(
    assets=[pharma_dbt_assets],
    jobs=[pharma_job],
    schedules=[pharma_schedule],
    resources={
        "dbt": DbtCliResource(project_dir="../")
    }
)
```

---

## 7. Linaje visual en Dagster

La UI de Dagster genera automáticamente un grafo de assets navegable:

```
[raw_ventas_arg.csv]    [raw_ventas_bra.csv]    [productos.csv]
        │                        │                      │
        ▼                        ▼                      │
 stg_ventas_arg          stg_ventas_bra                 │
        │                        │                      │
        └──────────┬─────────────┘                      │
                   ▼                                    │
            fact_ventas_latam ◄─────────────────────────┘
```

Para cada nodo en la UI podés ver:
- Última materialización y duración
- Filas procesadas
- Si los tests de dbt pasaron o fallaron
- El SQL ejecutado
- Logs completos del run

---

## 8. Deploy en Railway

### Infraestructura de servicios en Railway

```
Railway Project
│
├── [Servicio] dagster-webserver    ← UI navegable (linaje, logs, runs)
├── [Servicio] dagster-daemon       ← ejecuta schedules y sensores
├── [Servicio] postgresql           ← historial de runs de Dagster
│
└── [Volumen]  /data
        ├── ventas_arg.csv
        ├── ventas_bra.csv
        ├── productos.csv
        └── pharma_latam.duckdb    ← warehouse generado por dbt + DuckDB
```

### `requirements.txt`

```
dbt-duckdb==1.8.1
dagster==1.7.0
dagster-dbt==0.23.0
dagster-webserver==1.7.0
```

### `Dockerfile`

```dockerfile
FROM python:3.11-slim

WORKDIR /app

# Dependencias del sistema
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Dependencias Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar proyecto completo
COPY . .

# Compilar el manifest de dbt (necesario para que Dagster lea los assets)
RUN dbt parse --project-dir /app --profiles-dir /app

# Exponer puerto de la UI de Dagster
EXPOSE 3000

# Variable de entorno para Dagster
ENV DAGSTER_HOME=/app/dagster_home

CMD ["dagster-webserver", "-h", "0.0.0.0", "-p", "3000", "-w", "orchestration/definitions.py"]
```

### `dagster_home/dagster.yaml`

> Configura **dónde Dagster persiste su estado**: historial de runs, logs de eventos y schedules. Por defecto Dagster usaría almacenamiento en memoria o archivos locales, lo que no sirve en un entorno cloud. Este archivo le indica que use PostgreSQL (provisto por Railway) como backend, garantizando que el historial sobreviva reinicios del contenedor. La variable `DATABASE_URL` la inyecta Railway automáticamente al conectar el servicio de Postgres.

```yaml
run_storage:
  module: dagster_postgres.run_storage
  class: PostgresRunStorage
  config:
    postgres_url:
      env: DATABASE_URL        # Railway inyecta esta variable automáticamente

event_log_storage:
  module: dagster_postgres.event_log_storage
  class: PostgresEventLogStorage
  config:
    postgres_url:
      env: DATABASE_URL

schedule_storage:
  module: dagster_postgres.schedule_storage
  class: PostgresScheduleStorage
  config:
    postgres_url:
      env: DATABASE_URL
```

### Variables de entorno en Railway

```
DATABASE_URL=postgresql://...   # generada automáticamente por Railway al agregar Postgres
DAGSTER_HOME=/app/dagster_home
```

### Servicio dagster-daemon (override del CMD)

Para el servicio del daemon en Railway, el comando de inicio es:

```bash
dagster-daemon run -w orchestration/definitions.py
```

---

## 9. Comandos dbt útiles

```bash
# Verificar conexión
dbt debug

# Correr todo el pipeline
dbt run

# Correr solo staging (todos los modelos en models/staging/)
dbt run --select staging

# Correr solo consume
dbt run --select consume

# Correr un modelo específico
dbt run --select stg_ventas_arg

# Correr un modelo y todos sus descendientes
dbt run --select stg_ventas_arg+

# Correr un modelo y todos sus ancestros
dbt run --select +fact_ventas_latam

# Ejecutar tests
dbt test

# Generar y ver documentación
dbt docs generate
dbt docs serve
```

---

## 10. Flujo completo de ejecución

```
1. Dagster daemon detecta que son las 6:00 AM (cron schedule)
        │
        ▼
2. Dagster lanza el job pharma_pipeline
        │
        ▼
3. dbt run --select staging
   ├── DuckDB lee ventas_arg.csv → crea VIEW stg_ventas_arg
   └── DuckDB lee ventas_bra.csv → crea VIEW stg_ventas_bra
        │
        ▼
4. dbt test --select staging
   └── Valida unique + not_null en ambos staging models
        │
        ▼
5. dbt run --select consume
   └── DuckDB consolida, hace joins y crea TABLE fact_ventas_latam
        │
        ▼
6. dbt test --select consume
   └── Valida fact_ventas_latam
        │
        ▼
7. Dagster registra resultado del run en PostgreSQL
   └── UI muestra linaje actualizado, duración, filas procesadas
```

---

## 11. Agregar un país nuevo (ejemplo: Colombia)

Solo necesitás dos pasos:

**Paso 1:** crear `models/staging/stg_ventas_col.sql` siguiendo el mismo patrón de normalización.

**Paso 2:** agregar un `union all` en `fact_ventas_latam.sql`:

```sql
ventas_col as (
    select * from {{ ref('stg_ventas_col') }}
),

ventas_union as (
    select * from ventas_arg
    union all
    select * from ventas_bra
    union all
    select * from ventas_col   -- ← nueva línea
),
```

Dagster detecta el nuevo asset automáticamente en el próximo deploy.

---

## 12. Manejo de fallos de tests: warn vs error

### Concepto

dbt permite configurar la **severidad** de cada test. Esto determina si un fallo detiene el pipeline o solo emite una advertencia y deja continuar.

| Severidad | Comportamiento en dbt | Comportamiento en Dagster |
|---|---|---|
| `error` (default) | exit code != 0, fallo visible | cancela assets downstream |
| `warn` | exit code 0, advertencia en logs | pipeline continúa, asset queda en amarillo |

El caso de uso concreto para este pipeline:

- `stg_ventas_bra` tiene datos de un proveedor externo con calidad variable → **warn**: si hay duplicados, se loggea pero no bloqueamos toda la operación
- `fact_ventas_latam` es el dato que consume el negocio → **error**: si hay un problema acá, el pipeline se corta porque no queremos un reporte corrupto

---

### Configuración en dbt — schema.yml

**`models/staging/schema.yml`** — Brasil con warn, Argentina con error:

```yaml
version: 2

models:
  - name: stg_ventas_arg
    description: "Ventas Argentina normalizadas"
    columns:
      - name: invoice_id
        tests:
          - unique:
              severity: error      # duplicado en ARG es crítico, corta el pipeline
          - not_null:
              severity: error
      - name: fecha
        tests:
          - not_null:
              severity: error
      - name: precio_unit
        tests:
          - not_null:
              severity: error

  - name: stg_ventas_bra
    description: "Ventas Brasil normalizadas — proveedor externo, calidad variable"
    columns:
      - name: invoice_id
        tests:
          - unique:
              severity: warn       # duplicado en BRA se loggea pero no bloquea
          - not_null:
              severity: error      # nulo sí es crítico incluso en BRA
      - name: precio_unit
        tests:
          - not_null:
              severity: warn
```

**`models/consume/schema.yml`** — todo error porque es el dato final:

```yaml
version: 2

models:
  - name: fact_ventas_latam
    description: "Fact table consolidada LATAM en USD"
    columns:
      - name: invoice_id
        tests:
          - unique:
              severity: error      # dato consolidado no puede tener duplicados
          - not_null:
              severity: error
      - name: venta_total_usd
        tests:
          - not_null:
              severity: error      # métrica principal, nulo es inaceptable
```

---

### Configuración en Dagster — definitions.py

La clave es que Dagster necesita saber que debe correr `dbt test` entre capas, y manejar el resultado de forma diferente según el asset.

```python
from dagster import (
    Definitions,
    AssetSelection,
    AssetExecutionContext,
    define_asset_job,
    ScheduleDefinition,
    Output,
    AssetCheckResult,
    AssetCheckSpec,
)
from dagster_dbt import DbtProject, DbtCliResource, dbt_assets
import subprocess

pharma_project = DbtProject(project_dir="../")

# Assets dbt: Dagster convierte cada modelo en un asset automáticamente
@dbt_assets(manifest=pharma_project.manifest_path)
def pharma_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    # Correr los modelos
    yield from dbt.cli(["run"], context=context).stream()

    # Correr los tests y capturar el resultado
    test_result = dbt.cli(["test"], context=context)

    # Iterar sobre los eventos del test
    for event in test_result.stream():
        # dbt emite eventos por cada test ejecutado
        # los de severity: warn tienen exit code 0 → Dagster los trata como advertencia
        # los de severity: error tienen exit code != 0 → Dagster los trata como fallo
        yield event

    # Si algún test con severity: error falló, dbt retorna exit code != 0
    # Dagster detecta esto automáticamente y marca el asset como FAILED
    # Los assets downstream (fact_ventas_latam) quedan SKIPPED


# Job principal
pharma_job = define_asset_job(
    name="pharma_pipeline",
    selection=AssetSelection.all(),
    # Si un asset falla con error, los downstream se cancelan automáticamente
    # No hay configuración extra necesaria: Dagster lo maneja por el grafo de dependencias
)

# Schedule diario
pharma_schedule = ScheduleDefinition(
    job=pharma_job,
    cron_schedule="0 6 * * *"
)

defs = Definitions(
    assets=[pharma_dbt_assets],
    jobs=[pharma_job],
    schedules=[pharma_schedule],
    resources={
        "dbt": DbtCliResource(project_dir="../")
    }
)
```

---

### Qué ve Dagster en cada escenario

**Escenario A: falla un test con `warn` (ej: duplicado en BRA)**

```
stg_ventas_arg     → SUCCESS  ✓ (verde)
stg_ventas_bra     → SUCCESS  ⚠ (verde con advertencia en logs)
fact_ventas_latam  → SUCCESS  ✓ (verde, pipeline continuó)

Log de stg_ventas_bra:
  WARNING: 2 failures in test unique_stg_ventas_bra_invoice_id
  Rows: BRA-001 (aparece 2 veces)
  Severity: WARN — continuando ejecución
```

**Escenario B: falla un test con `error` (ej: duplicado en ARG)**

```
stg_ventas_arg     → FAILED   ✗ (rojo)
stg_ventas_bra     → SUCCESS  ✓ (verde)
fact_ventas_latam  → SKIPPED  ○ (gris, no ejecutado)

Log de stg_ventas_arg:
  ERROR: 1 failure in test unique_stg_ventas_arg_invoice_id
  Rows: ARG-001 (aparece 2 veces)
  Severity: ERROR — pipeline detenido
```

---

### Investigar qué filas fallaron

Cuando un test falla, podés ver exactamente qué registros rompieron la regla:

```bash
# Guarda las filas fallidas en una tabla dentro de DuckDB
dbt test --select stg_ventas_arg --store-failures
```

Esto crea automáticamente una tabla como:
`dbt_test__stg_ventas_arg_invoice_id_unique`

Que podés consultar directamente:

```sql
select * from dbt_test__stg_ventas_arg_invoice_id_unique;
-- Devuelve las filas duplicadas con todos sus campos
```

---

### Diagrama del flujo de fallo completo

```
Dagster lanza pharma_pipeline (6:00 AM)
        │
        ▼
dbt run + test --select staging
  ├── stg_ventas_arg → test unique FALLA (severity: error)
  │       │
  │       └── exit code != 0 → Dagster marca asset como FAILED (rojo)
  │
  └── stg_ventas_bra → test unique FALLA (severity: warn)
          │
          └── exit code 0 → Dagster marca asset como SUCCESS con warning (amarillo)
        │
        ▼
Dagster evalúa dependencias del grafo
  └── fact_ventas_latam depende de stg_ventas_arg (FAILED)
          │
          └── SKIPPED automáticamente (gris, no se ejecuta)
        │
        ▼
Dagster registra el run como FAILED en PostgreSQL
  └── UI muestra el grafo con colores: rojo / verde-amarillo / gris
```

El dato corrupto de Argentina nunca llega a la fact table.
El dato con advertencia de Brasil sí llega, pero queda loggeado para revisión.

---

*Stack: dbt-duckdb · dagster · dagster-dbt · Railway · PostgreSQL*
