# DuckLake

Data Lake Framework con DuckDB. Alternativa eficiente a Snowflake, Databricks y Synapse para empresas con 100GB-10TB de datos.

## Problema

- Empresas gastando $50K-200K/mes en infraestructura cloud innecesaria
- Data lakes que demoran 2-3 meses en implementarse
- Vendor lock-in total con clouds
- Sobre-ingeniería para volúmenes de datos moderados

## Propuesta de Valor

- **90-95% ahorro** en costos de infraestructura 
- **1-2 semanas** de implementación (vs 2-3 meses)
- **Performance igual o mejor** para volúmenes típicos
- **Zero vendor lock-in** — todo en Parquet estándar
- **Desarrollo local** en laptop, sin necesidad de cloud

## Arquitectura: Medallion (3 capas)

```
┌─────────────┐    ┌──────────────┐    ┌──────────────┐
│  RAW        │    │  STAGING     │    │  CONSUME     │
│  (Bronze)   │───>│  (Silver)    │───>│  (Gold)      │
│             │    │              │    │              │
│ Datos 1:1   │    │ Limpieza     │    │ BI / ML      │
│ Append-only │    │ Dedup        │    │ LLM / RAG    │
│ Particionado│    │ Validación   │    │ Exports      │
└─────────────┘    └──────────────┘    └──────────────┘
   Parquet            Parquet             Parquet
   por fecha          por dominio         por use case
```

## Quick Start

### Instalación

```bash
# Con Poetry
poetry install

# O con pip
pip install -e .
```

### Crear un proyecto

```bash
ducklake init mi_proyecto
cd mi_proyecto
```

### Configurar una fuente CSV

Editar `config/sources.yaml`:

```yaml
sources:
  - name: mis_datos
    type: csv
    enabled: true
    path: "data/input/*.csv"
    connection:
      delimiter: ","
      header: true
    tables:
      - ventas
    extract:
      mode: full
```

### Extraer datos

```bash
ducklake extract mis_datos
```

### Configurar un pipeline

Editar `config/pipelines.yaml`:

```yaml
pipelines:
  - name: ventas_staging
    source:
      layer: raw
      domain: mis_datos
      table: ventas
    destination:
      layer: staging
      domain: ventas
      table: ventas
    transforms:
      - type: filter
        condition: "total > 0"
      - type: deduplicate
        keys: [venta_id]
    quality_checks:
      - type: not_null
        columns: [venta_id, total]
```

### Ejecutar pipeline

```bash
ducklake run ventas_staging
```

### Ver catálogo

```bash
ducklake catalog
ducklake status
```

## Stack Tecnológico

| Componente | Tecnología |
|---|---|
| Motor analítico | DuckDB 1.0+ |
| Storage | Apache Parquet (PyArrow) |
| Configuración | YAML + Pydantic v2 |
| CLI | Click |
| Logging | Loguru |
| Language | Python 3.11+ |

## Estructura del Proyecto

```
ducklake/
├── core/           # Config, catalog, orchestrator, quality
├── connectors/     # MySQL, CSV (extensible)
├── layers/         # RAW, STAGING, CONSUME
├── transformations/# Cleaning, validation, enrichment
├── cli/            # Comandos CLI
└── utils/          # Logger, DuckDB helper, Parquet helper
config/             # YAML configs
data/               # Parquet data (gitignored)
tests/              # Unit + integration tests
```

## Conectores Disponibles

- **MySQL** — Extracción full e incremental
- **CSV** — Archivos individuales y glob patterns

Para agregar un nuevo conector, heredar de `BaseConnector` e implementar `validate_connection()`, `extract()` y `get_schema()`.

## Transformaciones

Disponibles en pipelines vía YAML:

- `rename` — Renombrar columnas
- `cast` — Cambiar tipos de datos
- `filter` — Filtrar registros
- `deduplicate` — Eliminar duplicados
- `custom_sql` — SQL arbitrario (usar `__INPUT__` como referencia a la tabla)
- `aggregate` — Agregaciones (para CONSUME layer)

## Desarrollo

```bash
# Instalar dependencias de desarrollo
poetry install --with dev

# Correr tests
pytest

# Formatear código
black ducklake/ tests/

# Type checking
mypy ducklake/
```

## Licencia

MIT
