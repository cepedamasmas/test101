# Nomenclatura — Convenciones del proyecto

Referencia única para nombrar todo en el proyecto. El objetivo es que cualquier persona pueda leer un nombre y entender qué es, de dónde viene y qué contiene, sin tener que abrir el archivo.

---

## 1. Modelos dbt

### Staging (`models/staging/`)

```
stg_{tabla}.sql
```

Un modelo por tabla fuente. Si dos fuentes distintas tienen una tabla con el mismo nombre, usar `stg_{fuente}_{tabla}` para desambiguar.

| Ejemplo | Qué es |
|---------|--------|
| `stg_clientes.sql` | Staging de la tabla `clientes` (fuente única) |
| `stg_pedidos.sql` | Staging de la tabla `pedidos` |
| `stg_mysql_pedidos.sql` | Si SFTP también tiene una tabla `pedidos` |

### Consume (`models/consume/`)

```
{caso_de_uso}.sql
```

Sin prefijo. El nombre describe qué responde el modelo, no de dónde viene.

| Ejemplo | Qué es |
|---------|--------|
| `revenue_mensual.sql` | Ventas agregadas por mes |
| `top_clientes.sql` | Clientes rankeados por gasto total |
| `alertas_stock.sql` | Productos con stock bajo umbral |
| `conciliacion_pagos.sql` | Cruce entre sistemas de pago |
| `dataset_churn.sql` | Dataset preparado para modelo ML de churn |

> **No usar** el prefijo `consume_` en el nombre del archivo. El directorio ya indica la capa.

### Tests custom (`tests/`)

```
{tabla}_{anomalia}.sql
```

| Ejemplo | Qué detecta |
|---------|-------------|
| `pedidos_sin_detalle.sql` | Pedidos que no tienen ninguna línea de detalle |
| `clientes_email_invalido.sql` | Emails que no tienen formato válido |
| `facturas_subtotal_incorrecto.sql` | Subtotal ≠ cantidad × precio |
| `envios_fecha_imposible.sql` | Fecha de envío anterior a fecha de pedido |

---

## 2. Columnas

La consistencia en nombres de columnas es lo que hace que los datos sean fáciles de consumir y cruzar entre tablas.

### Identificadores

| Patrón | Uso | Ejemplo |
|--------|-----|---------|
| `{entidad}_id` | PK y FK de cualquier entidad | `cliente_id`, `pedido_id`, `producto_id` |
| `{entidad}_codigo` | Código alfanumérico de negocio (no PK técnica) | `producto_codigo`, `sucursal_codigo` |

> En staging, siempre renombrar `id` → `{entidad}_id`. Nunca exponer columnas `id` desnudas en staging o consume.

### Fechas y timestamps

| Patrón | Uso | Ejemplo |
|--------|-----|---------|
| `{evento}_at` | Timestamp con hora (datetime) | `created_at`, `updated_at`, `cancelado_at` |
| `fecha_{evento}` | Fecha sin hora (date) | `fecha_pedido`, `fecha_entrega`, `fecha_vencimiento` |

> Nunca mezclar. Si la fuente da un timestamp, castearlo a date cuando la hora no importa: `cast(created_at as date) as fecha_pedido`.

### Booleanos

```
es_{condicion}      →  es_activo, es_valido, es_mayorista
tiene_{cosa}        →  tiene_descuento, tiene_envio_gratis
```

Siempre castear a `boolean` en staging. Nunca dejar `0/1` o `'S'/'N'` en staging/consume.

### Montos y métricas numéricas

| Patrón | Uso | Ejemplo |
|--------|-----|---------|
| `monto_{concepto}` | Valores monetarios | `monto_total`, `monto_descuento`, `monto_iva` |
| `cantidad_{cosa}` | Conteos discretos | `cantidad_items`, `cantidad_pedidos` |
| `total_{cosa}` | Suma acumulada (en consume) | `total_ventas`, `total_clientes` |
| `{concepto}_pct` | Porcentajes y ratios (0-100) | `descuento_pct`, `margen_pct`, `tasa_conversion_pct` |
| `{concepto}_dias` | Duraciones en días | `tiempo_entrega_dias`, `antiguedad_dias` |

### Columnas internas (metadata)

Prefijo `_` → indican que son columnas técnicas, no de negocio. Nunca llegan a consume.

| Columna | Qué es |
|---------|--------|
| `_ingestion_timestamp` | Cuándo se guardó el Parquet RAW |
| `_row_num` | Número de fila para dedup (se filtra en el `where`) |
| `_source_file` | Archivo de origen (SFTP) |

---

## 3. Python

### Archivos y módulos

| Tipo | Patrón | Ejemplo |
|------|--------|---------|
| Conector | `{tipo}_connector.py` | `mysql_connector.py`, `sftp_connector.py` |
| Exportador | `{destino}_export.py` | `postgres_export.py`, `duckdb_export.py` |
| Módulo genérico | `snake_case.py` | `config.py`, `pipeline.py` |

### Clases, funciones, variables

```python
# Clases: PascalCase
class MySQLConnector:
class RawLayer:

# Funciones y variables: snake_case
def extract_table(table_name: str) -> pd.DataFrame:
raw_tables = get_raw_tables(data_dir)

# Constantes de configuración: SCREAMING_SNAKE_CASE
MYSQL_CONFIG = {...}
DUCKDB_PATH = Path(...)

# Parámetros privados/internos de clase: _prefijo
self._connection = None
```

### Type hints y docstrings

Siempre en funciones públicas. Docstrings en formato Google:

```python
def save(self, df: pd.DataFrame, source: str, table: str) -> int:
    """Guarda un DataFrame como Parquet en la capa RAW.

    Args:
        df: Datos a guardar.
        source: Nombre del sistema fuente (mysql, sftp, api).
        table: Nombre de la tabla.

    Returns:
        Cantidad de filas guardadas.
    """
```

---

## 4. Variables de entorno

```
{SERVICIO}_{ATRIBUTO}
```

| Prefijo | Servicio |
|---------|----------|
| `MYSQL_` | Base de datos fuente MySQL |
| `SFTP_` | Servidor SFTP fuente |
| `PG_` | PostgreSQL destino |
| `DUCKDB_` | Configuración DuckDB |
| `OUTPUT_` | Paths de output del pipeline |

Ejemplos: `MYSQL_HOST`, `PG_PASSWORD`, `DUCKDB_PATH`, `OUTPUT_DIR`.

> Siempre en mayúsculas. Sin abreviaciones ambiguas (`DATABASE` no `DB`, `PASSWORD` no `PASS`).

---

## 5. Paths de datos (RAW)

```
data/raw/{fuente}/{tabla}/year={YYYY}/month={MM}/day={DD}/data.parquet
```

| Segmento | Valores posibles |
|----------|-----------------|
| `{fuente}` | `mysql`, `sftp`, `api` |
| `{tabla}` | nombre de la tabla en snake_case |
| `year=`, `month=`, `day=` | partición Hive — siempre los tres niveles |
| `data.parquet` | nombre fijo del archivo por partición |

Ejemplo: `data/raw/mysql/pedidos/year=2024/month=03/day=15/data.parquet`

---

## 6. Dagster assets

```python
@asset(group_name="{grupo}", ...)
def {nombre_asset}(context):
```

| Grupo | Assets que contiene |
|-------|---------------------|
| `ingestion` | `raw_ingestion` |
| `catalog` | `duckdb_catalog` |
| `export` | `postgres_export` |
| *(dbt automático)* | Un asset por modelo dbt, nombrado igual que el archivo `.sql` |

Los assets Dagster propios (no-dbt) usan el mismo nombre que la función Python.

---

## 7. Git

### Ramas

```
{tipo}/{descripcion-corta-en-kebab-case}
```

| Tipo | Cuándo |
|------|--------|
| `feature/` | Nueva funcionalidad o fuente |
| `fix/` | Corrección de bug |
| `chore/` | Mantenimiento, dependencias, docs |
| `refactor/` | Cambios internos sin cambio de comportamiento |

Ejemplos: `feature/fuente-sftp-ventas`, `fix/dedup-pedidos`, `chore/actualizar-dbt-1.9`

### Commits

```
{tipo}: {descripcion en imperativo, en español}
```

| Tipo | Cuándo |
|------|--------|
| `feat:` | Nueva funcionalidad |
| `fix:` | Corrección de bug |
| `docs:` | Solo documentación |
| `chore:` | Dependencias, configuración |
| `refactor:` | Sin cambio de comportamiento |
| `test:` | Solo tests |

Ejemplos:
```
feat: agregar conector SFTP para archivos de ventas
fix: corregir dedup en stg_pedidos cuando updated_at es null
docs: documentar proceso de deploy en Railway
chore: actualizar dbt-duckdb a 1.9.1
```

> Máximo 72 caracteres en la primera línea. Si necesita más contexto, dejar línea en blanco y agregar párrafo.

---

## Resumen rápido

| Contexto | Patrón |
|----------|--------|
| Modelo staging | `stg_{tabla}.sql` |
| Modelo consume | `{caso_de_uso}.sql` (sin prefijo) |
| Test custom | `{tabla}_{anomalia}.sql` |
| PK en staging | `{entidad}_id` (nunca `id` desnudo) |
| Timestamp | `{evento}_at` |
| Fecha (date) | `fecha_{evento}` |
| Booleano | `es_{condicion}` / `tiene_{cosa}` |
| Monto | `monto_{concepto}` |
| Porcentaje | `{concepto}_pct` |
| Columna técnica | `_{nombre}` |
| Variable de entorno | `SERVICIO_ATRIBUTO` |
| Rama git | `tipo/descripcion-kebab` |
| Commit | `tipo: descripcion en imperativo` |
