# DuckLake - Contexto para Claude Code

## Overview

DuckLake es un framework config-driven para implementar data lakes eficientes usando DuckDB + Parquet.
Alternativa a Snowflake/Databricks para empresas con 100GB-10TB de datos.

## Arquitectura: Medallion (3 capas)

- **RAW (Bronze)**: Datos crudos 1:1, append-only, particionados por fecha en `data/raw/{source}/{table}/year=YYYY/month=MM/day=DD/data.parquet`
- **STAGING (Silver)**: Limpieza, dedup, validación en `data/staging/{domain}/{table}/data.parquet`
- **CONSUME (Gold)**: Tablas listas para BI/ML/LLM en `data/consume/{use_case}/{table}/data.parquet`

## Estructura del proyecto

```
ducklake/                    # Package principal
├── core/                    # Config, catalog, orchestrator, quality
├── connectors/              # MySQL, CSV, etc. (heredan BaseConnector)
├── layers/                  # RawLayer, StagingLayer, ConsumeLayer (heredan BaseLayer)
├── transformations/         # cleaning, validation, enrichment
├── cli/                     # Click CLI: init, extract, run, catalog
└── utils/                   # logger (loguru), duckdb_helper, parquet_helper
config/                      # YAML configs (sources, pipelines, settings)
data/                        # Parquet data (gitignored)
tests/                       # pytest (unit + integration)
```

## Stack

Python 3.11+, DuckDB 1.0+, PyArrow, PyYAML, Loguru, Pydantic v2, Click 8

## Convenciones de código

- Type hints en funciones públicas
- Docstrings formato Google
- snake_case para archivos/funciones, PascalCase para clases
- Imports ordenados: stdlib, third-party, local
- Logging con loguru (info, warning, error, success)
- Formatter: black (line-length=100)

## Cómo agregar un nuevo conector

1. Crear `ducklake/connectors/nuevo.py` heredando `BaseConnector`
2. Implementar `validate_connection()`, `extract()`, `get_schema()`
3. Registrar en `ducklake/connectors/__init__.py` (`_CONNECTOR_MAP`)
4. Agregar ejemplo en `config/sources.yaml.example`

## Cómo crear un pipeline

1. Definir source y destination en `config/pipelines.yaml`
2. Listar transforms: rename, cast, filter, deduplicate, custom_sql
3. Agregar quality_checks: not_null, unique, valid_values, range
4. Ejecutar: `ducklake run nombre_pipeline`

## Demo Docker (E-commerce TechStore)

El directorio `docker/` contiene un pipeline completo de demostración:

- **Ejecutar**: `.\run.bat` desde la raíz (limpia data anterior, levanta servicios, corre pipeline)
- **Sources**: MySQL (4 tablas), SFTP (5 archivos: CSV, JSON, XML, XLSX, TXT), APIs (dólar blue, feriados)
- **Output**: `docker/output/datalake/` (parquets) + `docker/output/techstore.duckdb`
- **PostgreSQL**: puerto 5433, user `techstore`, pass `techstore123`, db `techstore_lake`
- **DuckDB**: RAW = vistas con hive_partitioning, STAGING/CONSUME = tablas
- **PostgreSQL**: las 3 capas se exportan como tablas (no soporta vistas sobre parquet)

## Filosofía

- Config-driven: todo vía YAML, código solo para lógica compleja
- Idempotente: re-ejecutar sin duplicar datos
- Incremental por defecto
- Performance para 100GB-10TB
- RAW layer: append-only, particionado por fecha (`year=YYYY/month=MM/day=DD`), nunca sobrescribir
