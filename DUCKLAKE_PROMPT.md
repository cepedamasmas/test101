# DuckLake - Data Lake Framework con DuckDB

## 🎯 CONTEXTO DEL PROYECTO

Estoy creando **DuckLake**, un framework y metodología para implementar data lakes eficientes usando DuckDB. Este proyecto nace como alternativa a soluciones sobredimensionadas (Azure Synapse, Snowflake, Databricks) para empresas medianas con 100GB-10TB de datos.

### Problema que Resolvemos
- Empresas gastando $50K-200K/mes en infraestructura cloud innecesaria
- Data lakes que demoran 2-3 meses en implementarse
- Vendor lock-in total con clouds
- Sobre-ingeniería para volúmenes de datos moderados
- Equipos pequeños que no pueden mantener soluciones complejas

### Propuesta de Valor
- **Ahorro del 90-95%** en costos de infra (de $100K/mes a $1-2K/mes)
- **Implementación en 1-2 semanas** vs 2-3 meses
- **Performance igual o mejor** para volúmenes típicos
- **Zero vendor lock-in** (todo en Parquet estándar)
- **Desarrollo local** en laptop (no necesita cloud para probar)

---

## 🏗️ ARQUITECTURA: MEDALLION (3 CAPAS)

### **RAW Layer (Bronze)**
**Propósito**: Datos 1:1 con la fuente, máxima fidelidad
- Formato: Parquet particionado por fecha
- Path: `data/raw/{source}/{table}/year={YYYY}/month={MM}/day={DD}/data.parquet`
- Filosofía: Append-only, NUNCA borrar
- Metadata: `ingestion_timestamp`, `source_id`, `row_count`, `schema_version`
- Compresión: snappy (balance speed/size)

### **STAGING Layer (Silver)**
**Propósito**: Limpieza, normalización, calidad de datos
- Formato: Parquet optimizado
- Path: `data/staging/{domain}/{table}/data.parquet`
- Transformaciones:
  - Cast de tipos de datos
  - Renombre de columnas (snake_case)
  - Deduplicación
  - Filtrado de nulos/errores
  - Validaciones de calidad
- Filosofía: Reemplazar particiones, idempotente

### **CONSUME Layer (Gold)**
**Propósito**: Tablas listas para BI/ML/LLM
- Formato: Parquet altamente optimizado
- Path: `data/consume/{use_case}/{table}/data.parquet`
- Tipos:
  - `bi/` - Agregaciones para dashboards
  - `ml/` - Features para modelos
  - `llm/` - Datos para embeddings/RAG
  - `exports/` - Salidas para otros sistemas
- Filosofía: Recrear o merge según caso de uso

---

## 📦 STACK TECNOLÓGICO

### Core
- **Python 3.11+**
- **DuckDB 1.0+** - Motor analítico
- **Parquet (pyarrow)** - Storage format
- **PyYAML** - Configuración
- **Loguru** - Logging estructurado
- **Pydantic** - Validación de configs y schemas

### Conectores
- **pymysql** - MySQL
- **psycopg2** - PostgreSQL  
- **pyodbc** - SQL Server
- **paramiko** - SFTP
- **requests** - APIs REST
- **pandas** - Transformaciones intermedias (uso mínimo)

### Dev/Testing
- **pytest** - Testing
- **black** - Code formatting
- **mypy** - Type checking
- **pre-commit** - Git hooks

---

## 📂 ESTRUCTURA DEL PROYECTO

```
ducklake/
├── README.md
├── claude.md                      # Contexto para Claude Code
├── pyproject.toml                 # Poetry/pip config
├── .gitignore
├── .env.example
│
├── ducklake/                      # Package principal
│   ├── __init__.py
│   ├── __version__.py
│   │
│   ├── core/                      # Componentes core
│   │   ├── __init__.py
│   │   ├── base.py                # BaseConnector, BaseLayer
│   │   ├── config.py              # Config loader y validador
│   │   ├── catalog.py             # Metadata catalog
│   │   ├── orchestrator.py        # Pipeline orchestrator
│   │   └── quality.py             # Data quality checks
│   │
│   ├── connectors/                # Conectores a fuentes
│   │   ├── __init__.py
│   │   ├── base.py                # BaseConnector abstracto
│   │   ├── mysql.py
│   │   ├── postgresql.py
│   │   ├── sqlserver.py
│   │   ├── sftp.py
│   │   ├── csv.py
│   │   └── api.py
│   │
│   ├── layers/                    # Lógica de cada capa
│   │   ├── __init__.py
│   │   ├── raw.py                 # RAW layer logic
│   │   ├── staging.py             # STAGING layer logic
│   │   └── consume.py             # CONSUME layer logic
│   │
│   ├── transformations/           # Transformaciones comunes
│   │   ├── __init__.py
│   │   ├── cleaning.py            # Limpieza de datos
│   │   ├── validation.py          # Validaciones
│   │   └── enrichment.py          # Enriquecimiento
│   │
│   ├── cli/                       # Command Line Interface
│   │   ├── __init__.py
│   │   ├── main.py                # Entry point
│   │   ├── init.py                # ducklake init
│   │   ├── source.py              # ducklake add-source
│   │   ├── run.py                 # ducklake run
│   │   └── catalog.py             # ducklake catalog
│   │
│   └── utils/                     # Utilidades
│       ├── __init__.py
│       ├── logger.py              # Setup de loguru
│       ├── duckdb_helper.py       # Helpers para DuckDB
│       └── parquet_helper.py      # Helpers para Parquet
│
├── config/                        # Configuraciones
│   ├── sources.yaml.example       # Template de sources
│   ├── pipelines.yaml.example     # Template de pipelines
│   └── settings.yaml.example      # Settings generales
│
├── data/                          # Data directory (gitignored)
│   ├── raw/
│   ├── staging/
│   └── consume/
│
├── tests/                         # Tests
│   ├── __init__.py
│   ├── conftest.py
│   ├── unit/
│   │   ├── test_connectors.py
│   │   ├── test_layers.py
│   │   └── test_transformations.py
│   └── integration/
│       └── test_pipeline_e2e.py
│
├── examples/                      # Ejemplos de uso
│   ├── example_mysql_to_consume.py
│   ├── example_csv_pipeline.py
│   └── example_custom_transformation.py
│
└── docs/                          # Documentación
    ├── architecture.md
    ├── quickstart.md
    ├── connectors.md
    └── best_practices.md
```

---

## 🎨 FILOSOFÍA DE DISEÑO: CONFIG-DRIVEN

**TODO debe ser configurable vía YAML, código solo para lógica compleja.**

### Ejemplo: `config/sources.yaml`
```yaml
sources:
  - name: mysql_ventas
    type: mysql
    enabled: true
    connection:
      host: ${MYSQL_HOST}
      port: 3306
      database: ventas
      user: ${MYSQL_USER}
      password: ${MYSQL_PASSWORD}
    tables:
      - clientes
      - pedidos
      - facturas
    extract:
      mode: incremental
      key_column: updated_at
      batch_size: 10000
    
  - name: sftp_reportes
    type: sftp
    enabled: true
    connection:
      host: sftp.cliente.com
      port: 22
      user: ${SFTP_USER}
      password: ${SFTP_PASSWORD}
    path: /exports/*.csv
    extract:
      mode: full
      schedule: "0 2 * * *"  # Cron expression
```

### Ejemplo: `config/pipelines.yaml`
```yaml
pipelines:
  - name: ventas_staging
    description: "Pipeline de ventas raw a staging"
    source:
      layer: raw
      domain: mysql_ventas
      table: clientes
    destination:
      layer: staging
      domain: ventas
      table: clientes
    transforms:
      - type: rename
        columns:
          cli_id: cliente_id
          cli_nom: nombre
          cli_email: email
      - type: cast
        columns:
          fecha_alta: date
          activo: boolean
      - type: filter
        condition: "estado != 'DELETED'"
      - type: deduplicate
        keys: [cliente_id]
    quality_checks:
      - type: not_null
        columns: [cliente_id, nombre]
      - type: unique
        columns: [cliente_id]
      - type: valid_values
        column: estado
        values: [ACTIVO, INACTIVO, PENDIENTE]
```

---

## 🔨 COMPONENTES A IMPLEMENTAR

### 1. **Base Classes** (`ducklake/core/base.py`)

```python
from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional
from datetime import datetime
import duckdb

class BaseConnector(ABC):
    """Base class para todos los conectores."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.name = config.get('name')
        self.logger = self._setup_logger()
    
    @abstractmethod
    def validate_connection(self) -> bool:
        """Validar que la conexión funciona."""
        pass
    
    @abstractmethod
    def extract(self, **kwargs) -> str:
        """
        Extraer datos y retornar path al parquet.
        Returns: path to parquet file
        """
        pass
    
    @abstractmethod
    def get_schema(self, table: str) -> Dict[str, str]:
        """Obtener schema de una tabla."""
        pass
    
    def get_last_extraction(self, table: str) -> Optional[datetime]:
        """Obtener timestamp de última extracción."""
        # Leer del catalog
        pass


class BaseLayer(ABC):
    """Base class para capas (RAW, STAGING, CONSUME)."""
    
    def __init__(self, base_path: str):
        self.base_path = base_path
        self.duckdb_conn = duckdb.connect()
        self.logger = self._setup_logger()
    
    @abstractmethod
    def write(self, data: Any, destination: Dict[str, Any]) -> str:
        """Escribir datos a la capa."""
        pass
    
    @abstractmethod
    def read(self, source: Dict[str, Any]) -> Any:
        """Leer datos de la capa."""
        pass
    
    def get_partition_path(self, base: str, date: datetime) -> str:
        """Generar path con particionado por fecha."""
        return f"{base}/year={date.year}/month={date.month:02d}/day={date.day:02d}"
```

### 2. **Connector Example** (`ducklake/connectors/mysql.py`)

```python
from ducklake.core.base import BaseConnector
import pymysql
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from typing import Dict, Any
from pathlib import Path

class MySQLConnector(BaseConnector):
    """Conector para MySQL."""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.connection_params = config['connection']
    
    def validate_connection(self) -> bool:
        """Validar conexión a MySQL."""
        try:
            conn = pymysql.connect(**self.connection_params)
            conn.close()
            self.logger.info(f"✓ Conexión exitosa a MySQL: {self.name}")
            return True
        except Exception as e:
            self.logger.error(f"✗ Error conectando a MySQL: {e}")
            return False
    
    def extract(self, table: str, output_path: str, **kwargs) -> str:
        """
        Extraer datos de MySQL a Parquet.
        
        Args:
            table: Nombre de la tabla
            output_path: Path donde guardar el parquet
            
        Returns:
            Path del archivo parquet creado
        """
        mode = self.config['extract'].get('mode', 'full')
        
        # Construir query
        if mode == 'incremental':
            key_column = self.config['extract']['key_column']
            last_value = self._get_last_value(table, key_column)
            query = f"SELECT * FROM {table} WHERE {key_column} > '{last_value}'"
        else:
            query = f"SELECT * FROM {table}"
        
        # Extraer datos
        conn = pymysql.connect(**self.connection_params)
        df = pd.read_sql(query, conn)
        conn.close()
        
        # Agregar metadata
        df['_ingestion_timestamp'] = pd.Timestamp.now()
        df['_source_name'] = self.name
        
        # Escribir a Parquet
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        table_pa = pa.Table.from_pandas(df)
        pq.write_table(table_pa, output_path, compression='snappy')
        
        self.logger.info(f"✓ Extraídos {len(df)} registros de {table}")
        return output_path
    
    def get_schema(self, table: str) -> Dict[str, str]:
        """Obtener schema de una tabla MySQL."""
        conn = pymysql.connect(**self.connection_params)
        cursor = conn.cursor()
        cursor.execute(f"DESCRIBE {table}")
        schema = {row[0]: row[1] for row in cursor.fetchall()}
        conn.close()
        return schema
```

### 3. **RAW Layer** (`ducklake/layers/raw.py`)

```python
from ducklake.core.base import BaseLayer
from datetime import datetime
from pathlib import Path
import shutil

class RawLayer(BaseLayer):
    """
    RAW Layer: Datos crudos append-only.
    
    Responsabilidades:
    - Recibir datos de conectores
    - Particionar por fecha
    - Mantener metadata
    - Append-only (nunca borrar)
    """
    
    def write(self, source_path: str, destination: Dict[str, Any]) -> str:
        """
        Escribir datos a RAW layer.
        
        Args:
            source_path: Path del parquet origen
            destination: {source, table}
            
        Returns:
            Path donde se guardó el archivo
        """
        source = destination['source']
        table = destination['table']
        date = datetime.now()
        
        # Generar path particionado
        base = f"{self.base_path}/raw/{source}/{table}"
        partition_path = self.get_partition_path(base, date)
        Path(partition_path).mkdir(parents=True, exist_ok=True)
        
        # Copiar archivo
        dest_file = f"{partition_path}/data.parquet"
        shutil.copy2(source_path, dest_file)
        
        # Registrar en catalog
        self._register_in_catalog(source, table, dest_file, date)
        
        self.logger.info(f"✓ Datos escritos en RAW: {dest_file}")
        return dest_file
    
    def read(self, source: Dict[str, Any]) -> str:
        """
        Leer datos de RAW layer.
        
        Args:
            source: {source, table, date_range}
            
        Returns:
            DuckDB query string
        """
        pattern = f"{self.base_path}/raw/{source['source']}/{source['table']}/**/*.parquet"
        
        # Filtrar por fecha si se especifica
        if 'date_from' in source:
            # Usar DuckDB para filtrar por particiones
            query = f"""
            SELECT * FROM read_parquet('{pattern}')
            WHERE _ingestion_timestamp >= '{source['date_from']}'
            """
        else:
            query = f"SELECT * FROM read_parquet('{pattern}')"
        
        return query
```

### 4. **STAGING Layer** (`ducklake/layers/staging.py`)

```python
from ducklake.core.base import BaseLayer
from ducklake.transformations import cleaning, validation
import duckdb
from typing import Dict, Any, List

class StagingLayer(BaseLayer):
    """
    STAGING Layer: Limpieza y normalización.
    
    Responsabilidades:
    - Aplicar transformaciones
    - Validar calidad
    - Deduplicar
    - Normalizar schemas
    """
    
    def process(self, pipeline_config: Dict[str, Any]) -> str:
        """
        Procesar datos de RAW a STAGING según config.
        
        Args:
            pipeline_config: Configuración del pipeline
            
        Returns:
            Path del parquet generado
        """
        # Leer datos de RAW
        raw_query = self._build_raw_query(pipeline_config['source'])
        
        # Aplicar transformaciones
        transforms = pipeline_config.get('transforms', [])
        query = self._apply_transforms(raw_query, transforms)
        
        # Ejecutar query y escribir resultado
        result = self.duckdb_conn.execute(query).fetchdf()
        
        # Validar calidad
        quality_checks = pipeline_config.get('quality_checks', [])
        self._run_quality_checks(result, quality_checks)
        
        # Escribir a STAGING
        dest_path = self._get_staging_path(pipeline_config['destination'])
        self._write_parquet(result, dest_path)
        
        self.logger.info(f"✓ Pipeline completado: {pipeline_config['name']}")
        return dest_path
    
    def _apply_transforms(self, base_query: str, transforms: List[Dict]) -> str:
        """Construir query SQL con todas las transformaciones."""
        query = f"WITH base AS ({base_query})"
        
        for i, transform in enumerate(transforms):
            transform_type = transform['type']
            
            if transform_type == 'rename':
                # Renombrar columnas
                renames = ', '.join([
                    f"{old} AS {new}" 
                    for old, new in transform['columns'].items()
                ])
                query += f""",
                step_{i} AS (
                    SELECT {renames}, * EXCLUDE ({', '.join(transform['columns'].keys())})
                    FROM {'base' if i == 0 else f'step_{i-1}'}
                )"""
            
            elif transform_type == 'cast':
                # Castear tipos
                casts = ', '.join([
                    f"CAST({col} AS {dtype}) AS {col}"
                    for col, dtype in transform['columns'].items()
                ])
                query += f""",
                step_{i} AS (
                    SELECT {casts}, * EXCLUDE ({', '.join(transform['columns'].keys())})
                    FROM {'base' if i == 0 else f'step_{i-1}'}
                )"""
            
            elif transform_type == 'filter':
                # Filtrar registros
                query += f""",
                step_{i} AS (
                    SELECT * FROM {'base' if i == 0 else f'step_{i-1}'}
                    WHERE {transform['condition']}
                )"""
            
            elif transform_type == 'deduplicate':
                # Deduplicar
                keys = ', '.join(transform['keys'])
                query += f""",
                step_{i} AS (
                    SELECT * FROM (
                        SELECT *, ROW_NUMBER() OVER (PARTITION BY {keys} ORDER BY _ingestion_timestamp DESC) as rn
                        FROM {'base' if i == 0 else f'step_{i-1}'}
                    ) WHERE rn = 1
                )"""
        
        # Query final
        last_step = f"step_{len(transforms)-1}" if transforms else "base"
        query += f" SELECT * EXCLUDE (rn) FROM {last_step}"
        
        return query
```

### 5. **Orchestrator** (`ducklake/core/orchestrator.py`)

```python
from typing import Dict, List, Any
from ducklake.core.catalog import Catalog
from ducklake.connectors import get_connector
from ducklake.layers import RawLayer, StagingLayer, ConsumeLayer
from loguru import logger

class Orchestrator:
    """
    Orquestador de pipelines.
    
    Responsabilidades:
    - Ejecutar pipelines según configuración
    - Manejar dependencias
    - Registrar ejecuciones en catalog
    - Manejo de errores y retries
    """
    
    def __init__(self, config_path: str, data_path: str):
        self.config = self._load_config(config_path)
        self.data_path = data_path
        self.catalog = Catalog(f"{data_path}/catalog.duckdb")
        
        # Inicializar layers
        self.raw = RawLayer(data_path)
        self.staging = StagingLayer(data_path)
        self.consume = ConsumeLayer(data_path)
    
    def run_extraction(self, source_name: str) -> Dict[str, Any]:
        """
        Ejecutar extracción de una fuente.
        
        Args:
            source_name: Nombre de la fuente a extraer
            
        Returns:
            Resultados de la extracción
        """
        logger.info(f"🚀 Iniciando extracción: {source_name}")
        
        # Obtener config de la fuente
        source_config = self._get_source_config(source_name)
        
        # Crear conector
        connector = get_connector(source_config)
        
        # Validar conexión
        if not connector.validate_connection():
            raise ConnectionError(f"No se pudo conectar a {source_name}")
        
        # Extraer cada tabla
        results = {}
        for table in source_config.get('tables', []):
            try:
                # Extraer a temporal
                temp_path = f"/tmp/{source_name}_{table}.parquet"
                connector.extract(table, temp_path)
                
                # Mover a RAW layer
                raw_path = self.raw.write(temp_path, {
                    'source': source_name,
                    'table': table
                })
                
                results[table] = {'status': 'success', 'path': raw_path}
                logger.success(f"✓ {table} extraído exitosamente")
                
            except Exception as e:
                results[table] = {'status': 'error', 'error': str(e)}
                logger.error(f"✗ Error extrayendo {table}: {e}")
        
        return results
    
    def run_pipeline(self, pipeline_name: str) -> Dict[str, Any]:
        """
        Ejecutar un pipeline completo.
        
        Args:
            pipeline_name: Nombre del pipeline a ejecutar
            
        Returns:
            Resultados de la ejecución
        """
        logger.info(f"🚀 Ejecutando pipeline: {pipeline_name}")
        
        # Obtener config del pipeline
        pipeline_config = self._get_pipeline_config(pipeline_name)
        
        try:
            # Ejecutar según capa destino
            if pipeline_config['destination']['layer'] == 'staging':
                result_path = self.staging.process(pipeline_config)
            elif pipeline_config['destination']['layer'] == 'consume':
                result_path = self.consume.process(pipeline_config)
            
            logger.success(f"✓ Pipeline {pipeline_name} completado: {result_path}")
            
            return {
                'status': 'success',
                'pipeline': pipeline_name,
                'output': result_path
            }
            
        except Exception as e:
            logger.error(f"✗ Error en pipeline {pipeline_name}: {e}")
            return {
                'status': 'error',
                'pipeline': pipeline_name,
                'error': str(e)
            }
```

### 6. **Catalog** (`ducklake/core/catalog.py`)

```python
import duckdb
from datetime import datetime
from typing import Dict, List, Any, Optional

class Catalog:
    """
    Metadata catalog usando DuckDB.
    
    Responsabilidades:
    - Registrar extracciones
    - Registrar pipelines ejecutados
    - Lineage de datos
    - Estadísticas de tablas
    """
    
    def __init__(self, db_path: str):
        self.conn = duckdb.connect(db_path)
        self._init_tables()
    
    def _init_tables(self):
        """Crear tablas de metadata."""
        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS extractions (
            id INTEGER PRIMARY KEY,
            source_name VARCHAR,
            table_name VARCHAR,
            extraction_date TIMESTAMP,
            rows_extracted INTEGER,
            file_path VARCHAR,
            status VARCHAR,
            error_message VARCHAR
        )
        """)
        
        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS pipeline_runs (
            id INTEGER PRIMARY KEY,
            pipeline_name VARCHAR,
            execution_date TIMESTAMP,
            source_layer VARCHAR,
            destination_layer VARCHAR,
            rows_processed INTEGER,
            duration_seconds FLOAT,
            status VARCHAR,
            error_message VARCHAR
        )
        """)
        
        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS data_quality (
            id INTEGER PRIMARY KEY,
            table_name VARCHAR,
            check_type VARCHAR,
            check_date TIMESTAMP,
            passed BOOLEAN,
            details VARCHAR
        )
        """)
    
    def register_extraction(self, extraction_info: Dict[str, Any]):
        """Registrar una extracción."""
        self.conn.execute("""
        INSERT INTO extractions 
        (source_name, table_name, extraction_date, rows_extracted, file_path, status)
        VALUES (?, ?, ?, ?, ?, ?)
        """, [
            extraction_info['source'],
            extraction_info['table'],
            datetime.now(),
            extraction_info['rows'],
            extraction_info['path'],
            extraction_info['status']
        ])
    
    def get_last_extraction(self, source: str, table: str) -> Optional[datetime]:
        """Obtener timestamp de última extracción exitosa."""
        result = self.conn.execute("""
        SELECT MAX(extraction_date) 
        FROM extractions 
        WHERE source_name = ? AND table_name = ? AND status = 'success'
        """, [source, table]).fetchone()
        
        return result[0] if result else None
    
    def get_lineage(self, table: str) -> List[Dict[str, Any]]:
        """Obtener lineage de una tabla."""
        # Implementar tracking de transformaciones
        pass
```

### 7. **CLI** (`ducklake/cli/main.py`)

```python
import click
from pathlib import Path
from ducklake.core.orchestrator import Orchestrator
from ducklake.utils.logger import setup_logger
import yaml

@click.group()
def cli():
    """DuckLake - Data Lake Framework con DuckDB"""
    setup_logger()

@cli.command()
@click.argument('project_name')
@click.option('--path', default='.', help='Path donde crear el proyecto')
def init(project_name: str, path: str):
    """Inicializar un nuevo proyecto DuckLake."""
    click.echo(f"🦆 Inicializando proyecto: {project_name}")
    
    # Crear estructura de carpetas
    project_path = Path(path) / project_name
    (project_path / 'config').mkdir(parents=True, exist_ok=True)
    (project_path / 'data' / 'raw').mkdir(parents=True, exist_ok=True)
    (project_path / 'data' / 'staging').mkdir(parents=True, exist_ok=True)
    (project_path / 'data' / 'consume').mkdir(parents=True, exist_ok=True)
    
    # Crear archivos de configuración ejemplo
    # ... (copiar templates)
    
    click.echo(f"✓ Proyecto creado en: {project_path}")

@cli.command()
@click.argument('source_name')
def extract(source_name: str):
    """Extraer datos de una fuente."""
    click.echo(f"🔄 Extrayendo datos de: {source_name}")
    
    orchestrator = Orchestrator('config', 'data')
    results = orchestrator.run_extraction(source_name)
    
    # Mostrar resultados
    for table, result in results.items():
        if result['status'] == 'success':
            click.echo(f"  ✓ {table}: {result['path']}")
        else:
            click.echo(f"  ✗ {table}: {result['error']}", err=True)

@cli.command()
@click.argument('pipeline_name')
def run(pipeline_name: str):
    """Ejecutar un pipeline."""
    click.echo(f"🚀 Ejecutando pipeline: {pipeline_name}")
    
    orchestrator = Orchestrator('config', 'data')
    result = orchestrator.run_pipeline(pipeline_name)
    
    if result['status'] == 'success':
        click.echo(f"✓ Completado: {result['output']}")
    else:
        click.echo(f"✗ Error: {result['error']}", err=True)

@cli.command()
def catalog():
    """Ver catálogo de datos."""
    from ducklake.core.catalog import Catalog
    
    cat = Catalog('data/catalog.duckdb')
    
    # Mostrar últimas extracciones
    click.echo("\n📊 Últimas Extracciones:")
    results = cat.conn.execute("""
    SELECT source_name, table_name, extraction_date, rows_extracted, status
    FROM extractions
    ORDER BY extraction_date DESC
    LIMIT 10
    """).fetchdf()
    
    click.echo(results.to_string(index=False))

if __name__ == '__main__':
    cli()
```

---

## 📋 BEST PRACTICES Y CONVENCIONES

### Código
- **Type hints SIEMPRE** en funciones públicas
- **Docstrings formato Google** en todas las clases y métodos públicos
- **Logging estructurado** con loguru (info, warning, error, success)
- **Naming**: snake_case para archivos/funciones, PascalCase para clases
- **Imports**: ordenados (stdlib, third-party, local)

### DuckDB
- **Usar Parquet** siempre que sea posible (no CSV en producción)
- **Particionado por fecha** en RAW layer
- **Lectura streaming** de RAW a STAGING cuando sea posible
- **Proyección de columnas** (SELECT solo lo necesario)
- **Pushdown filters** (WHERE en la lectura, no después)

### Data Quality
- **Validaciones en STAGING**: not_null, unique, valid_values, ranges
- **Estadísticas automáticas**: row_count, null_percentage, unique_count
- **Alertas**: configurar umbrales y notificaciones

### Performance
- **Batch size apropiado**: 10K-100K registros por batch
- **Compresión snappy** para balance speed/size
- **Índices en catalog**: para queries rápidas de metadata
- **Memory limits**: configurar en DuckDB según disponible

---

## 🎯 DELIVERABLES ESPERADOS

Por favor crea lo siguiente:

### 1. **claude.md**
Documentación completa del proyecto para Claude Code que incluya:
- Overview del proyecto
- Arquitectura de las 3 capas
- Estructura de carpetas y qué va en cada lugar
- Convenciones de código
- Cómo agregar nuevos conectores
- Cómo crear pipelines
- Best practices

### 2. **Estructura de Carpetas**
Crear toda la estructura de carpetas según el árbol definido arriba.

### 3. **Código Base**
Implementar:
- Base classes (BaseConnector, BaseLayer)
- Sistema de configuración (YAML loader y validator con Pydantic)
- Al menos 2 conectores funcionales (MySQL y CSV)
- Las 3 capas (RAW, STAGING, CONSUME)
- Orchestrator básico
- Catalog con DuckDB
- CLI con comandos: init, extract, run, catalog
- Utils (logger, duckdb_helper, parquet_helper)

### 4. **Archivos de Configuración**
Templates de ejemplo para:
- `sources.yaml.example` (con ejemplos de MySQL, CSV, SFTP)
- `pipelines.yaml.example` (con ejemplos de transformaciones)
- `settings.yaml.example` (configuración general)
- `.env.example` (variables de entorno)

### 5. **README.md**
Incluir:
- Descripción del proyecto
- Problema que resuelve
- Quick start
- Instalación
- Ejemplos de uso
- Arquitectura (diagrama de las 3 capas)

### 6. **pyproject.toml**
Configuración de Poetry con todas las dependencias necesarias.

### 7. **Tests Básicos**
Al menos:
- Test de BaseConnector
- Test de MySQL connector (con mock)
- Test de RAW layer
- Test de config loader

### 8. **Ejemplos**
Scripts de ejemplo:
- `examples/example_mysql_to_consume.py` - Pipeline completo E2E
- `examples/example_custom_transformation.py` - Cómo agregar transformación custom

---

## 🚀 ORDEN DE IMPLEMENTACIÓN SUGERIDO

1. **Setup básico**: estructura de carpetas, pyproject.toml, README
2. **claude.md**: documentación completa
3. **Utils**: logger, helpers
4. **Base classes**: BaseConnector, BaseLayer
5. **Config system**: loader, validator
6. **Catalog**: DuckDB metadata
7. **Layers**: RAW → STAGING → CONSUME
8. **Conectores**: MySQL, CSV
9. **Orchestrator**: lógica de ejecución
10. **CLI**: comandos principales
11. **Tests**: cobertura básica
12. **Examples**: scripts de ejemplo

---

## 💡 NOTAS IMPORTANTES

- **Idempotencia**: Los pipelines deben poder re-ejecutarse sin duplicar datos
- **Incremental por defecto**: Solo extraer datos nuevos cuando sea posible
- **Schema evolution**: Detectar cambios en schemas y alertar
- **Error handling**: Try-catch apropiados, logs detallados, no fallar silenciosamente
- **Performance**: Pensar en 100GB-10TB de datos, no en 10MB
- **Extensibilidad**: Fácil agregar nuevos conectores y transformaciones
- **Portabilidad**: Debe funcionar en laptop, VM, Docker, cloud

---

## 🎯 CRITERIO DE ÉXITO

El proyecto está listo cuando:

1. ✅ Puedo hacer `ducklake init mi_proyecto`
2. ✅ Puedo configurar una fuente MySQL en `sources.yaml`
3. ✅ Puedo hacer `ducklake extract mysql_ventas`
4. ✅ Los datos llegan a `data/raw/` en Parquet particionado
5. ✅ Puedo configurar un pipeline en `pipelines.yaml`
6. ✅ Puedo hacer `ducklake run ventas_staging`
7. ✅ Los datos transformados llegan a `data/staging/`
8. ✅ Puedo hacer `ducklake catalog` y ver metadata
9. ✅ Los tests pasan
10. ✅ El README tiene ejemplos claros

---

¡Gracias por ayudarme a construir DuckLake! 🦆🚀
