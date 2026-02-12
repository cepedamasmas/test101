"""Sistema de configuración con YAML + Pydantic."""

import os
import re
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional

import yaml
from loguru import logger
from pydantic import BaseModel, Field, field_validator


# ---------------------------------------------------------------------------
# Pydantic models para validar configs
# ---------------------------------------------------------------------------

class ConnectionConfig(BaseModel):
    """Configuración de conexión a una fuente."""
    host: str = "localhost"
    port: int = 3306
    database: str = ""
    user: str = ""
    password: str = ""


class ExtractConfig(BaseModel):
    """Configuración de extracción."""
    mode: Literal["full", "incremental"] = "full"
    key_column: str | None = None
    batch_size: int = 10_000
    schedule: str | None = None


class SourceConfig(BaseModel):
    """Configuración de una fuente de datos."""
    name: str
    type: str
    enabled: bool = True
    connection: ConnectionConfig | Dict[str, Any] = Field(default_factory=dict)
    tables: List[str] = Field(default_factory=list)
    path: str | None = None  # Para CSV/SFTP
    extract: ExtractConfig = Field(default_factory=ExtractConfig)


class TransformConfig(BaseModel):
    """Configuración de una transformación."""
    type: str
    columns: Dict[str, str] | None = None
    condition: str | None = None
    keys: List[str] | None = None
    sql: str | None = None


class QualityCheckConfig(BaseModel):
    """Configuración de un quality check."""
    type: str
    columns: List[str] | None = None
    column: str | None = None
    values: List[Any] | None = None
    min_value: float | None = None
    max_value: float | None = None


class LayerRef(BaseModel):
    """Referencia a una capa y tabla."""
    layer: str
    domain: str = ""
    table: str = ""


class PipelineConfig(BaseModel):
    """Configuración de un pipeline."""
    name: str
    description: str = ""
    source: LayerRef
    destination: LayerRef
    transforms: List[TransformConfig] = Field(default_factory=list)
    quality_checks: List[QualityCheckConfig] = Field(default_factory=list)


class SettingsConfig(BaseModel):
    """Configuración general de DuckLake."""
    data_path: str = "./data"
    config_path: str = "./config"
    log_level: str = "INFO"
    log_file: str | None = None
    duckdb_memory_limit: str = "4GB"
    duckdb_threads: int = 4


class DuckLakeConfig(BaseModel):
    """Configuración completa del proyecto."""
    sources: List[SourceConfig] = Field(default_factory=list)
    pipelines: List[PipelineConfig] = Field(default_factory=list)
    settings: SettingsConfig = Field(default_factory=SettingsConfig)


# ---------------------------------------------------------------------------
# Loader
# ---------------------------------------------------------------------------

_ENV_VAR_PATTERN = re.compile(r"\$\{(\w+)\}")


def _resolve_env_vars(value: Any) -> Any:
    """Resolver variables de entorno ${VAR} en strings."""
    if isinstance(value, str):
        def replacer(match: re.Match) -> str:
            var_name = match.group(1)
            env_val = os.environ.get(var_name, "")
            if not env_val:
                logger.warning(f"Variable de entorno no encontrada: {var_name}")
            return env_val
        return _ENV_VAR_PATTERN.sub(replacer, value)
    elif isinstance(value, dict):
        return {k: _resolve_env_vars(v) for k, v in value.items()}
    elif isinstance(value, list):
        return [_resolve_env_vars(item) for item in value]
    return value


def load_yaml(path: str) -> Dict[str, Any]:
    """Cargar y parsear un archivo YAML con resolución de env vars.

    Args:
        path: Path al archivo YAML.

    Returns:
        Dict con el contenido del YAML.

    Raises:
        FileNotFoundError: Si el archivo no existe.
    """
    file_path = Path(path)
    if not file_path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")

    with open(file_path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}

    return _resolve_env_vars(raw)


def load_config(config_path: str = "./config") -> DuckLakeConfig:
    """Cargar configuración completa del proyecto.

    Carga sources.yaml, pipelines.yaml y settings.yaml desde el directorio
    de configuración y los valida con Pydantic.

    Args:
        config_path: Path al directorio de configuración.

    Returns:
        DuckLakeConfig validado.
    """
    config_dir = Path(config_path)
    data: Dict[str, Any] = {}

    # Cargar cada archivo si existe
    for filename, key in [
        ("sources.yaml", "sources"),
        ("pipelines.yaml", "pipelines"),
        ("settings.yaml", "settings"),
    ]:
        filepath = config_dir / filename
        if filepath.exists():
            content = load_yaml(str(filepath))
            if key in content:
                data[key] = content[key]
            elif content:
                data[key] = content
        else:
            logger.debug(f"Config file not found (optional): {filepath}")

    config = DuckLakeConfig(**data)
    logger.info(
        f"Config loaded: {len(config.sources)} sources, {len(config.pipelines)} pipelines"
    )
    return config
