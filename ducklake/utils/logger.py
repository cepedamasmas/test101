"""Logging setup con loguru."""

import sys
from pathlib import Path

from loguru import logger


def setup_logger(
    level: str = "INFO",
    log_file: str | None = None,
    rotation: str = "10 MB",
    retention: str = "30 days",
) -> None:
    """Configurar loguru para el proyecto.

    Args:
        level: Nivel de logging (DEBUG, INFO, WARNING, ERROR).
        log_file: Path opcional para log en archivo.
        rotation: Tamaño máximo antes de rotar.
        retention: Tiempo de retención de logs.
    """
    # Limpiar handlers existentes
    logger.remove()

    # Console handler con formato legible
    logger.add(
        sys.stderr,
        level=level,
        format=(
            "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
            "<level>{level: <8}</level> | "
            "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | "
            "<level>{message}</level>"
        ),
        colorize=True,
    )

    # File handler opcional
    if log_file:
        Path(log_file).parent.mkdir(parents=True, exist_ok=True)
        logger.add(
            log_file,
            level=level,
            format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} | {message}",
            rotation=rotation,
            retention=retention,
            compression="gz",
        )
