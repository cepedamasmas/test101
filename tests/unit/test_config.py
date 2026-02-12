"""Tests para el sistema de configuración."""

import os
from pathlib import Path

import pytest

from ducklake.core.config import (
    DuckLakeConfig,
    SourceConfig,
    PipelineConfig,
    SettingsConfig,
    load_yaml,
    load_config,
    _resolve_env_vars,
)


class TestResolveEnvVars:
    def test_resolve_string(self, monkeypatch):
        monkeypatch.setenv("MY_HOST", "localhost")
        assert _resolve_env_vars("${MY_HOST}") == "localhost"

    def test_resolve_in_dict(self, monkeypatch):
        monkeypatch.setenv("DB_USER", "admin")
        result = _resolve_env_vars({"user": "${DB_USER}", "port": 3306})
        assert result == {"user": "admin", "port": 3306}

    def test_resolve_in_list(self, monkeypatch):
        monkeypatch.setenv("VAL", "hello")
        result = _resolve_env_vars(["${VAL}", "world"])
        assert result == ["hello", "world"]

    def test_missing_env_var(self):
        result = _resolve_env_vars("${NONEXISTENT_VAR_XYZ}")
        assert result == ""


class TestLoadYaml:
    def test_load_valid_yaml(self, tmp_path):
        yaml_file = tmp_path / "test.yaml"
        yaml_file.write_text("key: value\nlist:\n  - a\n  - b\n")
        result = load_yaml(str(yaml_file))
        assert result == {"key": "value", "list": ["a", "b"]}

    def test_load_missing_file(self):
        with pytest.raises(FileNotFoundError):
            load_yaml("/nonexistent/path.yaml")

    def test_load_empty_yaml(self, tmp_path):
        yaml_file = tmp_path / "empty.yaml"
        yaml_file.write_text("")
        result = load_yaml(str(yaml_file))
        assert result == {}


class TestLoadConfig:
    def test_load_full_config(self, config_dir):
        config = load_config(config_dir)
        assert isinstance(config, DuckLakeConfig)
        assert len(config.sources) == 1
        assert config.sources[0].name == "test_csv"
        assert len(config.pipelines) == 1
        assert config.pipelines[0].name == "test_pipeline"

    def test_load_empty_dir(self, tmp_path):
        config = load_config(str(tmp_path))
        assert isinstance(config, DuckLakeConfig)
        assert len(config.sources) == 0
        assert len(config.pipelines) == 0


class TestSourceConfig:
    def test_defaults(self):
        src = SourceConfig(name="test", type="csv")
        assert src.enabled is True
        assert src.extract.mode == "full"
        assert src.tables == []

    def test_full_config(self):
        src = SourceConfig(
            name="mysql_ventas",
            type="mysql",
            enabled=True,
            tables=["clientes", "pedidos"],
            extract={"mode": "incremental", "key_column": "updated_at"},
        )
        assert src.name == "mysql_ventas"
        assert src.extract.mode == "incremental"
        assert src.extract.key_column == "updated_at"


class TestPipelineConfig:
    def test_minimal(self):
        p = PipelineConfig(
            name="test",
            source={"layer": "raw", "domain": "src", "table": "t"},
            destination={"layer": "staging", "domain": "d", "table": "t"},
        )
        assert p.name == "test"
        assert p.transforms == []
        assert p.quality_checks == []
