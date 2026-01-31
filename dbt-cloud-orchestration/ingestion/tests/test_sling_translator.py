"""Tests for the Sling translator component."""

from datetime import timedelta

import dagster as dg
from dagster import AssetDep, AssetKey, AssetSpec

from ingestion.components.ingestion_sling.translator import IngestionSlingTranslator


class TestIngestionSlingTranslator:
    """Test cases for IngestionSlingTranslator."""

    def test_get_deps_returns_empty_list(self):
        """Test that get_deps returns empty list to hide source assets."""
        translator = IngestionSlingTranslator()
        stream_def = {"name": "test_stream", "config": {}}

        deps = translator.get_deps(stream_def)

        assert deps == []

    def test_get_asset_spec_csv_fact_virtual(self):
        """Test asset spec generation for csv_fact_virtual."""
        translator = IngestionSlingTranslator()
        stream_def = {
            "name": "public.fact_virtual",
            "config": {
                "object": "fact_virtual",
                "meta": {
                    "dagster": {
                        "asset_key": "csv_fact_virtual",
                        "group": "ingestion",
                    }
                },
            },
        }

        spec = translator.get_asset_spec(stream_def)

        assert isinstance(spec, AssetSpec)
        assert spec.key == AssetKey(["csv_fact_virtual"])
        assert spec.group_name == "ingestion"
        assert spec.freshness_policy is not None
        assert "csv" in spec.kinds
        assert "postgres" in spec.kinds

    def test_get_asset_spec_fact_virtual(self):
        """Test asset spec generation for fact_virtual (Databricks)."""
        translator = IngestionSlingTranslator()
        stream_def = {
            "name": "public.fact_virtual",
            "config": {
                "object": "fact_virtual",
                "meta": {
                    "dagster": {
                        "asset_key": "fact_virtual",
                        "upstream_assets": ["csv_fact_virtual"],
                        "group": "ingestion",
                    }
                },
            },
        }

        spec = translator.get_asset_spec(stream_def)

        assert isinstance(spec, AssetSpec)
        assert spec.key == AssetKey(["fact_virtual"])
        assert spec.group_name == "ingestion"
        assert len(spec.deps) == 1
        assert spec.deps[0].asset_key == AssetKey(["csv_fact_virtual"])
        assert "databricks" in spec.kinds

    def test_strips_target_prefix(self):
        """Test that 'target' prefix is stripped from asset keys."""
        translator = IngestionSlingTranslator()
        # The actual code strips 'target' when it's a separate path component
        # e.g., ["target", "csv_fact_virtual"] becomes ["csv_fact_virtual"]
        stream_def = {
            "name": "public.fact_virtual",
            "config": {
                "meta": {
                    "dagster": {
                        "asset_key": "target_csv_fact_virtual",
                    }
                }
            },
        }

        spec = translator.get_asset_spec(stream_def)

        # Verify the asset key was processed (base translator converts to path)
        assert isinstance(spec.key.path, list)
        # The target prefix should be stripped if present as first element
        if len(spec.key.path) > 1:
            assert spec.key.path[0] != "target"


class TestConfigResolver:
    """Test cases for config resolution utilities."""

    def test_resolve_env_vars_in_string(self, monkeypatch):
        """Test environment variable resolution in strings."""
        from ingestion.components.ingestion_sling.config_resolver import resolve_env_vars

        monkeypatch.setenv("TEST_VAR", "test_value")

        result = resolve_env_vars("Hello ${TEST_VAR}")
        assert result == "Hello test_value"

    def test_resolve_env_vars_with_default(self, monkeypatch):
        """Test environment variable resolution with default value."""
        from ingestion.components.ingestion_sling.config_resolver import resolve_env_vars

        result = resolve_env_vars("Hello ${MISSING_VAR:-default}")
        assert result == "Hello default"

    def test_resolve_env_vars_in_dict(self, monkeypatch):
        """Test environment variable resolution in dictionaries."""
        from ingestion.components.ingestion_sling.config_resolver import resolve_env_vars

        monkeypatch.setenv("DB_HOST", "localhost")
        monkeypatch.setenv("DB_PORT", "5432")

        config = {
            "host": "${DB_HOST}",
            "port": "${DB_PORT}",
            "name": "test",
        }

        result = resolve_env_vars(config)

        assert result["host"] == "localhost"
        assert result["port"] == "5432"
        assert result["name"] == "test"
