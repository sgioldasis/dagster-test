"""Tests for PackageAwareDbtTranslator."""

import sys
from pathlib import Path

# Add src to path for direct imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import pytest
from dagster import AssetKey
from dbt_orchestration.defs.assets import PackageAwareDbtTranslator


class TestPackageAwareDbtTranslator:
    """Test cases for PackageAwareDbtTranslator."""

    def test_get_asset_key_with_package(self):
        """Test that package name is prefixed to asset key."""
        translator = PackageAwareDbtTranslator()
        dbt_resource_props = {
            "package_name": "dbt_optimove",
            "name": "bonus_cost",
            "config": {"schema": "analytics"},
            "resource_type": "model",
        }
        
        asset_key = translator.get_asset_key(dbt_resource_props)
        
        # Package prefix + schema + model name
        assert asset_key == AssetKey(["dbt_optimove", "analytics", "bonus_cost"])

    def test_get_asset_key_without_package(self):
        """Test that asset key works without package name."""
        translator = PackageAwareDbtTranslator()
        dbt_resource_props = {
            "name": "bonus_cost",
            "config": {"schema": "analytics"},
            "resource_type": "model",
        }
        
        asset_key = translator.get_asset_key(dbt_resource_props)
        
        # Schema + model name (no package prefix)
        assert asset_key == AssetKey(["analytics", "bonus_cost"])

    def test_get_asset_key_avoids_collision(self):
        """Test that same model names in different packages get different keys."""
        translator = PackageAwareDbtTranslator()
        
        optimove_props = {
            "package_name": "dbt_optimove",
            "name": "bonus_cost",
            "config": {"schema": "analytics"},
            "resource_type": "model",
        }
        rewards_props = {
            "package_name": "dbt_rewards",
            "name": "bonus_cost",
            "config": {"schema": "analytics"},
            "resource_type": "model",
        }
        
        optimove_key = translator.get_asset_key(optimove_props)
        rewards_key = translator.get_asset_key(rewards_props)
        
        # Different packages produce different keys
        assert optimove_key != rewards_key
        assert optimove_key == AssetKey(["dbt_optimove", "analytics", "bonus_cost"])
        assert rewards_key == AssetKey(["dbt_rewards", "analytics", "bonus_cost"])

    def test_get_asset_key_for_source(self):
        """Test asset key generation for dbt sources."""
        translator = PackageAwareDbtTranslator()
        dbt_resource_props = {
            "package_name": "dbt_optimove",
            "name": "raw_customers",
            "resource_type": "source",
            "source_name": "raw",
        }
        
        asset_key = translator.get_asset_key(dbt_resource_props)
        
        # Sources use package + source_name + name
        assert asset_key == AssetKey(["dbt_optimove", "raw", "raw_customers"])
