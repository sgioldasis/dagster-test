
import os
import sys
import unittest.mock as mock
from dagster import AssetKey

# Mock environment variables and modules
with mock.patch("databricks.sdk.WorkspaceClient"):
    with mock.patch("dotenv.load_dotenv"):
        os.environ["DAGSTER_ENV"] = "LOCAL"
        # Add project root to sys.path
        project_root = "/home/savas/dagster-test/dbx-project"
        src_path = os.path.join(project_root, "src")
        if src_path not in sys.path:
            sys.path.insert(0, src_path)
            
        from dbx_project.definitions import defs

def verify():
    target_key = AssetKey(["target", "main", "customers"])
    found = False
    
    print(f"Looking for asset: {target_key}")
    
    for asset_def in (defs.assets or []):
        if hasattr(asset_def, "keys") and target_key in asset_def.keys:
            found = True
            print(f"\nFound asset: {target_key}")
            # Get the spec for this key
            spec = asset_def.specs_by_key[target_key]
            print(f"Group: {spec.group_name}")
            print(f"Legacy Freshness Policy: {spec.legacy_freshness_policy}")
            if spec.legacy_freshness_policy:
                print(f"  - Max Lag Minutes: {spec.legacy_freshness_policy.maximum_lag_minutes}")
                    
    if not found:
        print("\nAsset not found!")

if __name__ == "__main__":
    verify()
