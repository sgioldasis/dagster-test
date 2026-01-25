import sys
import os
# Add the project root to path
sys.path.append(os.getcwd())
from ingestion.definitions import ingestion_defs

try:
    defs = ingestion_defs()
    print("Definitions instance created successfully")
    print(f"Resources: {list(defs.resources.keys())}")
    
    # Check the asset
    for asset in defs.assets:
        if hasattr(asset, 'op'):
            print(f"Asset Op: {asset.op.name}")
            print(f"Required resources for op: {asset.op.required_resource_keys}")
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()
