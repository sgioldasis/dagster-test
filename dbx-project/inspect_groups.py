
from dbx_project.definitions import defs
from dagster import AssetKey

print("Inspecting Asset Groups...")
repo = defs.get_repository_def()
for asset_key in repo.asset_graph.get_all_asset_keys():
    asset_node = repo.asset_graph.get(asset_key)
    print(f"Asset: {asset_key.to_string()} | Group: {asset_node.group_name}")
