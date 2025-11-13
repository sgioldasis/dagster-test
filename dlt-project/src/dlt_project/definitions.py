from pathlib import Path
from dagster import Definitions, definitions, load_from_defs_folder, AutomationCondition


@definitions  
def defs():
    base_defs = load_from_defs_folder(path_within_project=Path(__file__).parent)
    
    # Apply eager automation to transformation group assets
    updated_assets = []
    for asset_def in base_defs.assets or []:
        # Check if this asset has any keys in the transformation group
        has_transformation = any(
            group == "transformation" 
            for group in asset_def.group_names_by_key.values()
        )
        
        if has_transformation:
            # Recreate the asset with automation condition
            updated_assets.append(
                asset_def.map_asset_specs(
                    lambda spec: spec._replace(
                        automation_condition=AutomationCondition.eager()
                    )
                )
            )
        else:
            updated_assets.append(asset_def)
    
    return Definitions(
        assets=updated_assets,
        resources=base_defs.resources,
        schedules=base_defs.schedules,
        sensors=base_defs.sensors,
        jobs=base_defs.jobs,
    )
