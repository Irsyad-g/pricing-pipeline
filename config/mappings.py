from processors.behaviour_factor import (
    get_group_lookup,
    get_group_to_region_group,
)

# group_region.json / group_mapping.json / region_mapping.json were removed.
# All mapping data now lives in data/mappings/country_map.json, read by
# processors/behaviour_factor.py. Import from there instead.
GROUP_MAP    = get_group_lookup()         # pattern → group
GROUP_REGION = get_group_to_region_group()  # group → region_group
REGION_JSON  = GROUP_REGION               # backwards-compat alias
