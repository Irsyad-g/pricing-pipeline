import json
from .paths import MAPPING_DATA


with open(MAPPING_DATA / "group_region.json", encoding="utf-8") as f:
    GROUP_MAP = json.load(f)


with open(MAPPING_DATA / "region_mapping.json", encoding="utf-8") as f:
    REGION_JSON = json.load(f)


with open(MAPPING_DATA / "group_mapping.json", encoding="utf-8") as f:
    GROUP_REGION = json.load(f)