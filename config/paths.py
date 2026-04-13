from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


DATA_DIR = PROJECT_ROOT / "data"


RAW_DATA = DATA_DIR / "raw"
MAPPING_DATA = DATA_DIR / "mappings"
OUTPUT_DATA = DATA_DIR / "output"


OUTPUT_FILE = OUTPUT_DATA / "FINAL_USAGE_REPORT.xlsx"