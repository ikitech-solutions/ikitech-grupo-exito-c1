# convert_layouts_to_toon.py
import json
from toon_format import encode
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[2]
sys.path.append(str(BASE_DIR))


LAYOUTS_JSON = Path(__file__).resolve().parent / "layouts.json"
LAYOUTS_TOON = Path(__file__).resolve().parent / "layouts.toon"

with open(LAYOUTS_JSON, "r", encoding="utf-8") as f:
    data = json.load(f)

toon_str = encode(data)

with open(LAYOUTS_TOON, "w", encoding="utf-8") as f:
    f.write(toon_str)
