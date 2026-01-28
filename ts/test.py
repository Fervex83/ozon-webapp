from __future__ import annotations

import json
from pathlib import Path

TS_CONFIG = {
    "id": "test",
    "name": "Test",
    "marketplace": "ozon",
    "presets_path": str(Path(__file__).resolve().parent.parent / "data" / "presets" / "test.json"),
}


def load_presets() -> dict:
    path = Path(TS_CONFIG["presets_path"])
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)
