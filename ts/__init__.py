from __future__ import annotations

from typing import Any

from .ozon_tecno import TS_CONFIG as OZON_TECNO_CONFIG, load_presets as load_ozon_tecno_presets
from .test import TS_CONFIG as TEST_CONFIG, load_presets as load_test_presets
from .test_wb import TS_CONFIG as TEST_WB_CONFIG, load_presets as load_test_wb_presets
from .test_ym import TS_CONFIG as TEST_YM_CONFIG, load_presets as load_test_ym_presets

TS_REGISTRY: dict[str, dict[str, Any]] = {
    OZON_TECNO_CONFIG["id"]: {**OZON_TECNO_CONFIG, "load_presets": load_ozon_tecno_presets},
    TEST_CONFIG["id"]: {**TEST_CONFIG, "load_presets": load_test_presets},
    TEST_WB_CONFIG["id"]: {**TEST_WB_CONFIG, "load_presets": load_test_wb_presets},
    TEST_YM_CONFIG["id"]: {**TEST_YM_CONFIG, "load_presets": load_test_ym_presets},
}


def list_ts_configs() -> list[dict[str, Any]]:
    configs = []
    for item in TS_REGISTRY.values():
        payload = dict(item)
        payload.pop("load_presets", None)
        configs.append(payload)
    return configs


def get_ts_config(ts_id: str) -> dict[str, Any] | None:
    return TS_REGISTRY.get(ts_id)


def load_ts_presets(ts_id: str) -> dict[str, Any]:
    config = get_ts_config(ts_id)
    if not config:
        return {}
    loader = config.get("load_presets")
    if not loader:
        return {}
    return loader()
