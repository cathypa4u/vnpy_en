import json
from pathlib import Path
from typing import Any, cast

from ..utility import TEMP_DIR


SETTING_FILENAME: str = "ui_setting.json"
SETTING_FILEPATH: Path = TEMP_DIR.joinpath(SETTING_FILENAME)


def _load_settings() -> dict[str, Any]:
    """Load all settings"""
    if not SETTING_FILEPATH.exists():
        return {}

    try:
        with open(SETTING_FILEPATH, encoding="utf-8") as f:
            return cast(dict[str, Any], json.load(f))
    except (json.JSONDecodeError, OSError):
        return {}


def _save_settings(data: dict[str, Any]) -> None:
    """Save all settings"""
    with open(SETTING_FILEPATH, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)


def get_setting(key: str, default: Any = None) -> Any:
    """获取配置项

    Args:
        key: 配置键名
        default: 默认值

    Returns:
        配置值或默认值
    """
    settings = _load_settings()
    return settings.get(key, default)


def set_setting(key: str, value: Any) -> None:
    """设置配置项

    Args:
        key: 配置键名
        value: 配置值
    """
    settings = _load_settings()
    settings[key] = value
    _save_settings(settings)


def load_favorite_models() -> list[str]:
    """Load commonly used models"""
    return cast(list[str], get_setting("favorite_models", []))


def save_favorite_models(models: list[str]) -> None:
    """Save commonly used models"""
    set_setting("favorite_models", models)


def load_zoom_factor() -> float:
    """Loading page zoom factor"""
    return cast(float, get_setting("zoom_factor", 1.0))


def save_zoom_factor(zoom_factor: float) -> None:
    """Save page zoom factor"""
    set_setting("zoom_factor", zoom_factor)


def load_font_family() -> str:
    """Load font name"""
    return cast(str, get_setting("font_family", "Microsoft Yahei"))


def save_font_family(font_family: str) -> None:
    """Save font name"""
    set_setting("font_family", font_family)


def load_font_size() -> int:
    """Load font size"""
    return cast(int, get_setting("font_size", 16))


def save_font_size(font_size: int) -> None:
    """Save font size"""
    set_setting("font_size", font_size)


def load_gateway_type() -> str:
    """Load the currently selected gateway type"""
    return cast(str, get_setting("gateway_type", "OpenAI"))


def save_gateway_type(gateway_type: str) -> None:
    """Save the currently selected gateway type"""
    set_setting("gateway_type", gateway_type)
