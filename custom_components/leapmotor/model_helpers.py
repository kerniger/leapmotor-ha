"""Model-specific command behavior confirmed on real vehicles."""

from __future__ import annotations

T03_AC_OFF_PAYLOAD = {
    "circle": "out",
    "mode": "wind",
    "operate": "off",
    "position": "all",
    "temperature": "26",
    "windlevel": "3",
    "wshld": "0",
}


def climate_off_payload(car_type: object) -> dict[str, str] | None:
    """Return the full T03 A/C-off body, or None for the standard command."""
    if str(car_type or "").strip().upper() != "T03":
        return None
    return dict(T03_AC_OFF_PAYLOAD)


def native_window_position(car_type: object, position_percent: int) -> int:
    """Convert a percentage to the model's native window command scale."""
    if str(car_type or "").strip().upper() == "B10":
        return round(position_percent / 10.0)
    return position_percent
