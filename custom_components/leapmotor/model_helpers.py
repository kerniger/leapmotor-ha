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

WINDOW_OPEN_PERCENT = 20
WINDOW_POSITION_SCALE = {
    "B05": 10,
    "B10": 10,
    "C10": 10,
}


class VehicleStatusPathResolver:
    """Remember the working model-specific status path for each VIN."""

    def __init__(self) -> None:
        self._proven_paths: dict[str, str] = {}
        self._fallback_attempted: set[str] = set()

    def path_for(self, vin: str, car_type: object) -> str:
        """Return a proven path, or the best path derived from the model."""
        return self._proven_paths.get(vin, vehicle_status_path(car_type))

    def remember(self, vin: str, path: str) -> None:
        """Remember a path after a successful status response."""
        self._proven_paths[vin] = path

    def should_try_c10_fallback(
        self,
        vin: str,
        current_path: str,
        http_status: object,
    ) -> bool:
        """Allow one C10 fallback per VIN for an unsupported status path."""
        if current_path == "c10" or http_status != 404 or vin in self._fallback_attempted:
            return False
        self._fallback_attempted.add(vin)
        return True


def vehicle_status_path(car_type: object) -> str:
    """Return the backend status path segment for a vehicle model."""
    normalized = str(car_type or "C10").strip().lower()
    if normalized in {"b05", "b10", "b11"}:
        return "c10"
    return normalized or "c10"


def climate_off_payload(car_type: object) -> dict[str, str] | None:
    """Return the full T03 A/C-off body, or None for the standard command."""
    if str(car_type or "").strip().upper() != "T03":
        return None
    return dict(T03_AC_OFF_PAYLOAD)


def native_window_position(car_type: object, position_percent: int) -> int:
    """Convert a percentage to the model's native window command scale."""
    normalized_car_type = str(car_type or "").strip().upper()
    full_open_value = WINDOW_POSITION_SCALE.get(normalized_car_type, 100)
    return round(position_percent / 100.0 * full_open_value)


def native_window_open_position(
    car_type: object,
    position_percent: int | None,
) -> int:
    """Return the native open position, defaulting to a 20 percent vent gap."""
    requested_percent = (
        WINDOW_OPEN_PERCENT if position_percent is None else position_percent
    )
    return native_window_position(car_type, requested_percent)
