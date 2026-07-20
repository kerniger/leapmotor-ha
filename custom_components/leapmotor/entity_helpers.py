"""Shared entity naming helpers for Leapmotor."""

from __future__ import annotations

from typing import Any


_MODEL_UNSUPPORTED_FEATURES: dict[str, frozenset[str]] = {
    "T03": frozenset(
        {
            "driver_seat_heating",
            "passenger_seat_heating",
            "driver_seat_ventilation",
            "passenger_seat_ventilation",
            "steering_wheel_heating",
            "steering_wheel_heating_remaining_minutes",
        }
    ),
}


def build_vehicle_display_name(vehicle: dict[str, Any]) -> str:
    """Return a stable, user-friendly vehicle device name."""
    nickname = vehicle.get("nickname")
    car_type = vehicle.get("car_type") or "Vehicle"
    year = vehicle.get("year")
    is_shared = vehicle.get("is_shared")
    vin = vehicle.get("vin") or ""
    vin_suffix = str(vin)[-6:] if vin else ""
    role = "Shared" if is_shared else "Main"

    base = f"Leapmotor {car_type}"
    if year:
        base = f"{base} {year}"

    if nickname:
        return f"{base} {nickname} ({role})"
    if vin_suffix:
        return f"{base} {vin_suffix} ({role})"
    return f"{base} ({role})"


def vehicle_ability_supported(vehicle: dict[str, Any], ability: int) -> bool | None:
    """Return declared ability support, or None when declarations are unavailable."""
    raw_abilities = vehicle.get("abilities")
    if not isinstance(raw_abilities, (list, tuple, set)) or not raw_abilities:
        return None
    abilities: set[int] = set()
    try:
        abilities.update(int(value) for value in raw_abilities)
    except (TypeError, ValueError):
        return None
    return ability in abilities


def vehicle_feature_supported(vehicle: dict[str, Any], feature: str) -> bool:
    """Return whether a feature is physically available on the vehicle model."""
    car_type = str(vehicle.get("car_type") or "").strip().upper()
    return feature not in _MODEL_UNSUPPORTED_FEATURES.get(car_type, frozenset())
