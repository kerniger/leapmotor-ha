"""Shared entity naming helpers for Leapmotor."""

from __future__ import annotations

from typing import Any


def build_vehicle_display_name(vehicle: dict[str, Any]) -> str:
    """Return a stable, user-friendly vehicle device name."""
    nickname = vehicle.get("vehicle_nickname") or vehicle.get("nickname")
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
