"""Helpers for validating mileage and energy history returned by the cloud."""

from __future__ import annotations

import math
from datetime import datetime, timedelta
from typing import Any


def seven_day_window_ms(now: datetime) -> tuple[int, int]:
    """Return today and the six preceding local calendar days, inclusive."""
    today = now.replace(hour=0, minute=0, second=0, microsecond=0)
    start = today - timedelta(days=6)
    end = today + timedelta(days=1) - timedelta(seconds=1)
    return int(start.timestamp() * 1000), int(end.timestamp() * 1000)


def normalize_weekly_consumption(rows: object) -> list[dict[str, Any]]:
    """Coerce cloud consumption rates while preserving other returned fields."""
    if not isinstance(rows, list):
        return []
    return [
        {
            **row,
            "hundredKmEC": _finite_float(row.get("hundredKmEC")),
            "hundredMiKwhEC": _finite_float(row.get("hundredMiKwhEC")),
        }
        for row in rows
        if isinstance(row, dict)
    ]


def summarize_mileage_energy_detail(
    detail: object,
    period_mileage_km: object,
    *,
    car_type: object = None,
) -> dict[str, Any]:
    """Normalize daily history and return energy only when coverage is complete."""
    # T03 samples (#67) contradict the kWh contract. Preserve source values,
    # but do not guess a Wh conversion from their magnitude.
    energy_unit = None if str(car_type or "").strip().upper() == "T03" else "kWh"
    expected_mileage = _finite_float(period_mileage_km)
    source_rows = detail if isinstance(detail, list) else []
    daily_detail: list[dict[str, Any]] = []
    rows_valid = isinstance(detail, list) and bool(source_rows)

    detail_mileage = 0.0
    covered_mileage = 0.0
    energy_total = 0.0

    for item in source_rows:
        if not isinstance(item, dict):
            rows_valid = False
            continue

        mileage_km = _finite_float(item.get("accumulatedMileage"))
        energy_raw = _finite_float(item.get("accumulatedEnergyConsume"))
        energy_kwh = energy_raw if energy_unit else None
        if mileage_km is None or energy_raw is None:
            rows_valid = False

        if mileage_km is not None:
            detail_mileage += mileage_km
        if mileage_km is not None and energy_raw is not None:
            covered_mileage += mileage_km
        if energy_kwh is not None:
            energy_total += energy_kwh

        daily_detail.append(
            {
                "date": item.get("day"),
                "timestamp": item.get("xDay"),
                "odometer_km": _finite_float(item.get("currentMileage")),
                "mileage_km": mileage_km,
                "mileage_mi": _finite_float(item.get("accumulatedMileageMile")),
                # Working interpretation from aligned B10 week data (#67).
                # Preserve cloud precision; truncation is not established.
                "driving_energy_kwh": energy_kwh,
                "energy_raw": energy_raw,
                "energy_unit": energy_unit,
                "energy_kwh": energy_kwh,  # Legacy compatibility alias.
            }
        )

    mileage_complete = (
        expected_mileage is not None
        and math.isclose(detail_mileage, expected_mileage, abs_tol=0.1)
    )
    energy_complete = rows_valid and mileage_complete

    return {
        "daily_detail": daily_detail,
        "detail_days": len(daily_detail),
        "detail_mileage_km": round(detail_mileage, 1) if daily_detail else None,
        "covered_mileage_km": round(covered_mileage, 1) if daily_detail else None,
        "period_mileage_km": expected_mileage,
        "energy_complete": energy_complete,
        "energy_unit": energy_unit,
        "energy_unavailable_reason": (
            "unverified_unit" if energy_unit is None
            else "incomplete_data" if not energy_complete else None
        ),
        "energy_kwh": round(energy_total, 1) if energy_complete and energy_unit else None,
    }


def _finite_float(value: object) -> float | None:
    """Return a finite float for an API value."""
    if value is None or isinstance(value, bool):
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None
