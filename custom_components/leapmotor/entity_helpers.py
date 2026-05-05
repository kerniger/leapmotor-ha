"""Shared entity naming helpers for Leapmotor."""

from __future__ import annotations

from functools import lru_cache
import json
from pathlib import Path
from typing import Any


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


def load_localized_entity_names(language: str | None, domain: str) -> dict[str, str]:
    """Return all localized entity names for one domain with English fallback."""
    names: dict[str, str] = {}
    for candidate in reversed(_language_candidates(language)):
        translations = _translation_data(candidate).get("entity", {}).get(domain, {})
        if isinstance(translations, dict):
            for key, value in translations.items():
                if isinstance(value, dict) and isinstance(value.get("name"), str):
                    names[key] = value["name"]
    return names


def _language_candidates(language: str | None) -> tuple[str, ...]:
    """Return normalized language candidates ending with English fallback."""
    candidates: list[str] = []
    if language:
        normalized = language.replace("-", "_").lower()
        candidates.append(normalized)
        base = normalized.split("_", 1)[0]
        if base != normalized:
            candidates.append(base)
    candidates.append("en")
    return tuple(dict.fromkeys(candidates))


@lru_cache(maxsize=16)
def _translation_data(language: str) -> dict[str, Any]:
    """Load one bundled translation file."""
    path = Path(__file__).with_name("translations") / f"{language}.json"
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
