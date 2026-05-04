"""Diagnostics support for the Leapmotor integration."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant

from .const import DOMAIN

_REDACTED = "**REDACTED**"
_SENSITIVE_KEYS = {
    "password",
    "operation_password",
    "account_p12_password",
    "token",
    "refreshToken",
    "refresh_token",
    "base64Cert",
    "sign",
    "signIkm",
    "signSalt",
    "signInfo",
    "gigyaSessionToken",
    "gigyaSessionSecret",
    "car_picture_url",
    "url",
    "abrp_token",
}


async def async_get_config_entry_diagnostics(
    hass: HomeAssistant,
    entry: ConfigEntry,
) -> dict[str, Any]:
    """Return sanitized diagnostics for support."""
    coordinator = hass.data.get(DOMAIN, {}).get(entry.entry_id)
    vehicles = {}
    if coordinator and coordinator.data:
        for vin, vehicle_data in (coordinator.data.get("vehicles") or {}).items():
            vehicle = vehicle_data.get("vehicle") or {}
            status = vehicle_data.get("status") or {}
            history = vehicle_data.get("history") or {}
            media = vehicle_data.get("media") or {}
            remote = vehicle_data.get("remote_control") or {}
            abrp = vehicle_data.get("abrp") or {}
            vehicles[_redact_vin(vin)] = {
                "vehicle": {
                    "vin": _redact_vin(vin),
                    "car_id": vehicle.get("car_id"),
                    "car_type": vehicle.get("car_type"),
                    "nickname": vehicle.get("vehicle_nickname") or vehicle.get("nickname"),
                    "is_shared": vehicle.get("is_shared"),
                    "year": vehicle.get("year"),
                    "rights": vehicle.get("rights"),
                    "abilities": vehicle.get("abilities"),
                    "module_rights": vehicle.get("module_rights"),
                },
                "status": {
                    "vehicle_state": status.get("vehicle_state"),
                    "vehicle_state_source": status.get("vehicle_state_source"),
                    "vehicle_state_age_seconds": status.get("vehicle_state_age_seconds"),
                    "vehicle_state_is_stale": status.get("vehicle_state_is_stale"),
                    "is_parked": status.get("is_parked"),
                    "is_locked": status.get("is_locked"),
                    "lock_state_source": status.get("lock_state_source"),
                    "lock_state_age_seconds": status.get("lock_state_age_seconds"),
                    "lock_state_is_stale": status.get("lock_state_is_stale"),
                    "raw_lock_status_code": status.get("raw_lock_status_code"),
                    "raw_charge_status_code": status.get("raw_charge_status_code"),
                    "raw_charge_connection_code": status.get("raw_charge_connection_code"),
                    "raw_drive_status_code": status.get("raw_drive_status_code"),
                    "raw_vehicle_state_code": status.get("raw_vehicle_state_code"),
                    "raw_parked_status_code": status.get("raw_parked_status_code"),
                    "last_vehicle_timestamp": status.get("last_vehicle_timestamp"),
                },
                "history": history,
                "diagnostics": _redact(vehicle_data.get("diagnostics") or {}),
                "location": {
                    "location_source": vehicle_data.get("location", {}).get("location_source"),
                    "location_age_seconds": vehicle_data.get("location", {}).get("location_age_seconds"),
                    "location_is_stale": vehicle_data.get("location", {}).get("location_is_stale"),
                    "privacy_gps": vehicle_data.get("location", {}).get("privacy_gps"),
                    "privacy_data": vehicle_data.get("location", {}).get("privacy_data"),
                },
                "media": _redact(media),
                "remote_control": remote,
                "abrp": _redact(abrp),
            }

    client = getattr(coordinator, "client", None) if coordinator else None
    static_cert = getattr(client, "static_cert", None)
    static_key = getattr(client, "static_key", None)
    return {
        "entry": _redact(
            {
                "title": entry.title,
                "data": dict(entry.data),
                "options": dict(entry.options),
            }
        ),
        "client": {
            "user_id": getattr(client, "user_id", None),
            "app_cert_present": Path(static_cert).exists() if static_cert else None,
            "app_key_present": Path(static_key).exists() if static_key else None,
            "account_cert_loaded": bool(getattr(client, "account_cert_file", None)),
            "account_p12_password_source": getattr(client, "account_p12_password_source", None),
            "operation_password_configured": bool(getattr(client, "operation_password", None)),
            "last_api_results": getattr(client, "last_api_results", {}),
            "integration_status": coordinator.integration_status if coordinator else None,
        },
        "vehicles": vehicles,
    }


def _redact(value: Any) -> Any:
    """Recursively redact secrets from diagnostics."""
    if isinstance(value, dict):
        return {
            key: _REDACTED if str(key) in _SENSITIVE_KEYS else _redact(item)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [_redact(item) for item in value]
    return value


def _redact_vin(vin: str) -> str:
    """Redact VIN while keeping enough suffix for support correlation."""
    if not vin:
        return ""
    return f"***{str(vin)[-6:]}"
