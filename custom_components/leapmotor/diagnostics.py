"""Diagnostics support for the Leapmotor integration."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant

from .const import DOMAIN

_REDACTED = "**REDACTED**"
_SENSITIVE_KEYS = {
    "password",
    "username",
    "email",
    "operation_password",
    "operatePassword",
    "account_p12_password",
    "token",
    "refreshToken",
    "refresh_token",
    "base64Cert",
    "device_id",
    "sign",
    "signIkm",
    "signSalt",
    "signInfo",
    "gigyaSessionToken",
    "gigyaSessionSecret",
    "car_picture_url",
    "car_picture_key",
    "car_picture_whole",
    "url",
    "abrp_token",
}
_SENSITIVE_KEY_NAMES = {key.casefold() for key in _SENSITIVE_KEYS}
_EMAIL_PATTERN = re.compile(
    r"(?<![\w.+-])[\w.+-]+@[\w.-]+\.[A-Za-z]{2,}(?![\w.-])"
)


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
            diagnostics = vehicle_data.get("diagnostics") or {}
            vehicles[_redact_vin(vin)] = {
                "vehicle": {
                    "vin": _redact_vin(vin),
                    "car_id": _redact_identifier(vehicle.get("car_id")),
                    "car_type": vehicle.get("car_type"),
                    "nickname": _REDACTED if vehicle.get("nickname") else None,
                    "is_shared": vehicle.get("is_shared"),
                    "year": vehicle.get("year"),
                    "rights": vehicle.get("rights"),
                    "abilities": vehicle.get("abilities"),
                    "module_rights": vehicle.get("module_rights"),
                },
                "status": _redact(status),
                "charging": _redact(vehicle_data.get("charging") or {}),
                "history": history,
                "raw_signals": _raw_signals_from_diagnostics(diagnostics),
                "diagnostics": _redact(diagnostics),
                "location": {
                    "location_source": vehicle_data.get("location", {}).get("location_source"),
                    "location_age_seconds": vehicle_data.get("location", {}).get("location_age_seconds"),
                    "location_is_stale": vehicle_data.get("location", {}).get("location_is_stale"),
                    "privacy_gps": vehicle_data.get("location", {}).get("privacy_gps"),
                    "privacy_data": vehicle_data.get("location", {}).get("privacy_data"),
                },
                "media": _redact(media),
                "remote_control": _redact(remote),
                "abrp": _redact(abrp),
            }

    client = getattr(coordinator, "client", None) if coordinator else None
    static_cert = getattr(client, "static_cert", None)
    static_key = getattr(client, "static_key", None)
    integration_status = coordinator.integration_status if coordinator else None
    return {
        "support_summary": _support_summary(
            vehicles=vehicles,
            integration_status=integration_status,
            client=client,
        ),
        "entry": _redact(
            {
                "title": entry.title,
                "data": dict(entry.data),
                "options": dict(entry.options),
            }
        ),
        "client": {
            "user_id": _redact_identifier(getattr(client, "user_id", None)),
            "app_cert_present": Path(static_cert).exists() if static_cert else None,
            "app_key_present": Path(static_key).exists() if static_key else None,
            "account_cert_loaded": bool(getattr(client, "account_cert_file", None)),
            "account_p12_password_source": getattr(client, "account_p12_password_source", None),
            "operation_password_configured": bool(getattr(client, "operation_password", None)),
            "language": getattr(client, "language", None),
            "last_api_results": _redact(getattr(client, "last_api_results", {})),
            "integration_status": integration_status,
        },
        "vehicles": vehicles,
    }


def _support_summary(
    *,
    vehicles: dict[str, Any],
    integration_status: dict[str, Any] | None,
    client: Any,
) -> dict[str, Any]:
    """Return a compact, safe summary suitable for support issue comments."""
    return {
        "vehicle_count": len(vehicles),
        "vehicles": [
            {
                "vin": vin,
                "car_type": (vehicle_data.get("vehicle") or {}).get("car_type"),
                "is_shared": (vehicle_data.get("vehicle") or {}).get("is_shared"),
                "status_endpoint_path": (
                    vehicle_data.get("diagnostics") or {}
                ).get("status_endpoint_path"),
                "status_signal_count": (
                    vehicle_data.get("diagnostics") or {}
                ).get("status_signal_count"),
                "charging_connection": (
                    vehicle_data.get("charging") or {}
                ).get("connection_state"),
                "vehicle_state": (vehicle_data.get("status") or {}).get("vehicle_state"),
            }
            for vin, vehicle_data in vehicles.items()
        ],
        "integration": integration_status or {},
        "last_api_result_labels": sorted(
            (getattr(client, "last_api_results", {}) or {}).keys()
        )
        if client
        else [],
    }


def _redact(value: Any) -> Any:
    """Recursively redact secrets from diagnostics."""
    if isinstance(value, dict):
        return {
            key: _REDACTED
            if str(key).casefold() in _SENSITIVE_KEY_NAMES
            else _redact(item)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [_redact(item) for item in value]
    if isinstance(value, str):
        return _EMAIL_PATTERN.sub(_REDACTED, value)
    return value


def _redact_vin(vin: str) -> str:
    """Redact VIN while keeping enough suffix for support correlation."""
    if not vin:
        return ""
    return f"***{str(vin)[-6:]}"


def _redact_identifier(value: Any) -> str | None:
    """Redact an identifier while keeping a short suffix for support correlation."""
    if value in (None, ""):
        return None
    text = str(value)
    return f"***{text[-6:]}"


def _raw_signals_from_diagnostics(diagnostics: dict[str, Any]) -> dict[str, Any]:
    """Return raw APK signal values keyed by signal id."""
    signals: dict[str, Any] = {}
    for key, value in diagnostics.items():
        if key.startswith("raw_signal_"):
            signals[key.removeprefix("raw_signal_")] = value
    return dict(
        sorted(
            signals.items(),
            key=lambda item: int(item[0]) if item[0].isdigit() else item[0],
        )
    )
