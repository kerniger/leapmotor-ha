"""VIN-scoped configuration helpers for multi-vehicle accounts."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

CONF_ABRP_ENABLED = "abrp_enabled"
CONF_ABRP_TOKEN = "abrp_token"
CONF_OPERATION_PASSWORD = "operation_password"
CONF_VIN = "vin"
SUBENTRY_TYPE_VEHICLE = "vehicle"


def vehicle_subentries(config_entry: Any) -> dict[str, Any]:
    """Return vehicle subentries indexed by VIN."""
    return {
        str(subentry.data[CONF_VIN]): subentry
        for subentry in config_entry.subentries.values()
        if subentry.subentry_type == SUBENTRY_TYPE_VEHICLE
        and subentry.data.get(CONF_VIN)
    }


def legacy_config_value(config_entry: Any, key: str, default: Any = None) -> Any:
    """Read a pre-0.7 option while old entries are being migrated."""
    if key in config_entry.options:
        return config_entry.options[key]
    return config_entry.data.get(key, default)


def operation_password_for_vehicle(config_entry: Any, vin: str) -> str | None:
    """Resolve the PIN for one VIN, with a legacy fallback."""
    subentry = vehicle_subentries(config_entry).get(vin)
    value = (
        subentry.data.get(CONF_OPERATION_PASSWORD)
        if subentry is not None
        else legacy_config_value(config_entry, CONF_OPERATION_PASSWORD, "")
    )
    value = str(value or "").strip()
    return value or None


def abrp_config_for_vehicle(
    config_entry: Any,
    vin: str,
    *,
    vehicle_count: int,
) -> tuple[bool, str]:
    """Resolve ABRP settings without sending multiple cars to one legacy token."""
    subentry = vehicle_subentries(config_entry).get(vin)
    if subentry is not None:
        data: Mapping[str, Any] = subentry.data
        return bool(data.get(CONF_ABRP_ENABLED)), str(data.get(CONF_ABRP_TOKEN) or "").strip()

    if vehicle_count != 1:
        return False, ""
    return (
        bool(legacy_config_value(config_entry, CONF_ABRP_ENABLED, False)),
        str(legacy_config_value(config_entry, CONF_ABRP_TOKEN, "") or "").strip(),
    )
