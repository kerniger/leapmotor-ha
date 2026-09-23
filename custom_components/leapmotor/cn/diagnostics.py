"""Allowlisted diagnostics, deliberately excluding all account/vehicle data."""
from .const import DOMAIN_CN


async def async_get_config_entry_diagnostics(hass, entry):
    coordinator = hass.data.get(DOMAIN_CN, {}).get(entry.entry_id)
    return {
        "integration": DOMAIN_CN,
        "read_only": True,
        "last_update_success": bool(coordinator and coordinator.last_update_success),
        "vehicle_count": len((coordinator.data or {}).get("vehicles", {})) if coordinator else 0,
    }
