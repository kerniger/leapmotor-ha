"""Home Assistant integration for Leapmotor vehicle data."""

from __future__ import annotations

from datetime import timedelta
from functools import partial
import logging

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import CONF_PASSWORD, CONF_USERNAME, Platform
from homeassistant.core import HomeAssistant, ServiceCall
from homeassistant.exceptions import HomeAssistantError
import voluptuous as vol

from .api import LeapmotorApiClient
from .button import BUTTON_SPECS
from .const import (
    CONF_ACCOUNT_P12_PASSWORD,
    CONF_OPERATION_PASSWORD,
    CONF_SCAN_INTERVAL,
    DEFAULT_SCAN_INTERVAL_MINUTES,
    DOMAIN,
    STATIC_CERT_STORAGE_DIR,
)
from .coordinator import LeapmotorDataUpdateCoordinator
from .lock import LOCK_ACTION, UNLOCK_ACTION
from .remote_helpers import async_execute_remote_action, format_remote_error, resolve_target_vin

_LOGGER = logging.getLogger(__name__)

SERVICE_FIELDS = vol.Schema(
    {
        vol.Optional("vin"): str,
        vol.Optional("entity_id"): str,
    }
)

WINDOW_POSITION_SERVICE_FIELDS = vol.Schema(
    {
        vol.Optional("value"): vol.All(vol.Coerce(int), vol.Range(min=0, max=100)),
        vol.Optional("vin"): str,
        vol.Optional("entity_id"): str,
    }
)

SUNSHADE_POSITION_SERVICE_FIELDS = vol.Schema(
    {
        vol.Optional("value"): vol.All(vol.Coerce(int), vol.Range(min=0, max=10)),
        vol.Optional("vin"): str,
        vol.Optional("entity_id"): str,
    }
)

_SERVICE_SCHEMAS = {
    "windows_open": WINDOW_POSITION_SERVICE_FIELDS,
    "windows_close": WINDOW_POSITION_SERVICE_FIELDS,
    "sunshade_open": SUNSHADE_POSITION_SERVICE_FIELDS,
    "sunshade_close": SUNSHADE_POSITION_SERVICE_FIELDS,
}

SET_CHARGE_LIMIT_FIELDS = vol.Schema(
    {
        vol.Required("charge_limit_percent"): vol.All(vol.Coerce(int), vol.Range(min=1, max=100)),
        vol.Optional("vin"): str,
        vol.Optional("entity_id"): str,
    }
)

SEND_DESTINATION_FIELDS = vol.Schema(
    {
        vol.Required("name"): vol.All(str, vol.Length(min=1)),
        vol.Required("latitude"): vol.All(vol.Coerce(float), vol.Range(min=-90, max=90)),
        vol.Required("longitude"): vol.All(vol.Coerce(float), vol.Range(min=-180, max=180)),
        vol.Optional("address"): vol.All(str, vol.Length(min=1)),
        vol.Optional("vin"): str,
        vol.Optional("entity_id"): str,
    }
)

PLATFORMS: list[Platform] = [
    Platform.BINARY_SENSOR,
    Platform.BUTTON,
    Platform.DEVICE_TRACKER,
    Platform.IMAGE,
    Platform.LOCK,
    Platform.NUMBER,
    Platform.SENSOR,
]


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up Leapmotor from a config entry."""
    scan_interval = int(
        entry.options.get(
            CONF_SCAN_INTERVAL,
            entry.data.get(CONF_SCAN_INTERVAL, DEFAULT_SCAN_INTERVAL_MINUTES),
        )
    )
    operation_password = (
        entry.options[CONF_OPERATION_PASSWORD]
        if CONF_OPERATION_PASSWORD in entry.options
        else entry.data.get(CONF_OPERATION_PASSWORD)
    )
    client = LeapmotorApiClient(
        username=entry.data[CONF_USERNAME],
        password=entry.data[CONF_PASSWORD],
        account_p12_password=entry.data.get(CONF_ACCOUNT_P12_PASSWORD),
        operation_password=operation_password or None,
        static_cert_dir=hass.config.path(STATIC_CERT_STORAGE_DIR),
    )
    coordinator = LeapmotorDataUpdateCoordinator(
        hass=hass,
        config_entry=entry,
        client=client,
        update_interval=timedelta(minutes=scan_interval),
    )
    await coordinator.async_config_entry_first_refresh()

    hass.data.setdefault(DOMAIN, {})[entry.entry_id] = coordinator
    if not hass.services.has_service(DOMAIN, "lock"):
        await _async_register_services(hass)
    entry.async_on_unload(entry.add_update_listener(async_reload_entry))
    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload a config entry."""
    unload_ok = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
    if unload_ok:
        coordinator = hass.data[DOMAIN].pop(entry.entry_id)
        await hass.async_add_executor_job(coordinator.client.close)
        if not hass.data.get(DOMAIN):
            _async_unregister_services(hass)
    return unload_ok


async def async_reload_entry(hass: HomeAssistant, entry: ConfigEntry) -> None:
    """Reload the integration when options change."""
    await hass.config_entries.async_reload(entry.entry_id)


async def _async_register_services(hass: HomeAssistant) -> None:
    """Register Leapmotor domain services once."""

    specs = {spec.service_name: spec for spec in BUTTON_SPECS}
    specs["lock"] = LOCK_ACTION
    specs["unlock"] = UNLOCK_ACTION

    async def handle_remote(service_action: str, call: ServiceCall) -> None:
        domain_data = hass.data.get(DOMAIN) or {}
        if not domain_data:
            raise HomeAssistantError("No Leapmotor config entry is loaded.")

        action_spec = specs[service_action]

        coordinator = None
        target_vin = call.data.get("vin")
        entity_id = call.data.get("entity_id")
        if entity_id:
            state = hass.states.get(entity_id)
            if state:
                target_vin = state.attributes.get("vin") or target_vin
        for candidate in domain_data.values():
            if target_vin and target_vin not in (candidate.data.get("vehicles") or {}):
                continue
            try:
                resolved_vin = resolve_target_vin(candidate, target_vin)
            except Exception:
                continue
            coordinator = candidate
            target_vin = resolved_vin
            break

        if coordinator is None or not target_vin:
            raise HomeAssistantError(
                "No matching Leapmotor vehicle found. Specify a VIN if multiple vehicles are configured."
            )

        raw_value = call.data.get("value")
        kwargs = {"value": raw_value} if raw_value is not None else None
        await async_execute_remote_action(coordinator, target_vin, action_spec, kwargs)

    async def handle_set_charge_limit(call: ServiceCall) -> None:
        domain_data = hass.data.get(DOMAIN) or {}
        if not domain_data:
            raise HomeAssistantError("No Leapmotor config entry is loaded.")

        coordinator = None
        target_vin = call.data.get("vin")
        entity_id = call.data.get("entity_id")
        if entity_id:
            state = hass.states.get(entity_id)
            if state:
                target_vin = state.attributes.get("vin") or target_vin
        for candidate in domain_data.values():
            if target_vin and target_vin not in (candidate.data.get("vehicles") or {}):
                continue
            try:
                resolved_vin = resolve_target_vin(candidate, target_vin)
            except Exception:
                continue
            coordinator = candidate
            target_vin = resolved_vin
            break

        if coordinator is None or not target_vin:
            raise HomeAssistantError(
                "No matching Leapmotor vehicle found. Specify a VIN if multiple vehicles are configured."
            )

        try:
            result = await hass.async_add_executor_job(
                coordinator.client.set_charge_limit,
                target_vin,
                call.data["charge_limit_percent"],
            )
        except Exception as exc:
            message = format_remote_error(exc)
            coordinator.record_remote_action(
                target_vin,
                "set_charge_limit",
                success=False,
                error=message,
            )
            raise HomeAssistantError(message) from exc

        coordinator.record_remote_action(
            target_vin,
            "set_charge_limit",
            success=True,
            result=result,
        )
        await coordinator.async_request_refresh()

    async def handle_send_destination(call: ServiceCall) -> None:
        domain_data = hass.data.get(DOMAIN) or {}
        if not domain_data:
            raise HomeAssistantError("No Leapmotor config entry is loaded.")

        coordinator = None
        target_vin = call.data.get("vin")
        entity_id = call.data.get("entity_id")
        if entity_id:
            state = hass.states.get(entity_id)
            if state:
                target_vin = state.attributes.get("vin") or target_vin
        for candidate in domain_data.values():
            if target_vin and target_vin not in (candidate.data.get("vehicles") or {}):
                continue
            try:
                resolved_vin = resolve_target_vin(candidate, target_vin)
            except Exception:
                continue
            coordinator = candidate
            target_vin = resolved_vin
            break

        if coordinator is None or not target_vin:
            raise HomeAssistantError(
                "No matching Leapmotor vehicle found. Specify a VIN if multiple vehicles are configured."
            )

        destination_name = call.data["name"].strip()
        address = call.data.get("address", destination_name).strip()
        try:
            result = await hass.async_add_executor_job(
                partial(
                    coordinator.client.send_destination,
                    target_vin,
                    address=address,
                    address_name=destination_name,
                    latitude=call.data["latitude"],
                    longitude=call.data["longitude"],
                )
            )
        except Exception as exc:
            message = format_remote_error(exc)
            coordinator.record_remote_action(
                target_vin,
                "send_destination",
                success=False,
                error=message,
            )
            raise HomeAssistantError(message) from exc

        coordinator.record_remote_action(
            target_vin,
            "send_destination",
            success=True,
            result=result,
        )
        await coordinator.async_request_refresh()

    def make_handler(service_action: str):
        async def _handler(call: ServiceCall) -> None:
            await handle_remote(service_action, call)

        return _handler

    for service_name in ("lock", "unlock", *(spec.service_name for spec in BUTTON_SPECS)):
        hass.services.async_register(
            DOMAIN,
            service_name,
            make_handler(service_name),
            schema=_SERVICE_SCHEMAS.get(service_name, SERVICE_FIELDS),
        )
        _LOGGER.debug("Registered Leapmotor service %s.%s", DOMAIN, service_name)
    hass.services.async_register(
        DOMAIN,
        "set_charge_limit",
        handle_set_charge_limit,
        schema=SET_CHARGE_LIMIT_FIELDS,
    )
    _LOGGER.debug("Registered Leapmotor service %s.%s", DOMAIN, "set_charge_limit")
    hass.services.async_register(
        DOMAIN,
        "send_destination",
        handle_send_destination,
        schema=SEND_DESTINATION_FIELDS,
    )
    _LOGGER.debug("Registered Leapmotor service %s.%s", DOMAIN, "send_destination")


def _async_unregister_services(hass: HomeAssistant) -> None:
    """Remove Leapmotor domain services when the last entry unloads."""
    for service_name in ("lock", "unlock", *(spec.service_name for spec in BUTTON_SPECS)):
        if hass.services.has_service(DOMAIN, service_name):
            hass.services.async_remove(DOMAIN, service_name)
    if hass.services.has_service(DOMAIN, "set_charge_limit"):
        hass.services.async_remove(DOMAIN, "set_charge_limit")
    if hass.services.has_service(DOMAIN, "send_destination"):
        hass.services.async_remove(DOMAIN, "send_destination")
