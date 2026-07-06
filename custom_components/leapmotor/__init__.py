"""Home Assistant integration for Leapmotor vehicle data."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from functools import partial
import json
import logging
from pathlib import Path
import uuid

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import CONF_PASSWORD, CONF_USERNAME, Platform
from homeassistant.core import HomeAssistant, ServiceCall
from homeassistant.exceptions import HomeAssistantError
import voluptuous as vol

from .api import LeapmotorApiClient
from .button import BUTTON_SPECS
from .const import (
    CONF_ACCOUNT_P12_PASSWORD,
    CONF_DEVICE_ID,
    CONF_ECO_POLLING_ENABLED,
    CONF_ECO_SCAN_INTERVAL,
    CONF_OPERATION_PASSWORD,
    CONF_SCAN_INTERVAL,
    DEFAULT_SCAN_INTERVAL_MINUTES,
    DEFAULT_ECO_SCAN_INTERVAL_MINUTES,
    DOMAIN,
    STATIC_CERT_STORAGE_DIR,
)
from .coordinator import LeapmotorDataUpdateCoordinator
from .entity_migration import async_remove_obsolete_entities
from .lock import LOCK_ACTION, UNLOCK_ACTION
from .remote_helpers import (
    RemoteActionSpec,
    async_execute_remote_action,
    format_remote_error,
    resolve_target_vin,
)

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

SET_CLIMATE_FIELDS = vol.Schema(
    {
        vol.Required("mode"): vol.In(("cold", "hot", "wind", "nohotcold")),
        vol.Optional("temperature", default=26): vol.All(vol.Coerce(int), vol.Range(min=18, max=32)),
        vol.Optional("fan_speed", default=3): vol.All(vol.Coerce(int), vol.Range(min=1, max=7)),
        vol.Optional("recirculate", default=False): bool,
        vol.Optional("windshield_defrost", default=False): bool,
        vol.Optional("vin"): str,
        vol.Optional("entity_id"): str,
    }
)

def _schedule_days(value: object) -> list[int]:
    """Validate climate schedule weekdays. The app uses 0=Sunday through 6=Saturday."""
    if value in (None, ""):
        return []
    if isinstance(value, str):
        raw_values = [part.strip() for part in value.split(",") if part.strip()]
    elif isinstance(value, (list, tuple, set)):
        raw_values = list(value)
    else:
        raise vol.Invalid("days must be a list or comma-separated string")

    days: list[int] = []
    for raw_value in raw_values:
        try:
            day = int(raw_value)
        except (TypeError, ValueError) as exc:
            raise vol.Invalid("days must contain integers from 0 to 6") from exc
        if not 0 <= day <= 6:
            raise vol.Invalid("days must contain integers from 0 to 6")
        if day not in days:
            days.append(day)
    return days


def _prepare_car_kwargs(data: dict[str, object]) -> dict[str, object]:
    """Extract prepare-car payload options from Home Assistant service data."""
    return {
        "climate_enabled": data["climate_enabled"],
        "mode": data["mode"],
        "operate": data["operate"],
        "temperature": data["temperature"],
        "fan_speed": data["fan_speed"],
        "recirculate": data["recirculate"],
        "windshield_defrost": data["windshield_defrost"],
        "driver_seat": data["driver_seat"],
        "driver_seat_level": data["driver_seat_level"],
        "passenger_seat": data["passenger_seat"],
        "passenger_seat_level": data["passenger_seat_level"],
        "steering_wheel_heat": data["steering_wheel_heat"],
        "mirror_heat": data["mirror_heat"],
        "destination_name": data.get("destination_name"),
        "destination_address": data.get("destination_address"),
        "destination_latitude": data.get("destination_latitude"),
        "destination_longitude": data.get("destination_longitude"),
    }


SET_CLIMATE_SCHEDULE_FIELDS = vol.Schema(
    {
        vol.Required("start_time"): vol.All(str, vol.Length(min=1)),
        vol.Optional("mode", default="nohotcold"): vol.In(("cold", "hot", "nohotcold")),
        vol.Optional("operate", default="manual"): vol.In(("manual", "auto")),
        vol.Optional("temperature", default=26): vol.All(vol.Coerce(int), vol.Range(min=18, max=32)),
        vol.Optional("fan_speed", default=4): vol.All(vol.Coerce(int), vol.Range(min=1, max=7)),
        vol.Optional("recirculate", default=False): bool,
        vol.Optional("windshield_defrost", default=False): bool,
        vol.Optional("days", default=[]): _schedule_days,
        vol.Optional("enabled", default=True): bool,
        vol.Optional("set_id"): str,
        vol.Optional("vin"): str,
        vol.Optional("entity_id"): str,
    }
)

PREPARE_CAR_BASE_FIELDS = {
    vol.Optional("climate_enabled", default=True): bool,
    vol.Optional("mode", default="cold"): vol.In(("cold", "hot", "nohotcold")),
    vol.Optional("operate", default="manual"): vol.In(("manual", "auto")),
    vol.Optional("temperature", default=18): vol.All(vol.Coerce(int), vol.Range(min=18, max=32)),
    vol.Optional("fan_speed", default=7): vol.All(vol.Coerce(int), vol.Range(min=1, max=7)),
    vol.Optional("recirculate", default=True): bool,
    vol.Optional("windshield_defrost", default=False): bool,
    vol.Optional("driver_seat", default="off"): vol.In(("off", "heat", "ventilation")),
    vol.Optional("driver_seat_level", default=3): vol.All(vol.Coerce(int), vol.Range(min=1, max=3)),
    vol.Optional("passenger_seat", default="off"): vol.In(("off", "heat", "ventilation")),
    vol.Optional("passenger_seat_level", default=3): vol.All(vol.Coerce(int), vol.Range(min=1, max=3)),
    vol.Optional("steering_wheel_heat", default=False): bool,
    vol.Optional("mirror_heat", default=False): bool,
    vol.Optional("destination_name"): vol.All(str, vol.Length(min=1)),
    vol.Optional("destination_address"): vol.All(str, vol.Length(min=1)),
    vol.Optional("destination_latitude"): vol.All(vol.Coerce(float), vol.Range(min=-90, max=90)),
    vol.Optional("destination_longitude"): vol.All(vol.Coerce(float), vol.Range(min=-180, max=180)),
    vol.Optional("vin"): str,
    vol.Optional("entity_id"): str,
}

PREPARE_CAR_FIELDS = vol.Schema(PREPARE_CAR_BASE_FIELDS)

PREPARE_CAR_SCHEDULE_FIELDS = vol.Schema(
    {
        vol.Required("start_time"): vol.All(str, vol.Length(min=1)),
        vol.Optional("days", default=[]): _schedule_days,
        vol.Optional("enabled", default=True): bool,
        vol.Optional("set_id"): str,
        **PREPARE_CAR_BASE_FIELDS,
    }
)

_SERVICE_SCHEMAS = {
    "windows_open": WINDOW_POSITION_SERVICE_FIELDS,
    "windows_close": WINDOW_POSITION_SERVICE_FIELDS,
    "sunshade_open": SUNSHADE_POSITION_SERVICE_FIELDS,
    "sunshade_close": SUNSHADE_POSITION_SERVICE_FIELDS,
}

SET_CLIMATE_ACTION = RemoteActionSpec(
    action="set_climate",
    translation_key="set_climate",
    icon="mdi:air-conditioner",
    method_name="set_climate",
    service_name="set_climate",
)

SET_CLIMATE_SCHEDULE_ACTION = RemoteActionSpec(
    action="set_climate_schedule",
    translation_key="set_climate_schedule",
    icon="mdi:calendar-clock",
    method_name="set_climate_schedule",
    service_name="set_climate_schedule",
)

CANCEL_CLIMATE_SCHEDULE_ACTION = RemoteActionSpec(
    action="cancel_climate_schedule",
    translation_key="cancel_climate_schedule",
    icon="mdi:calendar-remove",
    method_name="cancel_climate_schedule",
    service_name="cancel_climate_schedule",
)

PREPARE_CAR_ACTION = RemoteActionSpec(
    action="prepare_car",
    translation_key="prepare_car",
    icon="mdi:car-cog",
    method_name="prepare_car",
    service_name="prepare_car",
)

SET_PREPARE_CAR_SCHEDULE_ACTION = RemoteActionSpec(
    action="set_prepare_car_schedule",
    translation_key="set_prepare_car_schedule",
    icon="mdi:calendar-clock",
    method_name="set_prepare_car_schedule",
    service_name="set_prepare_car_schedule",
)

CANCEL_PREPARE_CAR_SCHEDULE_ACTION = RemoteActionSpec(
    action="cancel_prepare_car_schedule",
    translation_key="cancel_prepare_car_schedule",
    icon="mdi:calendar-remove",
    method_name="cancel_prepare_car_schedule",
    service_name="cancel_prepare_car_schedule",
)

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

EXPORT_DIAGNOSTICS_FIELDS = vol.Schema(
    {
        vol.Optional("filename"): str,
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
    Platform.SWITCH,
]


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up Leapmotor from a config entry."""
    entry_data = dict(entry.data)
    if not entry_data.get(CONF_DEVICE_ID):
        entry_data[CONF_DEVICE_ID] = uuid.uuid4().hex
        hass.config_entries.async_update_entry(entry, data=entry_data)

    scan_interval = int(
        entry.options.get(
            CONF_SCAN_INTERVAL,
            entry_data.get(CONF_SCAN_INTERVAL, DEFAULT_SCAN_INTERVAL_MINUTES),
        )
    )
    eco_scan_interval = int(
        entry.options.get(
            CONF_ECO_SCAN_INTERVAL,
            entry_data.get(CONF_ECO_SCAN_INTERVAL, DEFAULT_ECO_SCAN_INTERVAL_MINUTES),
        )
    )
    operation_password = (
        entry.options[CONF_OPERATION_PASSWORD]
        if CONF_OPERATION_PASSWORD in entry.options
        else entry_data.get(CONF_OPERATION_PASSWORD)
    )
    client = LeapmotorApiClient(
        username=entry_data[CONF_USERNAME],
        password=entry_data[CONF_PASSWORD],
        account_p12_password=entry_data.get(CONF_ACCOUNT_P12_PASSWORD),
        operation_password=operation_password or None,
        device_id=entry_data[CONF_DEVICE_ID],
        static_cert_dir=hass.config.path(STATIC_CERT_STORAGE_DIR),
    )
    coordinator = LeapmotorDataUpdateCoordinator(
        hass=hass,
        config_entry=entry,
        client=client,
        update_interval=timedelta(minutes=scan_interval),
        eco_polling_enabled=bool(
            entry.options.get(
                CONF_ECO_POLLING_ENABLED,
                entry_data.get(CONF_ECO_POLLING_ENABLED, False),
            )
        ),
        eco_update_interval=timedelta(minutes=max(eco_scan_interval, scan_interval)),
    )
    await coordinator.async_config_entry_first_refresh()
    await async_remove_obsolete_entities(hass)

    hass.data.setdefault(DOMAIN, {})[entry.entry_id] = coordinator
    if not hass.services.has_service(DOMAIN, "lock"):
        await _async_register_services(hass)
    entry.async_on_unload(entry.add_update_listener(async_reload_entry))
    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
    await async_remove_obsolete_entities(hass)
    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload a config entry."""
    unload_ok = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
    if unload_ok:
        coordinator = hass.data[DOMAIN].pop(entry.entry_id)
        coordinator.cancel_scheduled_followup_refreshes()
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
    specs["cancel_climate_schedule"] = CANCEL_CLIMATE_SCHEDULE_ACTION
    specs["cancel_prepare_car_schedule"] = CANCEL_PREPARE_CAR_SCHEDULE_ACTION

    def resolve_call_target(call: ServiceCall) -> tuple[LeapmotorDataUpdateCoordinator, str]:
        """Resolve one Leapmotor service call to coordinator and VIN."""
        domain_data = hass.data.get(DOMAIN) or {}
        if not domain_data:
            raise HomeAssistantError("No Leapmotor config entry is loaded.")

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
            return candidate, resolved_vin

        raise HomeAssistantError(
            "No matching Leapmotor vehicle found. Specify a VIN if multiple vehicles are configured."
        )

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

        kwargs = None
        if "value" in call.data:
            kwargs = {"value": call.data["value"]}
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
        coordinator.schedule_remote_followup_refresh(target_vin)

    async def handle_set_climate(call: ServiceCall) -> None:
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

        kwargs = {
            "mode": call.data["mode"],
            "temperature": call.data["temperature"],
            "fan_speed": call.data["fan_speed"],
            "recirculate": call.data["recirculate"],
            "windshield_defrost": call.data["windshield_defrost"],
        }
        await async_execute_remote_action(coordinator, target_vin, SET_CLIMATE_ACTION, kwargs)

    async def handle_set_climate_schedule(call: ServiceCall) -> None:
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

        kwargs = {
            "start_time": call.data["start_time"],
            "mode": call.data["mode"],
            "operate": call.data["operate"],
            "temperature": call.data["temperature"],
            "fan_speed": call.data["fan_speed"],
            "recirculate": call.data["recirculate"],
            "windshield_defrost": call.data["windshield_defrost"],
            "days": call.data["days"],
            "enabled": call.data["enabled"],
            "set_id": call.data.get("set_id"),
        }
        await async_execute_remote_action(
            coordinator,
            target_vin,
            SET_CLIMATE_SCHEDULE_ACTION,
            kwargs,
        )

    async def handle_prepare_car(call: ServiceCall) -> None:
        coordinator, target_vin = resolve_call_target(call)
        await async_execute_remote_action(
            coordinator,
            target_vin,
            PREPARE_CAR_ACTION,
            _prepare_car_kwargs(call.data),
        )

    async def handle_set_prepare_car_schedule(call: ServiceCall) -> None:
        coordinator, target_vin = resolve_call_target(call)
        kwargs = {
            "start_time": call.data["start_time"],
            "days": call.data["days"],
            "enabled": call.data["enabled"],
            "set_id": call.data.get("set_id"),
            **_prepare_car_kwargs(call.data),
        }
        await async_execute_remote_action(
            coordinator,
            target_vin,
            SET_PREPARE_CAR_SCHEDULE_ACTION,
            kwargs,
        )

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

    async def handle_export_diagnostics(call: ServiceCall) -> None:
        from .diagnostics import async_get_config_entry_diagnostics

        domain_data = hass.data.get(DOMAIN) or {}
        if not domain_data:
            raise HomeAssistantError("No Leapmotor config entry is loaded.")

        export: dict[str, object] = {
            "created_at": datetime.now(UTC).isoformat(),
            "entries": {},
        }
        for entry in hass.config_entries.async_entries(DOMAIN):
            if entry.entry_id not in domain_data:
                continue
            export["entries"][entry.entry_id] = await async_get_config_entry_diagnostics(
                hass,
                entry,
            )

        filename = str(call.data.get("filename") or "").strip()
        if not filename:
            timestamp = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
            filename = f"leapmotor-diagnostics-{timestamp}.json"
        filename = Path(filename).name
        if not filename.endswith(".json"):
            filename = f"{filename}.json"

        export_dir = Path(hass.config.path(STATIC_CERT_STORAGE_DIR))
        export_path = export_dir / filename
        await hass.async_add_executor_job(
            _write_json_export,
            export_path,
            export,
        )
        _LOGGER.info("Exported redacted Leapmotor diagnostics to %s", export_path)

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
        "set_climate",
        handle_set_climate,
        schema=SET_CLIMATE_FIELDS,
    )
    _LOGGER.debug("Registered Leapmotor service %s.%s", DOMAIN, "set_climate")
    hass.services.async_register(
        DOMAIN,
        "set_climate_schedule",
        handle_set_climate_schedule,
        schema=SET_CLIMATE_SCHEDULE_FIELDS,
    )
    _LOGGER.debug("Registered Leapmotor service %s.%s", DOMAIN, "set_climate_schedule")
    hass.services.async_register(
        DOMAIN,
        "cancel_climate_schedule",
        make_handler("cancel_climate_schedule"),
        schema=SERVICE_FIELDS,
    )
    _LOGGER.debug("Registered Leapmotor service %s.%s", DOMAIN, "cancel_climate_schedule")
    hass.services.async_register(
        DOMAIN,
        "prepare_car",
        handle_prepare_car,
        schema=PREPARE_CAR_FIELDS,
    )
    _LOGGER.debug("Registered Leapmotor service %s.%s", DOMAIN, "prepare_car")
    hass.services.async_register(
        DOMAIN,
        "set_prepare_car_schedule",
        handle_set_prepare_car_schedule,
        schema=PREPARE_CAR_SCHEDULE_FIELDS,
    )
    _LOGGER.debug("Registered Leapmotor service %s.%s", DOMAIN, "set_prepare_car_schedule")
    hass.services.async_register(
        DOMAIN,
        "cancel_prepare_car_schedule",
        make_handler("cancel_prepare_car_schedule"),
        schema=SERVICE_FIELDS,
    )
    _LOGGER.debug("Registered Leapmotor service %s.%s", DOMAIN, "cancel_prepare_car_schedule")
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
    hass.services.async_register(
        DOMAIN,
        "export_diagnostics",
        handle_export_diagnostics,
        schema=EXPORT_DIAGNOSTICS_FIELDS,
    )
    _LOGGER.debug("Registered Leapmotor service %s.%s", DOMAIN, "export_diagnostics")


def _async_unregister_services(hass: HomeAssistant) -> None:
    """Remove Leapmotor domain services when the last entry unloads."""
    for service_name in ("lock", "unlock", *(spec.service_name for spec in BUTTON_SPECS)):
        if hass.services.has_service(DOMAIN, service_name):
            hass.services.async_remove(DOMAIN, service_name)
    if hass.services.has_service(DOMAIN, "set_climate"):
        hass.services.async_remove(DOMAIN, "set_climate")
    if hass.services.has_service(DOMAIN, "set_climate_schedule"):
        hass.services.async_remove(DOMAIN, "set_climate_schedule")
    if hass.services.has_service(DOMAIN, "cancel_climate_schedule"):
        hass.services.async_remove(DOMAIN, "cancel_climate_schedule")
    if hass.services.has_service(DOMAIN, "prepare_car"):
        hass.services.async_remove(DOMAIN, "prepare_car")
    if hass.services.has_service(DOMAIN, "set_prepare_car_schedule"):
        hass.services.async_remove(DOMAIN, "set_prepare_car_schedule")
    if hass.services.has_service(DOMAIN, "cancel_prepare_car_schedule"):
        hass.services.async_remove(DOMAIN, "cancel_prepare_car_schedule")
    if hass.services.has_service(DOMAIN, "set_charge_limit"):
        hass.services.async_remove(DOMAIN, "set_charge_limit")
    if hass.services.has_service(DOMAIN, "send_destination"):
        hass.services.async_remove(DOMAIN, "send_destination")
    if hass.services.has_service(DOMAIN, "export_diagnostics"):
        hass.services.async_remove(DOMAIN, "export_diagnostics")


def _write_json_export(path: Path, payload: object) -> None:
    """Write a JSON export file from an executor thread."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
