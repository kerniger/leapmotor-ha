"""Binary sensor entities for Leapmotor."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from homeassistant.components.binary_sensor import (
    BinarySensorEntity,
    BinarySensorEntityDescription,
    BinarySensorDeviceClass,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import EntityCategory
from homeassistant.core import HomeAssistant
from homeassistant.helpers.device_registry import DeviceInfo
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from .const import DOMAIN
from .coordinator import LeapmotorDataUpdateCoordinator
from .entity_helpers import build_vehicle_display_name, load_localized_entity_names
from .entity_migration import english_entity_slug

OPTIONAL_BINARY_SENSOR_PATHS = {
    "is_driving": "status.is_driving",
    "charging_planned_enabled": "charging.charging_planned_enabled",
    "charging_planned_weekly": "charging.charging_planned_cycles",
    "remote_session_active": "diagnostics.remote_session_active",
    "vehicle_security_active": "diagnostics.vehicle_security_active",
    "battery_heating": "diagnostics.battery_heating",
    "skylight_open": "diagnostics.skylight_open",
    "fast_cooling_active": "diagnostics.fast_cooling_active",
    "fast_heating_active": "diagnostics.fast_heating_active",
    "windshield_defrosting": "diagnostics.windshield_defrosting",
    "rear_window_heating": "diagnostics.rear_window_heating",
    "air_recirculation": "diagnostics.air_recirculation",
    "climate_temp_mode": "diagnostics.climate_temp_mode",
    "steering_wheel_heating": "diagnostics.steering_wheel_heating",
    "left_mirror_heating": "diagnostics.left_mirror_heating",
    "right_mirror_heating": "diagnostics.right_mirror_heating",
    "speed_limit_enabled": "diagnostics.speed_limit_enabled",
    "park_assist_enabled": "diagnostics.park_assist_enabled",
    "sentinel_mode": "diagnostics.sentinel_mode",
    "parking_photo": "diagnostics.parking_photo",
    "fully_charged": "diagnostics.fully_charged",
    "bluetooth_enabled": "diagnostics.bluetooth_enabled",
    "hotspot_enabled": "diagnostics.hotspot_enabled",
    "windows_remote_supported": "diagnostics.windows_remote_supported",
    "door_control_allowed": "diagnostics.door_control_allowed",
}


@dataclass(frozen=True, kw_only=True)
class LeapmotorBinarySensorEntityDescription(BinarySensorEntityDescription):
    """Describes a Leapmotor binary sensor."""

    value_fn: Callable[[dict[str, Any]], Any]


BINARY_SENSOR_DESCRIPTIONS: tuple[LeapmotorBinarySensorEntityDescription, ...] = (
    LeapmotorBinarySensorEntityDescription(
        key="is_driving",
        translation_key="is_driving",
        icon="mdi:car-speed-limiter",
        device_class=BinarySensorDeviceClass.MOTION,
        value_fn=lambda data: data["status"].get("is_driving"),
    ),
    LeapmotorBinarySensorEntityDescription(
        key="is_charging",
        translation_key="is_charging",
        icon="mdi:ev-station",
        device_class=BinarySensorDeviceClass.BATTERY_CHARGING,
        value_fn=lambda data: data["charging"].get("is_charging"),
    ),
    LeapmotorBinarySensorEntityDescription(
        key="is_plugged_in",
        translation_key="is_plugged_in",
        icon="mdi:ev-plug-type2",
        device_class=BinarySensorDeviceClass.PLUG,
        value_fn=lambda data: data["charging"].get("is_plugged_in"),
    ),
    LeapmotorBinarySensorEntityDescription(
        key="is_regening",
        translation_key="is_regening",
        icon="mdi:car-electric",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["charging"].get("is_regening"),
    ),
    LeapmotorBinarySensorEntityDescription(
        key="dc_cable_connected",
        translation_key="dc_cable_connected",
        icon="mdi:ev-plug-ccs2",
        device_class=BinarySensorDeviceClass.PLUG,
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["charging"].get("dc_cable_connected"),
    ),
    LeapmotorBinarySensorEntityDescription(
        key="charging_planned_enabled",
        translation_key="charging_planned_enabled",
        icon="mdi:calendar-clock",
        value_fn=lambda data: data["charging"].get("charging_planned_enabled"),
    ),
    LeapmotorBinarySensorEntityDescription(
        key="charging_planned_weekly",
        translation_key="charging_planned_weekly",
        icon="mdi:calendar-week",
        value_fn=lambda data: (data["charging"].get("charging_planned_cycles") or "") == "1,1,1,1,1,1,1",
    ),
    LeapmotorBinarySensorEntityDescription(
        key="remote_session_active",
        translation_key="remote_session_active",
        icon="mdi:remote",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["diagnostics"].get("remote_session_active"),
    ),
    LeapmotorBinarySensorEntityDescription(
        key="vehicle_security_active",
        translation_key="vehicle_security_active",
        icon="mdi:shield-car",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["diagnostics"].get("vehicle_security_active"),
    ),
    LeapmotorBinarySensorEntityDescription(
        key="battery_heating",
        translation_key="battery_heating",
        icon="mdi:battery-heart",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["diagnostics"].get("battery_heating"),
    ),
    LeapmotorBinarySensorEntityDescription(
        key="driver_door_open",
        translation_key="driver_door_open",
        device_class=BinarySensorDeviceClass.DOOR,
        value_fn=lambda data: data["diagnostics"].get("driver_door_open"),
    ),
    LeapmotorBinarySensorEntityDescription(
        key="passenger_door_open",
        translation_key="passenger_door_open",
        device_class=BinarySensorDeviceClass.DOOR,
        value_fn=lambda data: data["diagnostics"].get("passenger_door_open"),
    ),
    LeapmotorBinarySensorEntityDescription(
        key="rear_left_door_open",
        translation_key="rear_left_door_open",
        device_class=BinarySensorDeviceClass.DOOR,
        value_fn=lambda data: data["diagnostics"].get("rear_left_door_open"),
    ),
    LeapmotorBinarySensorEntityDescription(
        key="rear_right_door_open",
        translation_key="rear_right_door_open",
        device_class=BinarySensorDeviceClass.DOOR,
        value_fn=lambda data: data["diagnostics"].get("rear_right_door_open"),
    ),
    LeapmotorBinarySensorEntityDescription(
        key="trunk_open",
        translation_key="trunk_open",
        device_class=BinarySensorDeviceClass.OPENING,
        value_fn=lambda data: data["diagnostics"].get("trunk_open"),
    ),
    LeapmotorBinarySensorEntityDescription(
        key="front_left_window_open",
        translation_key="front_left_window_open",
        device_class=BinarySensorDeviceClass.WINDOW,
        value_fn=lambda data: data["diagnostics"].get("front_left_window_open"),
    ),
    LeapmotorBinarySensorEntityDescription(
        key="front_right_window_open",
        translation_key="front_right_window_open",
        device_class=BinarySensorDeviceClass.WINDOW,
        value_fn=lambda data: data["diagnostics"].get("front_right_window_open"),
    ),
    LeapmotorBinarySensorEntityDescription(
        key="rear_left_window_open",
        translation_key="rear_left_window_open",
        device_class=BinarySensorDeviceClass.WINDOW,
        value_fn=lambda data: data["diagnostics"].get("rear_left_window_open"),
    ),
    LeapmotorBinarySensorEntityDescription(
        key="rear_right_window_open",
        translation_key="rear_right_window_open",
        device_class=BinarySensorDeviceClass.WINDOW,
        value_fn=lambda data: data["diagnostics"].get("rear_right_window_open"),
    ),
    LeapmotorBinarySensorEntityDescription(
        key="skylight_open",
        translation_key="skylight_open",
        icon="mdi:window-open",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["diagnostics"].get("skylight_open"),
    ),
    LeapmotorBinarySensorEntityDescription(
        key="climate_on",
        translation_key="climate_on",
        icon="mdi:air-conditioner",
        value_fn=lambda data: data["diagnostics"].get("climate_on"),
    ),
    LeapmotorBinarySensorEntityDescription(
        key="fast_cooling_active",
        translation_key="fast_cooling_active",
        icon="mdi:snowflake",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["diagnostics"].get("fast_cooling_active"),
    ),
    LeapmotorBinarySensorEntityDescription(
        key="fast_heating_active",
        translation_key="fast_heating_active",
        icon="mdi:heat-wave",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["diagnostics"].get("fast_heating_active"),
    ),
    LeapmotorBinarySensorEntityDescription(
        key="windshield_defrosting",
        translation_key="windshield_defrosting",
        icon="mdi:car-defrost-front",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["diagnostics"].get("windshield_defrosting"),
    ),
    LeapmotorBinarySensorEntityDescription(
        key="rear_window_heating",
        translation_key="rear_window_heating",
        icon="mdi:car-defrost-rear",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["diagnostics"].get("rear_window_heating"),
    ),
    LeapmotorBinarySensorEntityDescription(
        key="air_recirculation",
        translation_key="air_recirculation",
        icon="mdi:air-filter",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["diagnostics"].get("air_recirculation"),
    ),
    LeapmotorBinarySensorEntityDescription(
        key="climate_temp_mode",
        translation_key="climate_temp_mode",
        icon="mdi:thermometer-auto",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["diagnostics"].get("climate_temp_mode"),
    ),
    LeapmotorBinarySensorEntityDescription(
        key="steering_wheel_heating",
        translation_key="steering_wheel_heating",
        icon="mdi:steering",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["diagnostics"].get("steering_wheel_heating"),
    ),
    LeapmotorBinarySensorEntityDescription(
        key="left_mirror_heating",
        translation_key="left_mirror_heating",
        icon="mdi:car-side",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["diagnostics"].get("left_mirror_heating"),
    ),
    LeapmotorBinarySensorEntityDescription(
        key="right_mirror_heating",
        translation_key="right_mirror_heating",
        icon="mdi:car-side",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["diagnostics"].get("right_mirror_heating"),
    ),
    LeapmotorBinarySensorEntityDescription(
        key="speed_limit_enabled",
        translation_key="speed_limit_enabled",
        icon="mdi:speedometer",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["diagnostics"].get("speed_limit_enabled"),
    ),
    LeapmotorBinarySensorEntityDescription(
        key="park_assist_enabled",
        translation_key="park_assist_enabled",
        icon="mdi:car-brake-parking",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["diagnostics"].get("park_assist_enabled"),
    ),
    LeapmotorBinarySensorEntityDescription(
        key="sentinel_mode",
        translation_key="sentinel_mode",
        icon="mdi:shield-car",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["diagnostics"].get("sentinel_mode"),
    ),
    LeapmotorBinarySensorEntityDescription(
        key="parking_photo",
        translation_key="parking_photo",
        icon="mdi:camera",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["diagnostics"].get("parking_photo"),
    ),
    LeapmotorBinarySensorEntityDescription(
        key="fully_charged",
        translation_key="fully_charged",
        icon="mdi:battery-check",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["diagnostics"].get("fully_charged"),
    ),
    LeapmotorBinarySensorEntityDescription(
        key="bluetooth_enabled",
        translation_key="bluetooth_enabled",
        icon="mdi:bluetooth",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["diagnostics"].get("bluetooth_enabled"),
    ),
    LeapmotorBinarySensorEntityDescription(
        key="hotspot_enabled",
        translation_key="hotspot_enabled",
        icon="mdi:wifi",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["diagnostics"].get("hotspot_enabled"),
    ),
    LeapmotorBinarySensorEntityDescription(
        key="windows_remote_supported",
        translation_key="windows_remote_supported",
        icon="mdi:window-open-variant",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["diagnostics"].get("windows_remote_supported"),
    ),
    LeapmotorBinarySensorEntityDescription(
        key="door_control_allowed",
        translation_key="door_control_allowed",
        icon="mdi:car-door",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["diagnostics"].get("door_control_allowed"),
    ),
)


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up Leapmotor binary sensors."""
    coordinator: LeapmotorDataUpdateCoordinator = hass.data[DOMAIN][entry.entry_id]
    localized_names = await hass.async_add_executor_job(
        load_localized_entity_names,
        hass.config.language,
        "binary_sensor",
    )
    entities: list[LeapmotorBinarySensor] = []
    for vin, vehicle_data in coordinator.data.get("vehicles", {}).items():
        entities.extend(
            LeapmotorBinarySensor(coordinator, vin, description, localized_names)
            for description in BINARY_SENSOR_DESCRIPTIONS
            if _should_create_binary_sensor(vehicle_data, description.key)
        )
    async_add_entities(entities)


class LeapmotorBinarySensor(
    CoordinatorEntity[LeapmotorDataUpdateCoordinator],
    BinarySensorEntity,
):
    """Leapmotor binary sensor."""

    entity_description: LeapmotorBinarySensorEntityDescription

    def __init__(
        self,
        coordinator: LeapmotorDataUpdateCoordinator,
        vin: str,
        description: LeapmotorBinarySensorEntityDescription,
        localized_names: dict[str, str],
    ) -> None:
        super().__init__(coordinator)
        self.vin = vin
        self.entity_description = description
        self._attr_unique_id = f"{vin}_{description.key}"
        vehicle = self.vehicle_data["vehicle"]
        self._attr_has_entity_name = True
        self._attr_name = localized_names.get(
            description.translation_key or description.key,
            description.key.replace("_", " ").capitalize(),
        )
        self._attr_suggested_object_id = _suggested_object_id(
            vehicle,
            english_entity_slug("binary_sensor", description.key) or description.key,
        )
        self._attr_device_info = DeviceInfo(
            identifiers={(DOMAIN, vin)},
            manufacturer="Leapmotor",
            model=vehicle.get("car_type"),
            name=build_vehicle_display_name(vehicle),
            serial_number=vin,
        )

    @property
    def vehicle_data(self) -> dict[str, Any]:
        """Return current data for this vehicle."""
        return self.coordinator.data["vehicles"][self.vin]

    @property
    def translation_key(self) -> str | None:
        """Disable frontend-only name translations; names are localized in setup."""
        return None

    @property
    def is_on(self) -> bool | None:
        """Return the binary sensor state."""
        value = self.entity_description.value_fn(self.vehicle_data)
        if value is None:
            return None
        return bool(value)

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        """Return useful vehicle metadata."""
        vehicle = self.vehicle_data["vehicle"]
        return {
            "vin": self.vin,
            "car_id": vehicle.get("car_id"),
            "car_type": vehicle.get("car_type"),
            "is_shared": vehicle.get("is_shared"),
        }


def _suggested_object_id(vehicle: dict[str, Any], slug: str) -> str:
    """Return a stable English suggested object id independent from UI language."""
    prefix = str(vehicle.get("car_type") or "leapmotor").strip().lower()
    prefix = "".join(char if char.isalnum() else "_" for char in prefix).strip("_")
    return f"{prefix or 'leapmotor'}_{slug}"


def _should_create_binary_sensor(vehicle_data: dict[str, Any], key: str) -> bool:
    """Return whether a binary sensor is supported by the current vehicle payload."""
    path = OPTIONAL_BINARY_SENSOR_PATHS.get(key)
    if path is None:
        return True
    return _path_value(vehicle_data, path) is not None


def _path_value(data: dict[str, Any], path: str) -> Any:
    """Read a dotted path from nested dictionaries."""
    current: Any = data
    for part in path.split("."):
        if not isinstance(current, dict):
            return None
        current = current.get(part)
    return current
