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
from .entity_helpers import build_vehicle_display_name


@dataclass(frozen=True, kw_only=True)
class LeapmotorBinarySensorEntityDescription(BinarySensorEntityDescription):
    """Describes a Leapmotor binary sensor."""

    value_fn: Callable[[dict[str, Any]], Any]


BINARY_SENSOR_DESCRIPTIONS: tuple[LeapmotorBinarySensorEntityDescription, ...] = (
    LeapmotorBinarySensorEntityDescription(
        key="is_charging",
        name='Lädt',
        icon="mdi:ev-station",
        device_class=BinarySensorDeviceClass.BATTERY_CHARGING,
        value_fn=lambda data: data["charging"].get("is_charging"),
    ),
    LeapmotorBinarySensorEntityDescription(
        key="is_plugged_in",
        name='Ladekabel eingesteckt',
        icon="mdi:ev-plug-type2",
        device_class=BinarySensorDeviceClass.PLUG,
        value_fn=lambda data: data["charging"].get("is_plugged_in"),
    ),
    LeapmotorBinarySensorEntityDescription(
        key="is_regening",
        name='Rekuperation',
        icon="mdi:car-electric",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["charging"].get("is_regening"),
    ),
    LeapmotorBinarySensorEntityDescription(
        key="dc_cable_connected",
        name='DC-Ladekabel eingesteckt',
        icon="mdi:ev-plug-ccs2",
        device_class=BinarySensorDeviceClass.PLUG,
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["charging"].get("dc_cable_connected"),
    ),
    LeapmotorBinarySensorEntityDescription(
        key="charging_planned_enabled",
        name='Geplantes Laden',
        icon="mdi:calendar-clock",
        value_fn=lambda data: data["charging"].get("charging_planned_enabled"),
    ),
    LeapmotorBinarySensorEntityDescription(
        key="charging_planned_weekly",
        name='Ladeplanung wöchentlich',
        icon="mdi:calendar-week",
        value_fn=lambda data: (data["charging"].get("charging_planned_cycles") or "") == "1,1,1,1,1,1,1",
    ),
    LeapmotorBinarySensorEntityDescription(
        key="remote_session_active",
        name='Remote-Session aktiv',
        icon="mdi:remote",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["diagnostics"].get("remote_session_active"),
    ),
    LeapmotorBinarySensorEntityDescription(
        key="vehicle_security_active",
        name='Fahrzeugsicherung aktiv',
        icon="mdi:shield-car",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["diagnostics"].get("vehicle_security_active"),
    ),
    LeapmotorBinarySensorEntityDescription(
        key="battery_heating",
        name='Batterieheizung',
        icon="mdi:battery-heart",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["diagnostics"].get("battery_heating"),
    ),
    LeapmotorBinarySensorEntityDescription(
        key="driver_door_open",
        name='Fahrertür',
        device_class=BinarySensorDeviceClass.DOOR,
        value_fn=lambda data: data["diagnostics"].get("driver_door_open"),
    ),
    LeapmotorBinarySensorEntityDescription(
        key="passenger_door_open",
        name='Beifahrertür',
        device_class=BinarySensorDeviceClass.DOOR,
        value_fn=lambda data: data["diagnostics"].get("passenger_door_open"),
    ),
    LeapmotorBinarySensorEntityDescription(
        key="rear_left_door_open",
        name='Fondtür Fahrerseite',
        device_class=BinarySensorDeviceClass.DOOR,
        value_fn=lambda data: data["diagnostics"].get("rear_left_door_open"),
    ),
    LeapmotorBinarySensorEntityDescription(
        key="rear_right_door_open",
        name='Fondtür Beifahrerseite',
        device_class=BinarySensorDeviceClass.DOOR,
        value_fn=lambda data: data["diagnostics"].get("rear_right_door_open"),
    ),
    LeapmotorBinarySensorEntityDescription(
        key="trunk_open",
        name='Kofferraum',
        device_class=BinarySensorDeviceClass.OPENING,
        value_fn=lambda data: data["diagnostics"].get("trunk_open"),
    ),
    LeapmotorBinarySensorEntityDescription(
        key="front_left_window_open",
        name='Fenster vorne links',
        device_class=BinarySensorDeviceClass.WINDOW,
        value_fn=lambda data: data["diagnostics"].get("front_left_window_open"),
    ),
    LeapmotorBinarySensorEntityDescription(
        key="front_right_window_open",
        name='Fenster vorne rechts',
        device_class=BinarySensorDeviceClass.WINDOW,
        value_fn=lambda data: data["diagnostics"].get("front_right_window_open"),
    ),
    LeapmotorBinarySensorEntityDescription(
        key="rear_left_window_open",
        name='Fenster hinten links',
        device_class=BinarySensorDeviceClass.WINDOW,
        value_fn=lambda data: data["diagnostics"].get("rear_left_window_open"),
    ),
    LeapmotorBinarySensorEntityDescription(
        key="rear_right_window_open",
        name='Fenster hinten rechts',
        device_class=BinarySensorDeviceClass.WINDOW,
        value_fn=lambda data: data["diagnostics"].get("rear_right_window_open"),
    ),
    LeapmotorBinarySensorEntityDescription(
        key="skylight_open",
        name='Panoramadach geöffnet',
        icon="mdi:window-open",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["diagnostics"].get("skylight_open"),
    ),
    LeapmotorBinarySensorEntityDescription(
        key="climate_on",
        name='Klima',
        icon="mdi:air-conditioner",
        value_fn=lambda data: data["diagnostics"].get("climate_on"),
    ),
    LeapmotorBinarySensorEntityDescription(
        key="fast_cooling_active",
        name='Schnelles Kühlen aktiv',
        icon="mdi:snowflake",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["diagnostics"].get("fast_cooling_active"),
    ),
    LeapmotorBinarySensorEntityDescription(
        key="fast_heating_active",
        name='Schnelles Heizen aktiv',
        icon="mdi:heat-wave",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["diagnostics"].get("fast_heating_active"),
    ),
    LeapmotorBinarySensorEntityDescription(
        key="windshield_defrosting",
        name='Frontscheibenheizung',
        icon="mdi:car-defrost-front",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["diagnostics"].get("windshield_defrosting"),
    ),
    LeapmotorBinarySensorEntityDescription(
        key="rear_window_heating",
        name='Heckscheibenheizung',
        icon="mdi:car-defrost-rear",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["diagnostics"].get("rear_window_heating"),
    ),
    LeapmotorBinarySensorEntityDescription(
        key="air_recirculation",
        name='Umluft',
        icon="mdi:air-filter",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["diagnostics"].get("air_recirculation"),
    ),
    LeapmotorBinarySensorEntityDescription(
        key="steering_wheel_heating",
        name='Lenkradheizung',
        icon="mdi:steering",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["diagnostics"].get("steering_wheel_heating"),
    ),
    LeapmotorBinarySensorEntityDescription(
        key="left_mirror_heating",
        name='Spiegelheizung links',
        icon="mdi:car-side",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["diagnostics"].get("left_mirror_heating"),
    ),
    LeapmotorBinarySensorEntityDescription(
        key="right_mirror_heating",
        name='Spiegelheizung rechts',
        icon="mdi:car-side",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["diagnostics"].get("right_mirror_heating"),
    ),
    LeapmotorBinarySensorEntityDescription(
        key="speed_limit_enabled",
        name='Geschwindigkeitslimit aktiv',
        icon="mdi:speedometer",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["diagnostics"].get("speed_limit_enabled"),
    ),
    LeapmotorBinarySensorEntityDescription(
        key="park_assist_enabled",
        name='Parkassistent aktiv',
        icon="mdi:car-brake-parking",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["diagnostics"].get("park_assist_enabled"),
    ),
    LeapmotorBinarySensorEntityDescription(
        key="sentinel_mode",
        name='Sentinel-Modus',
        icon="mdi:shield-car",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["diagnostics"].get("sentinel_mode"),
    ),
    LeapmotorBinarySensorEntityDescription(
        key="parking_photo",
        name='Parking Photo',
        icon="mdi:camera",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["diagnostics"].get("parking_photo"),
    ),
    LeapmotorBinarySensorEntityDescription(
        key="fully_charged",
        name='Voll geladen',
        icon="mdi:battery-check",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["diagnostics"].get("fully_charged"),
    ),
)


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up Leapmotor binary sensors."""
    coordinator: LeapmotorDataUpdateCoordinator = hass.data[DOMAIN][entry.entry_id]
    entities: list[LeapmotorBinarySensor] = []
    for vin in coordinator.data.get("vehicles", {}):
        entities.extend(
            LeapmotorBinarySensor(coordinator, vin, description)
            for description in BINARY_SENSOR_DESCRIPTIONS
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
    ) -> None:
        super().__init__(coordinator)
        self.vin = vin
        self.entity_description = description
        self._attr_unique_id = f"{vin}_{description.key}"
        vehicle = self.vehicle_data["vehicle"]
        sensor_name = description.name or description.key
        self._attr_has_entity_name = True
        self._attr_name = sensor_name
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
