"""Sensor entities for Leapmotor."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from homeassistant.components.sensor import (
    SensorDeviceClass,
    SensorEntity,
    SensorEntityDescription,
    SensorStateClass,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import EntityCategory, PERCENTAGE, UnitOfLength, UnitOfTemperature
from homeassistant.core import HomeAssistant
from homeassistant.helpers.device_registry import DeviceInfo
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from .const import DOMAIN
from .coordinator import LeapmotorDataUpdateCoordinator
from .entity_helpers import build_vehicle_display_name

PRESSURE_BAR = "bar"
WHOLE_KILOMETER_KEYS = {"remaining_range_km", "odometer_km"}


@dataclass(frozen=True, kw_only=True)
class LeapmotorSensorEntityDescription(SensorEntityDescription):
    """Describes a Leapmotor sensor."""

    value_fn: Callable[[dict[str, Any]], Any]


SENSOR_DESCRIPTIONS: tuple[LeapmotorSensorEntityDescription, ...] = (
    LeapmotorSensorEntityDescription(
        key="battery_percent",
        name="Batterie",
        native_unit_of_measurement=PERCENTAGE,
        device_class=SensorDeviceClass.BATTERY,
        state_class=SensorStateClass.MEASUREMENT,
        value_fn=lambda data: data["status"].get("battery_percent"),
    ),
    LeapmotorSensorEntityDescription(
        key="remaining_range_km",
        name="Reichweite",
        native_unit_of_measurement=UnitOfLength.KILOMETERS,
        device_class=SensorDeviceClass.DISTANCE,
        state_class=SensorStateClass.MEASUREMENT,
        suggested_display_precision=0,
        icon="mdi:map-marker-distance",
        value_fn=lambda data: data["status"].get("remaining_range_km"),
    ),
    LeapmotorSensorEntityDescription(
        key="odometer_km",
        name="Kilometerstand",
        native_unit_of_measurement=UnitOfLength.KILOMETERS,
        device_class=SensorDeviceClass.DISTANCE,
        state_class=SensorStateClass.TOTAL_INCREASING,
        suggested_display_precision=0,
        icon="mdi:counter",
        value_fn=lambda data: data["status"].get("odometer_km"),
    ),
    LeapmotorSensorEntityDescription(
        key="interior_temp_c",
        name="Innentemperatur",
        native_unit_of_measurement=UnitOfTemperature.CELSIUS,
        device_class=SensorDeviceClass.TEMPERATURE,
        state_class=SensorStateClass.MEASUREMENT,
        value_fn=lambda data: data["status"].get("interior_temp_c"),
    ),
    LeapmotorSensorEntityDescription(
        key="climate_set_temp_left_c",
        name="Solltemperatur links",
        native_unit_of_measurement=UnitOfTemperature.CELSIUS,
        device_class=SensorDeviceClass.TEMPERATURE,
        state_class=SensorStateClass.MEASUREMENT,
        value_fn=lambda data: data["status"].get("climate_set_temp_left_c"),
    ),
    LeapmotorSensorEntityDescription(
        key="climate_set_temp_right_c",
        name="Solltemperatur rechts",
        native_unit_of_measurement=UnitOfTemperature.CELSIUS,
        device_class=SensorDeviceClass.TEMPERATURE,
        state_class=SensorStateClass.MEASUREMENT,
        value_fn=lambda data: data["status"].get("climate_set_temp_right_c"),
    ),
    LeapmotorSensorEntityDescription(
        key="charge_limit_percent",
        name="Ladelimit",
        native_unit_of_measurement=PERCENTAGE,
        state_class=SensorStateClass.MEASUREMENT,
        icon="mdi:battery-sync",
        value_fn=lambda data: data["charging"].get("charge_limit_percent"),
    ),
    LeapmotorSensorEntityDescription(
        key="vehicle_state",
        name="Fahrzeugstatus",
        icon="mdi:car-info",
        value_fn=lambda data: data["status"].get("vehicle_state"),
    ),
    LeapmotorSensorEntityDescription(
        key="last_remote_control",
        name="Letzte Fahrzeugaktion",
        icon="mdi:remote",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: (data.get("remote_control") or {}).get("status"),
    ),
    LeapmotorSensorEntityDescription(
        key="tire_pressure_front_left_bar",
        name="Reifendruck vorne links",
        native_unit_of_measurement=PRESSURE_BAR,
        device_class=SensorDeviceClass.PRESSURE,
        state_class=SensorStateClass.MEASUREMENT,
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["diagnostics"].get("tire_pressure_front_left_bar"),
    ),
    LeapmotorSensorEntityDescription(
        key="tire_pressure_front_right_bar",
        name="Reifendruck vorne rechts",
        native_unit_of_measurement=PRESSURE_BAR,
        device_class=SensorDeviceClass.PRESSURE,
        state_class=SensorStateClass.MEASUREMENT,
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["diagnostics"].get("tire_pressure_front_right_bar"),
    ),
    LeapmotorSensorEntityDescription(
        key="tire_pressure_rear_left_bar",
        name="Reifendruck hinten links",
        native_unit_of_measurement=PRESSURE_BAR,
        device_class=SensorDeviceClass.PRESSURE,
        state_class=SensorStateClass.MEASUREMENT,
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["diagnostics"].get("tire_pressure_rear_left_bar"),
    ),
    LeapmotorSensorEntityDescription(
        key="tire_pressure_rear_right_bar",
        name="Reifendruck hinten rechts",
        native_unit_of_measurement=PRESSURE_BAR,
        device_class=SensorDeviceClass.PRESSURE,
        state_class=SensorStateClass.MEASUREMENT,
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["diagnostics"].get("tire_pressure_rear_right_bar"),
    ),
)


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up Leapmotor sensors."""
    coordinator: LeapmotorDataUpdateCoordinator = hass.data[DOMAIN][entry.entry_id]
    entities: list[LeapmotorSensor] = []
    for vin in coordinator.data.get("vehicles", {}):
        entities.extend(
            LeapmotorSensor(coordinator, vin, description)
            for description in SENSOR_DESCRIPTIONS
        )
    async_add_entities(entities)


class LeapmotorSensor(CoordinatorEntity[LeapmotorDataUpdateCoordinator], SensorEntity):
    """Leapmotor sensor."""

    entity_description: LeapmotorSensorEntityDescription

    def __init__(
        self,
        coordinator: LeapmotorDataUpdateCoordinator,
        vin: str,
        description: LeapmotorSensorEntityDescription,
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
    def native_value(self) -> Any:
        """Return the sensor value."""
        value = self.entity_description.value_fn(self.vehicle_data)
        if self.entity_description.key in WHOLE_KILOMETER_KEYS:
            return _whole_number_if_possible(value)
        return value

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        """Return useful vehicle metadata."""
        vehicle = self.vehicle_data["vehicle"]
        attributes = {
            "vin": self.vin,
            "car_id": vehicle.get("car_id"),
            "car_type": vehicle.get("car_type"),
            "is_shared": vehicle.get("is_shared"),
        }
        if self.entity_description.key == "vehicle_state":
            status = self.vehicle_data["status"]
            attributes.update(
                {
                    "is_parked": status.get("is_parked"),
                    "raw_charge_status_code": status.get("raw_charge_status_code"),
                    "raw_drive_status_code": status.get("raw_drive_status_code"),
                    "raw_vehicle_state_code": status.get("raw_vehicle_state_code"),
                }
            )
        if self.entity_description.key == "last_remote_control":
            remote = self.vehicle_data.get("remote_control") or {}
            attributes.update(
                {
                    "action": remote.get("action"),
                    "success": remote.get("success"),
                    "updated_at": remote.get("updated_at"),
                    "api_code": remote.get("api_code"),
                    "api_message": remote.get("api_message"),
                    "remote_ctl_id": remote.get("remote_ctl_id"),
                    "error": remote.get("error"),
                }
            )
        return attributes


def _whole_number_if_possible(value: Any) -> Any:
    """Return int for whole numeric values to avoid unnecessary .00 display."""
    if value is None:
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return value
    if numeric.is_integer():
        return int(numeric)
    return value
