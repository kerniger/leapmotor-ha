"""Sensor entities for Leapmotor."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, UTC
from typing import Any

from homeassistant.components.sensor import (
    SensorDeviceClass,
    SensorEntity,
    SensorEntityDescription,
    SensorStateClass,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import (
    EntityCategory,
    PERCENTAGE,
    UnitOfElectricCurrent,
    UnitOfElectricPotential,
    UnitOfLength,
    UnitOfPower,
    UnitOfSpeed,
    UnitOfTemperature,
    UnitOfTime,
)
from homeassistant.core import HomeAssistant
from homeassistant.helpers.device_registry import DeviceInfo
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from .const import DOMAIN
from .coordinator import LeapmotorDataUpdateCoordinator
from .entity_helpers import build_vehicle_display_name

PRESSURE_BAR = "bar"
ENERGY_KWH = "kWh"
CONSUMPTION_KWH_PER_100KM = "kWh/100 km"
WHOLE_KILOMETER_KEYS = {
    "remaining_range_km",
    "cltc_range_km",
    "live_remaining_range_km",
    "odometer_km",
    "total_mileage_km",
    "last_7_days_mileage_km",
}


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
        key="battery_percent_precise",
        name="Batterie präzise",
        native_unit_of_measurement=PERCENTAGE,
        device_class=SensorDeviceClass.BATTERY,
        state_class=SensorStateClass.MEASUREMENT,
        suggested_display_precision=1,
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["status"].get("battery_percent_precise"),
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
        key="wltp_max_range_km",
        name="CLTC Restreichweite",
        native_unit_of_measurement=UnitOfLength.KILOMETERS,
        device_class=SensorDeviceClass.DISTANCE,
        state_class=SensorStateClass.MEASUREMENT,
        suggested_display_precision=0,
        icon="mdi:map-marker-radius",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["status"].get("wltp_max_range_km"),
    ),
    LeapmotorSensorEntityDescription(
        key="live_remaining_range_km",
        name="Live-Reichweite",
        native_unit_of_measurement=UnitOfLength.KILOMETERS,
        device_class=SensorDeviceClass.DISTANCE,
        state_class=SensorStateClass.MEASUREMENT,
        suggested_display_precision=0,
        icon="mdi:map-marker-distance",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["status"].get("live_remaining_range_km"),
    ),
    LeapmotorSensorEntityDescription(
        key="range_mode",
        name="Reichweitenmodus",
        icon="mdi:map-clock",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["status"].get("range_mode"),
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
        key="speed_kmh",
        name="Geschwindigkeit",
        native_unit_of_measurement=UnitOfSpeed.KILOMETERS_PER_HOUR,
        device_class=SensorDeviceClass.SPEED,
        state_class=SensorStateClass.MEASUREMENT,
        suggested_display_precision=0,
        icon="mdi:speedometer",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["status"].get("speed_kmh"),
    ),
    LeapmotorSensorEntityDescription(
        key="gear",
        name="Gang",
        icon="mdi:car-shift-pattern",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["status"].get("gear"),
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
        key="remaining_charge_minutes",
        name="Restladezeit",
        native_unit_of_measurement=UnitOfTime.MINUTES,
        device_class=SensorDeviceClass.DURATION,
        state_class=SensorStateClass.MEASUREMENT,
        icon="mdi:timer-outline",
        value_fn=lambda data: data["charging"].get("remaining_charge_minutes"),
    ),
    LeapmotorSensorEntityDescription(
        key="charging_power_kw",
        name="Ladeleistung",
        native_unit_of_measurement=UnitOfPower.KILO_WATT,
        device_class=SensorDeviceClass.POWER,
        state_class=SensorStateClass.MEASUREMENT,
        suggested_display_precision=1,
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["charging"].get("charging_power_kw"),
    ),
    LeapmotorSensorEntityDescription(
        key="charging_current_a",
        name="Ladestrom",
        native_unit_of_measurement=UnitOfElectricCurrent.AMPERE,
        device_class=SensorDeviceClass.CURRENT,
        state_class=SensorStateClass.MEASUREMENT,
        suggested_display_precision=1,
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["charging"].get("charging_current_a"),
    ),
    LeapmotorSensorEntityDescription(
        key="charging_voltage_v",
        name="Ladespannung",
        native_unit_of_measurement=UnitOfElectricPotential.VOLT,
        device_class=SensorDeviceClass.VOLTAGE,
        state_class=SensorStateClass.MEASUREMENT,
        suggested_display_precision=1,
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["charging"].get("charging_voltage_v"),
    ),
    LeapmotorSensorEntityDescription(
        key="charging_connection_state",
        name="Ladeverbindung",
        icon="mdi:ev-plug-type2",
        value_fn=lambda data: data["charging"].get("connection_state"),
    ),
    LeapmotorSensorEntityDescription(
        key="battery_min_temp_c",
        name="Batterie Minimaltemperatur",
        native_unit_of_measurement=UnitOfTemperature.CELSIUS,
        device_class=SensorDeviceClass.TEMPERATURE,
        state_class=SensorStateClass.MEASUREMENT,
        icon="mdi:thermometer-low",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["diagnostics"].get("battery_min_temp_c"),
    ),
    LeapmotorSensorEntityDescription(
        key="battery_thermal_request",
        name="Batterie Thermomanagement",
        state_class=SensorStateClass.MEASUREMENT,
        icon="mdi:battery-heart",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["diagnostics"].get("battery_thermal_request"),
    ),
    LeapmotorSensorEntityDescription(
        key="ptc_power_w",
        name="PTC-Leistung",
        native_unit_of_measurement=UnitOfPower.WATT,
        device_class=SensorDeviceClass.POWER,
        state_class=SensorStateClass.MEASUREMENT,
        icon="mdi:heat-wave",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["diagnostics"].get("ptc_power_w"),
    ),
    LeapmotorSensorEntityDescription(
        key="parking_camera_state",
        name="Parkkamera-Status",
        icon="mdi:camera-rear",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["diagnostics"].get("parking_camera_state"),
    ),
    LeapmotorSensorEntityDescription(
        key="charging_planned_start",
        name="Ladeplanung Start",
        icon="mdi:clock-start",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["charging"].get("charging_planned_start"),
    ),
    LeapmotorSensorEntityDescription(
        key="charging_planned_end",
        name="Ladeplanung Ende",
        icon="mdi:clock-end",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["charging"].get("charging_planned_end"),
    ),
    LeapmotorSensorEntityDescription(
        key="charging_planned_circulation",
        name="Ladeplanung Wiederholung",
        icon="mdi:calendar-sync",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["charging"].get("charging_planned_circulation"),
    ),
    LeapmotorSensorEntityDescription(
        key="charging_plan_updated_at",
        name="Ladeplanung aktualisiert",
        icon="mdi:calendar-edit",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["charging"].get("charging_plan_updated_at"),
    ),
    LeapmotorSensorEntityDescription(
        key="vehicle_state",
        name="Fahrzeugstatus",
        icon="mdi:car-info",
        value_fn=lambda data: data["status"].get("vehicle_state"),
    ),
    LeapmotorSensorEntityDescription(
        key="climate_mode",
        name="Klimamodus",
        icon="mdi:air-conditioner",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["diagnostics"].get("climate_mode"),
    ),
    LeapmotorSensorEntityDescription(
        key="steering_wheel_heating_remaining_minutes",
        name="Lenkradheizung Restlaufzeit",
        native_unit_of_measurement=UnitOfTime.MINUTES,
        device_class=SensorDeviceClass.DURATION,
        state_class=SensorStateClass.MEASUREMENT,
        icon="mdi:steering",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["diagnostics"].get("steering_wheel_heating_remaining_minutes"),
    ),
    LeapmotorSensorEntityDescription(
        key="driver_seat_heating_level",
        name="Sitzheizung Fahrer Stufe",
        state_class=SensorStateClass.MEASUREMENT,
        icon="mdi:car-seat-heater",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["diagnostics"].get("driver_seat_heating_level"),
    ),
    LeapmotorSensorEntityDescription(
        key="passenger_seat_heating_level",
        name="Sitzheizung Beifahrer Stufe",
        state_class=SensorStateClass.MEASUREMENT,
        icon="mdi:car-seat-heater",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["diagnostics"].get("passenger_seat_heating_level"),
    ),
    LeapmotorSensorEntityDescription(
        key="driver_seat_ventilation_level",
        name="Sitzlüftung Fahrer Stufe",
        state_class=SensorStateClass.MEASUREMENT,
        icon="mdi:car-seat-cooler",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["diagnostics"].get("driver_seat_ventilation_level"),
    ),
    LeapmotorSensorEntityDescription(
        key="passenger_seat_ventilation_level",
        name="Sitzlüftung Beifahrer Stufe",
        state_class=SensorStateClass.MEASUREMENT,
        icon="mdi:car-seat-cooler",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["diagnostics"].get("passenger_seat_ventilation_level"),
    ),
    LeapmotorSensorEntityDescription(
        key="speed_limit_kmh",
        name="Geschwindigkeitslimit",
        native_unit_of_measurement=UnitOfSpeed.KILOMETERS_PER_HOUR,
        device_class=SensorDeviceClass.SPEED,
        state_class=SensorStateClass.MEASUREMENT,
        icon="mdi:speedometer",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["diagnostics"].get("speed_limit_kmh"),
    ),
    LeapmotorSensorEntityDescription(
        key="front_left_window_position_percent",
        name="Fenster vorne links Position",
        native_unit_of_measurement=PERCENTAGE,
        state_class=SensorStateClass.MEASUREMENT,
        icon="mdi:window-open-variant",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["diagnostics"].get("front_left_window_position_percent"),
    ),
    LeapmotorSensorEntityDescription(
        key="front_right_window_position_percent",
        name="Fenster vorne rechts Position",
        native_unit_of_measurement=PERCENTAGE,
        state_class=SensorStateClass.MEASUREMENT,
        icon="mdi:window-open-variant",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["diagnostics"].get("front_right_window_position_percent"),
    ),
    LeapmotorSensorEntityDescription(
        key="rear_left_window_position_percent",
        name="Fenster hinten links Position",
        native_unit_of_measurement=PERCENTAGE,
        state_class=SensorStateClass.MEASUREMENT,
        icon="mdi:window-open-variant",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["diagnostics"].get("rear_left_window_position_percent"),
    ),
    LeapmotorSensorEntityDescription(
        key="rear_right_window_position_percent",
        name="Fenster hinten rechts Position",
        native_unit_of_measurement=PERCENTAGE,
        state_class=SensorStateClass.MEASUREMENT,
        icon="mdi:window-open-variant",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["diagnostics"].get("rear_right_window_position_percent"),
    ),
    LeapmotorSensorEntityDescription(
        key="last_successful_refresh",
        name="Zuletzt aktualisiert",
        device_class=SensorDeviceClass.TIMESTAMP,
        entity_category=EntityCategory.DIAGNOSTIC,
        icon="mdi:clock-check-outline",
        value_fn=lambda data: _coordinator_timestamp(
            data.get("_integration", {}).get("last_successful_update_at")
        ),
    ),
    LeapmotorSensorEntityDescription(
        key="total_mileage_km",
        name="Gesamtkilometer",
        native_unit_of_measurement=UnitOfLength.KILOMETERS,
        device_class=SensorDeviceClass.DISTANCE,
        state_class=SensorStateClass.TOTAL_INCREASING,
        suggested_display_precision=0,
        icon="mdi:chart-line",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["history"].get("total_mileage_km"),
    ),
    LeapmotorSensorEntityDescription(
        key="total_energy_kwh",
        name="Gesamtenergieverbrauch",
        native_unit_of_measurement=ENERGY_KWH,
        device_class=SensorDeviceClass.ENERGY,
        state_class=SensorStateClass.TOTAL_INCREASING,
        suggested_display_precision=0,
        icon="mdi:lightning-bolt",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["history"].get("total_energy_kwh"),
    ),
    LeapmotorSensorEntityDescription(
        key="last_7_days_mileage_km",
        name="Fahrleistung letzte 7 Tage",
        native_unit_of_measurement=UnitOfLength.KILOMETERS,
        device_class=SensorDeviceClass.DISTANCE,
        state_class=SensorStateClass.MEASUREMENT,
        suggested_display_precision=0,
        icon="mdi:calendar-week",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["history"].get("last_7_days_mileage_km"),
    ),
    LeapmotorSensorEntityDescription(
        key="last_7_days_energy_kwh",
        name="Energieverbrauch letzte 7 Tage",
        native_unit_of_measurement=ENERGY_KWH,
        state_class=SensorStateClass.MEASUREMENT,
        suggested_display_precision=1,
        icon="mdi:calendar-week",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["history"].get("last_7_days_energy_kwh"),
    ),
    LeapmotorSensorEntityDescription(
        key="average_consumption_6w_kwh_100km",
        name="Durchschnittsverbrauch 6 Wochen",
        native_unit_of_measurement=CONSUMPTION_KWH_PER_100KM,
        state_class=SensorStateClass.MEASUREMENT,
        suggested_display_precision=1,
        icon="mdi:chart-bar",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["history"].get("average_consumption_6w_kwh_100km"),
    ),
    LeapmotorSensorEntityDescription(
        key="last_week_driving_energy_percent",
        name="Fahrenergie letzte Woche",
        native_unit_of_measurement=PERCENTAGE,
        state_class=SensorStateClass.MEASUREMENT,
        suggested_display_precision=1,
        icon="mdi:car-electric",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["history"].get("last_week_driving_energy_percent"),
    ),
    LeapmotorSensorEntityDescription(
        key="last_week_climate_energy_percent",
        name="Klimaenergie letzte Woche",
        native_unit_of_measurement=PERCENTAGE,
        state_class=SensorStateClass.MEASUREMENT,
        suggested_display_precision=1,
        icon="mdi:air-conditioner",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["history"].get("last_week_climate_energy_percent"),
    ),
    LeapmotorSensorEntityDescription(
        key="last_week_other_energy_percent",
        name="Sonstige Energie letzte Woche",
        native_unit_of_measurement=PERCENTAGE,
        state_class=SensorStateClass.MEASUREMENT,
        suggested_display_precision=1,
        icon="mdi:lightning-bolt-outline",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["history"].get("last_week_other_energy_percent"),
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
    def available(self) -> bool:
        """Return entity availability."""
        if not super().available:
            return False
        if self.entity_description.key == "remaining_charge_minutes":
            return bool(self.vehicle_data["charging"].get("is_charging"))
        return True

    @property
    def native_value(self) -> Any:
        """Return the sensor value."""
        if self.entity_description.key == "last_successful_refresh":
            return _coordinator_timestamp(
                self.coordinator.integration_status.get("last_successful_update_at")
            )
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
                    "vehicle_state_source": status.get("vehicle_state_source", "cloud"),
                    "stale_vehicle_state": status.get("stale_vehicle_state"),
                    "vehicle_state_age_seconds": status.get("vehicle_state_age_seconds"),
                    "vehicle_state_is_stale": status.get("vehicle_state_is_stale"),
                    "raw_charge_status_code": status.get("raw_charge_status_code"),
                    "raw_drive_status_code": status.get("raw_drive_status_code"),
                    "raw_vehicle_state_code": status.get("raw_vehicle_state_code"),
                    "raw_parked_status_code": status.get("raw_parked_status_code"),
                }
            )
        if self.entity_description.key == "last_successful_refresh":
            integration = self.coordinator.integration_status
            attributes.update(
                {
                    "last_update_status": integration.get("last_update_status"),
                    "last_update_reason": integration.get("last_update_reason"),
                    "last_update_error_code": integration.get("last_update_error_code"),
                    "last_update_duration_seconds": integration.get("last_update_duration_seconds"),
                    "update_interval_seconds": integration.get("update_interval_seconds"),
                }
            )
        if self.entity_description.key == "average_consumption_6w_kwh_100km":
            history = self.vehicle_data["history"]
            attributes.update(
                {
                    "consumption_rank": history.get("consumption_rank"),
                    "weekly_consumption": history.get("weekly_consumption"),
                }
            )
        if self.entity_description.key in {
            "last_week_driving_energy_percent",
            "last_week_climate_energy_percent",
            "last_week_other_energy_percent",
        }:
            history = self.vehicle_data["history"]
            attributes.update(
                {
                    "driving_energy_kwh": history.get("last_week_driving_energy_kwh"),
                    "climate_energy_kwh": history.get("last_week_climate_energy_kwh"),
                    "other_energy_kwh": history.get("last_week_other_energy_kwh"),
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


def _coordinator_timestamp(value: Any) -> datetime | None:
    """Return a timezone-aware datetime from a UNIX timestamp."""
    if value is None:
        return None
    try:
        return datetime.fromtimestamp(float(value), tz=UTC)
    except (TypeError, ValueError, OSError):
        return None
