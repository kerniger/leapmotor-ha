"""Sensor entities for Leapmotor."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta, UTC
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
from .entity_helpers import build_vehicle_display_name, load_localized_entity_names
from .entity_migration import english_entity_slug

PRESSURE_BAR = "bar"
ENERGY_KWH = "kWh"
CONSUMPTION_KWH_PER_100KM = "kWh/100 km"
CONSUMPTION_MI_PER_KWH = "mi/kWh"
WHOLE_KILOMETER_KEYS = {
    "remaining_range_km",
    "cltc_range_km",
    "live_remaining_range_km",
    "odometer_km",
    "total_mileage_km",
    "last_7_days_mileage_km",
}
OPTIONAL_SENSOR_PATHS = {
    "battery_percent_precise": "status.battery_percent_precise",
    "wltp_max_range_km": "status.wltp_max_range_km",
    "live_remaining_range_km": "status.live_remaining_range_km",
    "range_mode": "status.range_mode",
    "interior_temp_c": "status.interior_temp_c",
    "climate_set_temp_left_c": "status.climate_set_temp_left_c",
    "climate_set_temp_right_c": "status.climate_set_temp_right_c",
    "battery_min_temp_c": "diagnostics.battery_min_temp_c",
    "battery_thermal_request": "diagnostics.battery_thermal_request",
    "ptc_power_w": "diagnostics.ptc_power_w",
    "ptc_state": "diagnostics.ptc_state",
    "ptc_power_setting_value": "diagnostics.ptc_power_setting_value",
    "available_energy_kwh": "diagnostics.available_energy_kwh",
    "charging_planned_start": "charging.charging_planned_start",
    "charging_planned_end": "charging.charging_planned_end",
    "charging_planned_circulation": "charging.charging_planned_circulation",
    "charging_plan_updated_at": "charging.charging_plan_updated_at",
    "lock_state_age_seconds": "status.lock_state_age_seconds",
    "raw_lock_status_code": "status.raw_lock_status_code",
    "climate_mode": "diagnostics.climate_mode",
    "outdoor_temp_c": "diagnostics.outdoor_temp_c",
    "climate_fan_volume": "diagnostics.climate_fan_volume",
    "climate_fan_volume_setting": "diagnostics.climate_fan_volume_setting",
    "climate_air_direction": "diagnostics.climate_air_direction",
    "climate_cooling_heating_mode": "diagnostics.climate_cooling_heating_mode",
    "climate_min_single_temp_c": "diagnostics.climate_min_single_temp_c",
    "sunshade_position": "diagnostics.sunshade_position",
    "steering_wheel_heating_remaining_minutes": (
        "diagnostics.steering_wheel_heating_remaining_minutes"
    ),
    "driver_seat_heating_level": "diagnostics.driver_seat_heating_level",
    "passenger_seat_heating_level": "diagnostics.passenger_seat_heating_level",
    "driver_seat_ventilation_level": "diagnostics.driver_seat_ventilation_level",
    "passenger_seat_ventilation_level": "diagnostics.passenger_seat_ventilation_level",
    "speed_limit_kmh": "diagnostics.speed_limit_kmh",
    "average_consumption_6w_mi_kwh": "history.average_consumption_6w_mi_kwh",
    "last_charge_energy_kwh": "charging_history.last_charge_energy_kwh",
}


@dataclass(frozen=True, kw_only=True)
class LeapmotorSensorEntityDescription(SensorEntityDescription):
    """Describes a Leapmotor sensor."""

    value_fn: Callable[[dict[str, Any]], Any]
    extra_attrs_fn: Callable[[dict[str, Any]], dict[str, Any]] | None = None


SENSOR_DESCRIPTIONS: tuple[LeapmotorSensorEntityDescription, ...] = (
    LeapmotorSensorEntityDescription(
        key="battery_percent",
        translation_key="battery_percent",
        native_unit_of_measurement=PERCENTAGE,
        device_class=SensorDeviceClass.BATTERY,
        state_class=SensorStateClass.MEASUREMENT,
        value_fn=lambda data: data["status"].get("battery_percent"),
    ),
    LeapmotorSensorEntityDescription(
        key="battery_percent_precise",
        translation_key="battery_percent_precise",
        native_unit_of_measurement=PERCENTAGE,
        device_class=SensorDeviceClass.BATTERY,
        state_class=SensorStateClass.MEASUREMENT,
        suggested_display_precision=1,
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["status"].get("battery_percent_precise"),
    ),
    LeapmotorSensorEntityDescription(
        key="remaining_range_km",
        translation_key="remaining_range_km",
        native_unit_of_measurement=UnitOfLength.KILOMETERS,
        device_class=SensorDeviceClass.DISTANCE,
        state_class=SensorStateClass.MEASUREMENT,
        suggested_display_precision=0,
        icon="mdi:map-marker-distance",
        value_fn=lambda data: data["status"].get("remaining_range_km"),
    ),
    LeapmotorSensorEntityDescription(
        key="wltp_max_range_km",
        translation_key="wltp_max_range_km",
        native_unit_of_measurement=UnitOfLength.KILOMETERS,
        device_class=SensorDeviceClass.DISTANCE,
        state_class=SensorStateClass.MEASUREMENT,
        suggested_display_precision=0,
        icon="mdi:map-marker-radius",
        entity_category=EntityCategory.DIAGNOSTIC,
        entity_registry_enabled_default=False,
        value_fn=lambda data: data["status"].get("wltp_max_range_km"),
    ),
    LeapmotorSensorEntityDescription(
        key="live_remaining_range_km",
        translation_key="live_remaining_range_km",
        native_unit_of_measurement=UnitOfLength.KILOMETERS,
        device_class=SensorDeviceClass.DISTANCE,
        state_class=SensorStateClass.MEASUREMENT,
        suggested_display_precision=0,
        icon="mdi:map-marker-distance",
        entity_category=EntityCategory.DIAGNOSTIC,
        entity_registry_enabled_default=False,
        value_fn=lambda data: data["status"].get("live_remaining_range_km"),
    ),
    LeapmotorSensorEntityDescription(
        key="range_mode",
        translation_key="range_mode",
        icon="mdi:map-clock",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["status"].get("range_mode"),
    ),
    LeapmotorSensorEntityDescription(
        key="odometer_km",
        translation_key="odometer_km",
        native_unit_of_measurement=UnitOfLength.KILOMETERS,
        device_class=SensorDeviceClass.DISTANCE,
        state_class=SensorStateClass.TOTAL_INCREASING,
        suggested_display_precision=0,
        icon="mdi:counter",
        value_fn=lambda data: data["status"].get("odometer_km"),
    ),
    LeapmotorSensorEntityDescription(
        key="speed_kmh",
        translation_key="speed_kmh",
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
        translation_key="gear",
        icon="mdi:car-shift-pattern",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["status"].get("gear"),
    ),
    LeapmotorSensorEntityDescription(
        key="interior_temp_c",
        translation_key="interior_temp_c",
        native_unit_of_measurement=UnitOfTemperature.CELSIUS,
        device_class=SensorDeviceClass.TEMPERATURE,
        state_class=SensorStateClass.MEASUREMENT,
        value_fn=lambda data: data["status"].get("interior_temp_c"),
    ),
    LeapmotorSensorEntityDescription(
        key="climate_set_temp_left_c",
        translation_key="climate_set_temp_left_c",
        native_unit_of_measurement=UnitOfTemperature.CELSIUS,
        device_class=SensorDeviceClass.TEMPERATURE,
        state_class=SensorStateClass.MEASUREMENT,
        value_fn=lambda data: data["status"].get("climate_set_temp_left_c"),
    ),
    LeapmotorSensorEntityDescription(
        key="climate_set_temp_right_c",
        translation_key="climate_set_temp_right_c",
        native_unit_of_measurement=UnitOfTemperature.CELSIUS,
        device_class=SensorDeviceClass.TEMPERATURE,
        state_class=SensorStateClass.MEASUREMENT,
        value_fn=lambda data: data["status"].get("climate_set_temp_right_c"),
    ),
    LeapmotorSensorEntityDescription(
        key="charge_limit_percent",
        translation_key="charge_limit_percent",
        native_unit_of_measurement=PERCENTAGE,
        state_class=SensorStateClass.MEASUREMENT,
        icon="mdi:battery-sync",
        value_fn=lambda data: data["charging"].get("charge_limit_percent"),
    ),
    LeapmotorSensorEntityDescription(
        key="remaining_charge_minutes",
        translation_key="remaining_charge_minutes",
        native_unit_of_measurement=UnitOfTime.MINUTES,
        device_class=SensorDeviceClass.DURATION,
        state_class=SensorStateClass.MEASUREMENT,
        icon="mdi:timer-outline",
        value_fn=lambda data: data["charging"].get("remaining_charge_minutes"),
    ),
    LeapmotorSensorEntityDescription(
        key="charging_finish_time",
        translation_key="charging_finish_time",
        device_class=SensorDeviceClass.TIMESTAMP,
        icon="mdi:battery-clock",
        value_fn=lambda data: _charging_finish_time(
            data["charging"].get("remaining_charge_minutes")
        ),
    ),
    LeapmotorSensorEntityDescription(
        key="charging_power_kw",
        translation_key="charging_power_kw",
        native_unit_of_measurement=UnitOfPower.KILO_WATT,
        device_class=SensorDeviceClass.POWER,
        state_class=SensorStateClass.MEASUREMENT,
        suggested_display_precision=1,
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["charging"].get("charging_power_kw"),
    ),
    LeapmotorSensorEntityDescription(
        key="charging_current_a",
        translation_key="charging_current_a",
        native_unit_of_measurement=UnitOfElectricCurrent.AMPERE,
        device_class=SensorDeviceClass.CURRENT,
        state_class=SensorStateClass.MEASUREMENT,
        suggested_display_precision=1,
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["charging"].get("charging_current_a"),
    ),
    LeapmotorSensorEntityDescription(
        key="charging_voltage_v",
        translation_key="charging_voltage_v",
        native_unit_of_measurement=UnitOfElectricPotential.VOLT,
        device_class=SensorDeviceClass.VOLTAGE,
        state_class=SensorStateClass.MEASUREMENT,
        suggested_display_precision=1,
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["charging"].get("charging_voltage_v"),
    ),
    LeapmotorSensorEntityDescription(
        key="charging_connection_state",
        translation_key="charging_connection_state",
        icon="mdi:ev-plug-type2",
        value_fn=lambda data: data["charging"].get("connection_state"),
    ),
    LeapmotorSensorEntityDescription(
        key="evcc_status",
        translation_key="evcc_status",
        icon="mdi:ev-station",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: {
            "unplugged": "A",
            "plugged_in": "B",
            "charging": "C",
            "finished": "B",
        }.get(data["charging"].get("connection_state")),
    ),
    LeapmotorSensorEntityDescription(
        key="battery_min_temp_c",
        translation_key="battery_min_temp_c",
        native_unit_of_measurement=UnitOfTemperature.CELSIUS,
        device_class=SensorDeviceClass.TEMPERATURE,
        state_class=SensorStateClass.MEASUREMENT,
        icon="mdi:thermometer-low",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["diagnostics"].get("battery_min_temp_c"),
    ),
    LeapmotorSensorEntityDescription(
        key="battery_thermal_request",
        translation_key="battery_thermal_request",
        state_class=SensorStateClass.MEASUREMENT,
        icon="mdi:battery-heart",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["diagnostics"].get("battery_thermal_request"),
    ),
    LeapmotorSensorEntityDescription(
        key="ptc_power_w",
        translation_key="ptc_power_w",
        native_unit_of_measurement=UnitOfPower.WATT,
        device_class=SensorDeviceClass.POWER,
        state_class=SensorStateClass.MEASUREMENT,
        icon="mdi:heat-wave",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["diagnostics"].get("ptc_power_w"),
    ),
    LeapmotorSensorEntityDescription(
        key="ptc_state",
        translation_key="ptc_state",
        icon="mdi:heat-wave",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["diagnostics"].get("ptc_state"),
    ),
    LeapmotorSensorEntityDescription(
        key="ptc_power_setting_value",
        translation_key="ptc_power_setting_value",
        icon="mdi:heat-wave",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["diagnostics"].get("ptc_power_setting_value"),
    ),
    LeapmotorSensorEntityDescription(
        key="available_energy_kwh",
        translation_key="available_energy_kwh",
        native_unit_of_measurement=ENERGY_KWH,
        device_class=SensorDeviceClass.ENERGY,
        state_class=SensorStateClass.MEASUREMENT,
        suggested_display_precision=1,
        icon="mdi:battery-lightning",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["diagnostics"].get("available_energy_kwh"),
    ),
    LeapmotorSensorEntityDescription(
        key="charging_planned_start",
        translation_key="charging_planned_start",
        icon="mdi:clock-start",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["charging"].get("charging_planned_start"),
    ),
    LeapmotorSensorEntityDescription(
        key="charging_planned_end",
        translation_key="charging_planned_end",
        icon="mdi:clock-end",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["charging"].get("charging_planned_end"),
    ),
    LeapmotorSensorEntityDescription(
        key="charging_planned_circulation",
        translation_key="charging_planned_circulation",
        icon="mdi:calendar-sync",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["charging"].get("charging_planned_circulation"),
    ),
    LeapmotorSensorEntityDescription(
        key="charging_plan_updated_at",
        translation_key="charging_plan_updated_at",
        icon="mdi:calendar-edit",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["charging"].get("charging_plan_updated_at"),
    ),
    LeapmotorSensorEntityDescription(
        key="vehicle_state",
        translation_key="vehicle_state",
        icon="mdi:car-info",
        value_fn=lambda data: data["status"].get("vehicle_state"),
    ),
    LeapmotorSensorEntityDescription(
        key="lock_state_source",
        translation_key="lock_state_source",
        icon="mdi:lock-question",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["status"].get("lock_state_source"),
    ),
    LeapmotorSensorEntityDescription(
        key="lock_state_age_seconds",
        translation_key="lock_state_age_seconds",
        native_unit_of_measurement=UnitOfTime.SECONDS,
        device_class=SensorDeviceClass.DURATION,
        state_class=SensorStateClass.MEASUREMENT,
        icon="mdi:timer-lock",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["status"].get("lock_state_age_seconds"),
    ),
    LeapmotorSensorEntityDescription(
        key="raw_lock_status_code",
        translation_key="raw_lock_status_code",
        icon="mdi:code-tags",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["status"].get("raw_lock_status_code"),
    ),
    LeapmotorSensorEntityDescription(
        key="climate_mode",
        translation_key="climate_mode",
        icon="mdi:air-conditioner",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["diagnostics"].get("climate_mode"),
    ),
    LeapmotorSensorEntityDescription(
        key="outdoor_temp_c",
        translation_key="outdoor_temp_c",
        native_unit_of_measurement=UnitOfTemperature.CELSIUS,
        device_class=SensorDeviceClass.TEMPERATURE,
        state_class=SensorStateClass.MEASUREMENT,
        icon="mdi:thermometer",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["diagnostics"].get("outdoor_temp_c"),
    ),
    LeapmotorSensorEntityDescription(
        key="climate_fan_volume",
        translation_key="climate_fan_volume",
        icon="mdi:fan",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["diagnostics"].get("climate_fan_volume"),
    ),
    LeapmotorSensorEntityDescription(
        key="climate_fan_volume_setting",
        translation_key="climate_fan_volume_setting",
        icon="mdi:fan-auto",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["diagnostics"].get("climate_fan_volume_setting"),
    ),
    LeapmotorSensorEntityDescription(
        key="climate_air_direction",
        translation_key="climate_air_direction",
        icon="mdi:arrow-decision",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["diagnostics"].get("climate_air_direction"),
    ),
    LeapmotorSensorEntityDescription(
        key="climate_cooling_heating_mode",
        translation_key="climate_cooling_heating_mode",
        icon="mdi:hvac",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["diagnostics"].get("climate_cooling_heating_mode"),
    ),
    LeapmotorSensorEntityDescription(
        key="climate_min_single_temp_c",
        translation_key="climate_min_single_temp_c",
        native_unit_of_measurement=UnitOfTemperature.CELSIUS,
        device_class=SensorDeviceClass.TEMPERATURE,
        state_class=SensorStateClass.MEASUREMENT,
        icon="mdi:thermometer-low",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["diagnostics"].get("climate_min_single_temp_c"),
    ),
    LeapmotorSensorEntityDescription(
        key="sunshade_position",
        translation_key="sunshade_position",
        icon="mdi:roller-shade",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["diagnostics"].get("sunshade_position"),
    ),
    LeapmotorSensorEntityDescription(
        key="steering_wheel_heating_remaining_minutes",
        translation_key="steering_wheel_heating_remaining_minutes",
        native_unit_of_measurement=UnitOfTime.MINUTES,
        device_class=SensorDeviceClass.DURATION,
        state_class=SensorStateClass.MEASUREMENT,
        icon="mdi:steering",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["diagnostics"].get("steering_wheel_heating_remaining_minutes"),
    ),
    LeapmotorSensorEntityDescription(
        key="driver_seat_heating_level",
        translation_key="driver_seat_heating_level",
        state_class=SensorStateClass.MEASUREMENT,
        icon="mdi:car-seat-heater",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["diagnostics"].get("driver_seat_heating_level"),
    ),
    LeapmotorSensorEntityDescription(
        key="passenger_seat_heating_level",
        translation_key="passenger_seat_heating_level",
        state_class=SensorStateClass.MEASUREMENT,
        icon="mdi:car-seat-heater",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["diagnostics"].get("passenger_seat_heating_level"),
    ),
    LeapmotorSensorEntityDescription(
        key="driver_seat_ventilation_level",
        translation_key="driver_seat_ventilation_level",
        state_class=SensorStateClass.MEASUREMENT,
        icon="mdi:car-seat-cooler",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["diagnostics"].get("driver_seat_ventilation_level"),
    ),
    LeapmotorSensorEntityDescription(
        key="passenger_seat_ventilation_level",
        translation_key="passenger_seat_ventilation_level",
        state_class=SensorStateClass.MEASUREMENT,
        icon="mdi:car-seat-cooler",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["diagnostics"].get("passenger_seat_ventilation_level"),
    ),
    LeapmotorSensorEntityDescription(
        key="speed_limit_kmh",
        translation_key="speed_limit_kmh",
        native_unit_of_measurement=UnitOfSpeed.KILOMETERS_PER_HOUR,
        device_class=SensorDeviceClass.SPEED,
        state_class=SensorStateClass.MEASUREMENT,
        icon="mdi:speedometer",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["diagnostics"].get("speed_limit_kmh"),
    ),
    LeapmotorSensorEntityDescription(
        key="front_left_window_position_percent",
        translation_key="front_left_window_position_percent",
        native_unit_of_measurement=PERCENTAGE,
        state_class=SensorStateClass.MEASUREMENT,
        icon="mdi:window-open-variant",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["diagnostics"].get("front_left_window_position_percent"),
    ),
    LeapmotorSensorEntityDescription(
        key="front_right_window_position_percent",
        translation_key="front_right_window_position_percent",
        native_unit_of_measurement=PERCENTAGE,
        state_class=SensorStateClass.MEASUREMENT,
        icon="mdi:window-open-variant",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["diagnostics"].get("front_right_window_position_percent"),
    ),
    LeapmotorSensorEntityDescription(
        key="rear_left_window_position_percent",
        translation_key="rear_left_window_position_percent",
        native_unit_of_measurement=PERCENTAGE,
        state_class=SensorStateClass.MEASUREMENT,
        icon="mdi:window-open-variant",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["diagnostics"].get("rear_left_window_position_percent"),
    ),
    LeapmotorSensorEntityDescription(
        key="rear_right_window_position_percent",
        translation_key="rear_right_window_position_percent",
        native_unit_of_measurement=PERCENTAGE,
        state_class=SensorStateClass.MEASUREMENT,
        icon="mdi:window-open-variant",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["diagnostics"].get("rear_right_window_position_percent"),
    ),
    LeapmotorSensorEntityDescription(
        key="last_successful_refresh",
        translation_key="last_successful_refresh",
        device_class=SensorDeviceClass.TIMESTAMP,
        entity_category=EntityCategory.DIAGNOSTIC,
        icon="mdi:clock-check-outline",
        value_fn=lambda data: _coordinator_timestamp(
            data.get("_integration", {}).get("last_successful_update_at")
        ),
    ),
    LeapmotorSensorEntityDescription(
        key="total_mileage_km",
        translation_key="total_mileage_km",
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
        translation_key="total_energy_kwh",
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
        translation_key="last_7_days_mileage_km",
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
        translation_key="last_7_days_energy_kwh",
        native_unit_of_measurement=ENERGY_KWH,
        state_class=SensorStateClass.MEASUREMENT,
        suggested_display_precision=1,
        icon="mdi:calendar-week",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["history"].get("last_7_days_energy_kwh"),
    ),
    LeapmotorSensorEntityDescription(
        key="average_consumption_6w_kwh_100km",
        translation_key="average_consumption_6w_kwh_100km",
        native_unit_of_measurement=CONSUMPTION_KWH_PER_100KM,
        state_class=SensorStateClass.MEASUREMENT,
        suggested_display_precision=1,
        icon="mdi:chart-bar",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["history"].get("average_consumption_6w_kwh_100km"),
    ),
    LeapmotorSensorEntityDescription(
        key="average_consumption_6w_mi_kwh",
        translation_key="average_consumption_6w_mi_kwh",
        native_unit_of_measurement=CONSUMPTION_MI_PER_KWH,
        state_class=SensorStateClass.MEASUREMENT,
        suggested_display_precision=1,
        icon="mdi:chart-bar",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["history"].get("average_consumption_6w_mi_kwh"),
    ),
    LeapmotorSensorEntityDescription(
        key="last_week_driving_energy_percent",
        translation_key="last_week_driving_energy_percent",
        native_unit_of_measurement=PERCENTAGE,
        state_class=SensorStateClass.MEASUREMENT,
        suggested_display_precision=1,
        icon="mdi:car-electric",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["history"].get("last_week_driving_energy_percent"),
    ),
    LeapmotorSensorEntityDescription(
        key="last_week_climate_energy_percent",
        translation_key="last_week_climate_energy_percent",
        native_unit_of_measurement=PERCENTAGE,
        state_class=SensorStateClass.MEASUREMENT,
        suggested_display_precision=1,
        icon="mdi:air-conditioner",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["history"].get("last_week_climate_energy_percent"),
    ),
    LeapmotorSensorEntityDescription(
        key="last_week_other_energy_percent",
        translation_key="last_week_other_energy_percent",
        native_unit_of_measurement=PERCENTAGE,
        state_class=SensorStateClass.MEASUREMENT,
        suggested_display_precision=1,
        icon="mdi:lightning-bolt-outline",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["history"].get("last_week_other_energy_percent"),
    ),
    LeapmotorSensorEntityDescription(
        key="last_charge_energy_kwh",
        translation_key="last_charge_energy_kwh",
        native_unit_of_measurement=ENERGY_KWH,
        device_class=SensorDeviceClass.ENERGY,
        state_class=SensorStateClass.MEASUREMENT,
        suggested_display_precision=2,
        icon="mdi:battery-charging-outline",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["charging_history"].get("last_charge_energy_kwh"),
        extra_attrs_fn=lambda data: {
            "charge_type": data["charging_history"].get("last_charge_type"),
            "start_ts": data["charging_history"].get("last_charge_start_ts"),
            "end_ts": data["charging_history"].get("last_charge_end_ts"),
        },
    ),
    LeapmotorSensorEntityDescription(
        key="tire_pressure_front_left_bar",
        translation_key="tire_pressure_front_left_bar",
        native_unit_of_measurement=PRESSURE_BAR,
        device_class=SensorDeviceClass.PRESSURE,
        state_class=SensorStateClass.MEASUREMENT,
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["diagnostics"].get("tire_pressure_front_left_bar"),
    ),
    LeapmotorSensorEntityDescription(
        key="tire_pressure_front_right_bar",
        translation_key="tire_pressure_front_right_bar",
        native_unit_of_measurement=PRESSURE_BAR,
        device_class=SensorDeviceClass.PRESSURE,
        state_class=SensorStateClass.MEASUREMENT,
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["diagnostics"].get("tire_pressure_front_right_bar"),
    ),
    LeapmotorSensorEntityDescription(
        key="tire_pressure_rear_left_bar",
        translation_key="tire_pressure_rear_left_bar",
        native_unit_of_measurement=PRESSURE_BAR,
        device_class=SensorDeviceClass.PRESSURE,
        state_class=SensorStateClass.MEASUREMENT,
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["diagnostics"].get("tire_pressure_rear_left_bar"),
    ),
    LeapmotorSensorEntityDescription(
        key="tire_pressure_rear_right_bar",
        translation_key="tire_pressure_rear_right_bar",
        native_unit_of_measurement=PRESSURE_BAR,
        device_class=SensorDeviceClass.PRESSURE,
        state_class=SensorStateClass.MEASUREMENT,
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data["diagnostics"].get("tire_pressure_rear_right_bar"),
    ),
    LeapmotorSensorEntityDescription(
        key="unread_message_count",
        translation_key="unread_message_count",
        icon="mdi:message-badge",
        state_class=SensorStateClass.MEASUREMENT,
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: (data.get("notifications") or {}).get("unread_count"),
    ),
    LeapmotorSensorEntityDescription(
        key="last_message_title",
        translation_key="last_message_title",
        icon="mdi:message-text",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: (data.get("notifications") or {}).get("last_message_title"),
    ),
    LeapmotorSensorEntityDescription(
        key="last_message_time",
        translation_key="last_message_time",
        device_class=SensorDeviceClass.TIMESTAMP,
        icon="mdi:message-clock",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: _message_timestamp(
            (data.get("notifications") or {}).get("last_message_time")
        ),
    ),
)


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up Leapmotor sensors."""
    coordinator: LeapmotorDataUpdateCoordinator = hass.data[DOMAIN][entry.entry_id]
    localized_names = await hass.async_add_executor_job(
        load_localized_entity_names,
        hass.config.language,
        "sensor",
    )
    entities: list[LeapmotorSensor] = []
    for vin, vehicle_data in coordinator.data.get("vehicles", {}).items():
        entities.extend(
            LeapmotorSensor(coordinator, vin, description, localized_names)
            for description in SENSOR_DESCRIPTIONS
            if _should_create_sensor(vehicle_data, description.key)
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
            english_entity_slug("sensor", description.key) or description.key,
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
    def available(self) -> bool:
        """Return entity availability."""
        if not super().available:
            return False
        if self.entity_description.key in {"remaining_charge_minutes", "charging_finish_time"}:
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
                    "raw_ac_operation_mode_code": status.get(
                        "raw_ac_operation_mode_code"
                    ),
                    "raw_charge_connection_code": status.get(
                        "raw_charge_connection_code"
                    ),
                    "raw_ac_fan_speed_code": status.get("raw_ac_fan_speed_code"),
                    "raw_vehicle_state_code": status.get("raw_vehicle_state_code"),
                    "raw_parked_status_code": status.get("raw_parked_status_code"),
                }
            )
        if self.entity_description.key in {
            "lock_state_source",
            "lock_state_age_seconds",
            "raw_lock_status_code",
        }:
            status = self.vehicle_data["status"]
            attributes.update(
                {
                    "is_locked": status.get("is_locked"),
                    "lock_state_source": status.get("lock_state_source"),
                    "lock_state_age_seconds": status.get("lock_state_age_seconds"),
                    "lock_state_is_stale": status.get("lock_state_is_stale"),
                    "raw_lock_status_code": status.get("raw_lock_status_code"),
                    "last_vehicle_timestamp": status.get("last_vehicle_timestamp"),
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
                    "polling_mode": integration.get("polling_mode"),
                    "eco_polling_enabled": integration.get("eco_polling_enabled"),
                    "normal_update_interval_seconds": integration.get(
                        "normal_update_interval_seconds"
                    ),
                    "eco_update_interval_seconds": integration.get(
                        "eco_update_interval_seconds"
                    ),
                }
            )
        if self.entity_description.key in {
            "average_consumption_6w_kwh_100km",
            "average_consumption_6w_mi_kwh",
        }:
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
        if self.entity_description.extra_attrs_fn is not None:
            attributes.update(self.entity_description.extra_attrs_fn(self.vehicle_data))
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


def _should_create_sensor(vehicle_data: dict[str, Any], key: str) -> bool:
    """Return whether a sensor is supported by the current vehicle payload."""
    path = OPTIONAL_SENSOR_PATHS.get(key)
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


def _suggested_object_id(vehicle: dict[str, Any], slug: str) -> str:
    """Return a stable English suggested object id independent from UI language."""
    prefix = str(vehicle.get("car_type") or "leapmotor").strip().lower()
    prefix = "".join(char if char.isalnum() else "_" for char in prefix).strip("_")
    return f"{prefix or 'leapmotor'}_{slug}"


def _coordinator_timestamp(value: Any) -> datetime | None:
    """Return a timezone-aware datetime from a UNIX timestamp."""
    if value is None:
        return None
    try:
        return datetime.fromtimestamp(float(value), tz=UTC)
    except (TypeError, ValueError, OSError):
        return None


def _message_timestamp(value: Any) -> datetime | None:
    """Return a timezone-aware datetime from a message API timestamp."""
    if value is None:
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        if not isinstance(value, str):
            return None
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    if numeric > 10_000_000_000:
        numeric /= 1000
    try:
        return datetime.fromtimestamp(numeric, tz=UTC)
    except (OSError, ValueError):
        return None


def _charging_finish_time(remaining_minutes: Any) -> datetime | None:
    """Return estimated charging finish time based on remaining charge minutes."""
    try:
        minutes = int(remaining_minutes)
    except (TypeError, ValueError):
        return None
    if minutes <= 0:
        return None
    return datetime.now(tz=UTC) + timedelta(minutes=minutes)
