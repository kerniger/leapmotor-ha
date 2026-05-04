"""Entity registry migrations for Leapmotor."""

from __future__ import annotations

import logging

from homeassistant.core import HomeAssistant
from homeassistant.helpers import entity_registry as er

from .const import DOMAIN

_LOGGER = logging.getLogger(__name__)


_ENGLISH_ENTITY_SLUGS: dict[tuple[str, str], str] = {
    ("binary_sensor", "is_charging"): "charging",
    ("binary_sensor", "is_plugged_in"): "charge_cable_plugged_in",
    ("binary_sensor", "is_regening"): "regenerative_braking",
    ("binary_sensor", "dc_cable_connected"): "dc_charge_cable_plugged_in",
    ("binary_sensor", "charging_planned_enabled"): "scheduled_charging",
    ("binary_sensor", "charging_planned_weekly"): "weekly_charging_schedule",
    ("binary_sensor", "remote_session_active"): "remote_session_active",
    ("binary_sensor", "vehicle_security_active"): "vehicle_security_active",
    ("binary_sensor", "battery_heating"): "battery_heating",
    ("binary_sensor", "driver_door_open"): "driver_door",
    ("binary_sensor", "passenger_door_open"): "passenger_door",
    ("binary_sensor", "rear_left_door_open"): "rear_left_door",
    ("binary_sensor", "rear_right_door_open"): "rear_right_door",
    ("binary_sensor", "trunk_open"): "trunk",
    ("binary_sensor", "front_left_window_open"): "front_left_window",
    ("binary_sensor", "front_right_window_open"): "front_right_window",
    ("binary_sensor", "rear_left_window_open"): "rear_left_window",
    ("binary_sensor", "rear_right_window_open"): "rear_right_window",
    ("binary_sensor", "skylight_open"): "skylight_open",
    ("binary_sensor", "climate_on"): "climate",
    ("binary_sensor", "fast_cooling_active"): "fast_cooling_active",
    ("binary_sensor", "fast_heating_active"): "fast_heating_active",
    ("binary_sensor", "windshield_defrosting"): "windshield_defrost",
    ("binary_sensor", "rear_window_heating"): "rear_window_heating",
    ("binary_sensor", "air_recirculation"): "air_recirculation",
    ("binary_sensor", "steering_wheel_heating"): "steering_wheel_heating",
    ("binary_sensor", "left_mirror_heating"): "left_mirror_heating",
    ("binary_sensor", "right_mirror_heating"): "right_mirror_heating",
    ("binary_sensor", "speed_limit_enabled"): "speed_limit_active",
    ("binary_sensor", "park_assist_enabled"): "park_assist_active",
    ("binary_sensor", "sentinel_mode"): "sentinel_mode",
    ("binary_sensor", "parking_photo"): "parking_photo",
    ("binary_sensor", "fully_charged"): "fully_charged",
    ("button", "refresh_data"): "refresh_data",
    ("button", "trunk_open"): "open_trunk",
    ("button", "trunk_close"): "close_trunk",
    ("button", "find_car"): "find_vehicle",
    ("button", "sunshade_open"): "open_sunshade",
    ("button", "sunshade_close"): "close_sunshade",
    ("button", "battery_preheat"): "battery_preheat",
    ("button", "windows_open"): "open_windows",
    ("button", "windows_close"): "close_windows",
    ("button", "ac_switch"): "climate_off",
    ("button", "quick_cool"): "quick_cool",
    ("button", "quick_heat"): "quick_heat",
    ("button", "windshield_defrost"): "windshield_defrost",
    ("device_tracker", "location"): "location",
    ("image", "vehicle_picture_pkg"): "vehicle_picture",
    ("lock", "vehicle_lock"): "lock",
    ("number", "charge_limit_setting"): "set_charge_limit",
    ("sensor", "battery_percent"): "battery",
    ("sensor", "battery_percent_precise"): "precise_battery",
    ("sensor", "remaining_range_km"): "range",
    ("sensor", "wltp_max_range_km"): "cltc_remaining_range",
    ("sensor", "live_remaining_range_km"): "live_range",
    ("sensor", "range_mode"): "range_mode",
    ("sensor", "odometer_km"): "odometer",
    ("sensor", "speed_kmh"): "speed",
    ("sensor", "gear"): "gear",
    ("sensor", "interior_temp_c"): "interior_temperature",
    ("sensor", "climate_set_temp_left_c"): "target_temperature_left",
    ("sensor", "climate_set_temp_right_c"): "target_temperature_right",
    ("sensor", "charge_limit_percent"): "charge_limit",
    ("sensor", "remaining_charge_minutes"): "remaining_charge_time",
    ("sensor", "charging_power_kw"): "charging_power",
    ("sensor", "charging_current_a"): "charging_current",
    ("sensor", "charging_voltage_v"): "charging_voltage",
    ("sensor", "charging_connection_state"): "charging_connection",
    ("sensor", "battery_min_temp_c"): "battery_minimum_temperature",
    ("sensor", "battery_thermal_request"): "battery_thermal_request",
    ("sensor", "ptc_power_w"): "ptc_power",
    ("sensor", "parking_camera_state"): "parking_camera_state",
    ("sensor", "charging_planned_start"): "charging_schedule_start",
    ("sensor", "charging_planned_end"): "charging_schedule_end",
    ("sensor", "charging_planned_circulation"): "charging_schedule_recurrence",
    ("sensor", "charging_plan_updated_at"): "charging_schedule_updated",
    ("sensor", "vehicle_state"): "vehicle_state",
    ("sensor", "climate_mode"): "climate_mode",
    ("sensor", "steering_wheel_heating_remaining_minutes"): "steering_wheel_heating_remaining_time",
    ("sensor", "driver_seat_heating_level"): "driver_seat_heating_level",
    ("sensor", "passenger_seat_heating_level"): "passenger_seat_heating_level",
    ("sensor", "driver_seat_ventilation_level"): "driver_seat_ventilation_level",
    ("sensor", "passenger_seat_ventilation_level"): "passenger_seat_ventilation_level",
    ("sensor", "speed_limit_kmh"): "speed_limit",
    ("sensor", "front_left_window_position_percent"): "front_left_window_position",
    ("sensor", "front_right_window_position_percent"): "front_right_window_position",
    ("sensor", "rear_left_window_position_percent"): "rear_left_window_position",
    ("sensor", "rear_right_window_position_percent"): "rear_right_window_position",
    ("sensor", "last_successful_refresh"): "last_refresh",
    ("sensor", "total_mileage_km"): "total_mileage",
    ("sensor", "total_energy_kwh"): "total_energy_consumption",
    ("sensor", "last_7_days_mileage_km"): "last_7_days_mileage",
    ("sensor", "last_7_days_energy_kwh"): "last_7_days_energy",
    ("sensor", "average_consumption_6w_kwh_100km"): "six_week_average_consumption",
    ("sensor", "last_week_driving_energy_percent"): "last_week_driving_energy",
    ("sensor", "last_week_climate_energy_percent"): "last_week_climate_energy",
    ("sensor", "last_week_other_energy_percent"): "last_week_other_energy",
    ("sensor", "tire_pressure_front_left_bar"): "front_left_tire_pressure",
    ("sensor", "tire_pressure_front_right_bar"): "front_right_tire_pressure",
    ("sensor", "tire_pressure_rear_left_bar"): "rear_left_tire_pressure",
    ("sensor", "tire_pressure_rear_right_bar"): "rear_right_tire_pressure",
    ("sensor", "unread_message_count"): "unread_message_count",
    ("sensor", "last_message_title"): "last_message_title",
    ("sensor", "last_message_time"): "last_message_time",
}


async def async_migrate_entity_registry_to_english(
    hass: HomeAssistant,
    vins: set[str],
) -> None:
    """Rename existing Leapmotor registry entries to English entity IDs."""
    registry = er.async_get(hass)
    for entry in list(registry.entities.values()):
        if entry.platform != DOMAIN or not isinstance(entry.unique_id, str):
            continue

        vin, separator, suffix = entry.unique_id.partition("_")
        if not separator or vin not in vins:
            continue
        domain, separator, object_id = entry.entity_id.partition(".")
        if not separator:
            continue
        desired_slug = _ENGLISH_ENTITY_SLUGS.get((domain, suffix))
        if desired_slug is None:
            continue

        vehicle_prefix = object_id.split("_", 1)[0] or "leapmotor"
        desired_entity_id = f"{domain}.{vehicle_prefix}_{desired_slug}"
        if desired_entity_id == entry.entity_id:
            continue
        if registry.async_get(desired_entity_id) is not None:
            _LOGGER.debug(
                "Skipping Leapmotor entity migration for %s: target %s already exists",
                entry.entity_id,
                desired_entity_id,
            )
            continue

        try:
            registry.async_update_entity(
                entry.entity_id,
                new_entity_id=desired_entity_id,
                name=None,
            )
            _LOGGER.info(
                "Migrated Leapmotor entity id from %s to %s",
                entry.entity_id,
                desired_entity_id,
            )
        except Exception as exc:
            _LOGGER.warning(
                "Could not migrate Leapmotor entity id %s to %s: %s",
                entry.entity_id,
                desired_entity_id,
                exc,
            )
