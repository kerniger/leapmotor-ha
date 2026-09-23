"""Bounded, signal-gated numeric sensors for CN vehicles."""
import math

from homeassistant.components.sensor import SensorDeviceClass, SensorEntity, SensorEntityDescription, SensorStateClass
from homeassistant.const import (
    PERCENTAGE,
    UnitOfElectricCurrent,
    UnitOfElectricPotential,
    UnitOfLength,
    UnitOfPressure,
    UnitOfTemperature,
    UnitOfTime,
)
from homeassistant.helpers.device_registry import DeviceInfo
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from .const import DOMAIN_CN
from .cn_api import CHARGE_STATES
from .entity_discovery import register_discovery

DESCRIPTIONS = (
    SensorEntityDescription(key="battery_percent", name="Battery", native_unit_of_measurement=PERCENTAGE, device_class=SensorDeviceClass.BATTERY, state_class=SensorStateClass.MEASUREMENT),
    SensorEntityDescription(key="remaining_range_km", name="Range", native_unit_of_measurement=UnitOfLength.KILOMETERS, device_class=SensorDeviceClass.DISTANCE, state_class=SensorStateClass.MEASUREMENT),
    SensorEntityDescription(key="odometer_km", name="Odometer", native_unit_of_measurement=UnitOfLength.KILOMETERS, device_class=SensorDeviceClass.DISTANCE, state_class=SensorStateClass.TOTAL_INCREASING),
    SensorEntityDescription(key="interior_temp_c", name="Interior temperature", native_unit_of_measurement=UnitOfTemperature.CELSIUS, device_class=SensorDeviceClass.TEMPERATURE, state_class=SensorStateClass.MEASUREMENT),
    SensorEntityDescription(key="precise_battery_percent", name="Precise battery", native_unit_of_measurement=PERCENTAGE, device_class=SensorDeviceClass.BATTERY, state_class=SensorStateClass.MEASUREMENT),
    SensorEntityDescription(key="standard_range_km", name="Standard range", native_unit_of_measurement=UnitOfLength.KILOMETERS, device_class=SensorDeviceClass.DISTANCE, state_class=SensorStateClass.MEASUREMENT),
    SensorEntityDescription(key="speed_kmh", name="Speed", native_unit_of_measurement="km/h", device_class=SensorDeviceClass.SPEED, state_class=SensorStateClass.MEASUREMENT),
    SensorEntityDescription(key="battery_voltage_v", name="Battery voltage", native_unit_of_measurement=UnitOfElectricPotential.VOLT, device_class=SensorDeviceClass.VOLTAGE, state_class=SensorStateClass.MEASUREMENT),
    SensorEntityDescription(key="battery_current_a", name="Battery current", native_unit_of_measurement=UnitOfElectricCurrent.AMPERE, device_class=SensorDeviceClass.CURRENT, state_class=SensorStateClass.MEASUREMENT),
    SensorEntityDescription(key="minimum_battery_temp_c", name="Minimum battery temperature", native_unit_of_measurement=UnitOfTemperature.CELSIUS, device_class=SensorDeviceClass.TEMPERATURE, state_class=SensorStateClass.MEASUREMENT),
    SensorEntityDescription(key="remaining_charge_time_min", name="Remaining charge time", native_unit_of_measurement=UnitOfTime.MINUTES, device_class=SensorDeviceClass.DURATION),
    SensorEntityDescription(key="climate_set_temp_left_c", name="Climate set temperature", native_unit_of_measurement=UnitOfTemperature.CELSIUS, device_class=SensorDeviceClass.TEMPERATURE, state_class=SensorStateClass.MEASUREMENT),
    SensorEntityDescription(key="left_front_tire_pressure_kpa", name="Left front tire pressure", native_unit_of_measurement=UnitOfPressure.KPA, device_class=SensorDeviceClass.PRESSURE, state_class=SensorStateClass.MEASUREMENT),
    SensorEntityDescription(key="right_front_tire_pressure_kpa", name="Right front tire pressure", native_unit_of_measurement=UnitOfPressure.KPA, device_class=SensorDeviceClass.PRESSURE, state_class=SensorStateClass.MEASUREMENT),
    SensorEntityDescription(key="left_rear_tire_pressure_kpa", name="Left rear tire pressure", native_unit_of_measurement=UnitOfPressure.KPA, device_class=SensorDeviceClass.PRESSURE, state_class=SensorStateClass.MEASUREMENT),
    SensorEntityDescription(key="right_rear_tire_pressure_kpa", name="Right rear tire pressure", native_unit_of_measurement=UnitOfPressure.KPA, device_class=SensorDeviceClass.PRESSURE, state_class=SensorStateClass.MEASUREMENT),
)

TEXT_DESCRIPTIONS = (SensorEntityDescription(key="charge_state", name="Charge state"),)

# Canonical entity keys deliberately map to the decoded CN signal names, rather
# than to numeric IDs.  Numeric IDs overlap with, but do not mean the same
# thing as, their EU counterparts.  Keeping this list here makes discovery
# fail closed for a model which does not return a confirmed telemetry signal.
SOURCE_SIGNALS = {
    "battery_percent": "soc",
    "remaining_range_km": "liveRemainingRange",
    "odometer_km": "totalMileage",
    "interior_temp_c": "interiorTemp",
    "precise_battery_percent": "preciseSoc",
    "standard_range_km": "electricRangeStandard",
    "speed_kmh": "speed",
    "battery_voltage_v": "batteryVoltage",
    "battery_current_a": "batteryCurrent",
    "minimum_battery_temp_c": "minBatteryTemp",
    "remaining_charge_time_min": "chargeRemainTime",
    "climate_set_temp_left_c": "acSetting",
    "charge_state": "chargeState",
    "left_front_tire_pressure_kpa": "leftFrontTirePressure",
    "right_front_tire_pressure_kpa": "rightFrontTirePressure",
    "left_rear_tire_pressure_kpa": "leftRearTirePressure",
    "right_rear_tire_pressure_kpa": "rightRearTirePressure",
}


def numeric_value(key, value):
    if isinstance(value, bool) or not isinstance(value, (int, float, str)):
        return None
    try:
        number = float(value)
    except (ValueError, OverflowError):
        return None
    limits = {
        "battery_percent": (0, 100), "precise_battery_percent": (0, 100),
        "remaining_range_km": (0, 3000), "standard_range_km": (0, 3000),
        "odometer_km": (0, 10_000_000), "speed_kmh": (0, 500),
        "interior_temp_c": (-80, 100), "battery_voltage_v": (0, 1500),
        "battery_current_a": (-2000, 2000), "minimum_battery_temp_c": (-80, 150),
        "remaining_charge_time_min": (0, 10_080), "climate_set_temp_left_c": (10, 40),
        # The B05 samples are 242--247.  Values below 50 are not published as
        # pressure: the app's kPa heuristic would otherwise make their unit ambiguous.
        "left_front_tire_pressure_kpa": (50, 1000),
        "right_front_tire_pressure_kpa": (50, 1000),
        "left_rear_tire_pressure_kpa": (50, 1000),
        "right_rear_tire_pressure_kpa": (50, 1000),
    }
    lower, upper = limits[key]
    return number if math.isfinite(number) and lower <= number <= upper else None


async def async_setup_entry(hass, entry, async_add_entities):
    coordinator = hass.data[DOMAIN_CN][entry.entry_id]
    def factory(coordinator, entry_id, vin, description):
        cls = CNTextSensor if description in TEXT_DESCRIPTIONS else CNSensor
        return cls(coordinator, entry_id, vin, description)
    register_discovery(coordinator, entry, async_add_entities, supported_descriptions, factory)


def supported_descriptions(data):
    """Return only entities whose confirmed CN source signal was returned."""
    vehicle = data.get("vehicle", {}) if isinstance(data, dict) else {}
    # B05 was the original, fully verified 15-entity profile.  Retain it even
    # if a sleeping car returns a partial first payload, so an upgrade cannot
    # orphan existing entities until the next Home Assistant restart.
    if isinstance(vehicle, dict) and str(vehicle.get("car_type", "")).upper() == "B05":
        return DESCRIPTIONS + TEXT_DESCRIPTIONS
    raw_names = data.get("cn_raw_signal_names", ()) if isinstance(data, dict) else ()
    if not isinstance(raw_names, (list, tuple, set, frozenset)):
        return ()
    available = {name for name in raw_names if isinstance(name, str)}
    sources = dict(SOURCE_SIGNALS)
    if "T03" in str(vehicle.get("car_type", "")).upper():
        sources["remaining_range_km"] = "expectedMileage"
    return tuple(description for description in DESCRIPTIONS + TEXT_DESCRIPTIONS
                 if sources[description.key] in available)


class CNSensor(CoordinatorEntity, SensorEntity):
    _attr_has_entity_name = True

    def __init__(self, coordinator, entry_id, vin, description):
        super().__init__(coordinator)
        self.vin = vin
        self.entity_description = description
        self._attr_unique_id = f"{DOMAIN_CN}_{entry_id}_{vin}_{description.key}"
        vehicle = (coordinator.data or {}).get("vehicles", {}).get(vin, {}).get("vehicle", {})
        model = str(vehicle.get("car_type") or "Unknown")
        self._attr_device_info = DeviceInfo(identifiers={(DOMAIN_CN, vin)}, manufacturer="Leapmotor",
                                           model=model, name=f"Leapmotor China {model}")

    @property
    def available(self):
        return super().available and self.vin in (self.coordinator.data or {}).get("vehicles", {})

    @property
    def native_value(self):
        data = (self.coordinator.data or {}).get("vehicles", {}).get(self.vin, {})
        return numeric_value(self.entity_description.key, data.get("status", {}).get(self.entity_description.key))


class CNTextSensor(CNSensor):
    @property
    def native_value(self):
        data = (self.coordinator.data or {}).get("vehicles", {}).get(self.vin, {})
        value = data.get("status", {}).get(self.entity_description.key)
        return value if isinstance(value, str) and value in CHARGE_STATES.values() else None
