"""Read-only CN binary status entities with explicit decoders."""
import time

from homeassistant.components.binary_sensor import (
    BinarySensorDeviceClass,
    BinarySensorEntity,
    BinarySensorEntityDescription,
)
from homeassistant.helpers.device_registry import DeviceInfo
from homeassistant.core import callback
from homeassistant.helpers.event import async_call_later
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from .const import DOMAIN_CN
from .entity_discovery import register_discovery
from .freshness import MAX_STATUS_AGE, is_fresh, timestamp_seconds

SENTRY_DESCRIPTION = BinarySensorEntityDescription(key="sentry_enabled", name="Sentry mode")
CHARGING_DESCRIPTIONS = (
    BinarySensorEntityDescription(
        key="ac_input", name="AC charging input", device_class=BinarySensorDeviceClass.PLUG
    ),
    BinarySensorEntityDescription(
        key="dc_input", name="DC charging input", device_class=BinarySensorDeviceClass.PLUG
    ),
    BinarySensorEntityDescription(key="charge_completed", name="Charge completed"),
)
CHARGING_SOURCE_SIGNALS = {
    "ac_input": "acInputSlowCharge",
    "dc_input": "dcInputFastCharge",
    "charge_completed": "chargeState",
}
DOOR_DESCRIPTIONS = tuple(
    BinarySensorEntityDescription(key=key, name=name, device_class=BinarySensorDeviceClass.DOOR)
    for key, name in (
        ("driver_door_open", "Driver door"), ("passenger_door_open", "Passenger door"),
        ("left_rear_door_open", "Left rear door"), ("right_rear_door_open", "Right rear door"),
    )
)
DOOR_SOURCE_SIGNALS = {
    "driver_door_open": "lbcmDriverDoorStatus", "passenger_door_open": "rbcmDriverDoorStatus",
    "left_rear_door_open": "lbcmLeftRearDoorStatus", "right_rear_door_open": "rbcmRightRearDoorStatus",
}


def supported_descriptions(data):
    names = data.get("cn_raw_signal_names", ())
    available = {name for name in names if isinstance(name, str)} if isinstance(names, (list, tuple, set, frozenset)) else set()
    model = str(data.get("vehicle", {}).get("car_type", "")).upper()
    descriptions = [SENTRY_DESCRIPTION] if model == "B05" or "sentryMode" in available else []
    sources = {**CHARGING_SOURCE_SIGNALS, **DOOR_SOURCE_SIGNALS}
    return descriptions + [d for d in CHARGING_DESCRIPTIONS + DOOR_DESCRIPTIONS if sources[d.key] in available]


async def async_setup_entry(hass, entry, async_add_entities):
    coordinator = hass.data[DOMAIN_CN][entry.entry_id]
    def factory(coordinator, entry_id, vin, description):
        if description == SENTRY_DESCRIPTION:
            return CNSentrySensor(coordinator, entry_id, vin)
        cls = CNDoorSensor if description in DOOR_DESCRIPTIONS else CNChargingSensor
        return cls(coordinator, entry_id, vin, description)
    register_discovery(coordinator, entry, async_add_entities, supported_descriptions, factory)


class CNSentrySensor(CoordinatorEntity, BinarySensorEntity):
    _attr_has_entity_name = True
    entity_description = SENTRY_DESCRIPTION

    def __init__(self, coordinator, entry_id, vin):
        super().__init__(coordinator)
        self.vin = vin
        self._attr_unique_id = f"{DOMAIN_CN}_{entry_id}_{vin}_sentry_enabled"
        self._set_device_info()

    def _set_device_info(self):
        vehicle = (self.coordinator.data or {}).get("vehicles", {}).get(self.vin, {}).get("vehicle", {})
        model = str(vehicle.get("car_type") or "Unknown")
        self._attr_device_info = DeviceInfo(
            identifiers={(DOMAIN_CN, self.vin)}, manufacturer="Leapmotor", model=model,
            name=f"Leapmotor China {model}",
        )

    @property
    def available(self):
        return super().available and self.vin in (self.coordinator.data or {}).get("vehicles", {})

    @property
    def is_on(self):
        return (self.coordinator.data or {}).get("vehicles", {}).get(self.vin, {}).get("status", {}).get("sentry_enabled")


class CNChargingSensor(CNSentrySensor):
    """A signal-gated charge input/completion state."""

    def __init__(self, coordinator, entry_id, vin, description):
        super().__init__(coordinator, entry_id, vin)
        self.entity_description = description
        self._attr_unique_id = f"{DOMAIN_CN}_{entry_id}_{vin}_{description.key}"

    @property
    def is_on(self):
        return (self.coordinator.data or {}).get("vehicles", {}).get(self.vin, {}).get("charging", {}).get(self.entity_description.key)


class CNDoorSensor(CNChargingSensor):
    """Explicit door state; old or undated cloud data stays unknown."""

    _cancel_expiry = None

    async def async_added_to_hass(self):
        await super().async_added_to_hass()
        self.async_on_remove(self._clear_expiry)
        self._schedule_expiry()

    @callback
    def _clear_expiry(self):
        if self._cancel_expiry is not None:
            self._cancel_expiry()
            self._cancel_expiry = None

    @callback
    def _schedule_expiry(self):
        self._clear_expiry()
        status = (self.coordinator.data or {}).get("vehicles", {}).get(self.vin, {}).get("status", {})
        timestamp = timestamp_seconds(status.get("last_vehicle_timestamp"))
        if timestamp is not None and is_fresh(timestamp):
            self._cancel_expiry = async_call_later(
                self.hass, max(0, timestamp + MAX_STATUS_AGE - time.time()) + 0.01,
                self._expire,
            )

    @callback
    def _expire(self, _now):
        self._cancel_expiry = None
        self.async_write_ha_state()

    @callback
    def _handle_coordinator_update(self):
        self._schedule_expiry()
        super()._handle_coordinator_update()

    @property
    def is_on(self):
        status = (self.coordinator.data or {}).get("vehicles", {}).get(self.vin, {}).get("status", {})
        if not is_fresh(status.get("last_vehicle_timestamp")):
            return None
        value = status.get(self.entity_description.key)
        return value if isinstance(value, bool) else None
