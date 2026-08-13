"""Lock entity for Leapmotor vehicle state and control."""

from __future__ import annotations

from typing import Any

from homeassistant.components.lock import LockEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.device_registry import DeviceInfo
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from .const import DOMAIN
from .coordinator import LeapmotorDataUpdateCoordinator
from .entity_helpers import build_vehicle_display_name
from .entity_migration import english_entity_slug
from .remote_helpers import RemoteActionSpec, async_execute_remote_action

LOCK_ACTION = RemoteActionSpec(
    action="lock",
    translation_key="vehicle_lock",
    icon="mdi:lock",
    method_name="lock_vehicle",
    service_name="lock",
)

UNLOCK_ACTION = RemoteActionSpec(
    action="unlock",
    translation_key="vehicle_lock",
    icon="mdi:lock-open",
    method_name="unlock_vehicle",
    service_name="unlock",
)


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up Leapmotor lock entities."""
    coordinator: LeapmotorDataUpdateCoordinator = hass.data[DOMAIN][entry.entry_id]
    for vin in coordinator.data.get("vehicles", {}):
        async_add_entities(
            [LeapmotorVehicleLock(coordinator, vin)],
            config_subentry_id=coordinator.config_subentry_id_for_vin(vin),
        )


class LeapmotorVehicleLock(CoordinatorEntity[LeapmotorDataUpdateCoordinator], LockEntity):
    """Vehicle lock control with clear locked/unlocked semantics."""

    _attr_has_entity_name = True
    _attr_translation_key = "vehicle_lock"
    _attr_icon = "mdi:car-door-lock"

    def __init__(self, coordinator: LeapmotorDataUpdateCoordinator, vin: str) -> None:
        super().__init__(coordinator)
        self.vin = vin
        self._attr_unique_id = f"{vin}_vehicle_lock"
        vehicle = self.vehicle_data["vehicle"]
        self._attr_suggested_object_id = _suggested_object_id(
            vehicle,
            english_entity_slug("lock", "vehicle_lock") or "lock",
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
    def available(self) -> bool:
        """Return entity availability."""
        return super().available

    @property
    def is_locked(self) -> bool | None:
        """Return lock state."""
        value = self.vehicle_data["status"].get("is_locked")
        if value is None:
            return None
        return bool(value)

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        """Return useful vehicle metadata."""
        vehicle = self.vehicle_data["vehicle"]
        remote = self.vehicle_data.get("remote_control") or {}
        return {
            "vin": self.vin,
            "car_id": vehicle.get("car_id"),
            "car_type": vehicle.get("car_type"),
            "is_shared": vehicle.get("is_shared"),
            "raw_lock_status_code": self.vehicle_data["status"].get("raw_lock_status_code"),
            "lock_state_source": self.vehicle_data["status"].get("lock_state_source", "cloud"),
            "lock_state_age_seconds": self.vehicle_data["status"].get("lock_state_age_seconds"),
            "lock_state_is_stale": self.vehicle_data["status"].get("lock_state_is_stale"),
            "operation_password_configured": self.coordinator.operation_password_configured(
                self.vin
            ),
            "last_remote_action": remote.get("action"),
            "last_remote_status": remote.get("status"),
            "last_remote_success": remote.get("success"),
            "last_remote_updated_at": remote.get("updated_at"),
            "last_remote_error": remote.get("error"),
        }

    async def async_lock(self, **kwargs: Any) -> None:
        """Lock the vehicle."""
        await async_execute_remote_action(self.coordinator, self.vin, LOCK_ACTION)

    async def async_unlock(self, **kwargs: Any) -> None:
        """Unlock the vehicle."""
        await async_execute_remote_action(self.coordinator, self.vin, UNLOCK_ACTION)


def _suggested_object_id(vehicle: dict[str, Any], slug: str) -> str:
    """Return a stable English suggested object id independent from UI language."""
    prefix = str(vehicle.get("car_type") or "leapmotor").strip().lower()
    prefix = "".join(char if char.isalnum() else "_" for char in prefix).strip("_")
    return f"{prefix or 'leapmotor'}_{slug}"
