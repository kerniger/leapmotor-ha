"""Number entities for Leapmotor."""

from __future__ import annotations

from typing import Any

from homeassistant.components.number import NumberDeviceClass, NumberEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import PERCENTAGE
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers.device_registry import DeviceInfo
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from .const import DOMAIN
from .coordinator import LeapmotorDataUpdateCoordinator
from .entity_helpers import build_vehicle_display_name
from .entity_migration import english_entity_slug
from .remote_helpers import format_remote_error


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up Leapmotor number entities."""
    coordinator: LeapmotorDataUpdateCoordinator = hass.data[DOMAIN][entry.entry_id]
    async_add_entities(
        LeapmotorChargeLimitNumber(coordinator, vin)
        for vin in coordinator.data.get("vehicles", {})
    )


class LeapmotorChargeLimitNumber(
    CoordinatorEntity[LeapmotorDataUpdateCoordinator],
    NumberEntity,
):
    """Editable charge limit entity."""

    _attr_has_entity_name = True
    _attr_translation_key = "charge_limit_setting"
    _attr_native_min_value = 1
    _attr_native_max_value = 100
    _attr_native_step = 1
    _attr_native_unit_of_measurement = PERCENTAGE
    _attr_device_class = NumberDeviceClass.BATTERY
    _attr_icon = "mdi:battery-sync"
    _attr_mode = "box"

    def __init__(
        self,
        coordinator: LeapmotorDataUpdateCoordinator,
        vin: str,
    ) -> None:
        super().__init__(coordinator)
        self.vin = vin
        self._attr_unique_id = f"{vin}_charge_limit_setting"
        vehicle = self.vehicle_data["vehicle"]
        self._attr_suggested_object_id = _suggested_object_id(
            vehicle,
            english_entity_slug("number", "charge_limit_setting") or "charge_limit_setting",
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
        return super().available and bool(self.coordinator.client.operation_password)

    @property
    def native_value(self) -> int | None:
        """Return the current charge limit."""
        value = self.vehicle_data["charging"].get("charge_limit_percent")
        if value is None:
            return None
        try:
            return int(round(float(value)))
        except (TypeError, ValueError):
            return None

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        """Return useful vehicle metadata."""
        vehicle = self.vehicle_data["vehicle"]
        return {
            "vin": self.vin,
            "car_id": vehicle.get("car_id"),
            "car_type": vehicle.get("car_type"),
            "is_shared": vehicle.get("is_shared"),
            "operation_password_configured": bool(self.coordinator.client.operation_password),
        }

    async def async_set_native_value(self, value: float) -> None:
        """Set the charge limit."""
        if not self.coordinator.client.operation_password:
            raise HomeAssistantError(
                "Vehicle PIN is not configured. Read-only data works without a PIN, "
                "but charge-limit changes require it."
            )

        charge_limit_percent = int(round(value))
        try:
            result = await self.hass.async_add_executor_job(
                self.coordinator.client.set_charge_limit,
                self.vin,
                charge_limit_percent,
            )
        except Exception as exc:
            message = format_remote_error(exc)
            self.coordinator.record_remote_action(
                self.vin,
                "set_charge_limit",
                success=False,
                error=message,
            )
            raise HomeAssistantError(message) from exc

        self.coordinator.record_remote_action(
            self.vin,
            "set_charge_limit",
            success=True,
            result=result,
        )
        await self.coordinator.async_request_refresh()


def _suggested_object_id(vehicle: dict[str, Any], slug: str) -> str:
    """Return a stable English suggested object id independent from UI language."""
    prefix = str(vehicle.get("car_type") or "leapmotor").strip().lower()
    prefix = "".join(char if char.isalnum() else "_" for char in prefix).strip("_")
    return f"{prefix or 'leapmotor'}_{slug}"
