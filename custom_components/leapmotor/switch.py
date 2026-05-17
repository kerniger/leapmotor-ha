"""Switch entities for Leapmotor."""

from __future__ import annotations

from typing import Any

from homeassistant.components.switch import SwitchEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers.device_registry import DeviceInfo
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from .const import DOMAIN
from .coordinator import LeapmotorDataUpdateCoordinator
from .entity_helpers import build_vehicle_display_name
from .entity_migration import english_entity_slug
from .remote_helpers import RemoteActionSpec, async_execute_remote_action, format_remote_error


BATTERY_PREHEAT_ON_ACTION = RemoteActionSpec(
    action="battery_preheat",
    translation_key="battery_preheat",
    icon="mdi:heat-wave",
    method_name="battery_preheat",
    service_name="battery_preheat",
)

BATTERY_PREHEAT_OFF_ACTION = RemoteActionSpec(
    action="battery_preheat_off",
    translation_key="battery_preheat_off",
    icon="mdi:heat-wave",
    method_name="battery_preheat_off",
    service_name="battery_preheat_off",
)


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up Leapmotor switch entities."""
    coordinator: LeapmotorDataUpdateCoordinator = hass.data[DOMAIN][entry.entry_id]
    entities: list[SwitchEntity] = []
    for vin in coordinator.data.get("vehicles", {}):
        entities.append(LeapmotorChargingScheduleSwitch(coordinator, vin))
        entities.append(LeapmotorBatteryPreheatSwitch(coordinator, vin))
    async_add_entities(entities)


class LeapmotorChargingScheduleSwitch(
    CoordinatorEntity[LeapmotorDataUpdateCoordinator],
    SwitchEntity,
):
    """Charging schedule enable switch."""

    _attr_has_entity_name = True
    _attr_translation_key = "charging_schedule"
    _attr_icon = "mdi:calendar-clock"

    def __init__(
        self,
        coordinator: LeapmotorDataUpdateCoordinator,
        vin: str,
    ) -> None:
        super().__init__(coordinator)
        self.vin = vin
        self._attr_unique_id = f"{vin}_charging_schedule"
        vehicle = self.vehicle_data["vehicle"]
        self._attr_suggested_object_id = _suggested_object_id(
            vehicle,
            english_entity_slug("switch", "charging_schedule") or "charging_schedule",
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
    def is_on(self) -> bool | None:
        """Return whether the charging schedule is enabled."""
        value = self.vehicle_data["charging"].get("charging_planned_enabled")
        if value is None:
            return None
        return bool(value)

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        """Return useful vehicle metadata."""
        vehicle = self.vehicle_data["vehicle"]
        charging = self.vehicle_data["charging"]
        return {
            "vin": self.vin,
            "car_id": vehicle.get("car_id"),
            "car_type": vehicle.get("car_type"),
            "is_shared": vehicle.get("is_shared"),
            "operation_password_configured": bool(self.coordinator.client.operation_password),
            "charge_limit_percent": charging.get("charge_limit_percent"),
            "charging_schedule_start": charging.get("charging_planned_start"),
            "charging_schedule_end": charging.get("charging_planned_end"),
            "charging_schedule_cycles": charging.get("charging_planned_cycles"),
        }

    async def async_turn_on(self, **kwargs: Any) -> None:
        """Enable the charging schedule."""
        await self._async_set_enabled(True)

    async def async_turn_off(self, **kwargs: Any) -> None:
        """Disable the charging schedule."""
        await self._async_set_enabled(False)

    async def _async_set_enabled(self, enabled: bool) -> None:
        """Set the charging schedule state."""
        if not self.coordinator.client.operation_password:
            raise HomeAssistantError(
                "Vehicle PIN is not configured. Read-only data works without a PIN, "
                "but charging schedule changes require it."
            )

        action = "set_charging_schedule"
        try:
            result = await self.hass.async_add_executor_job(
                self.coordinator.client.set_charging_plan_enabled,
                self.vin,
                enabled,
            )
        except Exception as exc:
            message = format_remote_error(exc)
            self.coordinator.record_remote_action(
                self.vin,
                action,
                success=False,
                error=message,
            )
            raise HomeAssistantError(message) from exc

        self.coordinator.record_remote_action(
            self.vin,
            action,
            success=True,
            result=result,
        )
        await self.coordinator.async_request_refresh()


class LeapmotorBatteryPreheatSwitch(
    CoordinatorEntity[LeapmotorDataUpdateCoordinator],
    SwitchEntity,
):
    """Battery preheat switch."""

    _attr_has_entity_name = True
    _attr_translation_key = "battery_preheat"
    _attr_icon = "mdi:heat-wave"

    def __init__(
        self,
        coordinator: LeapmotorDataUpdateCoordinator,
        vin: str,
    ) -> None:
        super().__init__(coordinator)
        self.vin = vin
        self._attr_unique_id = f"{vin}_battery_preheat"
        vehicle = self.vehicle_data["vehicle"]
        self._attr_suggested_object_id = _suggested_object_id(
            vehicle,
            english_entity_slug("switch", "battery_preheat") or "battery_preheat",
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
    def is_on(self) -> bool | None:
        """Return whether battery preheating is active."""
        value = self.vehicle_data["diagnostics"].get("battery_heating")
        if value is None:
            return None
        return bool(value)

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        """Return useful vehicle metadata."""
        vehicle = self.vehicle_data["vehicle"]
        diagnostics = self.vehicle_data["diagnostics"]
        return {
            "vin": self.vin,
            "car_id": vehicle.get("car_id"),
            "car_type": vehicle.get("car_type"),
            "is_shared": vehicle.get("is_shared"),
            "operation_password_configured": bool(self.coordinator.client.operation_password),
            "battery_thermal_request": diagnostics.get("battery_thermal_request"),
        }

    async def async_turn_on(self, **kwargs: Any) -> None:
        """Start battery preheating."""
        await async_execute_remote_action(
            self.coordinator,
            self.vin,
            BATTERY_PREHEAT_ON_ACTION,
        )

    async def async_turn_off(self, **kwargs: Any) -> None:
        """Stop battery preheating."""
        await async_execute_remote_action(
            self.coordinator,
            self.vin,
            BATTERY_PREHEAT_OFF_ACTION,
        )


def _suggested_object_id(vehicle: dict[str, Any], slug: str) -> str:
    """Return a stable English suggested object id independent from UI language."""
    prefix = str(vehicle.get("car_type") or "leapmotor").strip().lower()
    prefix = "".join(char if char.isalnum() else "_" for char in prefix).strip("_")
    return f"{prefix or 'leapmotor'}_{slug}"
