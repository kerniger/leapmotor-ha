"""Number entities for Leapmotor."""

from __future__ import annotations

from dataclasses import dataclass
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


@dataclass(frozen=True, slots=True)
class SeatComfortNumberDescription:
    """Description for one controllable seat-comfort number."""

    unique_suffix: str
    translation_key: str
    diagnostic_key: str
    method_name: str
    action_name: str
    position: str


SEAT_COMFORT_NUMBERS: tuple[SeatComfortNumberDescription, ...] = (
    SeatComfortNumberDescription(
        unique_suffix="driver_seat_heating",
        translation_key="driver_seat_heating",
        diagnostic_key="driver_seat_heating_level",
        method_name="seat_heat",
        action_name="seat_heat",
        position="driver",
    ),
    SeatComfortNumberDescription(
        unique_suffix="passenger_seat_heating",
        translation_key="passenger_seat_heating",
        diagnostic_key="passenger_seat_heating_level",
        method_name="seat_heat",
        action_name="seat_heat",
        position="copilot",
    ),
    SeatComfortNumberDescription(
        unique_suffix="driver_seat_ventilation",
        translation_key="driver_seat_ventilation",
        diagnostic_key="driver_seat_ventilation_level",
        method_name="seat_ventilation",
        action_name="seat_ventilation",
        position="driver",
    ),
    SeatComfortNumberDescription(
        unique_suffix="passenger_seat_ventilation",
        translation_key="passenger_seat_ventilation",
        diagnostic_key="passenger_seat_ventilation_level",
        method_name="seat_ventilation",
        action_name="seat_ventilation",
        position="copilot",
    ),
)


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up Leapmotor number entities."""
    coordinator: LeapmotorDataUpdateCoordinator = hass.data[DOMAIN][entry.entry_id]
    entities: list[NumberEntity] = []
    for vin in coordinator.data.get("vehicles", {}):
        entities.append(LeapmotorChargeLimitNumber(coordinator, vin))
        diagnostics = coordinator.data["vehicles"][vin].get("diagnostics", {})
        for description in SEAT_COMFORT_NUMBERS:
            if diagnostics.get(description.diagnostic_key) is not None:
                entities.append(LeapmotorSeatComfortNumber(coordinator, vin, description))
    async_add_entities(entities)


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


class LeapmotorSeatComfortNumber(
    CoordinatorEntity[LeapmotorDataUpdateCoordinator],
    NumberEntity,
):
    """Editable seat heating or ventilation level."""

    _attr_has_entity_name = True
    _attr_native_min_value = 0
    _attr_native_max_value = 3
    _attr_native_step = 1
    _attr_icon = "mdi:car-seat-heater"
    _attr_mode = "slider"

    def __init__(
        self,
        coordinator: LeapmotorDataUpdateCoordinator,
        vin: str,
        description: SeatComfortNumberDescription,
    ) -> None:
        super().__init__(coordinator)
        self.vin = vin
        self._description = description
        self._attr_translation_key = description.translation_key
        self._attr_unique_id = f"{vin}_{description.unique_suffix}"
        if "ventilation" in description.unique_suffix:
            self._attr_icon = "mdi:car-seat-cooler"
        vehicle = self.vehicle_data["vehicle"]
        self._attr_suggested_object_id = _suggested_object_id(
            vehicle,
            english_entity_slug("number", description.unique_suffix)
            or description.unique_suffix,
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
        """Return current seat comfort level."""
        value = self.vehicle_data["diagnostics"].get(self._description.diagnostic_key)
        if value is None:
            return None
        try:
            return int(value)
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
            "seat_position": self._description.position,
        }

    async def async_set_native_value(self, value: float) -> None:
        """Set the seat comfort level."""
        if not self.coordinator.client.operation_password:
            raise HomeAssistantError(
                "Vehicle PIN is not configured. Read-only data works without a PIN, "
                "but seat comfort changes require it."
            )

        level = max(0, min(3, int(round(value))))
        method = getattr(self.coordinator.client, self._description.method_name)
        try:
            result = await self.hass.async_add_executor_job(
                method,
                self.vin,
                self._description.position,
                level,
            )
        except Exception as exc:
            message = format_remote_error(exc)
            self.coordinator.record_remote_action(
                self.vin,
                self._description.action_name,
                success=False,
                error=message,
            )
            raise HomeAssistantError(message) from exc

        self.coordinator.record_remote_action(
            self.vin,
            self._description.action_name,
            success=True,
            result=result,
        )
        await self.coordinator.async_request_refresh()


def _suggested_object_id(vehicle: dict[str, Any], slug: str) -> str:
    """Return a stable English suggested object id independent from UI language."""
    prefix = str(vehicle.get("car_type") or "leapmotor").strip().lower()
    prefix = "".join(char if char.isalnum() else "_" for char in prefix).strip("_")
    return f"{prefix or 'leapmotor'}_{slug}"
