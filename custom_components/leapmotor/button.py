"""Button entities for Leapmotor."""

from __future__ import annotations

from typing import Any

from homeassistant.components.button import ButtonEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.device_registry import DeviceInfo
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from .const import (
    DOMAIN,
    REMOTE_CTL_FIND_CAR,
    REMOTE_CTL_QUICK_COOL,
    REMOTE_CTL_QUICK_HEAT,
    REMOTE_CTL_SUNSHADE_CLOSE,
    REMOTE_CTL_SUNSHADE_OPEN,
    REMOTE_CTL_TRUNK_CLOSE,
    REMOTE_CTL_TRUNK_OPEN,
    REMOTE_CTL_UNLOCK_CHARGER,
    REMOTE_CTL_WINDSHIELD_DEFROST,
    REMOTE_CTL_WINDOWS_CLOSE,
    REMOTE_CTL_WINDOWS_OPEN,
)
from .coordinator import LeapmotorDataUpdateCoordinator
from .entity_helpers import build_vehicle_display_name
from .entity_migration import english_entity_slug
from .remote_helpers import RemoteActionSpec, async_execute_remote_action


BUTTON_SPECS: tuple[RemoteActionSpec, ...] = (
    RemoteActionSpec(
        action=REMOTE_CTL_UNLOCK_CHARGER,
        translation_key="unlock_charger",
        icon="mdi:ev-plug-type2",
        method_name="unlock_charger",
        service_name=REMOTE_CTL_UNLOCK_CHARGER,
    ),
    RemoteActionSpec(
        action=REMOTE_CTL_TRUNK_OPEN,
        translation_key="open_trunk",
        icon="mdi:car-back",
        method_name="open_trunk",
        service_name=REMOTE_CTL_TRUNK_OPEN,
    ),
    RemoteActionSpec(
        action=REMOTE_CTL_TRUNK_CLOSE,
        translation_key="close_trunk",
        icon="mdi:car-back",
        method_name="close_trunk",
        service_name=REMOTE_CTL_TRUNK_CLOSE,
    ),
    RemoteActionSpec(
        action=REMOTE_CTL_FIND_CAR,
        translation_key="find_vehicle",
        icon="mdi:bullhorn",
        method_name="find_vehicle",
        service_name=REMOTE_CTL_FIND_CAR,
    ),
    RemoteActionSpec(
        action=REMOTE_CTL_SUNSHADE_OPEN,
        translation_key="open_sunshade",
        icon="mdi:blinds-open",
        method_name="open_sunshade",
        service_name=REMOTE_CTL_SUNSHADE_OPEN,
    ),
    RemoteActionSpec(
        action=REMOTE_CTL_SUNSHADE_CLOSE,
        translation_key="close_sunshade",
        icon="mdi:blinds-horizontal-closed",
        method_name="close_sunshade",
        service_name=REMOTE_CTL_SUNSHADE_CLOSE,
    ),
    RemoteActionSpec(
        action=REMOTE_CTL_WINDOWS_OPEN,
        translation_key="open_windows",
        icon="mdi:window-open",
        method_name="open_windows",
        service_name=REMOTE_CTL_WINDOWS_OPEN,
    ),
    RemoteActionSpec(
        action=REMOTE_CTL_WINDOWS_CLOSE,
        translation_key="close_windows",
        icon="mdi:window-closed",
        method_name="close_windows",
        service_name=REMOTE_CTL_WINDOWS_CLOSE,
    ),
    RemoteActionSpec(
        action=REMOTE_CTL_QUICK_COOL,
        translation_key="quick_cool",
        icon="mdi:snowflake",
        method_name="quick_cool",
        service_name=REMOTE_CTL_QUICK_COOL,
    ),
    RemoteActionSpec(
        action=REMOTE_CTL_QUICK_HEAT,
        translation_key="quick_heat",
        icon="mdi:fire",
        method_name="quick_heat",
        service_name=REMOTE_CTL_QUICK_HEAT,
    ),
    RemoteActionSpec(
        action=REMOTE_CTL_WINDSHIELD_DEFROST,
        translation_key="windshield_defrost",
        icon="mdi:car-defrost-front",
        method_name="windshield_defrost",
        service_name=REMOTE_CTL_WINDSHIELD_DEFROST,
    ),
)


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up Leapmotor buttons."""
    coordinator: LeapmotorDataUpdateCoordinator = hass.data[DOMAIN][entry.entry_id]
    entities: list[LeapmotorActionButton] = []
    for vin in coordinator.data.get("vehicles", {}):
        entities.append(LeapmotorRefreshButton(coordinator, vin))
        for spec in BUTTON_SPECS:
            entities.append(LeapmotorActionButton(coordinator, vin, spec))
    async_add_entities(entities)


class LeapmotorActionButton(CoordinatorEntity[LeapmotorDataUpdateCoordinator], ButtonEntity):
    """Leapmotor remote-control button."""

    _attr_has_entity_name = True

    def __init__(
        self,
        coordinator: LeapmotorDataUpdateCoordinator,
        vin: str,
        spec: RemoteActionSpec,
    ) -> None:
        super().__init__(coordinator)
        self.vin = vin
        self.spec = spec
        self._attr_translation_key = spec.translation_key
        self._attr_icon = spec.icon
        self._attr_unique_id = f"{vin}_{spec.action}"

        vehicle = self.vehicle_data["vehicle"]
        self._attr_suggested_object_id = _suggested_object_id(
            vehicle,
            english_entity_slug("button", spec.action) or spec.action,
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
        """Return button availability."""
        return super().available and bool(self.coordinator.client.operation_password)

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        """Return control metadata."""
        vehicle = self.vehicle_data["vehicle"]
        remote = self.vehicle_data.get("remote_control") or {}
        return {
            "vin": self.vin,
            "action": self.spec.action,
            "operation_password_configured": bool(self.coordinator.client.operation_password),
            "last_remote_action": remote.get("action"),
            "last_remote_status": remote.get("status"),
            "last_remote_success": remote.get("success"),
            "last_remote_updated_at": remote.get("updated_at"),
            "last_remote_error": remote.get("error"),
            "car_type": vehicle.get("car_type"),
            "is_shared": vehicle.get("is_shared"),
        }

    async def async_press(self) -> None:
        """Execute the configured remote-control action."""
        await async_execute_remote_action(self.coordinator, self.vin, self.spec)


class LeapmotorRefreshButton(CoordinatorEntity[LeapmotorDataUpdateCoordinator], ButtonEntity):
    """Manual refresh button for one vehicle."""

    _attr_has_entity_name = True
    _attr_translation_key = "refresh_data"
    _attr_icon = "mdi:refresh"

    def __init__(self, coordinator: LeapmotorDataUpdateCoordinator, vin: str) -> None:
        super().__init__(coordinator)
        self.vin = vin
        self._attr_unique_id = f"{vin}_refresh_data"
        vehicle = self.vehicle_data["vehicle"]
        self._attr_suggested_object_id = _suggested_object_id(
            vehicle,
            english_entity_slug("button", "refresh_data") or "refresh_data",
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
    def extra_state_attributes(self) -> dict[str, Any]:
        """Return refresh context."""
        integration = self.coordinator.integration_status
        return {
            "vin": self.vin,
            "last_update_status": integration.get("last_update_status"),
            "last_successful_update_at": integration.get("last_successful_update_at"),
            "last_update_error_code": integration.get("last_update_error_code"),
            "last_update_reason": integration.get("last_update_reason"),
            "update_interval_seconds": integration.get("update_interval_seconds"),
        }

    async def async_press(self) -> None:
        """Run an on-demand coordinator refresh."""
        await self.coordinator.async_manual_refresh()


def _suggested_object_id(vehicle: dict[str, Any], slug: str) -> str:
    """Return a stable English suggested object id independent from UI language."""
    prefix = str(vehicle.get("car_type") or "leapmotor").strip().lower()
    prefix = "".join(char if char.isalnum() else "_" for char in prefix).strip("_")
    return f"{prefix or 'leapmotor'}_{slug}"
