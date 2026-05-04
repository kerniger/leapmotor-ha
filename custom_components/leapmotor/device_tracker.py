"""Device tracker entities for Leapmotor."""

from __future__ import annotations

from typing import Any

from homeassistant.components.device_tracker.config_entry import TrackerEntity
from homeassistant.components.device_tracker.const import SourceType
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.device_registry import DeviceInfo
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from .const import DOMAIN
from .coordinator import LeapmotorDataUpdateCoordinator
from .entity_helpers import build_vehicle_display_name


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up Leapmotor device trackers."""
    coordinator: LeapmotorDataUpdateCoordinator = hass.data[DOMAIN][entry.entry_id]
    async_add_entities(
        LeapmotorDeviceTracker(coordinator, vin)
        for vin in coordinator.data.get("vehicles", {})
    )


class LeapmotorDeviceTracker(
    CoordinatorEntity[LeapmotorDataUpdateCoordinator],
    TrackerEntity,
):
    """Leapmotor GPS tracker."""

    def __init__(self, coordinator: LeapmotorDataUpdateCoordinator, vin: str) -> None:
        super().__init__(coordinator)
        self.vin = vin
        self._attr_unique_id = f"{vin}_location"
        vehicle = self.vehicle_data["vehicle"]
        self._attr_has_entity_name = True
        self._attr_name = "Location"
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
    def latitude(self) -> float | None:
        """Return GPS latitude."""
        lat, lon = self._coordinates
        if lat == 0 and lon == 0:
            return None
        return lat

    @property
    def longitude(self) -> float | None:
        """Return GPS longitude."""
        lat, lon = self._coordinates
        if lat == 0 and lon == 0:
            return None
        return lon

    @property
    def source_type(self) -> SourceType:
        """Return GPS source type."""
        return SourceType.GPS

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        """Return location metadata."""
        vehicle = self.vehicle_data["vehicle"]
        location = self.vehicle_data["location"]
        return {
            "vin": self.vin,
            "car_id": vehicle.get("car_id"),
            "car_type": vehicle.get("car_type"),
            "is_shared": vehicle.get("is_shared"),
            "privacy_gps": location.get("privacy_gps"),
            "privacy_data": location.get("privacy_data"),
            "location_source": location.get("location_source", "cloud"),
            "location_age_seconds": location.get("location_age_seconds"),
            "location_is_stale": location.get("location_is_stale"),
        }

    @property
    def _coordinates(self) -> tuple[float | None, float | None]:
        location = self.vehicle_data["location"]
        return (_to_float(location.get("latitude")), _to_float(location.get("longitude")))


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
