"""Image entities for Leapmotor."""

from __future__ import annotations

import io
import logging
import zipfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from homeassistant.components.image import ImageEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.device_registry import DeviceInfo
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from .const import DOMAIN
from .coordinator import LeapmotorDataUpdateCoordinator
from .entity_helpers import build_vehicle_display_name
from .entity_migration import english_entity_slug

_LOGGER = logging.getLogger(__name__)


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up Leapmotor image entities."""
    coordinator: LeapmotorDataUpdateCoordinator = hass.data[DOMAIN][entry.entry_id]
    async_add_entities(
        LeapmotorVehicleImage(coordinator, vin) for vin in coordinator.data.get("vehicles", {})
    )


class LeapmotorVehicleImage(
    CoordinatorEntity[LeapmotorDataUpdateCoordinator], ImageEntity
):
    """Expose the current vehicle image as a native HA image entity."""

    _attr_has_entity_name = True
    _attr_translation_key = "vehicle_picture"

    def __init__(self, coordinator: LeapmotorDataUpdateCoordinator, vin: str) -> None:
        """Initialize the vehicle image entity."""
        CoordinatorEntity.__init__(self, coordinator)
        ImageEntity.__init__(self, coordinator.hass)
        self.vin = vin
        self._attr_unique_id = f"{vin}_vehicle_picture_pkg"
        self._attr_icon = "mdi:car"
        self._attr_content_type = "image/png"
        self._cached_image: bytes | None = None
        self._last_picture_key: str | None = None
        self._cache_refresh_attempted = False
        self._attr_image_last_updated: datetime | None = None

        vehicle = self.vehicle_data["vehicle"]
        self._attr_suggested_object_id = _suggested_object_id(
            vehicle,
            english_entity_slug("image", "vehicle_picture_pkg") or "vehicle_picture",
        )
        self._attr_device_info = DeviceInfo(
            identifiers={(DOMAIN, vin)},
            manufacturer="Leapmotor",
            model=vehicle.get("car_type"),
            name=build_vehicle_display_name(vehicle),
            serial_number=vin,
        )
        self._update_image_metadata()

    @property
    def vehicle_data(self) -> dict[str, Any]:
        """Return current data for this vehicle."""
        return self.coordinator.data["vehicles"][self.vin]

    @property
    def available(self) -> bool:
        """Return if the image entity has picture data."""
        return super().available and self._picture_key is not None

    @property
    def _picture_key(self) -> str | None:
        """Return the current picture key."""
        return (self.vehicle_data.get("media") or {}).get("car_picture_key")

    def _cache_path(self, picture_key: str) -> Path:
        """Return the on-disk cache path for the static vehicle image."""
        cache_dir = Path(self.hass.config.path(".storage", DOMAIN, "car_images"))
        cache_dir.mkdir(parents=True, exist_ok=True)
        return cache_dir / f"{self.vin}_{picture_key}_tripsum.png"

    async def async_image(self) -> bytes | None:
        """Return the current image bytes."""
        picture_key = self._picture_key
        if not picture_key:
            return None
        if self._cached_image is not None:
            return self._cached_image
        cache_path = self._cache_path(picture_key)
        if cache_path.exists() and not self._cache_refresh_attempted:
            self._cache_refresh_attempted = True
            try:
                image = await self.hass.async_add_executor_job(
                    self._download_static_vehicle_image,
                    picture_key,
                    cache_path,
                )
            except Exception:  # noqa: BLE001
                _LOGGER.warning(
                    "Could not refresh the Leapmotor vehicle image; using the cached image"
                )
                image = await self.hass.async_add_executor_job(cache_path.read_bytes)
            self._cached_image = image
            return image
        if cache_path.exists():
            image = await self.hass.async_add_executor_job(cache_path.read_bytes)
            self._cached_image = image
            return image

        self._cache_refresh_attempted = True
        image = await self.hass.async_add_executor_job(
            self._download_static_vehicle_image,
            picture_key,
            cache_path,
        )
        self._cached_image = image
        return image

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        """Return non-sensitive picture metadata."""
        media = self.vehicle_data.get("media") or {}
        return {
            "vin": self.vin,
            "key_present": media.get("car_picture_key_present"),
            "whole_present": media.get("car_picture_whole_present"),
            "render_mode": "package_tripsum",
        }

    def _update_image_metadata(self) -> None:
        """Refresh the last-updated timestamp when the picture key changes."""
        picture_key = self._picture_key
        if picture_key != self._last_picture_key:
            self._last_picture_key = picture_key
            self._cached_image = None
            self._cache_refresh_attempted = False
            self._attr_image_last_updated = datetime.now(UTC)

    def _handle_coordinator_update(self) -> None:
        """Handle updated data from the coordinator."""
        self._update_image_metadata()
        super()._handle_coordinator_update()

    def _download_static_vehicle_image(self, picture_key: str, cache_path: Path) -> bytes:
        """Download, extract, and cache the static vehicle image from the package ZIP."""
        package_bytes = self.coordinator.client.download_car_picture_package(picture_key=picture_key)
        with zipfile.ZipFile(io.BytesIO(package_bytes)) as package_zip:
            with package_zip.open("android/xxhdpi/carpic_for_tripsum.png") as image_file:
                image = image_file.read()
        cache_path.write_bytes(image)
        return image


def _suggested_object_id(vehicle: dict[str, Any], slug: str) -> str:
    """Return a stable English suggested object id independent from UI language."""
    prefix = str(vehicle.get("car_type") or "leapmotor").strip().lower()
    prefix = "".join(char if char.isalnum() else "_" for char in prefix).strip("_")
    return f"{prefix or 'leapmotor'}_{slug}"
