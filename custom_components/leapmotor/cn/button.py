"""Manual refresh button for CN vehicles."""
from homeassistant.components.button import ButtonDeviceClass, ButtonEntity, ButtonEntityDescription
from homeassistant.helpers.device_registry import DeviceInfo
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from .const import DOMAIN_CN
from .entity_discovery import register_discovery

async def async_setup_entry(hass, entry, async_add_entities):
    coordinator = hass.data[DOMAIN_CN][entry.entry_id]

    def factory(coordinator, entry_id, vin, description):
        return CNRefreshButton(coordinator, entry_id, vin, description)

    register_discovery(coordinator, entry, async_add_entities, supported_descriptions, factory)

def supported_descriptions(data):
    """Return the refresh button description if the vehicle is present."""
    if isinstance(data, dict) and "vehicle" in data:
        return (ButtonEntityDescription(key="refresh", name="Refresh", device_class=ButtonDeviceClass.UPDATE),)
    return ()

class CNRefreshButton(CoordinatorEntity, ButtonEntity):
    _attr_has_entity_name = True
    _attr_translation_key = "refresh"

    def __init__(self, coordinator, entry_id, vin, description):
        super().__init__(coordinator)
        self.vin = vin
        self.entity_description = description
        self._attr_unique_id = f"{DOMAIN_CN}_{entry_id}_{vin}_{description.key}"
        vehicle = (coordinator.data or {}).get("vehicles", {}).get(vin, {}).get("vehicle", {})
        model = str(vehicle.get("car_type") or "Unknown")
        self._attr_device_info = DeviceInfo(
            identifiers={(DOMAIN_CN, vin)},
            manufacturer="Leapmotor",
            model=model,
            name=f"Leapmotor China {model}"
        )

    @property
    def available(self):
        """Unavailable when the VIN is no longer in coordinator data."""
        return super().available and self.vin in (self.coordinator.data or {}).get("vehicles", {})

    async def async_press(self):
        """Request a coordinator refresh."""
        await self.coordinator.async_request_refresh()
