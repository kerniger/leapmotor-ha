"""Independent, read-only Leapmotor China beta."""
from homeassistant.const import Platform
from homeassistant.exceptions import ConfigEntryAuthFailed
from homeassistant.helpers.storage import Store

from .cn_api import CNReadOnlyApiClient
from .const import CONF_CN_SESSION, CONF_SCAN_INTERVAL, DEFAULT_SCAN_INTERVAL_MINUTES, DOMAIN_CN
from .coordinator import CNCoordinator
from .models import LeapmotorAuthError
from .const import CONF_PROXY_URL

PLATFORMS = [Platform.SENSOR, Platform.BINARY_SENSOR, Platform.BUTTON]


def session_store(hass, entry):
    return Store(hass, 1, f"{DOMAIN_CN}.cn_session.{entry.entry_id}", private=True)


async def async_setup_entry(hass, entry):
    store = session_store(hass, entry)
    saved = await store.async_load()
    try:
        client = CNReadOnlyApiClient(saved if saved is not None else entry.data[CONF_CN_SESSION],
                                    entry.options.get(CONF_PROXY_URL, entry.data.get(CONF_PROXY_URL, "")))
    except LeapmotorAuthError:
        raise ConfigEntryAuthFailed("Invalid CN session; reauthentication required.") from None
    interval = entry.options.get(CONF_SCAN_INTERVAL, entry.data.get(CONF_SCAN_INTERVAL, DEFAULT_SCAN_INTERVAL_MINUTES))
    coordinator = CNCoordinator(hass, entry, client, store, interval)
    try:
        await coordinator.async_config_entry_first_refresh()
        hass.data.setdefault(DOMAIN_CN, {})[entry.entry_id] = coordinator
        await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
    except BaseException:
        hass.data.get(DOMAIN_CN, {}).pop(entry.entry_id, None)
        await hass.async_add_executor_job(client.close)
        raise
    entry.async_on_unload(entry.add_update_listener(async_reload_entry))
    return True


async def async_unload_entry(hass, entry):
    unloaded = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
    if unloaded:
        coordinator = hass.data[DOMAIN_CN].pop(entry.entry_id)
        await hass.async_add_executor_job(coordinator.client.close)
    return unloaded


async def async_reload_entry(hass, entry):
    await hass.config_entries.async_reload(entry.entry_id)


async def async_remove_entry(hass, entry):
    await session_store(hass, entry).async_remove()
