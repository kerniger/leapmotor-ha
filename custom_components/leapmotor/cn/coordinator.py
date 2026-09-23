"""CN read-only polling and durable gateway renewal."""
from datetime import timedelta
import logging

from homeassistant.exceptions import ConfigEntryAuthFailed
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed

from .const import DOMAIN_CN
from .models import LeapmotorApiError, LeapmotorAuthError


class CNCoordinator(DataUpdateCoordinator):
    """Persist rotated credentials before reading again; never send SMS."""

    def __init__(self, hass, entry, client, store, interval):
        super().__init__(hass, logging.getLogger(__name__), name=DOMAIN_CN,
                         config_entry=entry, update_interval=timedelta(minutes=interval))
        self.client = client
        self.store = store

    async def _save_session(self):
        if self.client.session_pending_save:
            try:
                await self.store.async_save(self.client.session_snapshot())
            except Exception:
                raise LeapmotorApiError("CN session storage failed; polling paused.") from None
            self.client.session_pending_save = False

    async def _async_update_data(self):
        try:
            await self._save_session()
            try:
                return await self.hass.async_add_executor_job(self.client.fetch_data)
            except LeapmotorAuthError:
                await self.hass.async_add_executor_job(self.client.refresh_session)
            await self._save_session()
            return await self.hass.async_add_executor_job(self.client.fetch_data)
        except LeapmotorAuthError:
            raise ConfigEntryAuthFailed("Please reauthenticate your CN account.") from None
        except LeapmotorApiError as error:
            raise UpdateFailed(str(error)) from None
