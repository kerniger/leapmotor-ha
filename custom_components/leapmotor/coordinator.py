"""Data coordinator for Leapmotor."""

from __future__ import annotations

from datetime import timedelta
from functools import partial
import logging
import time
from typing import Any

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed

from .abrp import build_abrp_telemetry, send_abrp_telemetry
from .api import LeapmotorApiClient, LeapmotorApiError
from .const import (
    CONF_ABRP_ENABLED,
    CONF_ABRP_TOKEN,
    DEFAULT_ABRP_API_KEY,
    DEFAULT_STATE_STALE_SECONDS,
    DOMAIN,
    REMOTE_ACTION_COOLDOWN_SECONDS,
)

_LOGGER = logging.getLogger(__name__)


class LeapmotorDataUpdateCoordinator(DataUpdateCoordinator[dict]):
    """Fetch Leapmotor vehicle data."""

    def __init__(
        self,
        *,
        hass: HomeAssistant,
        config_entry: ConfigEntry,
        client: LeapmotorApiClient,
        update_interval: timedelta,
    ) -> None:
        super().__init__(
            hass,
            _LOGGER,
            name=DOMAIN,
            config_entry=config_entry,
            update_interval=update_interval,
        )
        self.client = client
        self._lock_state_overrides: dict[str, tuple[bool, float]] = {}
        self._last_remote_results: dict[str, dict[str, Any]] = {}
        self._last_abrp_results: dict[str, dict[str, Any]] = {}
        self._last_vehicle_states: dict[str, str] = {}
        self._integration_status: dict[str, Any] = {
            "last_update_status": "unknown",
            "last_update_success": None,
            "last_update_error": None,
            "last_update_error_code": None,
            "last_update_started_at": None,
            "last_update_completed_at": None,
            "last_successful_update_at": None,
            "last_update_duration_seconds": None,
            "last_update_reason": "startup",
            "update_interval_seconds": int(update_interval.total_seconds()),
            "vehicle_count": 0,
        }
        self._pending_update_reason = "startup"

    def remote_action_cooldown_remaining(self, vin: str) -> int:
        """Return remaining remote-action cooldown seconds for one vehicle."""
        last_result = self._last_remote_results.get(vin)
        if not last_result:
            return 0
        updated_at = last_result.get("updated_at")
        if not isinstance(updated_at, (int, float)):
            return 0
        remaining = REMOTE_ACTION_COOLDOWN_SECONDS - (time.time() - updated_at)
        return max(0, int(remaining + 0.999))

    async def _async_update_data(self) -> dict:
        started_at = time.time()
        update_reason = self._pending_update_reason
        self._pending_update_reason = "poll"
        try:
            data = await self.hass.async_add_executor_job(self.client.fetch_data)
        except LeapmotorApiError as exc:
            self._integration_status = {
                "last_update_status": "error",
                "last_update_success": False,
                "last_update_error": str(exc),
                "last_update_error_code": self._classify_error(str(exc)),
                "last_update_started_at": started_at,
                "last_update_completed_at": time.time(),
                "last_successful_update_at": self._integration_status.get("last_successful_update_at"),
                "last_update_duration_seconds": round(time.time() - started_at, 3),
                "last_update_reason": update_reason,
                "update_interval_seconds": self._integration_status.get("update_interval_seconds"),
                "vehicle_count": self._integration_status.get("vehicle_count", 0),
            }
            raise UpdateFailed(str(exc)) from exc
        self._integration_status = {
            "last_update_status": "ok",
            "last_update_success": True,
            "last_update_error": None,
            "last_update_error_code": None,
            "last_update_started_at": started_at,
            "last_update_completed_at": time.time(),
            "last_successful_update_at": time.time(),
            "last_update_duration_seconds": round(time.time() - started_at, 3),
            "last_update_reason": update_reason,
            "update_interval_seconds": self._integration_status.get("update_interval_seconds"),
            "vehicle_count": len((data.get("vehicles") or {})),
        }
        self._stabilize_vehicle_states(data)
        self._apply_state_freshness(data)
        await self._async_push_abrp(data)
        self._apply_lock_state_overrides(data)
        self._apply_remote_results(data)
        self._apply_abrp_results(data)
        self._apply_integration_status(data)
        return data

    @property
    def integration_status(self) -> dict[str, Any]:
        """Return the current integration-wide update status."""
        return dict(self._integration_status)

    async def async_manual_refresh(self) -> None:
        """Force an immediate manual refresh outside the normal polling cadence."""
        self._pending_update_reason = "manual"
        await self.async_request_refresh()

    def set_lock_state_override(
        self,
        vin: str,
        is_locked: bool,
        *,
        ttl_seconds: int = 120,
    ) -> None:
        """Temporarily prefer confirmed remote-control state over stale cloud status."""
        self._lock_state_overrides[vin] = (is_locked, time.time() + ttl_seconds)
        if self.data:
            data = dict(self.data)
            self._apply_single_lock_override(data, vin, is_locked)
            self.async_set_updated_data(data)

    def record_remote_action(
        self,
        vin: str,
        action: str,
        *,
        success: bool,
        result: dict[str, Any] | None = None,
        error: str | None = None,
    ) -> None:
        """Store the last remote-control result for diagnostics and attributes."""
        remote_data = (result or {}).get("data") if isinstance(result, dict) else None
        info = {
            "action": action,
            "success": success,
            "status": "success" if success else "failed",
            "updated_at": time.time(),
            "api_code": (result or {}).get("code") if isinstance(result, dict) else None,
            "api_message": (result or {}).get("message") if isinstance(result, dict) else None,
            "remote_ctl_id": remote_data.get("remoteCtlId") if isinstance(remote_data, dict) else None,
            "error": error,
        }
        self._last_remote_results[vin] = info
        if self.data:
            data = dict(self.data)
            self._apply_single_remote_result(data, vin, info)
            self.async_set_updated_data(data)

    def _apply_lock_state_overrides(self, data: dict[str, Any]) -> None:
        """Apply non-expired optimistic lock states to freshly fetched data."""
        now = time.time()
        expired = [
            vin for vin, (_, expires_at) in self._lock_state_overrides.items()
            if expires_at <= now
        ]
        for vin in expired:
            self._lock_state_overrides.pop(vin, None)

        for vin, (is_locked, _) in self._lock_state_overrides.items():
            self._apply_single_lock_override(data, vin, is_locked)

    def _apply_remote_results(self, data: dict[str, Any]) -> None:
        """Apply remembered remote-control results to freshly fetched data."""
        for vin, info in self._last_remote_results.items():
            self._apply_single_remote_result(data, vin, info)

    def _apply_abrp_results(self, data: dict[str, Any]) -> None:
        """Apply remembered ABRP telemetry results to freshly fetched data."""
        for vin, info in self._last_abrp_results.items():
            vehicle_data = (data.get("vehicles") or {}).get(vin)
            if vehicle_data:
                vehicle_data["abrp"] = dict(info)

    def _apply_integration_status(self, data: dict[str, Any]) -> None:
        """Expose the current update status inside the coordinator payload."""
        data["_integration"] = dict(self._integration_status)

    def _apply_state_freshness(self, data: dict[str, Any]) -> None:
        """Mark critical states as stale when the cloud timestamp is too old."""
        for vehicle_data in (data.get("vehicles") or {}).values():
            status = vehicle_data.get("status") or {}
            location = vehicle_data.get("location") or {}
            age_seconds = _state_age_seconds(status.get("last_vehicle_timestamp"))
            is_stale = age_seconds is not None and age_seconds > DEFAULT_STATE_STALE_SECONDS

            status["lock_state_age_seconds"] = age_seconds
            status["lock_state_is_stale"] = is_stale
            if is_stale:
                status["lock_state_source"] = "cloud_stale"
                if status.get("is_locked") is False:
                    status["is_locked"] = None
            elif status.get("is_locked") is not None and status.get("lock_state_source") is None:
                status["lock_state_source"] = "cloud"

            status["vehicle_state_age_seconds"] = age_seconds
            status["vehicle_state_is_stale"] = is_stale
            if is_stale:
                status["stale_vehicle_state"] = status.get("vehicle_state")
                status["vehicle_state"] = None
                status["is_parked"] = None
                status["vehicle_state_source"] = "cloud_stale"

            location["location_age_seconds"] = age_seconds
            location["location_is_stale"] = is_stale
            if location.get("location_source") is None:
                location["location_source"] = "cloud_stale" if is_stale else "cloud"

    async def _async_push_abrp(self, data: dict[str, Any]) -> None:
        """Push vehicle telemetry to ABRP when configured."""
        if not self._config_value(CONF_ABRP_ENABLED, False):
            return
        api_key = DEFAULT_ABRP_API_KEY
        token = str(self._config_value(CONF_ABRP_TOKEN, "") or "")
        if not api_key.strip() or not token.strip():
            return

        for vin, vehicle_data in (data.get("vehicles") or {}).items():
            telemetry = build_abrp_telemetry(vehicle_data)
            started_at = time.time()
            try:
                result = await self.hass.async_add_executor_job(
                    partial(
                        send_abrp_telemetry,
                        api_key=api_key,
                        token=token,
                        telemetry=telemetry,
                    )
                )
            except Exception as exc:  # noqa: BLE001
                self._last_abrp_results[vin] = {
                    "enabled": True,
                    "status": "error",
                    "success": False,
                    "updated_at": time.time(),
                    "duration_seconds": round(time.time() - started_at, 3),
                    "error": str(exc),
                    "telemetry_keys": sorted(telemetry),
                }
                _LOGGER.debug("Leapmotor ABRP telemetry push failed for %s: %s", vin, exc)
                continue

            self._last_abrp_results[vin] = {
                "enabled": True,
                "status": result.get("status", "ok"),
                "success": True,
                "updated_at": time.time(),
                "duration_seconds": round(time.time() - started_at, 3),
                "http_status": result.get("http_status"),
                "missing": result.get("missing"),
                "telemetry_keys": sorted(telemetry),
            }

    def _config_value(self, key: str, default: Any = None) -> Any:
        """Return an option value with config-entry data as migration fallback."""
        if key in self.config_entry.options:
            return self.config_entry.options[key]
        return self.config_entry.data.get(key, default)

    def _stabilize_vehicle_states(self, data: dict[str, Any]) -> None:
        """Keep the last valid parked/driving state across weak startup polls."""
        for vin, vehicle_data in (data.get("vehicles") or {}).items():
            status = vehicle_data.get("status") or {}
            vehicle_state = status.get("vehicle_state")
            if vehicle_state in {"parked", "driving"}:
                self._last_vehicle_states[vin] = vehicle_state
                continue
            last_vehicle_state = self._last_vehicle_states.get(vin)
            if last_vehicle_state:
                status["vehicle_state"] = last_vehicle_state
                status["vehicle_state_source"] = "cached_last_valid"
                status["is_parked"] = last_vehicle_state == "parked"

    @staticmethod
    def _apply_single_lock_override(
        data: dict[str, Any],
        vin: str,
        is_locked: bool,
    ) -> None:
        vehicle_data = (data.get("vehicles") or {}).get(vin)
        if not vehicle_data:
            return
        status = vehicle_data.setdefault("status", {})
        status["is_locked"] = is_locked
        status["lock_state_source"] = "remote_control_confirmed"

    @staticmethod
    def _apply_single_remote_result(
        data: dict[str, Any],
        vin: str,
        info: dict[str, Any],
    ) -> None:
        vehicle_data = (data.get("vehicles") or {}).get(vin)
        if not vehicle_data:
            return
        vehicle_data["remote_control"] = dict(info)

    @staticmethod
    def _classify_error(message: str) -> str:
        """Map raw update errors to a stable diagnostic code."""
        lowered = message.lower()
        if "missing local app certificate material" in lowered:
            return "missing_app_cert"
        if "account certificate" in lowered or "account_cert_error" in lowered:
            return "account_cert_error"
        if "no vehicle linked to this account" in lowered:
            return "no_vehicle"
        if "anmeldung fehlgeschlagen" in lowered or "login" in lowered and "failed" in lowered:
            return "invalid_auth"
        if "betriebspasswort" in lowered or "operatepassword" in lowered:
            return "invalid_operation_password"
        return "api_error"


def _state_age_seconds(raw_timestamp: Any) -> int | None:
    """Convert Leapmotor status timestamp into age in seconds."""
    if raw_timestamp is None:
        return None
    try:
        numeric = int(str(raw_timestamp))
    except (TypeError, ValueError):
        return None
    if numeric <= 0:
        return None
    if numeric > 1_000_000_000_000:
        event_ts = numeric / 1000.0
    elif numeric > 1_000_000_000:
        event_ts = float(numeric)
    else:
        return None
    age = int(time.time() - event_ts)
    return max(age, 0)
