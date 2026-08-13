"""Data coordinator for Leapmotor."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from functools import partial
import logging
import time
from typing import Any

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.storage import Store
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed

from .abrp import build_abrp_telemetry, send_abrp_telemetry
from .api import LeapmotorApiClient
from .const import (
    CONF_ABRP_ENABLED,
    CONF_ABRP_TOKEN,
    DEFAULT_ABRP_API_KEY,
    DEFAULT_STATE_STALE_SECONDS,
    DOMAIN,
    REMOTE_ACTION_COOLDOWN_SECONDS,
)
from .leap_api import LeapmotorApiError
from .location import CoordinateResolution, resolve_coordinate

_LOGGER = logging.getLogger(__name__)

GPS_SIGN_STORAGE_VERSION = 1


class LeapmotorDataUpdateCoordinator(DataUpdateCoordinator[dict]):
    """Fetch Leapmotor vehicle data."""

    def __init__(
        self,
        *,
        hass: HomeAssistant,
        config_entry: ConfigEntry,
        client: LeapmotorApiClient,
        update_interval: timedelta,
        eco_polling_enabled: bool = False,
        eco_update_interval: timedelta | None = None,
    ) -> None:
        super().__init__(
            hass,
            _LOGGER,
            name=DOMAIN,
            config_entry=config_entry,
            update_interval=update_interval,
        )
        self.client = client
        self._normal_update_interval = update_interval
        self._eco_polling_enabled = eco_polling_enabled
        self._eco_update_interval = eco_update_interval or update_interval
        self._polling_mode = "normal"
        self._lock_state_overrides: dict[str, tuple[bool, float]] = {}
        self._last_remote_results: dict[str, dict[str, Any]] = {}
        self._last_abrp_results: dict[str, dict[str, Any]] = {}
        self._last_vehicle_states: dict[str, str] = {}
        self._followup_refresh_tasks: dict[str, asyncio.Task[None]] = {}
        self._location_signs: dict[str, dict[str, int]] = {}
        self._location_sign_pending: dict[tuple[str, str], int] = {}
        self._location_sign_store = Store[dict[str, dict[str, int]]](
            hass,
            GPS_SIGN_STORAGE_VERSION,
            f"{DOMAIN}.gps_signs.{config_entry.entry_id}",
        )
        self._location_sign_save_task: asyncio.Task[None] | None = None
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
            "normal_update_interval_seconds": int(update_interval.total_seconds()),
            "eco_update_interval_seconds": int(self._eco_update_interval.total_seconds()),
            "eco_polling_enabled": self._eco_polling_enabled,
            "polling_mode": self._polling_mode,
            "vehicle_count": 0,
        }
        self._pending_update_reason = "startup"

    async def async_load_location_signs(self) -> None:
        """Load remembered GPS hemisphere signs before the first poll."""
        stored = await self._location_sign_store.async_load() or {}
        if not isinstance(stored, dict):
            stored = {}
        self._location_signs = {
            str(vin): {
                axis: int(sign)
                for axis, sign in signs.items()
                if axis in {"latitude", "longitude"} and sign in {-1, 1}
            }
            for vin, signs in stored.items()
            if isinstance(signs, dict)
        }

    async def async_flush_location_signs(self) -> None:
        """Wait for a pending GPS sign write before unloading."""
        task = self._location_sign_save_task
        if task and not task.done():
            await task

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
                "update_interval_seconds": int(self.update_interval.total_seconds())
                if self.update_interval
                else self._integration_status.get("update_interval_seconds"),
                "normal_update_interval_seconds": int(self._normal_update_interval.total_seconds()),
                "eco_update_interval_seconds": int(self._eco_update_interval.total_seconds()),
                "eco_polling_enabled": self._eco_polling_enabled,
                "polling_mode": self._polling_mode,
                "vehicle_count": self._integration_status.get("vehicle_count", 0),
            }
            raise UpdateFailed(str(exc)) from exc
        self._stabilize_vehicle_states(data)
        self._apply_state_freshness(data)
        self._normalize_locations(data)
        self._update_polling_interval(data)
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
            "update_interval_seconds": int(self.update_interval.total_seconds())
            if self.update_interval
            else None,
            "normal_update_interval_seconds": int(self._normal_update_interval.total_seconds()),
            "eco_update_interval_seconds": int(self._eco_update_interval.total_seconds()),
            "eco_polling_enabled": self._eco_polling_enabled,
            "polling_mode": self._polling_mode,
            "vehicle_count": len((data.get("vehicles") or {})),
        }
        await self._async_push_abrp(data)
        self._apply_lock_state_overrides(data)
        self._apply_remote_results(data)
        self._apply_abrp_results(data)
        self._apply_integration_status(data)
        _LOGGER.debug(
            "Leapmotor update completed: reason=%s vehicles=%s polling_mode=%s duration=%ss",
            update_reason,
            self._integration_status.get("vehicle_count"),
            self._polling_mode,
            self._integration_status.get("last_update_duration_seconds"),
        )
        return data

    @property
    def integration_status(self) -> dict[str, Any]:
        """Return the current integration-wide update status."""
        return dict(self._integration_status)

    async def async_manual_refresh(self) -> None:
        """Force an immediate manual refresh outside the normal polling cadence."""
        self._pending_update_reason = "manual"
        await self.async_request_refresh()

    def schedule_remote_followup_refresh(
        self,
        vin: str,
        *,
        delay_seconds: float = 4.0,
    ) -> None:
        """Schedule one coalesced refresh after remote-command state has settled."""
        previous_task = self._followup_refresh_tasks.pop(vin, None)
        if previous_task and not previous_task.done():
            previous_task.cancel()
        self._followup_refresh_tasks[vin] = self.hass.async_create_task(
            self._async_remote_followup_refresh(vin, delay_seconds)
        )

    def cancel_scheduled_followup_refreshes(self) -> None:
        """Cancel pending delayed refreshes during unload."""
        for task in self._followup_refresh_tasks.values():
            if not task.done():
                task.cancel()
        self._followup_refresh_tasks.clear()

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

    async def _async_remote_followup_refresh(
        self,
        vin: str,
        delay_seconds: float,
    ) -> None:
        """Refresh shortly after a command because vehicle signals can lag."""
        try:
            await asyncio.sleep(delay_seconds)
            self._pending_update_reason = "remote_followup"
            await self.async_request_refresh()
        except asyncio.CancelledError:
            raise
        except Exception:
            _LOGGER.debug(
                "Leapmotor follow-up refresh failed for VIN %s",
                vin,
                exc_info=True,
            )
        finally:
            current_task = asyncio.current_task()
            if self._followup_refresh_tasks.get(vin) is current_task:
                self._followup_refresh_tasks.pop(vin, None)

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

    def _update_polling_interval(self, data: dict[str, Any]) -> None:
        """Switch to slower polling only when every vehicle is clearly idle."""
        if not self._eco_polling_enabled:
            self.update_interval = self._normal_update_interval
            self._polling_mode = "normal"
            return

        target_interval = (
            self._eco_update_interval
            if _all_vehicles_quiet(data)
            else self._normal_update_interval
        )
        previous_mode = self._polling_mode
        self.update_interval = target_interval
        self._polling_mode = "eco" if target_interval == self._eco_update_interval else "normal"
        if self._polling_mode != previous_mode:
            _LOGGER.debug(
                "Leapmotor polling mode changed from %s to %s (interval=%ss)",
                previous_mode,
                self._polling_mode,
                int(target_interval.total_seconds()),
            )

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

    def _normalize_locations(self, data: dict[str, Any]) -> None:
        """Resolve API coordinates and retain authoritative signs per VIN."""
        home_latitude = _safe_float(getattr(self.hass.config, "latitude", None))
        home_longitude = _safe_float(getattr(self.hass.config, "longitude", None))
        signs_changed = False
        for vin, vehicle_data in (data.get("vehicles") or {}).items():
            location = vehicle_data.get("location") or {}
            location["raw_latitude"] = location.get("latitude")
            location["raw_longitude"] = location.get("longitude")

            signed_latitude = location.pop("_signed_latitude", None)
            signed_longitude = location.pop("_signed_longitude", None)
            unsigned_latitude = location.pop("_unsigned_latitude", None)
            unsigned_longitude = location.pop("_unsigned_longitude", None)
            raw_latitude = _safe_float(
                signed_latitude if signed_latitude is not None else unsigned_latitude
            )
            raw_longitude = _safe_float(
                signed_longitude if signed_longitude is not None else unsigned_longitude
            )

            vehicle_signs = self._location_signs.setdefault(vin, {})
            if _should_flip_southern_latitude(
                raw_latitude,
                raw_longitude,
                home_latitude,
                home_longitude,
            ) and "latitude" not in vehicle_signs:
                vehicle_signs["latitude"] = -1
                signs_changed = True

            if _should_flip_western_longitude(
                raw_latitude,
                raw_longitude,
                home_latitude,
                home_longitude,
            ) and "longitude" not in vehicle_signs:
                vehicle_signs["longitude"] = -1
                signs_changed = True

            latitude = self._resolve_location_axis(
                vin, "latitude", signed_latitude, unsigned_latitude
            )
            longitude = self._resolve_location_axis(
                vin, "longitude", signed_longitude, unsigned_longitude
            )
            if latitude.sign is not None and vehicle_signs.get("latitude") != latitude.sign:
                vehicle_signs["latitude"] = latitude.sign
                signs_changed = True
            if longitude.sign is not None and vehicle_signs.get("longitude") != longitude.sign:
                vehicle_signs["longitude"] = longitude.sign
                signs_changed = True

            location["latitude"] = latitude.value
            location["longitude"] = longitude.value
            location["latitude_corrected"] = (
                latitude.value is not None
                and raw_latitude is not None
                and latitude.value != raw_latitude
            )
            location["longitude_corrected"] = (
                longitude.value is not None
                and raw_longitude is not None
                and longitude.value != raw_longitude
            )
            location["latitude_correction_source"] = latitude.source
            location["longitude_correction_source"] = longitude.source

        if signs_changed:
            self._schedule_location_sign_save()

    def _resolve_location_axis(
        self,
        vin: str,
        axis: str,
        signed_value: object,
        unsigned_value: object,
    ) -> CoordinateResolution:
        """Resolve and track one coordinate axis for a vehicle."""
        pending_key = (vin, axis)
        result = resolve_coordinate(
            signed_value=signed_value,
            unsigned_value=unsigned_value,
            remembered_sign=self._location_signs.get(vin, {}).get(axis),
            pending_flip_count=self._location_sign_pending.get(pending_key, 0),
        )
        if result.pending_flip_count:
            self._location_sign_pending[pending_key] = result.pending_flip_count
        else:
            self._location_sign_pending.pop(pending_key, None)
        return result

    def _schedule_location_sign_save(self) -> None:
        """Persist changed signs without delaying a coordinator refresh."""
        if self._location_sign_save_task and not self._location_sign_save_task.done():
            return
        self._location_sign_save_task = self.hass.async_create_task(
            self._async_save_location_signs()
        )

    async def _async_save_location_signs(self) -> None:
        """Save again if signs changed while a storage write was in progress."""
        while True:
            snapshot = {
                vin: dict(signs) for vin, signs in self._location_signs.items()
            }
            await self._location_sign_store.async_save(snapshot)
            if snapshot == self._location_signs:
                return

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
        numeric = float(raw_timestamp)
    except (TypeError, ValueError):
        try:
            parsed = datetime.fromisoformat(
                str(raw_timestamp).replace("Z", "+00:00")
            )
        except ValueError:
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        event_ts = parsed.timestamp()
    else:
        if numeric <= 0:
            return None
        if numeric > 10_000_000_000:
            event_ts = numeric / 1000.0
        elif numeric > 1_000_000_000:
            event_ts = numeric
        else:
            return None
    age = int(time.time() - event_ts)
    return max(age, 0)


def _all_vehicles_quiet(data: dict[str, Any]) -> bool:
    """Return true when every vehicle is safe for slower cloud polling."""
    vehicles = (data.get("vehicles") or {}).values()
    seen_vehicle = False
    for vehicle_data in vehicles:
        seen_vehicle = True
        status = vehicle_data.get("status") or {}
        charging = vehicle_data.get("charging") or {}
        if status.get("is_locked") is not True:
            return False
        if status.get("is_parked") is not True:
            return False
        if charging.get("is_charging") is True:
            return False
        if charging.get("is_plugged_in") is not False:
            return False
    return seen_vehicle


def _should_flip_southern_latitude(
    latitude: float | None,
    longitude: float | None,
    home_latitude: float | None,
    home_longitude: float | None,
) -> bool:
    """Return true when a positive API latitude is likely missing the southern sign."""
    if latitude is None or longitude is None or home_latitude is None or home_longitude is None:
        return False
    if latitude <= 0 or home_latitude >= 0:
        return False
    if not (-90 <= latitude <= 90 and -180 <= longitude <= 180):
        return False
    if not (-90 <= home_latitude <= 90 and -180 <= home_longitude <= 180):
        return False

    longitude_delta = _longitude_delta_degrees(longitude, home_longitude)
    if longitude_delta > 50:
        return False

    as_reported_distance = _coordinate_distance_score(latitude, longitude, home_latitude, home_longitude)
    flipped_distance = _coordinate_distance_score(-abs(latitude), longitude, home_latitude, home_longitude)
    return flipped_distance + 1 < as_reported_distance


def _should_flip_western_longitude(
    latitude: float | None,
    longitude: float | None,
    home_latitude: float | None,
    home_longitude: float | None,
) -> bool:
    """Return true when a positive API longitude is likely missing the western sign."""
    if latitude is None or longitude is None or home_latitude is None or home_longitude is None:
        return False
    if longitude <= 0 or home_longitude >= 0:
        return False
    if not (-90 <= latitude <= 90 and -180 <= longitude <= 180):
        return False
    if not (-90 <= home_latitude <= 90 and -180 <= home_longitude <= 180):
        return False

    as_reported_distance = _coordinate_distance_score(latitude, longitude, home_latitude, home_longitude)
    flipped_distance = _coordinate_distance_score(latitude, -abs(longitude), home_latitude, home_longitude)
    return flipped_distance * 2 < as_reported_distance


def _coordinate_distance_score(
    latitude: float,
    longitude: float,
    home_latitude: float,
    home_longitude: float,
) -> float:
    """Return a cheap distance score in degrees, sufficient for hemisphere guards."""
    longitude_delta = _longitude_delta_degrees(longitude, home_longitude)
    return (latitude - home_latitude) ** 2 + longitude_delta**2


def _longitude_delta_degrees(left: float, right: float) -> float:
    """Return the shortest absolute longitude delta in degrees."""
    delta = abs(left - right) % 360
    return min(delta, 360 - delta)


def _safe_float(value: Any) -> float | None:
    """Return value as float or None."""
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
