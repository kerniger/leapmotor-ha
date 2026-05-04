"""Compatibility shim: wraps leapmotor_api for use inside the HA integration."""

from __future__ import annotations

import hashlib
import hmac
import logging
import random
import time

_LOGGER = logging.getLogger(__name__)
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import quote
from zoneinfo import ZoneInfo

from leapmotor_api import LeapmotorApiClient as _LeapmotorApiClient
from leapmotor_api import normalize_vehicle
from leapmotor_api.const import (
    DEFAULT_APP_VERSION,
    DEFAULT_BASE_URL,
    DEFAULT_CHANNEL,
    DEFAULT_DEVICE_TYPE,
    DEFAULT_LANGUAGE,
    DEFAULT_P12_ENC_ALG,
    DEFAULT_SOURCE,
)
from leapmotor_api.exceptions import (
    LeapmotorApiError,
    LeapmotorAuthError,
    LeapmotorMissingAppCertError,
)
from leapmotor_api.models import Vehicle

__all__ = [
    "LeapmotorApiClient",
    "LeapmotorApiError",
    "LeapmotorAuthError",
    "LeapmotorMissingAppCertError",
    "LeapmotorNoVehicleError",
]

_STATIC_APP_CERT = "app_cert.pem"
_STATIC_APP_KEY = "app_key.pem"


class LeapmotorNoVehicleError(LeapmotorApiError):
    """No vehicle linked to this account."""


class LeapmotorApiClient(_LeapmotorApiClient):
    """HA integration wrapper — adapts static_cert_dir and adds extra history endpoints."""

    def __init__(
        self,
        *,
        username: str,
        password: str,
        operation_password: str | None = None,
        account_p12_password: str | None = None,
        base_url: str = DEFAULT_BASE_URL,
        static_cert_dir: str | Path | None = None,
    ) -> None:
        cert_dir = Path(static_cert_dir) if static_cert_dir else Path(__file__).resolve().parent
        super().__init__(
            username=username,
            password=password,
            app_cert_path=cert_dir / _STATIC_APP_CERT,
            app_key_path=cert_dir / _STATIC_APP_KEY,
            operation_password=operation_password,
            account_p12_password=account_p12_password,
            base_url=base_url,
        )

    # ------------------------------------------------------------------
    # Override: add consumption history + is_plugged to each vehicle dict
    # ------------------------------------------------------------------

    def _fetch_authenticated_data(self) -> dict[str, Any]:
        vehicles = self.get_vehicle_list()
        result: dict[str, Any] = {
            "user_id": self.user_id,
            "vehicles": {},
            "account_p12_password_source": self.account_p12_password_source,
        }
        message_list = self._fetch_message_list()
        for vehicle in vehicles:
            status = self.get_vehicle_raw_status(vehicle)
            mileage = self._fetch_optional_read("mileage energy detail", self.get_mileage_energy_detail, vehicle)
            rank = self._fetch_optional_read("consumption weekly rank", self.get_consumption_weekly_rank, vehicle)
            breakdown = self._fetch_optional_read(
                "consumption last week breakdown", self.get_consumption_last_week_breakdown, vehicle
            )
            picture = self._fetch_optional_read("car picture", self.get_car_picture, vehicle)
            vehicle_data = normalize_vehicle(vehicle, status, self.user_id, mileage_json=mileage, picture_json=picture)
            _augment_history(vehicle_data["history"], mileage, rank, breakdown)
            _augment_charging(vehicle_data["charging"], status)
            vehicle_data["messages"] = _build_messages_dict(vehicle.vin, message_list)
            _augment_status(vehicle_data["status"], status)
            _augment_windows(vehicle_data, status)
            result["vehicles"][vehicle.vin] = vehicle_data
        return result

    def _fetch_message_list(self) -> list[Any] | None:
        try:
            return self.get_message_list(page_size=50).messages
        except LeapmotorApiError as exc:
            _LOGGER.debug("Leapmotor optional read failed for message list: %s", exc)
            return None

    # ------------------------------------------------------------------
    # Extra read endpoints not yet in leapmotor_api
    # ------------------------------------------------------------------

    def get_consumption_weekly_rank(self, vehicle: Vehicle) -> dict[str, Any]:
        """Fetch six-week energy consumption and ranking data."""
        self._ensure_token()
        return self._retry_on_token_expiry(self._get_consumption_weekly_rank, vehicle)

    def _get_consumption_weekly_rank(self, vehicle: Vehicle) -> dict[str, Any]:
        nonce = str(random.randint(100000, 9999999))
        timestamp = str(int(time.time() * 1000))
        sign_input = "".join([
            DEFAULT_LANGUAGE, vehicle.vin, DEFAULT_CHANNEL, self.device_id,
            DEFAULT_DEVICE_TYPE, nonce, DEFAULT_SOURCE, timestamp, DEFAULT_APP_VERSION,
        ])
        headers = _build_signed_headers(self.sign_key, self.device_id, nonce, timestamp, sign_input)
        headers.update(self._auth_headers(content_type="application/x-www-form-urlencoded"))
        response = self._post(
            path="/carownerservice/oversea/drivingRecord/v1/getLastNweeks100kmECAndRank",
            headers=headers,
            data=f"carvin={quote(vehicle.vin, safe='')}",
            cert=self.account_cert,
        )
        return self._parse_api_body(response["status_code"], response["body"], "consumption weekly rank")

    def get_consumption_last_week_breakdown(self, vehicle: Vehicle) -> dict[str, Any]:
        """Fetch last-week energy split by driving, A/C, and other."""
        self._ensure_token()
        return self._retry_on_token_expiry(self._get_consumption_last_week_breakdown, vehicle)

    def _get_consumption_last_week_breakdown(self, vehicle: Vehicle) -> dict[str, Any]:
        begintime, endtime = _previous_week_window_seconds()
        nonce = str(random.randint(100000, 9999999))
        timestamp = str(int(time.time() * 1000))
        sign_input = "".join([
            DEFAULT_LANGUAGE, str(begintime), vehicle.vin, DEFAULT_CHANNEL, self.device_id,
            DEFAULT_DEVICE_TYPE, str(endtime), nonce, DEFAULT_SOURCE, timestamp, DEFAULT_APP_VERSION,
        ])
        headers = _build_signed_headers(self.sign_key, self.device_id, nonce, timestamp, sign_input)
        headers.update(self._auth_headers(content_type="application/x-www-form-urlencoded"))
        body = f"endtime={endtime}&begintime={begintime}&carvin={quote(vehicle.vin, safe='')}"
        response = self._post(
            path="/carownerservice/oversea/drivingRecord/v1/getLastweekEC",
            headers=headers,
            data=body,
            cert=self.account_cert,
        )
        return self._parse_api_body(response["status_code"], response["body"], "consumption last week breakdown")


# ------------------------------------------------------------------
# Module-level helpers
# ------------------------------------------------------------------

def _build_signed_headers(
    sign_key: bytes,
    device_id: str,
    nonce: str,
    timestamp: str,
    sign_input: str,
) -> dict[str, str]:
    return {
        "acceptLanguage": DEFAULT_LANGUAGE,
        "channel": DEFAULT_CHANNEL,
        "deviceType": DEFAULT_DEVICE_TYPE,
        "X-P12_ENC_ALG": DEFAULT_P12_ENC_ALG,
        "source": DEFAULT_SOURCE,
        "version": DEFAULT_APP_VERSION,
        "nonce": nonce,
        "deviceId": device_id,
        "timestamp": timestamp,
        "sign": hmac.new(sign_key, sign_input.encode("utf-8"), hashlib.sha256).hexdigest(),
    }


def _augment_history(
    history: dict[str, Any],
    mileage_json: dict[str, Any] | None,
    rank_json: dict[str, Any] | None,
    breakdown_json: dict[str, Any] | None,
) -> None:
    """Add consumption + extended mileage fields to the history dict in-place."""
    mileage_data = (mileage_json or {}).get("data") or {}
    rank_data = (rank_json or {}).get("data") or {}
    rank_result = rank_data.get("rankResult") or {}
    weekly_ec = rank_data.get("weeklyEC") or []
    breakdown_data = (breakdown_json or {}).get("data") or {}
    last_7_days_energy = _sum_detail_field(mileage_data.get("detail"), "accumulatedEnergyConsume")
    last_week_split = _energy_breakdown_percentages(breakdown_data)
    history.update({
        "total_energy_kwh": _safe_float(mileage_data.get("totalEnergy")),
        "last_7_days_mileage_km": mileage_data.get("totalAccumulatedMileage"),
        "last_7_days_mileage_mi": _safe_float(mileage_data.get("totalAccumulatedMileageMile")),
        "last_7_days_energy_kwh": last_7_days_energy,
        "average_consumption_6w_kwh_100km": _safe_float(rank_result.get("hundredKmEC")),
        "average_consumption_6w_mi_kwh": _safe_float(rank_result.get("hundredMiKwhEC")),
        "consumption_rank": rank_result.get("rank"),
        "weekly_consumption": weekly_ec,
        "last_week_driving_energy_kwh": _safe_float(breakdown_data.get("driverEC")),
        "last_week_climate_energy_kwh": _safe_float(breakdown_data.get("acEC")),
        "last_week_other_energy_kwh": _safe_float(breakdown_data.get("otherEC")),
        "last_week_driving_energy_percent": last_week_split.get("driving"),
        "last_week_climate_energy_percent": last_week_split.get("climate"),
        "last_week_other_energy_percent": last_week_split.get("other"),
    })


def _augment_status(status: dict[str, Any], status_json: dict[str, Any]) -> None:
    """Correct is_locked: markoceri uses signal 47 (wrong); validated signal is 1298."""
    signal = (status_json.get("data") or {}).get("signal") or {}
    lock_raw = _safe_int(signal.get("1298"))
    if lock_raw is not None:
        status["is_locked"] = lock_raw == 1
        status["raw_lock_status_code"] = lock_raw
        status["lock_state_source"] = "raw_signal_1298"


def _augment_charging(charging: dict[str, Any], status_json: dict[str, Any]) -> None:
    """Add is_plugged_in and is_regening to the charging dict."""
    signal = (status_json.get("data") or {}).get("signal") or {}
    # Signal 47 = charge cable physically plugged in (1=yes); fallback to 1149
    is_plugged_in: bool | None = None
    plug = _safe_int(signal.get("47"))
    if plug is not None:
        is_plugged_in = plug == 1
    else:
        conn = _safe_int(signal.get("1149"))
        if conn is not None:
            is_plugged_in = conn in (1, 2)
    # is_regening: charging power flowing while NOT connected to external charger
    is_regening: bool | None = None
    charging_power_kw = charging.get("charging_power_kw")
    if charging_power_kw is not None and is_plugged_in is not None:
        is_regening = not is_plugged_in and charging_power_kw > 0
    charging["is_plugged_in"] = is_plugged_in
    charging["is_regening"] = is_regening


def _augment_windows(vehicle_data: dict[str, Any], status_json: dict[str, Any]) -> None:
    """Add window and sunshade positions to vehicle_data."""
    status_data = status_json.get("data") or {}
    vehicle_data["windows"] = {
        "left_front_percent": status_data.get("leftFrontWindowPercent"),
        "right_front_percent": status_data.get("rightFrontWindowPercent"),
        "left_rear_percent": status_data.get("leftRearWindowPercent"),
        "right_rear_percent": status_data.get("rightRearWindowPercent"),
        "sun_shade": status_data.get("sunShade"),
    }


def _previous_week_window_seconds() -> tuple[int, int]:
    now = _berlin_now()
    this_monday = (now - timedelta(days=now.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
    start = this_monday - timedelta(days=7)
    end = this_monday - timedelta(seconds=1)
    return int(start.timestamp()), int(end.timestamp())


def _berlin_now() -> datetime:
    try:
        return datetime.now(ZoneInfo("Europe/Berlin"))
    except Exception:
        return datetime.now().astimezone()


def _sum_detail_field(detail: Any, field: str) -> float | None:
    if not isinstance(detail, list):
        return None
    total = 0.0
    found = False
    for item in detail:
        if not isinstance(item, dict):
            continue
        value = _safe_float(item.get(field))
        if value is None:
            continue
        total += value
        found = True
    return total if found else None


def _energy_breakdown_percentages(data: dict[str, Any]) -> dict[str, float | None]:
    values = {
        "driving": _safe_float(data.get("driverEC")),
        "climate": _safe_float(data.get("acEC")),
        "other": _safe_float(data.get("otherEC")),
    }
    total = sum(v for v in values.values() if v is not None)
    if total <= 0:
        return {k: None for k in values}
    return {k: round(v * 100 / total, 1) if v is not None else None for k, v in values.items()}


def _build_messages_dict(vin: str, messages: list[Any] | None) -> dict[str, Any]:
    """Build per-vehicle message summary dict."""
    if messages is None:
        return {"unread_count": None, "last_title": None, "last_send_time": None}
    vehicle_msgs = [m for m in messages if m.vin == vin or m.vin is None]
    unread = sum(1 for m in vehicle_msgs if not m.is_read)
    latest = max(vehicle_msgs, key=lambda m: m.send_time or 0, default=None)
    return {
        "unread_count": unread,
        "last_title": latest.title if latest else None,
        "last_send_time": latest.send_time if latest else None,
    }


def _safe_int(raw: Any) -> int | None:
    if raw is None:
        return None
    try:
        return int(raw)
    except (TypeError, ValueError):
        return None


def _safe_float(raw: Any) -> float | None:
    if raw is None:
        return None
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None
