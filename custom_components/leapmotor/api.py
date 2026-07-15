"""Compatibility entrypoint for the internal Leapmotor cloud API client."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import logging
import random
import tempfile
import time
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import requests
import urllib3
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.kdf.hkdf import HKDF
from cryptography.hazmat.primitives.serialization import pkcs12

from .const import (
    DEFAULT_APP_VERSION,
    DEFAULT_BASE_URL,
    DEFAULT_CHANNEL,
    DEFAULT_DEVICE_TYPE,
    DEFAULT_LANGUAGE,
    DEFAULT_P12_ENC_ALG,
    DEFAULT_SOURCE,
    KNOWN_ACCOUNT_P12_PASSWORDS,
    REMOTE_CTL_AC_SWITCH,
    REMOTE_CTL_AC_ON,
    REMOTE_CTL_AC_OFF,
    REMOTE_CTL_BATTERY_PREHEAT,
    REMOTE_CTL_BATTERY_PREHEAT_OFF,
    REMOTE_CTL_FIND_CAR,
    REMOTE_CTL_LOCK,
    REMOTE_CTL_QUICK_COOL,
    REMOTE_CTL_QUICK_HEAT,
    REMOTE_CTL_REARVIEW_MIRROR_HEAT_OFF,
    REMOTE_CTL_REARVIEW_MIRROR_HEAT_ON,
    REMOTE_CTL_SEAT_HEAT,
    REMOTE_CTL_SEAT_VENTILATION,
    REMOTE_CTL_STEERING_WHEEL_HEAT_OFF,
    REMOTE_CTL_STEERING_WHEEL_HEAT_ON,
    REMOTE_CTL_SUNSHADE,
    REMOTE_CTL_SUNSHADE_CLOSE,
    REMOTE_CTL_SUNSHADE_OPEN,
    REMOTE_CTL_TRUNK,
    REMOTE_CTL_TRUNK_CLOSE,
    REMOTE_CTL_TRUNK_OPEN,
    REMOTE_CTL_UNLOCK,
    REMOTE_CTL_UNLOCK_CHARGER,
    REMOTE_CTL_WINDSHIELD_DEFROST,
    REMOTE_CTL_WINDOWS,
    REMOTE_CTL_WINDOWS_CLOSE,
    REMOTE_CTL_WINDOWS_OPEN,
    STATIC_APP_CERT,
    STATIC_APP_KEY,
)
from .leap_api import (
    REMOTE_ACTION_SPECS,
    CurlTransport,
    LeapmotorAccountCertError,
    LeapmotorApiError,
    LeapmotorAuthError,
    LeapmotorMissingAppCertError,
    LeapmotorNoVehicleError,
    Vehicle,
    build_seat_comfort_payload,
    derive_operate_password,
    derive_session_device_id,
)
from .p12 import derive_account_p12_password

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

_LOGGER = logging.getLogger(__name__)


def _redact_vin_for_log(vin: str | None) -> str:
    """Keep logs useful without writing full VINs."""
    if not vin:
        return "unknown"
    vin_text = str(vin)
    return f"***{vin_text[-4:]}" if len(vin_text) > 4 else "***"


class LeapmotorApiClient:
    """Minimal client based on reverse-engineered Leapmotor app traffic."""

    def __init__(
        self,
        *,
        username: str,
        password: str,
        operation_password: str | None = None,
        account_p12_password: str | None = None,
        base_url: str = DEFAULT_BASE_URL,
        device_id: str | None = None,
        language: str = DEFAULT_LANGUAGE,
        static_cert_dir: str | Path | None = None,
    ) -> None:
        self.username = username
        self.password = password
        self.operation_password = operation_password.strip() if operation_password else None
        self.account_p12_password = account_p12_password
        self.base_url = base_url.rstrip("/")
        self.transport = CurlTransport(self.base_url)
        self.session = requests.Session()
        self.login_device_id = device_id or uuid.uuid4().hex
        self.device_id = self.login_device_id
        self.language = language
        self.user_id: str | None = None
        self.token: str | None = None
        self.refresh_token: str | None = None
        self.sign_ikm: str | None = None
        self.sign_salt: str | None = None
        self.sign_info: str | None = None
        self.account_cert_file: str | None = None
        self.account_key_file: str | None = None
        self.account_p12_password_used: str | None = None
        self.account_p12_password_source: str | None = None
        self.remote_cert_synced = False
        self.last_api_results: dict[str, dict[str, Any]] = {}
        cert_dir = Path(static_cert_dir) if static_cert_dir else Path(__file__).resolve().parent
        self.static_cert = str(cert_dir / STATIC_APP_CERT)
        self.static_key = str(cert_dir / STATIC_APP_KEY)

    def close(self) -> None:
        """Close HTTP resources and remove temporary account cert files."""
        self.session.close()
        self._clear_account_cert_files()

    def _clear_account_cert_files(self) -> None:
        """Remove temporary account cert files."""
        for file_name in (self.account_cert_file, self.account_key_file):
            if file_name:
                try:
                    Path(file_name).unlink(missing_ok=True)
                except OSError:
                    pass
        self.account_cert_file = None
        self.account_key_file = None

    def _clear_auth(self) -> None:
        """Clear token and account certificate state before re-login."""
        self.token = None
        self.refresh_token = None
        self.device_id = self.login_device_id
        self.user_id = None
        self.sign_ikm = None
        self.sign_salt = None
        self.sign_info = None
        self.account_p12_password_used = None
        self.account_p12_password_source = None
        self.remote_cert_synced = False
        self._clear_account_cert_files()

    @property
    def account_cert(self) -> tuple[str, str]:
        if not self.account_cert_file or not self.account_key_file:
            raise LeapmotorAuthError("No account certificate loaded.")
        return (self.account_cert_file, self.account_key_file)

    def _ensure_account_cert_files(self) -> None:
        """Recreate missing temporary account certificate files before a request."""
        if (
            self.account_cert_file
            and self.account_key_file
            and Path(self.account_cert_file).is_file()
            and Path(self.account_key_file).is_file()
        ):
            return
        if self.token:
            _LOGGER.info("Leapmotor account certificate files are missing; logging in again")
            self._clear_auth()
        self.login()

    @property
    def sign_key(self) -> bytes:
        if self.sign_ikm is None or self.sign_salt is None or self.sign_info is None:
            raise LeapmotorAuthError("No account sign material loaded.")
        return HKDF(
            algorithm=hashes.SHA256(),
            length=32,
            salt=self.sign_salt.encode("utf-8"),
            info=self.sign_info.encode("utf-8"),
        ).derive(self.sign_ikm.encode("utf-8"))

    def _ensure_static_cert_files(self) -> None:
        """Require local app certificate material for the current login flow."""
        missing = [
            path.name
            for path in (Path(self.static_cert), Path(self.static_key))
            if not path.exists()
        ]
        if missing:
            raise LeapmotorMissingAppCertError(
                "Missing local app certificate material: "
                + ", ".join(missing)
                + ". This public repository does not ship app_cert.pem/app_key.pem."
            )

    def fetch_data(self) -> dict[str, Any]:
        """Authenticate if needed and fetch all read-only vehicle data."""
        if not self.token:
            self._ensure_static_cert_files()
            self.login()

        try:
            return self._fetch_authenticated_data()
        except LeapmotorApiError as exc:
            self._recover_session(exc)
            return self._fetch_authenticated_data()

    def lock_vehicle(self, vin: str) -> dict[str, Any]:
        """Lock one vehicle via remote control."""
        return self._remote_control(vin=vin, action=REMOTE_CTL_LOCK)

    def unlock_vehicle(self, vin: str) -> dict[str, Any]:
        """Unlock one vehicle via remote control."""
        return self._remote_control(vin=vin, action=REMOTE_CTL_UNLOCK)

    def unlock_charger(self, vin: str) -> dict[str, Any]:
        """Unlock the charging gun via remote control."""
        return self._remote_control(vin=vin, action=REMOTE_CTL_UNLOCK_CHARGER)

    def open_trunk(self, vin: str) -> dict[str, Any]:
        """Open the trunk via remote control."""
        return self._remote_control(vin=vin, action=REMOTE_CTL_TRUNK)

    def close_trunk(self, vin: str) -> dict[str, Any]:
        """Close the trunk via remote control."""
        return self._remote_control(vin=vin, action=REMOTE_CTL_TRUNK_CLOSE)

    def find_vehicle(self, vin: str) -> dict[str, Any]:
        """Locate the vehicle via horn."""
        return self._remote_control(vin=vin, action=REMOTE_CTL_FIND_CAR)

    def control_sunshade(self, vin: str) -> dict[str, Any]:
        """Trigger the verified sunshade action."""
        return self._remote_control(vin=vin, action=REMOTE_CTL_SUNSHADE)

    def open_sunshade(self, vin: str, value: int | None = None) -> dict[str, Any]:
        """Open the sunshade via remote control."""
        return self._remote_control(vin=vin, action=REMOTE_CTL_SUNSHADE_OPEN, value=value)

    def close_sunshade(self, vin: str, value: int | None = None) -> dict[str, Any]:
        """Close the sunshade via remote control."""
        return self._remote_control(vin=vin, action=REMOTE_CTL_SUNSHADE_CLOSE, value=value)

    def battery_preheat(self, vin: str) -> dict[str, Any]:
        """Trigger the verified battery-preheat action."""
        return self._remote_control(vin=vin, action=REMOTE_CTL_BATTERY_PREHEAT)

    def battery_preheat_off(self, vin: str) -> dict[str, Any]:
        """Turn off battery preheating."""
        return self._remote_control(vin=vin, action=REMOTE_CTL_BATTERY_PREHEAT_OFF)

    def steering_wheel_heat_on(self, vin: str) -> dict[str, Any]:
        """Turn on steering wheel heating."""
        return self._remote_control(vin=vin, action=REMOTE_CTL_STEERING_WHEEL_HEAT_ON)

    def steering_wheel_heat_off(self, vin: str) -> dict[str, Any]:
        """Turn off steering wheel heating."""
        return self._remote_control(vin=vin, action=REMOTE_CTL_STEERING_WHEEL_HEAT_OFF)

    def rearview_mirror_heat_on(self, vin: str) -> dict[str, Any]:
        """Turn on rearview mirror heating."""
        return self._remote_control(vin=vin, action=REMOTE_CTL_REARVIEW_MIRROR_HEAT_ON)

    def rearview_mirror_heat_off(self, vin: str) -> dict[str, Any]:
        """Turn off rearview mirror heating."""
        return self._remote_control(vin=vin, action=REMOTE_CTL_REARVIEW_MIRROR_HEAT_OFF)

    def seat_heat(self, vin: str, position: str, level: int) -> dict[str, Any]:
        """Set the driver or passenger seat heating level."""
        cmd_content = build_seat_comfort_payload(position, level)
        return self._remote_control(
            vin=vin,
            action=REMOTE_CTL_SEAT_HEAT,
            cmd_content=cmd_content,
        )

    def seat_ventilation(self, vin: str, position: str, level: int) -> dict[str, Any]:
        """Set the driver or passenger seat ventilation level."""
        cmd_content = build_seat_comfort_payload(position, level)
        return self._remote_control(
            vin=vin,
            action=REMOTE_CTL_SEAT_VENTILATION,
            cmd_content=cmd_content,
        )

    def windows(self, vin: str) -> dict[str, Any]:
        """Trigger the verified window action."""
        return self._remote_control(vin=vin, action=REMOTE_CTL_WINDOWS)

    def open_windows(self, vin: str, value: int | None = None) -> dict[str, Any]:
        """Open the windows via remote control."""
        if value is not None:
            vehicle = self._find_vehicle_by_vin(vin)
            if vehicle.car_type in ("B10", "C10"):
                value = round(value / 10.0)
        return self._remote_control(vin=vin, action=REMOTE_CTL_WINDOWS_OPEN, value=value)

    def close_windows(self, vin: str, value: int | None = None) -> dict[str, Any]:
        """Close the windows via remote control."""
        if value is not None:
            vehicle = self._find_vehicle_by_vin(vin)
            if vehicle.car_type in ("B10", "C10"):
                value = round(value / 10.0)
        return self._remote_control(vin=vin, action=REMOTE_CTL_WINDOWS_CLOSE, value=value)

    def ac_switch(self, vin: str) -> dict[str, Any]:
        """Backward-compatible alias for turning climate control off."""
        return self.ac_off(vin)

    def ac_on(
        self,
        vin: str,
        *,
        temperature: int | None = None,
        mode: str | None = None,
        windlevel: int | None = None,
        circle: str | None = None,
    ) -> dict[str, Any]:
        """Turn climate control on with optional mode, temperature and fan settings."""
        params = _build_climate_payload(
            temperature=temperature,
            mode=mode,
            windlevel=windlevel,
            circle=circle,
            operate="manual",
        )
        return self._remote_control(vin=vin, action=REMOTE_CTL_AC_ON, cmd_content=params)

    def ac_off(self, vin: str) -> dict[str, Any]:
        """Turn climate control fully off."""
        return self._remote_control(vin=vin, action=REMOTE_CTL_AC_OFF)

    def set_climate(
        self,
        vin: str,
        *,
        mode: str,
        temperature: int = 26,
        fan_speed: int = 3,
        recirculate: bool = False,
        windshield_defrost: bool = False,
    ) -> dict[str, Any]:
        """Send a parameterised climate command."""
        params = _build_climate_payload(
            temperature=temperature,
            mode=mode,
            windlevel=fan_speed,
            circle="in" if recirculate else "out",
            operate="manual",
        )
        params["wshld"] = "2" if windshield_defrost else "1"
        return self._remote_control(vin=vin, action=REMOTE_CTL_AC_ON, cmd_content=params)

    def set_climate_schedule(
        self,
        vin: str,
        *,
        start_time: str,
        mode: str = "nohotcold",
        operate: str = "manual",
        temperature: int = 26,
        fan_speed: int = 4,
        recirculate: bool = False,
        windshield_defrost: bool = False,
        days: list[int] | None = None,
        enabled: bool = True,
        set_id: str | None = None,
    ) -> dict[str, Any]:
        """Replace the climate schedule list with one planned pre-conditioning entry."""
        vehicle = self._find_vehicle_by_vin(vin)
        try:
            entry = _build_climate_schedule_entry(
                start_time=start_time,
                mode=mode,
                operate=operate,
                temperature=temperature,
                fan_speed=fan_speed,
                recirculate=recirculate,
                windshield_defrost=windshield_defrost,
                days=days,
                enabled=enabled,
                set_id=set_id,
            )
        except ValueError as exc:
            raise LeapmotorApiError(str(exc)) from exc
        cmd_content = json.dumps({"controls": [entry]}, separators=(",", ":"))
        return self._remote_control_raw(
            vin=vehicle.vin,
            cmd_id="171",
            cmd_content=cmd_content,
            action_label="set_climate_schedule",
            vehicle=vehicle,
        )

    def cancel_climate_schedule(self, vin: str) -> dict[str, Any]:
        """Cancel all climate pre-conditioning schedules."""
        vehicle = self._find_vehicle_by_vin(vin)
        return self._remote_control_raw(
            vin=vehicle.vin,
            cmd_id="171",
            cmd_content='{"controls":[]}',
            action_label="cancel_climate_schedule",
            vehicle=vehicle,
        )

    def prepare_car(
        self,
        vin: str,
        *,
        climate_enabled: bool = True,
        mode: str = "cold",
        operate: str = "manual",
        temperature: int = 18,
        fan_speed: int = 7,
        recirculate: bool = True,
        windshield_defrost: bool = False,
        driver_seat: str = "off",
        driver_seat_level: int = 3,
        passenger_seat: str = "off",
        passenger_seat_level: int = 3,
        steering_wheel_heat: bool = False,
        mirror_heat: bool = False,
        destination_name: str | None = None,
        destination_address: str | None = None,
        destination_latitude: float | None = None,
        destination_longitude: float | None = None,
    ) -> dict[str, Any]:
        """Run one-touch vehicle preparation immediately with cmdId 360."""
        vehicle = self._find_vehicle_by_vin(vin)
        try:
            datacontent = _build_prepare_car_datacontent(
                climate_enabled=climate_enabled,
                mode=mode,
                operate=operate,
                temperature=temperature,
                fan_speed=fan_speed,
                recirculate=recirculate,
                windshield_defrost=windshield_defrost,
                driver_seat=driver_seat,
                driver_seat_level=driver_seat_level,
                passenger_seat=passenger_seat,
                passenger_seat_level=passenger_seat_level,
                steering_wheel_heat=steering_wheel_heat,
                mirror_heat=mirror_heat,
                destination_name=destination_name,
                destination_address=destination_address,
                destination_latitude=destination_latitude,
                destination_longitude=destination_longitude,
            )
        except ValueError as exc:
            raise LeapmotorApiError(str(exc)) from exc
        cmd_content = json.dumps(datacontent, separators=(",", ":"))
        return self._remote_control_raw(
            vin=vehicle.vin,
            cmd_id="360",
            cmd_content=cmd_content,
            action_label="prepare_car",
            vehicle=vehicle,
        )

    def set_prepare_car_schedule(
        self,
        vin: str,
        *,
        start_time: str,
        climate_enabled: bool = True,
        mode: str = "cold",
        operate: str = "manual",
        temperature: int = 18,
        fan_speed: int = 7,
        recirculate: bool = True,
        windshield_defrost: bool = False,
        driver_seat: str = "off",
        driver_seat_level: int = 3,
        passenger_seat: str = "off",
        passenger_seat_level: int = 3,
        steering_wheel_heat: bool = False,
        mirror_heat: bool = False,
        destination_name: str | None = None,
        destination_address: str | None = None,
        destination_latitude: float | None = None,
        destination_longitude: float | None = None,
        days: list[int] | None = None,
        enabled: bool = True,
        set_id: str | None = None,
    ) -> dict[str, Any]:
        """Replace prepare-car schedules with one cmdId 361 schedule entry."""
        vehicle = self._find_vehicle_by_vin(vin)
        try:
            entry = _build_prepare_car_schedule_entry(
                start_time=start_time,
                climate_enabled=climate_enabled,
                mode=mode,
                operate=operate,
                temperature=temperature,
                fan_speed=fan_speed,
                recirculate=recirculate,
                windshield_defrost=windshield_defrost,
                driver_seat=driver_seat,
                driver_seat_level=driver_seat_level,
                passenger_seat=passenger_seat,
                passenger_seat_level=passenger_seat_level,
                steering_wheel_heat=steering_wheel_heat,
                mirror_heat=mirror_heat,
                destination_name=destination_name,
                destination_address=destination_address,
                destination_latitude=destination_latitude,
                destination_longitude=destination_longitude,
                days=days,
                enabled=enabled,
                set_id=set_id,
            )
        except ValueError as exc:
            raise LeapmotorApiError(str(exc)) from exc
        cmd_content = json.dumps({"controls": [entry]}, separators=(",", ":"))
        return self._remote_control_raw(
            vin=vehicle.vin,
            cmd_id="361",
            cmd_content=cmd_content,
            action_label="set_prepare_car_schedule",
            vehicle=vehicle,
        )

    def cancel_prepare_car_schedule(self, vin: str) -> dict[str, Any]:
        """Cancel all one-touch prepare-car schedules."""
        vehicle = self._find_vehicle_by_vin(vin)
        return self._remote_control_raw(
            vin=vehicle.vin,
            cmd_id="361",
            cmd_content='{"controls":[]}',
            action_label="cancel_prepare_car_schedule",
            vehicle=vehicle,
        )

    def quick_cool(self, vin: str) -> dict[str, Any]:
        """Trigger the verified quick-cool profile."""
        return self._remote_control(vin=vin, action=REMOTE_CTL_QUICK_COOL)

    def quick_heat(self, vin: str) -> dict[str, Any]:
        """Trigger the verified quick-heat profile."""
        return self._remote_control(vin=vin, action=REMOTE_CTL_QUICK_HEAT)

    def windshield_defrost(self, vin: str) -> dict[str, Any]:
        """Trigger the verified windshield-defrost profile."""
        return self._remote_control(vin=vin, action=REMOTE_CTL_WINDSHIELD_DEFROST)

    def set_charge_limit(self, vin: str, charge_limit_percent: int) -> dict[str, Any]:
        """Set the charge limit while preserving the current charging plan values."""
        return self._set_charging_plan(vin, charge_limit_percent=charge_limit_percent)

    def set_charging_plan_enabled(self, vin: str, enabled: bool) -> dict[str, Any]:
        """Enable or disable the charging plan while preserving its values."""
        return self._set_charging_plan(vin, charge_plan_enabled=enabled)

    def _set_charging_plan(
        self,
        vin: str,
        *,
        charge_limit_percent: int | None = None,
        charge_plan_enabled: bool | None = None,
    ) -> dict[str, Any]:
        """Update the charging plan command payload while preserving existing values."""
        vehicle = self._find_vehicle_by_vin(vin)
        try:
            status_json = self.get_vehicle_status(vehicle)
        except LeapmotorApiError as exc:
            if not _is_token_error(exc):
                raise
            self._recover_session(exc)
            status_json = self.get_vehicle_status(vehicle)
        status_charge_plan = (
            ((status_json.get("data") or {}).get("config") or {}).get("3") or {}
        )
        charge_plan = _normalize_charge_plan(status_charge_plan)
        if not _charge_plan_is_complete(charge_plan):
            charge_plan = _merge_charge_plans(
                charge_plan,
                _normalize_charge_plan(self.get_charge_schedule(vin)),
            )

        if charge_plan_enabled is not None and not _charge_plan_is_complete(charge_plan):
            raise LeapmotorApiError(
                "Current charging plan is incomplete, cannot safely enable or disable it."
            )

        start_time = charge_plan.get("beginTime")
        end_time = charge_plan.get("endTime")
        cycles = charge_plan.get("cycles")
        current_charge_limit = _safe_int(charge_plan.get("percent"))
        if not start_time:
            start_time = "00:00"
        if not end_time:
            end_time = "08:00"
        if not cycles:
            cycles = "1,2,3,4,5,6,7"
        if charge_limit_percent is None:
            charge_limit_percent = current_charge_limit if current_charge_limit is not None else 80
        charge_enable = (
            int(bool(charge_plan_enabled))
            if charge_plan_enabled is not None
            else 1 if _safe_int(charge_plan.get("isEnable")) else 0
        )

        cmd_content = json.dumps(
            {
                "chargeEnable": charge_enable,
                "chargesoc": int(charge_limit_percent),
                "circulation": _safe_int(charge_plan.get("circulation")) or 0,
                "cycles": str(cycles),
                "endtime": str(end_time),
                "recharge": _safe_int(charge_plan.get("recharge")) or 0,
                "starttime": str(start_time),
            },
            separators=(",", ":"),
        )
        return self._remote_control_raw(
            vin=vin,
            cmd_id="190",
            cmd_content=cmd_content,
            action_label=(
                "set_charging_plan_enabled"
                if charge_plan_enabled is not None
                else "set_charge_limit"
            ),
            vehicle=vehicle,
        )

    def get_charge_schedule(self, vin: str) -> dict[str, Any]:
        """Return the current charging schedule from the appointment endpoint."""
        try:
            return self._get_charge_schedule(vin)
        except LeapmotorApiError as exc:
            if not _is_token_error(exc):
                raise
            self._recover_session(exc)
            return self._get_charge_schedule(vin)

    def _get_charge_schedule(self, vin: str) -> dict[str, Any]:
        """Fetch the charging schedule without authentication retry."""
        headers = self._build_signed_headers(
            vin=vin,
            body_params={"cmdId": "190"},
        )
        headers.update(self._auth_headers(content_type="application/x-www-form-urlencoded"))
        body = f"vin={requests.utils.quote(vin, safe='')}&cmdId=190"
        response = self._post_with_curl(
            path="/carownerservice/oversea/vehicle/v1/app/remote/ctl/getAppointment",
            headers=headers,
            data=body,
            cert=self.account_cert,
        )
        try:
            response_body = json.loads(response["body"])
        except ValueError as exc:
            self._record_api_result(
                "charge schedule",
                status_code=response["status_code"],
                code=None,
                message="non_json",
            )
            raise LeapmotorApiError(
                f"charge schedule returned non-JSON response: {response['body'][:200]}"
            ) from exc

        result_code = response_body.get("result", response_body.get("code"))
        message = response_body.get("message")
        self._record_api_result(
            "charge schedule",
            status_code=response["status_code"],
            code=result_code,
            message=message,
        )
        if response["status_code"] != 200 or result_code != 0:
            if "permission" in str(message or "").lower():
                return {}
            raise LeapmotorApiError(
                f"Leapmotor charge schedule failed: {message or response['body'][:200]}"
            )

        schedule = response_body.get("data")
        if not schedule:
            return {}
        if isinstance(schedule, str):
            try:
                schedule = json.loads(schedule)
            except ValueError:
                return {}
        return dict(schedule) if isinstance(schedule, dict) else {}

    def send_destination(
        self,
        vin: str,
        *,
        address: str,
        address_name: str,
        latitude: float,
        longitude: float,
    ) -> dict[str, Any]:
        """Send a navigation destination to the vehicle."""
        vehicle = self._find_vehicle_by_vin(vin)
        cmd_content = json.dumps(
            {
                "address": address,
                "addressname": address_name,
                "latitude": str(latitude),
                "linenum": "0",
                "longitude": str(longitude),
            },
            ensure_ascii=False,
            separators=(",", ":"),
        )
        return self._remote_control_without_pin_raw(
            vin=vehicle.vin,
            cmd_id="180",
            cmd_content=cmd_content,
            action_label="send_destination",
        )

    def _fetch_authenticated_data(self) -> dict[str, Any]:
        """Fetch all read-only vehicle data with a current session."""
        vehicles = self.get_vehicle_list()
        result: dict[str, Any] = {
            "user_id": self.user_id,
            "vehicles": {},
            "account_p12_password_source": self.account_p12_password_source,
        }
        notifications = self._fetch_account_notifications()
        for vehicle in vehicles:
            status = self.get_vehicle_status(vehicle)
            mileage = self._fetch_optional_read(
                "mileage energy detail",
                self.get_mileage_energy_detail,
                vehicle,
            )
            consumption_rank = self._fetch_optional_read(
                "consumption weekly rank",
                self.get_consumption_weekly_rank,
                vehicle,
            )
            consumption_breakdown = self._fetch_optional_read(
                "consumption last week breakdown",
                self.get_consumption_last_week_breakdown,
                vehicle,
            )
            consumption_today = self._fetch_optional_read(
                "consumption today breakdown",
                self.get_consumption_today_breakdown,
                vehicle,
            )
            picture = self._fetch_optional_read(
                "car picture",
                self.get_car_picture,
                vehicle,
            )
            charging_daily = self._fetch_optional_read(
                "charging daily detail",
                self.get_charging_daily_detail,
                vehicle,
            )
            vehicle_data = normalize_vehicle(
                vehicle,
                status,
                self.user_id,
                mileage_json=mileage,
                consumption_rank_json=consumption_rank,
                consumption_breakdown_json=consumption_breakdown,
                consumption_today_json=consumption_today,
                picture_json=picture,
                charging_daily_json=charging_daily,
            )
            vehicle_data["notifications"] = notifications
            result["vehicles"][vehicle.vin] = vehicle_data
        return result

    def _fetch_optional_read(
        self,
        label: str,
        fetcher: Any,
        vehicle: Vehicle,
    ) -> dict[str, Any] | None:
        """Fetch optional read-only data without failing the whole update."""
        try:
            return fetcher(vehicle)
        except LeapmotorApiError as exc:
            _LOGGER.debug("Leapmotor optional read failed for %s: %s", label, exc)
            return None

    def _fetch_account_notifications(self) -> dict[str, Any]:
        """Fetch account-level notification data without failing vehicle updates."""
        empty: dict[str, Any] = {
            "unread_count": None,
            "last_message_title": None,
            "last_message_time": None,
        }
        try:
            headers = self._build_signed_headers()
            headers.update(self._auth_headers(content_type="application/x-www-form-urlencoded"))
            resp = self._post_with_curl(
                path="/carownerservice/oversea/message/v1/unread/count",
                headers=headers,
                data="",
                cert=self.account_cert,
            )
            body = self._parse_api_body(resp["status_code"], resp["body"], "unread count")
            unread = self._extract_unread_count(body.get("data"))

            list_headers = self._build_message_list_headers()
            list_headers.update(self._auth_headers(content_type="application/x-www-form-urlencoded"))
            resp = self._post_with_curl(
                path="/carownerservice/oversea/message/v1/list",
                headers=list_headers,
                data="pageNo=1&pageSize=1",
                cert=self.account_cert,
            )
            body = self._parse_api_body(resp["status_code"], resp["body"], "message list")
            messages = self._extract_message_list(body.get("data"))
            latest = messages[0] if messages else {}
            return {
                "unread_count": unread,
                "last_message_title": latest.get("title"),
                "last_message_time": latest.get("sendTime"),
            }
        except LeapmotorApiError as exc:
            _LOGGER.debug("Leapmotor notification fetch failed: %s", exc)
            return empty

    @staticmethod
    def _extract_unread_count(data: Any) -> int | None:
        """Return unread count from known message API response variants."""
        if isinstance(data, int):
            return data
        if isinstance(data, str):
            try:
                return int(data)
            except ValueError:
                return None
        if isinstance(data, dict):
            for key in ("unread", "unreadCount", "count"):
                if key in data:
                    try:
                        return int(data[key])
                    except (TypeError, ValueError):
                        return None
        return None

    @staticmethod
    def _extract_message_list(data: Any) -> list[dict[str, Any]]:
        """Return message list from known message API response variants."""
        if isinstance(data, list):
            return [item for item in data if isinstance(item, dict)]
        if isinstance(data, dict):
            messages = data.get("list") or data.get("records") or data.get("rows")
            if isinstance(messages, list):
                return [item for item in messages if isinstance(item, dict)]
        return []

    def login(self) -> None:
        """Login with the static app cert and load the account cert from the response."""
        self._ensure_static_cert_files()
        headers = self._build_login_headers()
        body = self._build_login_form_body()
        response = self._post_with_curl(
            path="/carownerservice/oversea/acct/v1/login",
            headers=headers,
            data=body,
            cert=(self.static_cert, self.static_key),
        )
        data = self._parse_api_body(response["status_code"], response["body"], "login")
        login_data = data.get("data") or {}
        self.user_id = str(login_data.get("id"))
        self.token = str(login_data.get("token"))
        self.device_id = derive_session_device_id(self.token, fallback=self.login_device_id)
        self.sign_ikm = str(login_data.get("signIkm"))
        self.sign_salt = str(login_data.get("signSalt"))
        self.sign_info = str(login_data.get("signInfo"))
        self.refresh_token = str(login_data.get("refreshToken") or "") or None
        self._load_account_cert(login_data)
        self.remote_cert_synced = False

    def token_refresh(self) -> None:
        """Refresh the access token without repeating the full account login."""
        if not self.refresh_token:
            raise LeapmotorAuthError("No refresh token available; a full login is required.")

        current_refresh_token = self.refresh_token
        headers = self._build_signed_headers(
            body_params={"refreshToken": current_refresh_token},
        )
        headers.update(self._auth_headers(content_type="application/x-www-form-urlencoded"))
        response = self._post_with_curl(
            path="/carownerservice/oversea/acct/v1/token/refresh",
            headers=headers,
            data=f"refreshToken={requests.utils.quote(current_refresh_token, safe='')}",
            cert=self.account_cert,
        )
        result = self._parse_api_body(
            response["status_code"],
            response["body"],
            "token refresh",
        )
        refresh_data = result.get("data") or {}
        refreshed_token = refresh_data.get("token")
        if not refreshed_token:
            raise LeapmotorAuthError("Leapmotor token refresh returned no access token.")
        self.token = str(refreshed_token)
        self.refresh_token = (
            str(refresh_data.get("refreshToken") or "") or current_refresh_token
        )
        _LOGGER.debug("Leapmotor access token refreshed successfully")

    def _recover_session(self, exc: LeapmotorApiError) -> None:
        """Refresh an expired token, falling back to a complete login."""
        if _is_token_error(exc) and self.refresh_token:
            try:
                self.token_refresh()
                return
            except LeapmotorApiError as refresh_exc:
                _LOGGER.debug(
                    "Leapmotor token refresh failed, using full login: %s",
                    refresh_exc,
                )
        self._clear_auth()
        self._ensure_static_cert_files()
        self.login()

    def get_vehicle_list(self) -> list[Vehicle]:
        """Fetch the account vehicle list."""
        headers = self._build_signed_headers()
        headers.update(self._auth_headers(content_type="application/x-www-form-urlencoded"))
        response = self._post_with_curl(
            path="/carownerservice/oversea/vehicle/v1/list",
            headers=headers,
            data="",
            cert=self.account_cert,
        )
        body = self._parse_api_body(response["status_code"], response["body"], "vehicle list")
        list_data = body.get("data") or {}
        vehicles: list[Vehicle] = []
        for bucket, is_shared in (("bindcars", False), ("sharedcars", True)):
            for item in list_data.get(bucket, []) or []:
                vin = item.get("vin")
                if not vin:
                    continue
                vehicles.append(
                    Vehicle(
                        vin=str(vin),
                        car_id=str(item["carId"]) if item.get("carId") is not None else None,
                        car_type=str(item.get("carType") or "C10"),
                        nickname=item.get("nickName"),
                        is_shared=is_shared,
                        year=_safe_int(item.get("year")),
                        rights=item.get("rightList"),
                        abilities=[str(value) for value in item.get("abilities") or []],
                        module_rights=item.get("moduleRights"),
                    )
                )
        return vehicles

    def get_vehicle_status(self, vehicle: Vehicle) -> dict[str, Any]:
        """Fetch read-only status for one vehicle."""
        car_type_path = _vehicle_status_car_type_path(vehicle.car_type)
        body = f"vin={requests.utils.quote(vehicle.vin, safe='')}"
        try:
            status = self._get_vehicle_status_raw(
                vehicle,
                car_type_path=car_type_path,
                body=body,
                label="vehicle status",
            )
        except LeapmotorApiError:
            result = self.last_api_results.get("vehicle status") or {}
            if car_type_path == "c10" or result.get("http_status") != 404:
                raise
            _LOGGER.info(
                "Leapmotor status path /%s is unavailable for model %s; trying /c10",
                car_type_path,
                vehicle.car_type,
            )
            car_type_path = "c10"
            status = self._get_vehicle_status_raw(
                vehicle,
                car_type_path=car_type_path,
                body=body,
                label="vehicle status c10 fallback",
            )
        status["_status_endpoint_path"] = car_type_path
        if (
            vehicle.is_shared
            and vehicle.car_id
            and not _status_signal_count(status)
        ):
            shared_body = (
                f"vin={requests.utils.quote(vehicle.vin, safe='')}"
                f"&carId={requests.utils.quote(vehicle.car_id, safe='')}"
            )
            try:
                shared_status = self._get_vehicle_status_raw(
                    vehicle,
                    car_type_path=car_type_path,
                    body=shared_body,
                    label="vehicle status shared carId",
                )
            except LeapmotorApiError:
                shared_status = None
            if shared_status and _status_signal_count(shared_status):
                shared_status["_status_endpoint_path"] = car_type_path
                return shared_status
        return status

    def _get_vehicle_status_raw(
        self,
        vehicle: Vehicle,
        *,
        car_type_path: str,
        body: str,
        label: str,
    ) -> dict[str, Any]:
        """Fetch read-only status with an explicit form body."""
        headers = self._build_signed_headers(vin=vehicle.vin)
        headers.update(self._auth_headers(content_type="application/x-www-form-urlencoded"))
        response = self._post_with_curl(
            path=f"/carownerservice/oversea/vehicle/v1/status/get/{car_type_path}",
            headers=headers,
            data=body,
            cert=self.account_cert,
        )
        return self._parse_api_body(response["status_code"], response["body"], label)

    def get_mileage_energy_detail(self, vehicle: Vehicle) -> dict[str, Any]:
        """Fetch read-only mileage and energy history summary."""
        begintime, endtime = _last_seven_day_window_ms()
        headers = self._build_mileage_energy_detail_headers(
            vin=vehicle.vin,
            begintime=str(begintime),
            endtime=str(endtime),
        )
        headers.update(self._auth_headers(content_type="application/x-www-form-urlencoded"))
        body = (
            f"endtime={endtime}"
            f"&begintime={begintime}"
            f"&vin={requests.utils.quote(vehicle.vin, safe='')}"
        )
        response = self._post_with_curl(
            path="/carownerservice/oversea/drivingRecord/v1/mileage/energy/detail",
            headers=headers,
            data=body,
            cert=self.account_cert,
        )
        return self._parse_api_body(response["status_code"], response["body"], "mileage energy detail")

    def get_consumption_weekly_rank(self, vehicle: Vehicle) -> dict[str, Any]:
        """Fetch read-only six-week energy consumption and ranking data."""
        headers = self._build_consumption_weekly_rank_headers(carvin=vehicle.vin)
        headers.update(self._auth_headers(content_type="application/x-www-form-urlencoded"))
        response = self._post_with_curl(
            path="/carownerservice/oversea/drivingRecord/v1/getLastNweeks100kmECAndRank",
            headers=headers,
            data=f"carvin={requests.utils.quote(vehicle.vin, safe='')}",
            cert=self.account_cert,
        )
        return self._parse_api_body(response["status_code"], response["body"], "consumption weekly rank")

    def get_consumption_last_week_breakdown(self, vehicle: Vehicle) -> dict[str, Any]:
        """Fetch read-only last-week energy split by driving, A/C, and other."""
        begintime, endtime = _previous_week_window_seconds()
        headers = self._build_consumption_last_week_headers(
            carvin=vehicle.vin,
            begintime=str(begintime),
            endtime=str(endtime),
        )
        headers.update(self._auth_headers(content_type="application/x-www-form-urlencoded"))
        body = (
            f"endtime={endtime}"
            f"&begintime={begintime}"
            f"&carvin={requests.utils.quote(vehicle.vin, safe='')}"
        )
        response = self._post_with_curl(
            path="/carownerservice/oversea/drivingRecord/v1/getLastweekEC",
            headers=headers,
            data=body,
            cert=self.account_cert,
        )
        return self._parse_api_body(response["status_code"], response["body"], "consumption last week breakdown")

    def get_consumption_today_breakdown(self, vehicle: Vehicle) -> dict[str, Any]:
        """Fetch read-only today's energy split by driving, A/C, and other."""
        begintime, endtime = _today_window_seconds()
        headers = self._build_consumption_last_week_headers(
            carvin=vehicle.vin,
            begintime=str(begintime),
            endtime=str(endtime),
        )
        headers.update(self._auth_headers(content_type="application/x-www-form-urlencoded"))
        body = (
            f"endtime={endtime}"
            f"&begintime={begintime}"
            f"&carvin={requests.utils.quote(vehicle.vin, safe='')}"
        )
        response = self._post_with_curl(
            path="/carownerservice/oversea/drivingRecord/v1/getLastweekEC",
            headers=headers,
            data=body,
            cert=self.account_cert,
        )
        return self._parse_api_body(response["status_code"], response["body"], "consumption today breakdown")

    def get_charging_daily_detail(self, vehicle: Vehicle) -> dict[str, Any]:
        """Fetch recent per-session charging details."""
        end_date = datetime.now(UTC).date()
        start_date = end_date - timedelta(days=7)
        timezone = "GMT+00:00"
        page_num = 1
        page_size = 10
        body_params = {
            "vin": vehicle.vin,
            "timeZone": timezone,
            "startTime": start_date.isoformat(),
            "endTime": end_date.isoformat(),
            "pageNum": str(page_num),
            "pageSize": str(page_size),
        }
        headers = self._build_charging_daily_detail_headers(body_params=body_params)
        headers.update(self._auth_headers(content_type="application/json"))
        body = json.dumps(
            {
                "vin": vehicle.vin,
                "timeZone": timezone,
                "startTime": start_date.isoformat(),
                "endTime": end_date.isoformat(),
                "pageNum": page_num,
                "pageSize": page_size,
            },
            separators=(",", ":"),
        )
        response = self._post_with_curl(
            path="/carownerservice/charge/daily/detail/page",
            headers=headers,
            data=body,
            cert=self.account_cert,
        )
        return self._parse_api_body(response["status_code"], response["body"], "charging daily detail")

    def get_car_picture(self, vehicle: Vehicle) -> dict[str, Any]:
        """Fetch read-only car picture metadata."""
        headers = self._build_car_picture_headers(vin=vehicle.vin)
        headers.update(self._auth_headers(content_type="application/x-www-form-urlencoded"))
        body = (
            f"deviceID={requests.utils.quote(self.device_id, safe='')}"
            f"&vin={requests.utils.quote(vehicle.vin, safe='')}"
        )
        response = self._post_with_curl(
            path="/carownerservice/oversea/vehicle/v1/carpicture/key",
            headers=headers,
            data=body,
            cert=self.account_cert,
        )
        return self._parse_api_body(response["status_code"], response["body"], "car picture")

    def download_car_picture_package(self, *, picture_key: str) -> bytes:
        """Download the picture package ZIP for one already-resolved picture key."""
        headers = self._build_car_picture_package_headers(picture_key=picture_key)
        headers.update(self._auth_headers(content_type="application/x-www-form-urlencoded"))
        response = self._post_binary_with_curl(
            path="/carownerservice/oversea/vehicle/v1/carpicture/package",
            headers=headers,
            data=f"key={requests.utils.quote(picture_key, safe='')}",
            cert=self.account_cert,
        )
        if response["status_code"] != 200:
            raise LeapmotorApiError(
                f"car picture package failed with HTTP {response['status_code']}"
            )
        return response["body"]

    def _remote_control(
        self,
        *,
        vin: str,
        action: str,
        value: int | None = None,
        cmd_content: dict[str, Any] | str | None = None,
    ) -> dict[str, Any]:
        """Execute a remote-control action using the verified operatePassword flow."""
        if not self.token:
            self.login()
        self._ensure_account_cert_files()
        if not self.operation_password:
            raise LeapmotorAuthError(
                "No vehicle PIN configured. Read-only data works without a PIN, "
                "but remote-control actions require it."
            )
        if action not in REMOTE_ACTION_SPECS:
            raise LeapmotorApiError(f"Remote action not configured: {action}")

        vehicle = self._find_vehicle_by_vin(vin)
        spec = REMOTE_ACTION_SPECS[action]
        resolved_cmd_content = spec.cmd_content
        if isinstance(cmd_content, dict):
            resolved_cmd_content = json.dumps(cmd_content, separators=(",", ":"))
        elif isinstance(cmd_content, str):
            resolved_cmd_content = cmd_content
        if value is not None:
            resolved_cmd_content = json.dumps({"value": str(value)}, separators=(",", ":"))
        return self._remote_control_raw(
            vin=vehicle.vin,
            cmd_id=spec.cmd_id,
            cmd_content=resolved_cmd_content,
            action_label=action,
            vehicle=vehicle,
        )

    def _remote_control_raw(
        self,
        *,
        vin: str,
        cmd_id: str,
        cmd_content: str,
        action_label: str,
        vehicle: Vehicle | None = None,
    ) -> dict[str, Any]:
        """Execute one raw remote-control command with the verified write flow."""
        _LOGGER.debug(
            "Starting Leapmotor remote action %s for VIN %s",
            action_label,
            _redact_vin_for_log(vin),
        )
        if not self.token:
            self.login()
        self._ensure_account_cert_files()
        if not self.operation_password:
            raise LeapmotorAuthError(
                "No vehicle PIN configured. Read-only data works without a PIN, "
                "but remote-control actions require it."
            )
        if vehicle is None:
            vehicle = self._find_vehicle_by_vin(vin)

        operate_password = derive_operate_password(self.operation_password, self.token)
        self._ensure_remote_cert_sync()

        verify_headers = self._build_operpwd_verify_headers(vin=vin, operation_password=operate_password)
        verify_headers.update(self._auth_headers(content_type="application/x-www-form-urlencoded"))
        verify_body = (
            f"operatePassword={requests.utils.quote(operate_password, safe='')}"
            f"&vin={requests.utils.quote(vin, safe='')}"
        )
        verify_response = self._post_with_curl(
            path="/carownerservice/oversea/vehicle/v1/operPwd/verify",
            headers=verify_headers,
            data=verify_body,
            cert=self.account_cert,
        )
        _LOGGER.debug(
            "Leapmotor remote verify response for %s: HTTP %s",
            action_label,
            verify_response["status_code"],
        )
        self._parse_api_body(verify_response["status_code"], verify_response["body"], "remote verify")

        headers = self._build_remote_ctl_write_headers(
            vin=vin,
            cmd_content=cmd_content,
            cmd_id=cmd_id,
            operation_password=operate_password,
        )
        headers.update(self._auth_headers(content_type="application/x-www-form-urlencoded"))
        body = (
            f"cmdContent={requests.utils.quote(cmd_content, safe='')}"
            f"&vin={requests.utils.quote(vin, safe='')}"
            f"&cmdId={requests.utils.quote(cmd_id, safe='')}"
            f"&operatePassword={requests.utils.quote(operate_password, safe='')}"
        )
        response = self._post_with_curl(
            path="/carownerservice/oversea/vehicle/v1/app/remote/ctl",
            headers=headers,
            data=body,
            cert=self.account_cert,
        )
        _LOGGER.debug(
            "Leapmotor remote ctl response for %s: HTTP %s",
            action_label,
            response["status_code"],
        )
        result = self._parse_api_body(
            response["status_code"],
            response["body"],
            f"remote {action_label}",
        )
        remote_data = result.get("data") or {}
        remote_ctl_id = remote_data.get("remoteCtlId")
        if remote_ctl_id:
            self._poll_remote_control_result(
                vin=vehicle.vin,
                car_id=vehicle.car_id,
                remote_ctl_id=str(remote_ctl_id),
                timeout_ms=int(remote_data.get("queryRemoteCtlResultTimeout") or 30000),
                interval_ms=int(remote_data.get("queryInterval") or 2000),
            )
        return result

    def _remote_control_without_pin_raw(
        self,
        *,
        vin: str,
        cmd_id: str,
        cmd_content: str,
        action_label: str,
    ) -> dict[str, Any]:
        """Execute a remote-control command that does not use operatePassword."""
        _LOGGER.debug(
            "Starting Leapmotor remote action %s for VIN %s",
            action_label,
            _redact_vin_for_log(vin),
        )
        if not self.token:
            self.login()
        self._ensure_account_cert_files()

        headers = self._build_remote_ctl_write_headers_without_pin(
            vin=vin,
            cmd_content=cmd_content,
            cmd_id=cmd_id,
        )
        headers.update(self._auth_headers(content_type="application/x-www-form-urlencoded"))
        body = (
            f"cmdContent={requests.utils.quote(cmd_content, safe='')}"
            f"&vin={requests.utils.quote(vin, safe='')}"
            f"&cmdId={requests.utils.quote(cmd_id, safe='')}"
        )
        response = self._post_with_curl(
            path="/carownerservice/oversea/vehicle/v1/app/remote/ctl",
            headers=headers,
            data=body,
            cert=self.account_cert,
        )
        _LOGGER.debug(
            "Leapmotor remote ctl response for %s: HTTP %s",
            action_label,
            response["status_code"],
        )
        return self._parse_api_body(
            response["status_code"],
            response["body"],
            f"remote {action_label}",
        )

    def _ensure_remote_cert_sync(self) -> None:
        """Bootstrap the remote-control session by syncing the account cert once."""
        if self.remote_cert_synced:
            return
        headers = self._build_signed_headers()
        headers.update(self._auth_headers(content_type="application/x-www-form-urlencoded"))
        response = self._post_with_curl(
            path="/carownerservice/oversea/vehicle/v1/cert/sync",
            headers=headers,
            data="",
            cert=(self.static_cert, self.static_key),
        )
        self._parse_api_body(response["status_code"], response["body"], "cert sync")
        self.remote_cert_synced = True

    def _poll_remote_control_result(
        self,
        *,
        vin: str,
        car_id: str | None,
        remote_ctl_id: str,
        timeout_ms: int,
        interval_ms: int,
    ) -> dict[str, Any]:
        """Poll the remote-control result endpoint until the command finishes or times out."""
        del vin, car_id
        data = f"remoteCtlId={requests.utils.quote(remote_ctl_id, safe='')}"

        deadline = time.monotonic() + max(timeout_ms, 1000) / 1000.0
        last_result: dict[str, Any] | None = None
        while time.monotonic() < deadline:
            headers = self._build_remote_ctl_result_headers(remote_ctl_id=remote_ctl_id)
            headers.update(self._auth_headers(content_type="application/x-www-form-urlencoded"))
            response = self._post_with_curl(
                path="/carownerservice/oversea/vehicle/v1/app/remote/ctl/result/query",
                headers=headers,
                data=data,
                cert=self.account_cert,
            )
            last_result = self._parse_api_body(
                response["status_code"],
                response["body"],
                "remote control result",
            )
            if (last_result.get("data")) == 1:
                return last_result
            sleep_seconds = max(interval_ms, 250) / 1000.0
            if time.monotonic() + sleep_seconds >= deadline:
                break
            time.sleep(sleep_seconds)

        raise LeapmotorApiError(f"Timed out waiting for remote control result: {last_result}")

    def _find_vehicle_by_vin(self, vin: str) -> Vehicle:
        """Resolve VIN to current vehicle metadata."""
        try:
            vehicles = self.get_vehicle_list()
        except LeapmotorApiError as exc:
            if not _is_token_error(exc):
                raise
            # Resolving the vehicle happens before any remote command is sent,
            # so refreshing and retrying here cannot duplicate a vehicle action.
            self._recover_session(exc)
            vehicles = self.get_vehicle_list()

        for vehicle in vehicles:
            if vehicle.vin == vin:
                return vehicle
        raise LeapmotorApiError(f"Vehicle not found for VIN {vin}")

    def _load_account_cert(self, login_data: dict[str, Any]) -> None:
        base64_cert = str(login_data.get("base64Cert", ""))
        p12_bytes = base64.b64decode(base64_cert)
        candidates: list[tuple[str, str]] = []
        if self.account_p12_password:
            candidates.append(("provided", self.account_p12_password))
        try:
            derived_password = derive_account_p12_password(login_data["id"], str(login_data["uid"]))
        except (KeyError, TypeError, ValueError):
            derived_password = None
        if derived_password and all(password != derived_password for _, password in candidates):
            candidates.append(("derived", derived_password))
        candidates.extend(
            ("fallback", password)
            for password in KNOWN_ACCOUNT_P12_PASSWORDS
            if all(candidate != password for _, candidate in candidates)
        )

        last_error: Exception | None = None
        for source, password in candidates:
            try:
                key, cert, _additional = pkcs12.load_key_and_certificates(
                    p12_bytes,
                    password.encode("utf-8"),
                )
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                continue
            if key is None or cert is None:
                continue

            cert_file = tempfile.NamedTemporaryFile(delete=False, suffix="-leapmotor-cert.pem")
            key_file = tempfile.NamedTemporaryFile(delete=False, suffix="-leapmotor-key.pem")
            cert_file.write(cert.public_bytes(serialization.Encoding.PEM))
            key_file.write(
                key.private_bytes(
                    serialization.Encoding.PEM,
                    serialization.PrivateFormat.TraditionalOpenSSL,
                    serialization.NoEncryption(),
                )
            )
            cert_file.close()
            key_file.close()
            self.account_cert_file = cert_file.name
            self.account_key_file = key_file.name
            self.account_p12_password_used = password
            self.account_p12_password_source = source
            return

        raise LeapmotorAccountCertError(f"Could not open account certificate: {last_error}")

    def _build_login_form_body(self) -> str:
        return (
            "isRecoverAcct=0"
            f"&password={requests.utils.quote(self.password, safe='')}"
            "&policyId=20260204"
            "&loginMethod=1"
            f"&email={requests.utils.quote(self.username, safe='')}"
        )

    def _build_login_headers(self) -> dict[str, str]:
        nonce = str(random.randint(100000, 9999999))
        timestamp = str(int(time.time() * 1000))
        sign_input = "".join(
            [
                self.language,
                DEFAULT_DEVICE_TYPE,
                self.device_id,
                "1",
                self.username,
                "0",
                "1",
                nonce,
                self.password,
                "20260204",
                DEFAULT_SOURCE,
                timestamp,
                DEFAULT_APP_VERSION,
            ]
        )
        return {
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "acceptLanguage": self.language,
            "channel": DEFAULT_CHANNEL,
            "deviceType": DEFAULT_DEVICE_TYPE,
            "X-P12_ENC_ALG": DEFAULT_P12_ENC_ALG,
            "source": DEFAULT_SOURCE,
            "version": DEFAULT_APP_VERSION,
            "nonce": nonce,
            "deviceId": self.device_id,
            "timestamp": timestamp,
            "sign": hashlib.sha256(sign_input.encode("utf-8")).hexdigest(),
        }

    def _build_signed_headers(
        self,
        *,
        vin: str | None = None,
        body_params: dict[str, str] | None = None,
    ) -> dict[str, str]:
        nonce = str(random.randint(100000, 9999999))
        timestamp = str(int(time.time() * 1000))
        sign_fields = {
            "acceptLanguage": self.language,
            "channel": DEFAULT_CHANNEL,
            "deviceId": self.device_id,
            "deviceType": DEFAULT_DEVICE_TYPE,
            "nonce": nonce,
            "source": DEFAULT_SOURCE,
            "timestamp": timestamp,
            "version": DEFAULT_APP_VERSION,
            **(body_params or {}),
        }
        if vin:
            sign_fields["vin"] = vin
        sign_input = "".join(value for _, value in sorted(sign_fields.items()))
        return {
            "acceptLanguage": self.language,
            "channel": DEFAULT_CHANNEL,
            "deviceType": DEFAULT_DEVICE_TYPE,
            "X-P12_ENC_ALG": DEFAULT_P12_ENC_ALG,
            "source": DEFAULT_SOURCE,
            "version": DEFAULT_APP_VERSION,
            "nonce": nonce,
            "deviceId": self.device_id,
            "timestamp": timestamp,
            "sign": hmac.new(self.sign_key, sign_input.encode("utf-8"), hashlib.sha256).hexdigest(),
        }

    def _build_message_list_headers(self, *, page_no: int = 1, page_size: int = 1) -> dict[str, str]:
        """Build signed headers for the message list endpoint."""
        nonce = str(random.randint(100000, 9999999))
        timestamp = str(int(time.time() * 1000))
        sign_input = "".join(
            [
                self.language,
                DEFAULT_CHANNEL,
                self.device_id,
                DEFAULT_DEVICE_TYPE,
                nonce,
                str(page_no),
                str(page_size),
                DEFAULT_SOURCE,
                timestamp,
                DEFAULT_APP_VERSION,
            ]
        )
        return self._signed_header_dict(nonce=nonce, timestamp=timestamp, sign_input=sign_input)

    def _build_consumption_weekly_rank_headers(self, *, carvin: str) -> dict[str, str]:
        """Build the signature variant used by getLastNweeks100kmECAndRank."""
        nonce = str(random.randint(100000, 9999999))
        timestamp = str(int(time.time() * 1000))
        sign_input = "".join(
            [
                self.language,
                carvin,
                DEFAULT_CHANNEL,
                self.device_id,
                DEFAULT_DEVICE_TYPE,
                nonce,
                DEFAULT_SOURCE,
                timestamp,
                DEFAULT_APP_VERSION,
            ]
        )
        return self._signed_header_dict(nonce=nonce, timestamp=timestamp, sign_input=sign_input)

    def _build_mileage_energy_detail_headers(
        self,
        *,
        vin: str,
        begintime: str,
        endtime: str,
    ) -> dict[str, str]:
        """Build the signature variant used by mileage/energy/detail with date range."""
        nonce = str(random.randint(100000, 9999999))
        timestamp = str(int(time.time() * 1000))
        sign_input = "".join(
            [
                self.language,
                begintime,
                DEFAULT_CHANNEL,
                self.device_id,
                DEFAULT_DEVICE_TYPE,
                endtime,
                nonce,
                DEFAULT_SOURCE,
                timestamp,
                DEFAULT_APP_VERSION,
                vin,
            ]
        )
        return self._signed_header_dict(nonce=nonce, timestamp=timestamp, sign_input=sign_input)

    def _build_consumption_last_week_headers(
        self,
        *,
        carvin: str,
        begintime: str,
        endtime: str,
    ) -> dict[str, str]:
        """Build the signature variant used by getLastweekEC."""
        nonce = str(random.randint(100000, 9999999))
        timestamp = str(int(time.time() * 1000))
        sign_input = "".join(
            [
                self.language,
                begintime,
                carvin,
                DEFAULT_CHANNEL,
                self.device_id,
                DEFAULT_DEVICE_TYPE,
                endtime,
                nonce,
                DEFAULT_SOURCE,
                timestamp,
                DEFAULT_APP_VERSION,
            ]
        )
        return self._signed_header_dict(nonce=nonce, timestamp=timestamp, sign_input=sign_input)

    def _build_charging_daily_detail_headers(self, *, body_params: dict[str, str]) -> dict[str, str]:
        """Build the signature variant used by charge/daily/detail/page."""
        nonce = str(random.randint(100000, 9999999))
        timestamp = str(int(time.time() * 1000))
        sign_fields = {
            "acceptLanguage": self.language,
            "channel": DEFAULT_CHANNEL,
            "deviceId": self.device_id,
            "deviceType": DEFAULT_DEVICE_TYPE,
            "nonce": nonce,
            "source": DEFAULT_SOURCE,
            "timestamp": timestamp,
            "version": DEFAULT_APP_VERSION,
            **body_params,
        }
        sign_input = "".join(value for _, value in sorted(sign_fields.items()))
        return self._signed_header_dict(nonce=nonce, timestamp=timestamp, sign_input=sign_input)

    def _signed_header_dict(
        self,
        *,
        nonce: str,
        timestamp: str,
        sign_input: str,
    ) -> dict[str, str]:
        """Return common signed app headers for account-certificate requests."""
        return {
            "acceptLanguage": self.language,
            "channel": DEFAULT_CHANNEL,
            "deviceType": DEFAULT_DEVICE_TYPE,
            "X-P12_ENC_ALG": DEFAULT_P12_ENC_ALG,
            "source": DEFAULT_SOURCE,
            "version": DEFAULT_APP_VERSION,
            "nonce": nonce,
            "deviceId": self.device_id,
            "timestamp": timestamp,
            "sign": hmac.new(self.sign_key, sign_input.encode("utf-8"), hashlib.sha256).hexdigest(),
        }

    def _build_car_picture_headers(self, *, vin: str) -> dict[str, str]:
        """Build the signature variant used by vehicle/v1/carpicture/key."""
        nonce = str(random.randint(100000, 9999999))
        timestamp = str(int(time.time() * 1000))
        sign_input = (
            f"{self.language}"
            f"{DEFAULT_CHANNEL}"
            f"{self.device_id}"
            f"{self.device_id}"
            f"{DEFAULT_DEVICE_TYPE}"
            f"{nonce}"
            f"{DEFAULT_SOURCE}"
            f"{timestamp}"
            f"{DEFAULT_APP_VERSION}"
            f"{vin}"
        )
        return {
            "acceptLanguage": self.language,
            "channel": DEFAULT_CHANNEL,
            "deviceType": DEFAULT_DEVICE_TYPE,
            "X-P12_ENC_ALG": DEFAULT_P12_ENC_ALG,
            "source": DEFAULT_SOURCE,
            "version": DEFAULT_APP_VERSION,
            "nonce": nonce,
            "deviceId": self.device_id,
            "timestamp": timestamp,
            "sign": hmac.new(self.sign_key, sign_input.encode("utf-8"), hashlib.sha256).hexdigest(),
        }

    def _build_car_picture_package_headers(self, *, picture_key: str) -> dict[str, str]:
        """Build the signature variant used by vehicle/v1/carpicture/package."""
        nonce = str(random.randint(100000, 9999999))
        timestamp = str(int(time.time() * 1000))
        sign_input = (
            f"{self.language}"
            f"{DEFAULT_CHANNEL}"
            f"{self.device_id}"
            f"{DEFAULT_DEVICE_TYPE}"
            f"{picture_key}"
            f"{nonce}"
            f"{DEFAULT_SOURCE}"
            f"{timestamp}"
            f"{DEFAULT_APP_VERSION}"
        )
        return {
            "acceptLanguage": self.language,
            "channel": DEFAULT_CHANNEL,
            "deviceType": DEFAULT_DEVICE_TYPE,
            "source": DEFAULT_SOURCE,
            "version": DEFAULT_APP_VERSION,
            "nonce": nonce,
            "deviceId": self.device_id,
            "timestamp": timestamp,
            "sign": hmac.new(self.sign_key, sign_input.encode("utf-8"), hashlib.sha256).hexdigest(),
        }

    def _build_operpwd_verify_headers(self, *, vin: str, operation_password: str) -> dict[str, str]:
        nonce = str(random.randint(100000, 9999999))
        timestamp = str(int(time.time() * 1000))
        sign_input = (
            f"{self.language}"
            f"{DEFAULT_CHANNEL}"
            f"{self.device_id}"
            f"{DEFAULT_DEVICE_TYPE}"
            f"{nonce}"
            f"{operation_password}"
            f"{DEFAULT_SOURCE}"
            f"{timestamp}"
            f"{DEFAULT_APP_VERSION}"
            f"{vin}"
        )
        return {
            "acceptLanguage": self.language,
            "channel": DEFAULT_CHANNEL,
            "deviceType": DEFAULT_DEVICE_TYPE,
            "X-P12_ENC_ALG": DEFAULT_P12_ENC_ALG,
            "source": DEFAULT_SOURCE,
            "version": DEFAULT_APP_VERSION,
            "nonce": nonce,
            "deviceId": self.device_id,
            "timestamp": timestamp,
            "sign": hmac.new(self.sign_key, sign_input.encode("utf-8"), hashlib.sha256).hexdigest(),
        }

    def _build_remote_ctl_write_headers(
        self,
        *,
        vin: str,
        cmd_content: str,
        cmd_id: str,
        operation_password: str,
    ) -> dict[str, str]:
        nonce = str(random.randint(100000, 9999999))
        timestamp = str(int(time.time() * 1000))
        sign_input = (
            f"{self.language}"
            f"{DEFAULT_CHANNEL}"
            f"{cmd_content}"
            f"{cmd_id}"
            f"{self.device_id}"
            f"{DEFAULT_DEVICE_TYPE}"
            f"{nonce}"
            f"{operation_password}"
            f"{DEFAULT_SOURCE}"
            f"{timestamp}"
            f"{DEFAULT_APP_VERSION}"
            f"{vin}"
        )
        return {
            "acceptLanguage": self.language,
            "channel": DEFAULT_CHANNEL,
            "deviceType": DEFAULT_DEVICE_TYPE,
            "X-P12_ENC_ALG": DEFAULT_P12_ENC_ALG,
            "source": DEFAULT_SOURCE,
            "version": DEFAULT_APP_VERSION,
            "nonce": nonce,
            "deviceId": self.device_id,
            "timestamp": timestamp,
            "sign": hmac.new(self.sign_key, sign_input.encode("utf-8"), hashlib.sha256).hexdigest(),
        }

    def _build_remote_ctl_write_headers_without_pin(
        self,
        *,
        vin: str,
        cmd_content: str,
        cmd_id: str,
    ) -> dict[str, str]:
        nonce = str(random.randint(100000, 9999999))
        timestamp = str(int(time.time() * 1000))
        sign_input = (
            f"{self.language}"
            f"{DEFAULT_CHANNEL}"
            f"{cmd_content}"
            f"{cmd_id}"
            f"{self.device_id}"
            f"{DEFAULT_DEVICE_TYPE}"
            f"{nonce}"
            f"{DEFAULT_SOURCE}"
            f"{timestamp}"
            f"{DEFAULT_APP_VERSION}"
            f"{vin}"
        )
        return {
            "acceptLanguage": self.language,
            "channel": DEFAULT_CHANNEL,
            "deviceType": DEFAULT_DEVICE_TYPE,
            "X-P12_ENC_ALG": DEFAULT_P12_ENC_ALG,
            "source": DEFAULT_SOURCE,
            "version": DEFAULT_APP_VERSION,
            "nonce": nonce,
            "deviceId": self.device_id,
            "timestamp": timestamp,
            "sign": hmac.new(self.sign_key, sign_input.encode("utf-8"), hashlib.sha256).hexdigest(),
        }

    def _build_remote_ctl_result_headers(self, *, remote_ctl_id: str) -> dict[str, str]:
        nonce = str(random.randint(100000, 9999999))
        timestamp = str(int(time.time() * 1000))
        sign_input = (
            f"{self.language}"
            f"{DEFAULT_CHANNEL}"
            f"{self.device_id}"
            f"{DEFAULT_DEVICE_TYPE}"
            f"{nonce}"
            f"{remote_ctl_id}"
            f"{DEFAULT_SOURCE}"
            f"{timestamp}"
            f"{DEFAULT_APP_VERSION}"
        )
        return {
            "acceptLanguage": self.language,
            "channel": DEFAULT_CHANNEL,
            "deviceType": DEFAULT_DEVICE_TYPE,
            "X-P12_ENC_ALG": DEFAULT_P12_ENC_ALG,
            "source": DEFAULT_SOURCE,
            "version": DEFAULT_APP_VERSION,
            "nonce": nonce,
            "deviceId": self.device_id,
            "timestamp": timestamp,
            "sign": hmac.new(self.sign_key, sign_input.encode("utf-8"), hashlib.sha256).hexdigest(),
        }

    def _auth_headers(self, *, content_type: str) -> dict[str, str]:
        if not self.user_id or not self.token:
            raise LeapmotorAuthError("Not authenticated.")
        return {
            "Content-Type": content_type,
            "userId": self.user_id,
            "token": self.token,
        }

    def _parse_api_body(self, status_code: int, body: str, label: str) -> dict[str, Any]:
        try:
            data = json.loads(body)
        except ValueError as exc:
            self._record_api_result(label, status_code=status_code, code=None, message="non_json")
            raise LeapmotorApiError(f"{label} returned non-JSON response: {body[:200]}") from exc
        self._record_api_result(
            label,
            status_code=status_code,
            code=data.get("code"),
            message=data.get("message"),
        )
        if status_code != 200 or data.get("code") != 0:
            message = data.get("message") or body[:200]
            if label == "login":
                raise LeapmotorAuthError(f"Leapmotor login failed: {message}")
            if label == "remote verify":
                raise LeapmotorAuthError(
                    "Leapmotor remote verify failed: "
                    f"{message}. The backend currently rejects the verification "
                    "request before any vehicle action is sent."
                )
            raise LeapmotorApiError(f"Leapmotor {label} failed: {message}")
        return data

    def _record_api_result(
        self,
        label: str,
        *,
        status_code: int,
        code: Any,
        message: Any,
    ) -> None:
        """Store compact API result metadata for diagnostics."""
        self.last_api_results[label] = {
            "http_status": status_code,
            "code": code,
            "message": message,
            "updated_at": time.time(),
        }
        _LOGGER.debug(
            "Leapmotor API result for %s: HTTP %s code=%s message=%s",
            label,
            status_code,
            code,
            message,
        )

    def _post_with_curl(
        self,
        *,
        path: str,
        headers: dict[str, str],
        data: str,
        cert: tuple[str, str],
    ) -> dict[str, Any]:
        """Send a POST with the configured API transport."""
        try:
            return self.transport.post(path=path, headers=headers, data=data, cert=cert)
        except LeapmotorApiError as exc:
            self._record_api_result(
                f"transport {path}",
                status_code=0,
                code="transport_error",
                message=str(exc),
            )
            raise

    def _post_binary_with_curl(
        self,
        *,
        path: str,
        headers: dict[str, str],
        data: str,
        cert: tuple[str, str],
    ) -> dict[str, Any]:
        """Send a POST with the configured API transport and return raw bytes."""
        try:
            return self.transport.post_binary(path=path, headers=headers, data=data, cert=cert)
        except LeapmotorApiError as exc:
            self._record_api_result(
                f"transport {path}",
                status_code=0,
                code="transport_error",
                message=str(exc),
            )
            raise


def normalize_vehicle(
    vehicle: Vehicle,
    status_json: dict[str, Any],
    user_id: str | None,
    *,
    mileage_json: dict[str, Any] | None = None,
    consumption_rank_json: dict[str, Any] | None = None,
    consumption_breakdown_json: dict[str, Any] | None = None,
    consumption_today_json: dict[str, Any] | None = None,
    picture_json: dict[str, Any] | None = None,
    charging_daily_json: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Normalize Leapmotor status payload into Home Assistant-friendly values."""
    status_data = status_json.get("data") or {}
    signal = _status_data_signal(status_data)
    config = status_data.get("config") or {}
    charge_plan = config.get("3") or _charge_plan_from_named_status(status_data)
    mileage_data = (mileage_json or {}).get("data") or {}
    rank_data = (consumption_rank_json or {}).get("data") or {}
    rank_result = rank_data.get("rankResult") or {}
    weekly_ec = rank_data.get("weeklyEC") or []
    breakdown_data = (consumption_breakdown_json or {}).get("data") or {}
    today_data = (consumption_today_json or {}).get("data") or {}
    picture_data = (picture_json or {}).get("data") or {}
    charge_records = ((charging_daily_json or {}).get("data") or {}).get("list") or []
    last_charge = charge_records[0] if charge_records else None
    vehicle_state = _derive_vehicle_state(signal)
    tire_pressures = _tire_pressures_bar(vehicle.car_type, signal)
    last_7_days_energy = _sum_detail_field(mileage_data.get("detail"), "accumulatedEnergyConsume")
    last_week_split = _energy_breakdown_percentages(breakdown_data)
    today_split = _energy_breakdown_percentages(today_data)
    status_endpoint_path = str(
        status_json.get("_status_endpoint_path")
        or _vehicle_status_car_type_path(vehicle.car_type)
    )
    status_payload_keys = sorted(str(key) for key in status_data)
    support_raw_signals = _support_raw_signals(signal)

    return {
        "vehicle": {
            "vin": vehicle.vin,
            "user_id": user_id,
            "car_id": vehicle.car_id,
            "car_type": vehicle.car_type,
            "nickname": vehicle.nickname,
            "is_shared": vehicle.is_shared,
            "year": vehicle.year,
            "rights": vehicle.rights,
            "abilities": vehicle.abilities or [],
            "module_rights": vehicle.module_rights,
        },
        "status": {
            "battery_percent": signal.get("1204"),
            "fuel_level_percent": _safe_float(signal.get("3235")),
            "fuel_level_liters": _safe_float(signal.get("2363")) / 1000.0 if signal.get("2363") is not None else None,
            "remaining_range_km": signal.get("3260"),
            "fuel_range_km": _safe_int(signal.get("3259")),
            "max_fuel_range_km": _safe_int(signal.get("3256")),
            "combined_range_km": _safe_int(signal.get("3261")),
            "max_combined_range_km": _safe_int(signal.get("3258")),
            "odometer_km": signal.get("1318"),
            "speed_kmh": _safe_float(signal.get("1319")),
            "gear": _gear_state(signal),
            "is_driving": vehicle_state == "driving" if vehicle_state is not None else None,
            "battery_percent_precise": _safe_float(signal.get("100003")),
            "cltc_range_km": _safe_int(signal.get("3257")),
            "wltp_max_range_km": _safe_int(signal.get("3257")),
            "live_remaining_range_km": _safe_int(signal.get("2188")),
            "range_mode": _range_mode(signal),
            "is_locked": _is_locked(signal),
            "raw_lock_status_code": signal.get("1298"),
            "lock_state_source": "raw_signal_1298",
            "is_parked": vehicle_state == "parked" if vehicle_state is not None else None,
            "vehicle_state": vehicle_state,
            "vehicle_state_source": "raw_signal",
            "raw_ac_operation_mode_code": signal.get("1939"),
            "raw_charge_connection_code": signal.get("1149"),
            "raw_ac_fan_speed_code": signal.get("1941"),
            "raw_vehicle_state_code": signal.get("1944"),
            "raw_parked_status_code": signal.get("1298"),
            "interior_temp_c": signal.get("1349"),
            "climate_set_temp_left_c": signal.get("2183"),
            "climate_set_temp_right_c": signal.get("2184"),
            "last_vehicle_timestamp": signal.get("sts"),
        },
        "location": {
            # Signals 2/3 retain the West/South hemisphere sign. The newer
            # 3724/3725 and legacy 2191/2190 variants contain absolute values.
            "latitude": signal.get("3", signal.get("3725", signal.get("2190"))),
            "longitude": signal.get("2", signal.get("3724", signal.get("2191"))),
            "privacy_gps": status_data.get("privacyGPS"),
            "privacy_data": status_data.get("privacyData"),
            "last_vehicle_timestamp": signal.get("sts"),
        },
        "charging": {
            "is_charging": _is_charging(signal),
            "is_plugged_in": _is_plugged_in(signal),
            "is_regening": _is_regening(signal),
            "connection_state": _charging_connection_state(signal),
            "charge_limit_percent": charge_plan.get("percent"),
            "remaining_charge_minutes": _safe_int(signal.get("1200")),
            "charging_power_kw": _charging_power_kw(signal),
            "charging_current_a": _safe_float(signal.get("1178")),
            "charging_voltage_v": _safe_float(signal.get("1177")),
            "dc_cable_connected": _not_zero(signal.get("1197")),
            "charging_planned_enabled": charge_plan.get("isEnable"),
            "charging_planned_start": charge_plan.get("beginTime"),
            "charging_planned_end": charge_plan.get("endTime"),
            "charging_planned_cycles": charge_plan.get("cycles"),
            "charging_planned_circulation": charge_plan.get("circulation"),
            "charging_plan_updated_at": charge_plan.get("updateTime"),
        },
        "history": {
            "total_mileage_km": mileage_data.get("totalmileage"),
            "total_mileage_mi": _safe_float(mileage_data.get("totalmileageMile")),
            "delivery_days": mileage_data.get("deliveryDays"),
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
            "today_driving_energy_kwh": _safe_float(today_data.get("driverEC")),
            "today_climate_energy_kwh": _safe_float(today_data.get("acEC")),
            "today_other_energy_kwh": _safe_float(today_data.get("otherEC")),
            "today_driving_energy_percent": today_split.get("driving"),
            "today_climate_energy_percent": today_split.get("climate"),
            "today_other_energy_percent": today_split.get("other"),
        },
        "charging_history": {
            "last_charge_energy_kwh": (
                _safe_float(last_charge.get("chargeInEnergy")) if last_charge else None
            ),
            "last_charge_type": last_charge.get("chargeType") if last_charge else None,
            "last_charge_start_ts": last_charge.get("chargeGunStartTs") if last_charge else None,
            "last_charge_end_ts": last_charge.get("chargeGunEndTs") if last_charge else None,
        },
        "media": {
            "car_picture_status": "available" if picture_data.get("key") else "unavailable",
            "car_picture_url": picture_data.get("shareBindUrl"),
            "car_picture_key": picture_data.get("key"),
            "car_picture_whole": picture_data.get("whole"),
            "car_picture_key_present": bool(picture_data.get("key")),
            "car_picture_whole_present": bool(picture_data.get("whole")),
        },
        "diagnostics": {
            **tire_pressures,
            "status_endpoint_path": status_endpoint_path,
            "status_payload_keys": status_payload_keys,
            "status_signal_count": len(signal),
            "status_has_config": bool(config),
            "charge_plug_signal": signal.get("47"),
            "raw_signal_47": signal.get("47"),
            "raw_signal_1149": signal.get("1149"),
            "remote_session_active": _one_is_on(signal.get("1256")) or _one_is_on(signal.get("1257")),
            "vehicle_security_active": _positive_int(signal.get("1255")),
            "vehicle_ready": _one_is_on(signal.get("1258")),
            "on3_open": _one_is_on(signal.get("1258")),
            "driver_door_open": _one_is_on(signal.get("1277")),
            "passenger_door_open": _one_is_on(signal.get("1278")),
            "rear_left_door_open": _one_is_on(signal.get("1279")),
            "rear_right_door_open": _one_is_on(signal.get("1280")),
            "trunk_open": _one_is_on(signal.get("1281")),
            "ptc_power_w": _safe_int(signal.get("1348")),
            "ptc_state": _safe_int(status_data.get("ptcState")),
            "ptc_power_setting_value": _safe_int(status_data.get("ptcPowerSettingValue")),
            "parking_brake_active": _one_is_on(signal.get("1480")),
            "battery_min_temp_c": _safe_int(signal.get("1182")),
            "battery_thermal_request": _safe_int(signal.get("1186")),
            "battery_heating": _safe_int(signal.get("1186")) == 4 if signal.get("1186") is not None else None,
            "available_energy_kwh": _wh_to_kwh(status_data.get("dumpEnergy")),
            "front_left_window_open": _not_zero(signal.get("1693")),
            "front_right_window_open": _not_zero(signal.get("1694")),
            "rear_left_window_open": _not_zero(signal.get("1695")),
            "rear_right_window_open": _not_zero(signal.get("1696")),
            "skylight_open": _not_zero(signal.get("1724")),
            "sunshade_position": _safe_int(status_data.get("sunShade")),
            "windows_remote_supported": _safe_bool(status_data.get("isSupportWindowsRemoteControl")),
            "front_left_window_position_percent": _safe_int(signal.get("3727")),
            "front_right_window_position_percent": _safe_int(signal.get("3728")),
            "rear_left_window_position_percent": _safe_int(signal.get("1879")),
            "rear_right_window_position_percent": _safe_int(signal.get("1880")),
            "climate_on": _one_is_on(signal.get("1938")),
            "climate_mode": _climate_mode(signal),
            "ac_operation_mode": _ac_operation_mode(signal),
            "outdoor_temp_c": _safe_float(status_data.get("outdoorTemp")),
            "climate_fan_volume": _safe_int(status_data.get("acAirVolume")),
            "climate_fan_volume_setting": _safe_int(status_data.get("acAirVolumeSetting")),
            "climate_air_direction": _safe_int(status_data.get("acWindDirection")),
            "climate_temp_mode": _safe_bool(status_data.get("acTempMode")),
            "climate_cooling_heating_mode": _safe_int(status_data.get("acCoolingAndHeating")),
            "climate_min_single_temp_c": _safe_float(status_data.get("minSingleTemp")),
            "air_recirculation": _safe_bool(status_data.get("acCircleMode"))
            if status_data.get("acCircleMode") is not None
            else _not_zero(signal.get("1943")),
            "bluetooth_enabled": _safe_bool(status_data.get("bluetoothState")),
            "hotspot_enabled": _safe_bool(status_data.get("hotspotState")),
            "door_control_allowed": _safe_bool(status_data.get("bcmDoorCtrlAllow")),
            "fast_cooling_active": _two_is_on(signal.get("2669")),
            "fast_heating_active": _two_is_on(signal.get("2681")),
            "windshield_defrosting": _positive_int(signal.get("1945")),
            "rear_window_heating": _one_is_on(signal.get("1946")),
            "steering_wheel_heating": _two_is_on(signal.get("1816")),
            "steering_wheel_heating_remaining_minutes": _safe_int(signal.get("1624")),
            "driver_seat_heating_level": _safe_int(signal.get("2100")),
            "passenger_seat_heating_level": _safe_int(signal.get("2118")),
            "driver_seat_ventilation_level": _safe_int(signal.get("2101")),
            "passenger_seat_ventilation_level": _safe_int(signal.get("2119")),
            "left_mirror_heating": _one_is_on(signal.get("49")),
            "right_mirror_heating": _one_is_on(signal.get("50")),
            "park_assist_enabled": _one_is_on(signal.get("2189")),
            "sentinel_mode": _one_is_on(signal.get("3636")),
            "parking_photo": _one_is_on(signal.get("3638")),
            "fully_charged": _one_is_on(signal.get("3736"))
            if signal.get("3736") is not None
            else _safe_bool(status_data.get("chargeCompleted")),
            "healthy_charging_enabled": _one_is_on(signal.get("48")),
            "charging_schedule_cancelled_once": _one_is_on(signal.get("3737")),
            "speed_limit_enabled": _one_is_on(signal.get("12054")),
            "speed_limit_kmh": _safe_int(signal.get("6048")),
            "speed_limit_unit": signal.get("6047"),
            "tire_pressure_alarm_front_left": _safe_int(signal.get("2641")),
            "tire_pressure_alarm_front_right": _safe_int(signal.get("2648")),
            "tire_pressure_alarm_rear_left": _safe_int(signal.get("2655")),
            "tire_pressure_alarm_rear_right": _safe_int(signal.get("2662")),
            "raw_signal_1010": signal.get("1010"),
            "raw_signal_48": signal.get("48"),
            "raw_signal_1182": signal.get("1182"),
            "raw_signal_1186": signal.get("1186"),
            "raw_signal_1197": signal.get("1197"),
            "raw_signal_1255": signal.get("1255"),
            "raw_signal_1256": signal.get("1256"),
            "raw_signal_1257": signal.get("1257"),
            "raw_signal_1258": signal.get("1258"),
            "raw_signal_1319": signal.get("1319"),
            "raw_signal_1348": signal.get("1348"),
            "raw_signal_1480": signal.get("1480"),
            "raw_signal_1277": signal.get("1277"),
            "raw_signal_1278": signal.get("1278"),
            "raw_signal_1279": signal.get("1279"),
            "raw_signal_1280": signal.get("1280"),
            "raw_signal_1281": signal.get("1281"),
            "raw_signal_1693": signal.get("1693"),
            "raw_signal_1694": signal.get("1694"),
            "raw_signal_1695": signal.get("1695"),
            "raw_signal_1696": signal.get("1696"),
            "raw_signal_1724": signal.get("1724"),
            "raw_signal_1816": signal.get("1816"),
            "raw_signal_1879": signal.get("1879"),
            "raw_signal_1880": signal.get("1880"),
            "raw_signal_1938": signal.get("1938"),
            "raw_signal_1939": signal.get("1939"),
            "raw_signal_1943": signal.get("1943"),
            "raw_signal_1945": signal.get("1945"),
            "raw_signal_1946": signal.get("1946"),
            "raw_signal_2100": signal.get("2100"),
            "raw_signal_2101": signal.get("2101"),
            "raw_signal_2118": signal.get("2118"),
            "raw_signal_2119": signal.get("2119"),
            "raw_signal_2189": signal.get("2189"),
            "raw_signal_2188": signal.get("2188"),
            "raw_signal_2641": signal.get("2641"),
            "raw_signal_2648": signal.get("2648"),
            "raw_signal_2655": signal.get("2655"),
            "raw_signal_2662": signal.get("2662"),
            "raw_signal_2669": signal.get("2669"),
            "raw_signal_2681": signal.get("2681"),
            "raw_signal_3262": signal.get("3262"),
            "raw_signal_3636": signal.get("3636"),
            "raw_signal_3638": signal.get("3638"),
            "raw_signal_3710": signal.get("3710"),
            "raw_signal_3712": signal.get("3712"),
            "raw_signal_3713": signal.get("3713"),
            "raw_signal_3727": signal.get("3727"),
            "raw_signal_3728": signal.get("3728"),
            "raw_signal_3736": signal.get("3736"),
            "raw_signal_3737": signal.get("3737"),
            "raw_signal_3257": signal.get("3257"),
            "raw_signal_6047": signal.get("6047"),
            "raw_signal_6048": signal.get("6048"),
            "raw_signal_12054": signal.get("12054"),
            "raw_signal_100003": signal.get("100003"),
            "raw_signal_100010": signal.get("100010"),
            "raw_signal_100011": signal.get("100011"),
            "raw_signal_100012": signal.get("100012"),
            "raw_signal_100013": signal.get("100013"),
            "raw_signal_100014": signal.get("100014"),
            "raw_signal_100015": signal.get("100015"),
            "raw_signal_100016": signal.get("100016"),
            "raw_signal_100017": signal.get("100017"),
            **support_raw_signals,
        },
        "raw_updated_at": time.time(),
    }


def _support_raw_signals(signal: dict[str, Any]) -> dict[str, Any]:
    """Return status signals safe to include in redacted support diagnostics."""
    location_signal_ids = {"2", "3", "2190", "2191", "3724", "3725"}
    return {
        f"raw_signal_{signal_id}": value
        for signal_id, value in signal.items()
        if str(signal_id).isdigit() and str(signal_id) not in location_signal_ids
    }


def _vehicle_status_car_type_path(car_type: str | None) -> str:
    """Return the backend status path segment for a vehicle model."""
    normalized = str(car_type or "C10").strip().lower()
    if normalized in {"b10", "b11"}:
        # The international backend reports these B-series model names in the
        # vehicle list, but their status endpoint is shared with C10.
        return "c10"
    return normalized or "c10"


def _normalize_charge_plan(plan: Any) -> dict[str, Any]:
    """Normalize status and appointment charge-plan key variants."""
    if not isinstance(plan, dict):
        return {}
    return {
        "isEnable": plan.get("isEnable", plan.get("chargeEnable")),
        "percent": plan.get("percent", plan.get("chargesoc")),
        "circulation": plan.get("circulation"),
        "cycles": plan.get("cycles"),
        "endTime": plan.get("endTime", plan.get("endtime")),
        "recharge": plan.get("recharge"),
        "beginTime": plan.get("beginTime", plan.get("starttime")),
    }


def _charge_plan_is_complete(plan: dict[str, Any]) -> bool:
    """Return whether a plan contains the fields needed for safe preservation."""
    return bool(
        plan.get("beginTime")
        and plan.get("endTime")
        and plan.get("cycles")
        and _safe_int(plan.get("percent")) is not None
    )


def _merge_charge_plans(
    primary: dict[str, Any],
    fallback: dict[str, Any],
) -> dict[str, Any]:
    """Fill missing primary charge-plan values from a fallback source."""
    return {
        key: value if value not in (None, "") else fallback.get(key)
        for key, value in primary.items()
    }


def _build_climate_schedule_entry(
    *,
    start_time: str,
    mode: str,
    operate: str,
    temperature: int,
    fan_speed: int,
    recirculate: bool,
    windshield_defrost: bool,
    days: list[int] | None,
    enabled: bool,
    set_id: str | None,
) -> dict[str, Any]:
    """Build one climate pre-conditioning schedule entry for cmdId 171."""
    if mode not in {"cold", "hot", "nohotcold"}:
        raise ValueError(f"Unsupported climate schedule mode: {mode}")
    if operate not in {"manual", "auto"}:
        raise ValueError(f"Unsupported climate schedule operation: {operate}")
    if not 18 <= int(temperature) <= 32:
        raise ValueError(f"Climate schedule temperature must be 18..32: {temperature}")
    if not 1 <= int(fan_speed) <= 7:
        raise ValueError(f"Climate schedule fan speed must be 1..7: {fan_speed}")

    normalized_days = _normalize_climate_schedule_days(days or [])
    now_ms = int(time.time() * 1000)
    return {
        "mode": mode,
        "operate": operate,
        "temperature": str(int(temperature)),
        "circle": "in" if recirculate else "out",
        "windlevel": str(int(fan_speed)),
        "wshld": "2" if windshield_defrost else "1",
        "days": normalized_days,
        "on": "1" if enabled else "0",
        "position": "all",
        "start_time": _normalize_climate_schedule_start_time(start_time),
        "set_id": set_id.strip() if set_id and set_id.strip() else _new_climate_schedule_id(now_ms),
        "update_time": str(now_ms),
    }


def _normalize_climate_schedule_days(days: list[int]) -> list[int]:
    """Normalize app weekday values where 0=Sunday and 6=Saturday."""
    normalized: list[int] = []
    for day in days:
        day_int = int(day)
        if not 0 <= day_int <= 6:
            raise ValueError(f"Climate schedule day must be 0..6: {day}")
        if day_int not in normalized:
            normalized.append(day_int)
    return normalized


def _normalize_climate_schedule_start_time(start_time: str) -> str:
    """Normalize a service datetime value to the app's local string format."""
    text = start_time.strip()
    if not text:
        raise ValueError("Climate schedule start_time is required.")
    parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    return parsed.strftime("%Y-%m-%d %H:%M:00")


def _new_climate_schedule_id(now_ms: int) -> str:
    """Return an app-shaped opaque schedule id."""
    return f"ios_{uuid.uuid4().hex}{now_ms // 1000}"


def _build_prepare_car_schedule_entry(
    *,
    start_time: str,
    climate_enabled: bool,
    mode: str,
    operate: str,
    temperature: int,
    fan_speed: int,
    recirculate: bool,
    windshield_defrost: bool,
    driver_seat: str,
    driver_seat_level: int,
    passenger_seat: str,
    passenger_seat_level: int,
    steering_wheel_heat: bool,
    mirror_heat: bool,
    destination_name: str | None,
    destination_address: str | None,
    destination_latitude: float | None,
    destination_longitude: float | None,
    days: list[int] | None,
    enabled: bool,
    set_id: str | None,
) -> dict[str, Any]:
    """Build one one-touch vehicle preparation schedule entry for cmdId 361."""
    now_ms = int(time.time() * 1000)
    return {
        "datacontent": _build_prepare_car_datacontent(
            climate_enabled=climate_enabled,
            mode=mode,
            operate=operate,
            temperature=temperature,
            fan_speed=fan_speed,
            recirculate=recirculate,
            windshield_defrost=windshield_defrost,
            driver_seat=driver_seat,
            driver_seat_level=driver_seat_level,
            passenger_seat=passenger_seat,
            passenger_seat_level=passenger_seat_level,
            steering_wheel_heat=steering_wheel_heat,
            mirror_heat=mirror_heat,
            destination_name=destination_name,
            destination_address=destination_address,
            destination_latitude=destination_latitude,
            destination_longitude=destination_longitude,
        ),
        "days": _normalize_climate_schedule_days(days or []),
        "enable": bool(enabled),
        "set_id": set_id.strip() if set_id and set_id.strip() else _new_climate_schedule_id(now_ms),
        "start_time": _normalize_climate_schedule_start_time(start_time),
    }


def _build_prepare_car_datacontent(
    *,
    climate_enabled: bool,
    mode: str,
    operate: str,
    temperature: int,
    fan_speed: int,
    recirculate: bool,
    windshield_defrost: bool,
    driver_seat: str,
    driver_seat_level: int,
    passenger_seat: str,
    passenger_seat_level: int,
    steering_wheel_heat: bool,
    mirror_heat: bool,
    destination_name: str | None,
    destination_address: str | None,
    destination_latitude: float | None,
    destination_longitude: float | None,
) -> dict[str, Any]:
    """Build the cmdId 360/361 datacontent bundle captured from the B10 app."""
    datacontent: dict[str, Any] = {}
    if climate_enabled:
        if mode not in {"cold", "hot", "nohotcold"}:
            raise ValueError(f"Prepare-car climate mode must be cold, hot or nohotcold: {mode}")
        if operate not in {"manual", "auto"}:
            raise ValueError(f"Prepare-car operation must be manual or auto: {operate}")
        if not 18 <= int(temperature) <= 32:
            raise ValueError(f"Prepare-car temperature must be 18..32: {temperature}")
        if not 1 <= int(fan_speed) <= 7:
            raise ValueError(f"Prepare-car fan speed must be 1..7: {fan_speed}")
        datacontent["air_condition"] = {
            "mode": mode,
            "temperature": str(int(temperature)),
            "circle": "in" if recirculate else "out",
            "windlevel": str(int(fan_speed)),
            "wshld": "2" if windshield_defrost else "1",
            "operate": operate,
            "position": "all",
            "enable": True,
        }

    seat_setting = {
        "driver": _prepare_car_seat_code(driver_seat, driver_seat_level, "driver"),
        "copilot": _prepare_car_seat_code(passenger_seat, passenger_seat_level, "passenger"),
        "left_rear": "0",
        "right_rear": "0",
    }
    if any(value != "0" for value in seat_setting.values()):
        datacontent["seat_setting"] = {**seat_setting, "enable": True}

    if steering_wheel_heat:
        datacontent["steeringWheelHeatCtrl"] = {"enable": True, "level": "2"}
    if mirror_heat:
        datacontent["rearMirrorHeating"] = {"enable": True, "value": "2"}

    destination = _build_prepare_car_destination(
        destination_name=destination_name,
        destination_address=destination_address,
        destination_latitude=destination_latitude,
        destination_longitude=destination_longitude,
    )
    if destination:
        datacontent["syn_path"] = destination

    if not datacontent:
        raise ValueError("Prepare-car requires at least one enabled dimension.")
    return datacontent


def _prepare_car_seat_code(mode: str, level: int, label: str) -> str:
    """Return the prepare-car seat setting code."""
    if mode not in {"off", "heat", "ventilation"}:
        raise ValueError(f"Prepare-car {label} seat mode must be off, heat or ventilation: {mode}")
    if mode == "off":
        return "0"
    level_int = int(level)
    if not 1 <= level_int <= 3:
        raise ValueError(f"Prepare-car {label} seat level must be 1..3: {level}")
    if mode == "heat":
        return str(level_int)
    return str(10 + level_int)


def _build_prepare_car_destination(
    *,
    destination_name: str | None,
    destination_address: str | None,
    destination_latitude: float | None,
    destination_longitude: float | None,
) -> dict[str, Any] | None:
    """Build the optional navigation sync payload inside prepare-car."""
    has_destination = any(
        value not in (None, "")
        for value in (
            destination_name,
            destination_address,
            destination_latitude,
            destination_longitude,
        )
    )
    if not has_destination:
        return None
    if destination_latitude is None or destination_longitude is None:
        raise ValueError("Prepare-car destination requires latitude and longitude.")
    name = (destination_name or "").strip()
    if not name:
        raise ValueError("Prepare-car destination requires destination_name.")
    address = (destination_address or name).strip()
    return {
        "address": address,
        "addressname": name,
        "addresskey": "",
        "config": "0110",
        "latitude": _format_prepare_car_coordinate(destination_latitude),
        "longitude": _format_prepare_car_coordinate(destination_longitude),
        "linenum": "0",
        "enable": True,
    }


def _format_prepare_car_coordinate(value: float) -> str:
    """Format coordinates like the app payload while avoiding scientific notation."""
    return f"{float(value):.8f}".rstrip("0").rstrip(".")


def _is_token_error(exc: Exception) -> bool:
    """Return whether an API error indicates an invalid or expired token."""
    message = str(exc).lower()
    return "token" in message and any(
        marker in message
        for marker in ("invalid", "expired", "expire", "unauthorized", "not valid")
    )


def _status_data_signal(status_data: dict[str, Any]) -> dict[str, Any]:
    """Return the numeric signal map, including fallback values from named fields."""
    raw_signal = status_data.get("signal") or {}
    signal = dict(raw_signal) if isinstance(raw_signal, dict) else {}
    named_signal = _named_status_to_signal(status_data)
    for key, value in named_signal.items():
        signal.setdefault(key, value)
    return signal


def _named_status_to_signal(status_data: dict[str, Any]) -> dict[str, Any]:
    """Map legacy/named T03-style status fields to the APK numeric signal IDs."""
    mapped: dict[str, Any] = {}
    field_map = {
        "soc": "1204",
        "chargeRemainTime": "1200",
        "batteryCurrent": "1178",
        "batteryVoltage": "1177",
        "dcInputFastCharge": "1197",
        "expectedMileage": "3260",
        "speed": "1319",
        "totalMileage": "1318",
        "gearStatus": "1010",
        "latitude": "3725",
        "longitude": "3724",
        "acSwitch": "1938",
        "acSetting": "2183",
        "leftFrontWindowPercent": "3727",
        "rightFrontWindowPercent": "3728",
        "leftRearWindowPercent": "1879",
        "rightRearWindowPercent": "1880",
        "leftFrontTirePressure": "2646",
        "rightFrontTirePressure": "2653",
        "leftRearTirePressure": "2660",
        "rightRearTirePressure": "2667",
        "leftFrontTirePressureState": "2641",
        "rightFrontTirePressureState": "2648",
        "leftRearTirePressureState": "2655",
        "rightRearTirePressureState": "2662",
    }
    for source, target in field_map.items():
        if status_data.get(source) is not None:
            mapped[target] = status_data[source]

    if status_data.get("expectedMileage") is not None:
        mapped["2188"] = status_data["expectedMileage"]
    if status_data.get("acSetting") is not None:
        mapped["2184"] = status_data["acSetting"]

    bool_map = {
        "driverDoorLockStatus": "1298",
        "lbcmDriverDoorStatus": "1277",
        "rbcmDriverDoorStatus": "1278",
        "lbcmLeftRearDoorStatus": "1279",
        "rbcmRightRearDoorStatus": "1280",
        "bbcmBackDoorStatus": "1281",
        "driverWindowStatus": "1693",
        "rightFrontWindowStatus": "1694",
        "leftRearWindowStatus": "1695",
        "rightRearWindowStatus": "1696",
        "bcmKeyPositionOn1": "1256",
        "bcmKeyPositionOn2": "1257",
        "bcmKeyPositionOn3": "1258",
    }
    for source, target in bool_map.items():
        if status_data.get(source) is not None:
            mapped[target] = 1 if bool(status_data[source]) else 0

    if status_data.get("chargeState") is not None:
        charge_state = _safe_int(status_data.get("chargeState"))
        mapped["1149"] = charge_state
        mapped["47"] = 1 if charge_state in (1, 2) else 0
    if status_data.get("collectTime") is not None:
        mapped["sts"] = status_data["collectTime"]
    elif status_data.get("collectTimeMs") is not None:
        mapped["sts"] = status_data["collectTimeMs"]
    return mapped


def _charge_plan_from_named_status(status_data: dict[str, Any]) -> dict[str, Any]:
    """Return a charge-plan-like dict from flat status fields when config is absent."""
    charge_limit = status_data.get("chargesocSetting")
    charge_time = status_data.get("chargeTimeSetting")
    if charge_limit is None and charge_time is None:
        return {}
    plan: dict[str, Any] = {}
    if charge_limit is not None:
        plan["percent"] = charge_limit
    if charge_time is not None:
        plan["beginTime"] = charge_time
    return plan


def _status_signal_count(status_json: dict[str, Any]) -> int:
    """Return how many raw status signals the backend returned."""
    signal = _status_data_signal(status_json.get("data") or {})
    return len(signal) if isinstance(signal, dict) else 0


def _tire_pressures_bar(car_type: str | None, signal: dict[str, Any]) -> dict[str, float | None]:
    """Return tire-pressure slots verified against APK names and live C10/B10 checks."""
    return {
        "tire_pressure_front_left_bar": _to_bar(signal.get("2646")),
        "tire_pressure_front_right_bar": _to_bar(signal.get("2653")),
        "tire_pressure_rear_left_bar": _to_bar(signal.get("2660")),
        "tire_pressure_rear_right_bar": _to_bar(signal.get("2667")),
    }


def _last_seven_day_window_ms() -> tuple[int, int]:
    """Return the local app-style window used for 7-day mileage/energy detail."""
    now = _berlin_now()
    today = now.replace(hour=0, minute=0, second=0, microsecond=0)
    start = today - timedelta(days=7)
    end = today + timedelta(days=1) - timedelta(seconds=1)
    return int(start.timestamp() * 1000), int(end.timestamp() * 1000)


def _previous_week_window_seconds() -> tuple[int, int]:
    """Return the previous Monday-Sunday window used by getLastweekEC."""
    now = _berlin_now()
    this_monday = (now - timedelta(days=now.weekday())).replace(
        hour=0,
        minute=0,
        second=0,
        microsecond=0,
    )
    start = this_monday - timedelta(days=7)
    end = this_monday - timedelta(seconds=1)
    return int(start.timestamp()), int(end.timestamp())


def _today_window_seconds() -> tuple[int, int]:
    """Return the start of today to the current time, in seconds."""
    now = _berlin_now()
    start = now.replace(
        hour=0,
        minute=0,
        second=0,
        microsecond=0,
    )
    return int(start.timestamp()), int(now.timestamp())


def _berlin_now() -> datetime:
    """Return current time in the locale observed in the app traces."""
    try:
        return datetime.now(ZoneInfo("Europe/Berlin"))
    except Exception:
        return datetime.now().astimezone()


def _sum_detail_field(detail: Any, field: str) -> float | None:
    """Sum one numeric field from an API detail list."""
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
    """Convert last-week kWh split values to percentages."""
    values = {
        "driving": _safe_float(data.get("driverEC")),
        "climate": _safe_float(data.get("acEC")),
        "other": _safe_float(data.get("otherEC")),
    }
    total = sum(value for value in values.values() if value is not None)
    if total <= 0:
        return {key: None for key in values}
    return {
        key: round(value * 100 / total, 1) if value is not None else None
        for key, value in values.items()
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


def _build_climate_payload(
    *,
    temperature: int | None,
    mode: str | None,
    windlevel: int | None,
    circle: str | None,
    operate: str,
) -> dict[str, str]:
    """Build the cmd_id=170 climate payload used by the official app."""
    resolved_mode = mode or "nohotcold"
    if resolved_mode == "wind":
        resolved_mode = "nohotcold"
    resolved_circle = circle or "out"
    resolved_temperature = 24 if temperature is None else int(temperature)
    resolved_windlevel = 4 if windlevel is None else int(windlevel)

    if resolved_mode not in {"cold", "hot", "nohotcold"}:
        raise LeapmotorApiError("Climate mode must be one of: cold, hot, wind, nohotcold.")
    if resolved_circle not in {"in", "out"}:
        raise LeapmotorApiError("Climate circulation must be one of: in, out.")
    if resolved_temperature < 18 or resolved_temperature > 32:
        raise LeapmotorApiError("Climate temperature must be between 18 and 32.")
    if resolved_windlevel < 1 or resolved_windlevel > 7:
        raise LeapmotorApiError("Climate fan level must be between 1 and 7.")

    return {
        "circle": resolved_circle,
        "mode": resolved_mode,
        "operate": operate,
        "position": "all",
        "temperature": str(resolved_temperature),
        "windlevel": str(resolved_windlevel),
        "wshld": "1",
    }


def _to_bar(raw: Any) -> float | None:
    if raw is None:
        return None
    try:
        return round(float(raw) / 100.0, 2)
    except (TypeError, ValueError):
        return None


def _one_is_on(raw: Any) -> bool | None:
    value = _safe_int(raw)
    if value is None:
        return None
    return value == 1


def _two_is_on(raw: Any) -> bool | None:
    value = _safe_int(raw)
    if value is None:
        return None
    return value == 2


def _not_zero(raw: Any) -> bool | None:
    if raw is None:
        return None
    return str(raw) != "0"


def _positive_int(raw: Any) -> bool | None:
    value = _safe_int(raw)
    if value is None:
        return None
    return value > 0


def _safe_bool(raw: Any) -> bool | None:
    """Return a bool for backend bool/int/string flags."""
    if raw is None:
        return None
    if isinstance(raw, bool):
        return raw
    if isinstance(raw, (int, float)):
        return raw != 0
    normalized = str(raw).strip().lower()
    if normalized in ("1", "true", "yes", "on"):
        return True
    if normalized in ("0", "false", "no", "off"):
        return False
    return None


def _wh_to_kwh(raw: Any) -> float | None:
    value = _safe_float(raw)
    if value is None:
        return None
    return round(value / 1000.0, 2)


def _gear_state(signal: dict[str, Any]) -> str | None:
    return {
        0: "P",
        1: "R",
        2: "N",
        3: "D",
    }.get(_safe_int(signal.get("1010")))


def _range_mode(signal: dict[str, Any]) -> str | None:
    return {
        0: "CLTC",
        1: "WLTP",
    }.get(_safe_int(signal.get("3262")))


def _derive_vehicle_state(signal: dict[str, Any]) -> str | None:
    """Return the movement state independent from charging and HVAC signals."""
    gear = _safe_int(signal.get("1010"))
    if gear is not None:
        if gear in (1, 3):
            return "driving"
        if gear in (0, 2):
            return "parked"

    speed = _safe_float(signal.get("1319"))
    if speed is not None:
        return "driving" if speed > 0 else "parked"

    on3 = _one_is_on(signal.get("1258"))
    if on3 is not None:
        return "parked"

    return None


def _is_locked(signal: dict[str, Any]) -> bool | None:
    """Return app-correlated door-lock state from the validated home-screen signal."""
    lock_status = _safe_int(signal.get("1298"))
    if lock_status is None:
        return None
    if lock_status == 1:
        return True
    if lock_status == 0:
        return False
    return None


def _is_charging(signal: dict[str, Any]) -> bool:
    """Return whether the vehicle is currently charging."""
    remaining_charge_minutes = _safe_int(signal.get("1200"))
    charging_current_a = _safe_float(signal.get("1178"))
    charging_power_kw = _charging_power_kw(signal)
    if charging_current_a is not None:
        # Confirmed charging sessions show a clearly non-zero current
        # (typically negative while energy flows into the pack). After
        # charge completion the backend can keep 1149=2 while current is 0.
        if abs(charging_current_a) < 1.0:
            return False
        # B10 can actively AC-charge around 2.5 A. C10 plugged-idle snapshots
        # can sit around 1.5 A, so the grey zone needs an extra confirmation.
        if abs(charging_current_a) < 3.0:
            return remaining_charge_minutes is not None and (
                remaining_charge_minutes > 0
                or (charging_power_kw is not None and charging_power_kw >= 1.0)
            )
        return remaining_charge_minutes is not None or (
            charging_power_kw is not None and charging_power_kw >= 1.0
        )

    if charging_power_kw is not None:
        return charging_power_kw >= 1.0 and remaining_charge_minutes is not None

    connection_status = _safe_int(signal.get("1149"))
    if connection_status == 2:
        return True
    if connection_status in (0, 1):
        return False

    return False


def _is_plugged_in(signal: dict[str, Any]) -> bool | None:
    """Return whether the charge cable is plugged in."""
    plug = _safe_int(signal.get("47"))
    if plug is not None:
        return plug == 1
    connection_status = _safe_int(signal.get("1149"))
    if connection_status is not None:
        return connection_status in (1, 2)
    return None


def _is_regening(signal: dict[str, Any]) -> bool | None:
    """Return whether energy is flowing into the battery without a charge cable."""
    plugged_in = _is_plugged_in(signal)
    if plugged_in is None:
        return None
    if plugged_in:
        return False
    return _is_charging(signal)


def _charging_connection_state(signal: dict[str, Any]) -> str | None:
    """Return the observed charge-connection state."""
    if _is_charging(signal):
        return "charging"
    if _charge_is_finished(signal):
        return "finished"
    charging_current_a = _safe_float(signal.get("1178"))
    if charging_current_a is not None and abs(charging_current_a) < 1.0:
        return "plugged_in" if _is_plugged_in(signal) else "unplugged"
    connection_status = _safe_int(signal.get("1149"))
    if connection_status == 0:
        return "unplugged"
    if connection_status == 1:
        return "plugged_in"
    if connection_status == 2:
        return "plugged_in" if _is_plugged_in(signal) else "charging"
    return None


def _charge_is_finished(signal: dict[str, Any]) -> bool:
    """Return whether the backend still reports connected while charging is complete."""
    if _is_charging(signal):
        return False
    if not _is_plugged_in(signal):
        return False
    if _one_is_on(signal.get("3736")):
        return True
    connection_status = _safe_int(signal.get("1149"))
    if connection_status != 2:
        return False
    remaining_charge_minutes = _safe_int(signal.get("1200"))
    charging_current_a = _safe_float(signal.get("1178"))
    charging_power_kw = _charging_power_kw(signal)
    current_idle = charging_current_a is not None and abs(charging_current_a) < 1.0
    power_idle = charging_power_kw is None or charging_power_kw < 1.0
    return current_idle and power_idle and remaining_charge_minutes in (None, 0)


def _climate_mode(signal: dict[str, Any]) -> str | None:
    mode = _safe_int(signal.get("3713"))
    return {
        0: "off",
        1: "fast_cool",
        3: "fast_heat",
        4: "quick_ventilation",
    }.get(mode)


def _ac_operation_mode(signal: dict[str, Any]) -> str | None:
    mode = _safe_int(signal.get("1939"))
    return {
        0: "auto",
        1: "manual",
    }.get(mode)


def _charging_power_kw(signal: dict[str, Any]) -> float | None:
    """Return charging power without using GPS longitude-like signal 2191."""
    current = _safe_float(signal.get("1178"))
    voltage = _safe_float(signal.get("1177"))
    if current is None or voltage is None:
        return None
    abs_current = abs(current)
    raw_power_kw = abs(current * voltage) / 1000.0
    if abs_current < 1.0:
        return 0.0
    # The C10 plugged-idle snapshot shows about 1.5 A without active charging,
    # while B10 can actively AC-charge around 2.5 A. In this grey zone, require
    # either remaining charge time or a clearly non-trivial calculated power.
    if abs_current < 3.0:
        remaining_charge_minutes = _safe_int(signal.get("1200"))
        if remaining_charge_minutes is None and raw_power_kw < 1.0:
            return None
    return round(raw_power_kw, 3)
