"""Leapmotor cloud API client."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import logging
import random
import subprocess
import tempfile
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import requests
import urllib3
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives import padding
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives.kdf.hkdf import HKDF
from cryptography.hazmat.primitives.serialization import pkcs12

from .const import (
    DEFAULT_APP_VERSION,
    DEFAULT_BASE_URL,
    DEFAULT_CHANNEL,
    DEFAULT_DEVICE_ID,
    DEFAULT_DEVICE_TYPE,
    DEFAULT_LANGUAGE,
    DEFAULT_OPERPWD_AES_IV,
    DEFAULT_OPERPWD_AES_KEY,
    DEFAULT_P12_ENC_ALG,
    DEFAULT_SOURCE,
    KNOWN_ACCOUNT_P12_PASSWORDS,
    REMOTE_CTL_AC_SWITCH,
    REMOTE_CTL_BATTERY_PREHEAT,
    REMOTE_CTL_FIND_CAR,
    REMOTE_CTL_LOCK,
    REMOTE_CTL_QUICK_COOL,
    REMOTE_CTL_QUICK_HEAT,
    REMOTE_CTL_SUNSHADE,
    REMOTE_CTL_SUNSHADE_CLOSE,
    REMOTE_CTL_SUNSHADE_OPEN,
    REMOTE_CTL_TRUNK,
    REMOTE_CTL_TRUNK_CLOSE,
    REMOTE_CTL_TRUNK_OPEN,
    REMOTE_CTL_UNLOCK,
    REMOTE_CTL_WINDSHIELD_DEFROST,
    REMOTE_CTL_WINDOWS,
    REMOTE_CTL_WINDOWS_CLOSE,
    REMOTE_CTL_WINDOWS_OPEN,
    STATIC_APP_CERT,
    STATIC_APP_KEY,
)
from .p12 import derive_account_p12_password

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

_LOGGER = logging.getLogger(__name__)


class LeapmotorApiError(Exception):
    """Base Leapmotor API error."""


class LeapmotorAuthError(LeapmotorApiError):
    """Leapmotor authentication failed."""


class LeapmotorAccountCertError(LeapmotorAuthError):
    """Leapmotor account certificate could not be opened."""


class LeapmotorMissingAppCertError(LeapmotorAuthError):
    """Local app certificate material is missing."""


class LeapmotorNoVehicleError(LeapmotorApiError):
    """The account login worked, but no vehicle is linked to the account."""


@dataclass(slots=True)
class Vehicle:
    """Vehicle metadata from the vehicle list."""

    vin: str
    car_id: str | None
    car_type: str
    nickname: str | None
    is_shared: bool
    year: int | None = None
    rights: str | None = None
    abilities: list[str] | None = None
    module_rights: str | None = None


@dataclass(frozen=True, slots=True)
class RemoteActionSpec:
    """Verified remote-control action payload."""

    cmd_id: str
    cmd_content: str


REMOTE_ACTION_SPECS: dict[str, RemoteActionSpec] = {
    REMOTE_CTL_UNLOCK: RemoteActionSpec(cmd_id="110", cmd_content='{"value":"unlock"}'),
    REMOTE_CTL_LOCK: RemoteActionSpec(cmd_id="110", cmd_content='{"value":"lock"}'),
    REMOTE_CTL_TRUNK: RemoteActionSpec(cmd_id="130", cmd_content='{"value":"true"}'),
    REMOTE_CTL_TRUNK_OPEN: RemoteActionSpec(cmd_id="130", cmd_content='{"value":"true"}'),
    REMOTE_CTL_TRUNK_CLOSE: RemoteActionSpec(cmd_id="130", cmd_content='{"value":"false"}'),
    REMOTE_CTL_FIND_CAR: RemoteActionSpec(cmd_id="120", cmd_content='{"value":"true"}'),
    REMOTE_CTL_SUNSHADE: RemoteActionSpec(cmd_id="240", cmd_content='{"value":"10"}'),
    REMOTE_CTL_SUNSHADE_OPEN: RemoteActionSpec(cmd_id="240", cmd_content='{"value":"10"}'),
    REMOTE_CTL_SUNSHADE_CLOSE: RemoteActionSpec(cmd_id="240", cmd_content='{"value":"0"}'),
    REMOTE_CTL_BATTERY_PREHEAT: RemoteActionSpec(cmd_id="160", cmd_content='{"value":"ptcon"}'),
    REMOTE_CTL_WINDOWS: RemoteActionSpec(cmd_id="230", cmd_content='{"value":"2"}'),
    REMOTE_CTL_WINDOWS_OPEN: RemoteActionSpec(cmd_id="230", cmd_content='{"value":"2"}'),
    REMOTE_CTL_WINDOWS_CLOSE: RemoteActionSpec(cmd_id="230", cmd_content='{"value":"0"}'),
    REMOTE_CTL_AC_SWITCH: RemoteActionSpec(
        cmd_id="170",
        cmd_content='{"circle":"out","mode":"nohotcold","operate":"manual","position":"all","temperature":"24","windlevel":"4","wshld":"1"}',
    ),
    REMOTE_CTL_QUICK_COOL: RemoteActionSpec(
        cmd_id="170",
        cmd_content='{"circle":"in","mode":"cold","operate":"manual","position":"all","temperature":"18","windlevel":"7","wshld":"1"}',
    ),
    REMOTE_CTL_QUICK_HEAT: RemoteActionSpec(
        cmd_id="170",
        cmd_content='{"circle":"in","mode":"hot","operate":"manual","position":"all","temperature":"32","windlevel":"7","wshld":"1"}',
    ),
    REMOTE_CTL_WINDSHIELD_DEFROST: RemoteActionSpec(
        cmd_id="170",
        cmd_content='{"circle":"in","mode":"hot","operate":"manual","position":"all","temperature":"32","windlevel":"7","wshld":"2"}',
    ),
}


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
        static_cert_dir: str | Path | None = None,
    ) -> None:
        self.username = username
        self.password = password
        self.operation_password = operation_password.strip() if operation_password else None
        self.account_p12_password = account_p12_password
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        self.device_id = DEFAULT_DEVICE_ID
        self.user_id: str | None = None
        self.token: str | None = None
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
        self.device_id = DEFAULT_DEVICE_ID
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
        except LeapmotorApiError:
            self._clear_auth()
            self._ensure_static_cert_files()
            self.login()
            return self._fetch_authenticated_data()

    def lock_vehicle(self, vin: str) -> dict[str, Any]:
        """Lock one vehicle via remote control."""
        return self._remote_control(vin=vin, action=REMOTE_CTL_LOCK)

    def unlock_vehicle(self, vin: str) -> dict[str, Any]:
        """Unlock one vehicle via remote control."""
        return self._remote_control(vin=vin, action=REMOTE_CTL_UNLOCK)

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

    def open_sunshade(self, vin: str) -> dict[str, Any]:
        """Open the sunshade via remote control."""
        return self._remote_control(vin=vin, action=REMOTE_CTL_SUNSHADE_OPEN)

    def close_sunshade(self, vin: str) -> dict[str, Any]:
        """Close the sunshade via remote control."""
        return self._remote_control(vin=vin, action=REMOTE_CTL_SUNSHADE_CLOSE)

    def battery_preheat(self, vin: str) -> dict[str, Any]:
        """Trigger the verified battery-preheat action."""
        return self._remote_control(vin=vin, action=REMOTE_CTL_BATTERY_PREHEAT)

    def windows(self, vin: str) -> dict[str, Any]:
        """Trigger the verified window action."""
        return self._remote_control(vin=vin, action=REMOTE_CTL_WINDOWS)

    def open_windows(self, vin: str) -> dict[str, Any]:
        """Open the windows via remote control."""
        return self._remote_control(vin=vin, action=REMOTE_CTL_WINDOWS_OPEN)

    def close_windows(self, vin: str) -> dict[str, Any]:
        """Close the windows via remote control."""
        return self._remote_control(vin=vin, action=REMOTE_CTL_WINDOWS_CLOSE)

    def ac_switch(self, vin: str) -> dict[str, Any]:
        """Trigger the verified A/C switch profile."""
        return self._remote_control(vin=vin, action=REMOTE_CTL_AC_SWITCH)

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
        vehicle = self._find_vehicle_by_vin(vin)
        status_json = self.get_vehicle_status(vehicle)
        charge_plan = (((status_json.get("data") or {}).get("config") or {}).get("3") or {})

        start_time = charge_plan.get("beginTime")
        end_time = charge_plan.get("endTime")
        cycles = charge_plan.get("cycles")
        if not start_time or not end_time or not cycles:
            raise LeapmotorApiError(
                "Current charging plan is incomplete, cannot safely update charge limit."
            )

        cmd_content = json.dumps(
            {
                "chargeEnable": 1 if _safe_int(charge_plan.get("isEnable")) else 0,
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
            action_label="set_charge_limit",
            vehicle=vehicle,
        )

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
            picture = self._fetch_optional_read(
                "car picture",
                self.get_car_picture,
                vehicle,
            )
            result["vehicles"][vehicle.vin] = normalize_vehicle(
                vehicle,
                status,
                self.user_id,
                mileage_json=mileage,
                consumption_rank_json=consumption_rank,
                consumption_breakdown_json=consumption_breakdown,
                picture_json=picture,
            )
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
        self.device_id = self._derive_session_device_id(self.token)
        self.sign_ikm = str(login_data.get("signIkm"))
        self.sign_salt = str(login_data.get("signSalt"))
        self.sign_info = str(login_data.get("signInfo"))
        self._load_account_cert(login_data)
        self.remote_cert_synced = False

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
        headers = self._build_signed_headers(vin=vehicle.vin)
        headers.update(self._auth_headers(content_type="application/x-www-form-urlencoded"))
        response = self._post_with_curl(
            path=f"/carownerservice/oversea/vehicle/v1/status/get/{car_type_path}",
            headers=headers,
            data=f"vin={requests.utils.quote(vehicle.vin, safe='')}",
            cert=self.account_cert,
        )
        return self._parse_api_body(response["status_code"], response["body"], "vehicle status")

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

    def _remote_control(self, *, vin: str, action: str) -> dict[str, Any]:
        """Execute a remote-control action using the verified operatePassword flow."""
        if not self.token:
            self.login()
        if not self.operation_password:
            raise LeapmotorAuthError(
                "No vehicle PIN configured. Read-only data works without a PIN, "
                "but remote-control actions require it."
            )
        if action not in REMOTE_ACTION_SPECS:
            raise LeapmotorApiError(f"Remote action not configured: {action}")

        vehicle = self._find_vehicle_by_vin(vin)
        spec = REMOTE_ACTION_SPECS[action]
        return self._remote_control_raw(
            vin=vehicle.vin,
            cmd_id=spec.cmd_id,
            cmd_content=spec.cmd_content,
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
        _LOGGER.info("Starting Leapmotor remote action %s for VIN %s", action_label, vin)
        if not self.token:
            self.login()
        if not self.operation_password:
            raise LeapmotorAuthError(
                "No vehicle PIN configured. Read-only data works without a PIN, "
                "but remote-control actions require it."
            )
        if vehicle is None:
            vehicle = self._find_vehicle_by_vin(vin)

        operate_password = self._derive_operate_password(self.operation_password)
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
        _LOGGER.info(
            "Leapmotor remote verify response for %s: HTTP %s %s",
            action_label,
            verify_response["status_code"],
            verify_response["body"],
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
        _LOGGER.info(
            "Leapmotor remote ctl response for %s: HTTP %s %s",
            action_label,
            response["status_code"],
            response["body"],
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
        _LOGGER.info("Starting Leapmotor remote action %s for VIN %s", action_label, vin)
        if not self.token:
            self.login()

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
        _LOGGER.info(
            "Leapmotor remote ctl response for %s: HTTP %s %s",
            action_label,
            response["status_code"],
            response["body"],
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
        for vehicle in self.get_vehicle_list():
            if vehicle.vin == vin:
                return vehicle
        raise LeapmotorApiError(f"Vehicle not found for VIN {vin}")

    def _derive_operate_password(self, pin: str) -> str:
        """Derive operatePassword from the vehicle PIN using the current session token."""
        key_text, iv_text = self._derive_operpwd_key_iv()
        padder = padding.PKCS7(128).padder()
        padded = padder.update(pin.encode("utf-8")) + padder.finalize()
        cipher = Cipher(
            algorithms.AES(key_text.encode("utf-8")),
            modes.CBC(iv_text.encode("utf-8")),
        )
        encryptor = cipher.encryptor()
        ciphertext = encryptor.update(padded) + encryptor.finalize()
        return base64.b64encode(ciphertext).decode("ascii")

    def _derive_operpwd_key_iv(self) -> tuple[str, str]:
        """Mirror MD5Util.getEncryptPassword with token-derived AES key/IV."""
        if not self.token:
            return DEFAULT_OPERPWD_AES_KEY, DEFAULT_OPERPWD_AES_IV
        if len(self.token) < 64:
            raise LeapmotorAuthError("Access token is too short for operatePassword derivation.")
        key_source = self.token[:32]
        iv_source = self.token[32:64]
        key_text = hashlib.md5(key_source.encode("utf-8")).hexdigest()[8:24]
        iv_text = hashlib.md5(iv_source.encode("utf-8")).hexdigest()[8:24]
        return key_text, iv_text

    @staticmethod
    def _derive_session_device_id(token: str | None) -> str:
        """Extract the session deviceId from the JWT payload."""
        if not token:
            return DEFAULT_DEVICE_ID
        try:
            payload_b64 = token.split(".")[1]
            payload_b64 += "=" * (-len(payload_b64) % 4)
            payload = json.loads(base64.urlsafe_b64decode(payload_b64.encode("ascii")))
            user_name = str(payload.get("user_name") or "")
            parts = user_name.split(",")
            if len(parts) >= 4 and parts[2]:
                return parts[2]
        except Exception:  # noqa: BLE001
            pass
        return DEFAULT_DEVICE_ID

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
                DEFAULT_LANGUAGE,
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
            "acceptLanguage": DEFAULT_LANGUAGE,
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

    def _build_signed_headers(self, *, vin: str | None = None) -> dict[str, str]:
        nonce = str(random.randint(100000, 9999999))
        timestamp = str(int(time.time() * 1000))
        sign_input_parts = [
            DEFAULT_LANGUAGE,
            DEFAULT_CHANNEL,
            self.device_id,
            DEFAULT_DEVICE_TYPE,
            nonce,
            DEFAULT_SOURCE,
            timestamp,
            DEFAULT_APP_VERSION,
        ]
        if vin:
            sign_input_parts.append(vin)
        sign_input = "".join(sign_input_parts)
        return {
            "acceptLanguage": DEFAULT_LANGUAGE,
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

    def _build_consumption_weekly_rank_headers(self, *, carvin: str) -> dict[str, str]:
        """Build the signature variant used by getLastNweeks100kmECAndRank."""
        nonce = str(random.randint(100000, 9999999))
        timestamp = str(int(time.time() * 1000))
        sign_input = "".join(
            [
                DEFAULT_LANGUAGE,
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
                DEFAULT_LANGUAGE,
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
                DEFAULT_LANGUAGE,
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

    def _signed_header_dict(
        self,
        *,
        nonce: str,
        timestamp: str,
        sign_input: str,
    ) -> dict[str, str]:
        """Return common signed app headers for account-certificate requests."""
        return {
            "acceptLanguage": DEFAULT_LANGUAGE,
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
            f"{DEFAULT_LANGUAGE}"
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
            "acceptLanguage": DEFAULT_LANGUAGE,
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
            f"{DEFAULT_LANGUAGE}"
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
            "acceptLanguage": DEFAULT_LANGUAGE,
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
            f"{DEFAULT_LANGUAGE}"
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
            "acceptLanguage": DEFAULT_LANGUAGE,
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
            f"{DEFAULT_LANGUAGE}"
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
            "acceptLanguage": DEFAULT_LANGUAGE,
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
            f"{DEFAULT_LANGUAGE}"
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
            "acceptLanguage": DEFAULT_LANGUAGE,
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
            f"{DEFAULT_LANGUAGE}"
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
            "acceptLanguage": DEFAULT_LANGUAGE,
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

    def _post_with_curl(
        self,
        *,
        path: str,
        headers: dict[str, str],
        data: str,
        cert: tuple[str, str],
    ) -> dict[str, Any]:
        """Send a POST with curl, matching the verified reverse-engineered client."""
        url = f"{self.base_url}/{path.lstrip('/')}"
        with tempfile.NamedTemporaryFile() as header_file, tempfile.NamedTemporaryFile() as body_file:
            cmd = [
                "curl",
                "--silent",
                "--show-error",
                "--insecure",
                "-X",
                "POST",
                url,
                "-D",
                header_file.name,
                "-o",
                body_file.name,
                "--cert",
                cert[0],
                "--key",
                cert[1],
            ]
            for key, value in headers.items():
                cmd.extend(["-H", f"{key}: {value}"])
            cmd.extend(["--data", data])

            result = subprocess.run(cmd, capture_output=True, text=True, check=False)
            body_text = Path(body_file.name).read_text(encoding="utf-8", errors="replace")
            header_text = Path(header_file.name).read_text(encoding="utf-8", errors="replace")
            if result.returncode != 0:
                raise LeapmotorApiError(f"curl request failed: {result.stderr.strip()}")

        status_code = 0
        for line in header_text.splitlines():
            if line.startswith("HTTP/"):
                parts = line.split()
                if len(parts) >= 2 and parts[1].isdigit():
                    status_code = int(parts[1])
        return {"status_code": status_code, "body": body_text, "headers": header_text}

    def _post_binary_with_curl(
        self,
        *,
        path: str,
        headers: dict[str, str],
        data: str,
        cert: tuple[str, str],
    ) -> dict[str, Any]:
        """Send a POST with curl and return the raw response body bytes."""
        url = f"{self.base_url}/{path.lstrip('/')}"
        with tempfile.NamedTemporaryFile() as header_file, tempfile.NamedTemporaryFile() as body_file:
            cmd = [
                "curl",
                "--silent",
                "--show-error",
                "--insecure",
                "-X",
                "POST",
                url,
                "-D",
                header_file.name,
                "-o",
                body_file.name,
                "--cert",
                cert[0],
                "--key",
                cert[1],
            ]
            for key, value in headers.items():
                cmd.extend(["-H", f"{key}: {value}"])
            cmd.extend(["--data", data])

            result = subprocess.run(cmd, capture_output=True, text=True, check=False)
            body_bytes = Path(body_file.name).read_bytes()
            header_text = Path(header_file.name).read_text(encoding="utf-8", errors="replace")
            if result.returncode != 0:
                raise LeapmotorApiError(f"curl request failed: {result.stderr.strip()}")

        status_code = 0
        for line in header_text.splitlines():
            if line.startswith("HTTP/"):
                parts = line.split()
                if len(parts) >= 2 and parts[1].isdigit():
                    status_code = int(parts[1])
        return {"status_code": status_code, "body": body_bytes, "headers": header_text}


def normalize_vehicle(
    vehicle: Vehicle,
    status_json: dict[str, Any],
    user_id: str | None,
    *,
    mileage_json: dict[str, Any] | None = None,
    consumption_rank_json: dict[str, Any] | None = None,
    consumption_breakdown_json: dict[str, Any] | None = None,
    picture_json: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Normalize Leapmotor status payload into Home Assistant-friendly values."""
    status_data = status_json.get("data") or {}
    signal = status_data.get("signal") or {}
    config = status_data.get("config") or {}
    charge_plan = config.get("3") or {}
    mileage_data = (mileage_json or {}).get("data") or {}
    rank_data = (consumption_rank_json or {}).get("data") or {}
    rank_result = rank_data.get("rankResult") or {}
    weekly_ec = rank_data.get("weeklyEC") or []
    breakdown_data = (consumption_breakdown_json or {}).get("data") or {}
    picture_data = (picture_json or {}).get("data") or {}
    vehicle_state = _derive_vehicle_state(signal)
    tire_pressures = _tire_pressures_bar(vehicle.car_type, signal)
    last_7_days_energy = _sum_detail_field(mileage_data.get("detail"), "accumulatedEnergyConsume")
    last_week_split = _energy_breakdown_percentages(breakdown_data)

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
            "remaining_range_km": signal.get("3260"),
            "odometer_km": signal.get("1318"),
            "speed_kmh": _safe_float(signal.get("1319")),
            "gear": _gear_state(signal),
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
            "raw_charge_status_code": signal.get("1939"),
            "raw_ac_fan_mode_code": signal.get("1939"),
            "raw_charge_connection_code": signal.get("1149"),
            "raw_drive_status_code": signal.get("1941"),
            "raw_vehicle_state_code": signal.get("1944"),
            "raw_parked_status_code": signal.get("1298"),
            "interior_temp_c": signal.get("1349"),
            "climate_set_temp_left_c": signal.get("2183"),
            "climate_set_temp_right_c": signal.get("2184"),
            "last_vehicle_timestamp": signal.get("sts"),
        },
        "location": {
            "latitude": signal.get("3725", signal.get("2190")),
            "longitude": signal.get("3724", signal.get("2191")),
            "privacy_gps": status_data.get("privacyGPS"),
            "privacy_data": status_data.get("privacyData"),
            "last_vehicle_timestamp": signal.get("sts"),
        },
        "charging": {
            "is_charging": _is_charging(signal),
            "is_plugged_in": _is_plugged_in(signal),
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
            "charge_plug_signal": signal.get("47"),
            "raw_signal_47": signal.get("47"),
            "raw_signal_1149": signal.get("1149"),
            "remote_session_active": _one_is_on(signal.get("1256")) or _one_is_on(signal.get("1257")),
            "vehicle_security_active": _positive_int(signal.get("1255")),
            "on3_open": _one_is_on(signal.get("1258")),
            "driver_door_open": _one_is_on(signal.get("1277")),
            "passenger_door_open": _one_is_on(signal.get("1278")),
            "rear_left_door_open": _one_is_on(signal.get("1279")),
            "rear_right_door_open": _one_is_on(signal.get("1280")),
            "trunk_open": _one_is_on(signal.get("1281")),
            "ptc_power_w": _safe_int(signal.get("1348")),
            "parking_camera_state": signal.get("1480"),
            "battery_min_temp_c": _safe_int(signal.get("1182")),
            "battery_thermal_request": _safe_int(signal.get("1186")),
            "battery_heating": _safe_int(signal.get("1186")) == 4 if signal.get("1186") is not None else None,
            "front_left_window_open": _not_zero(signal.get("1693")),
            "front_right_window_open": _not_zero(signal.get("1694")),
            "rear_left_window_open": _not_zero(signal.get("1695")),
            "rear_right_window_open": _not_zero(signal.get("1696")),
            "skylight_open": _not_zero(signal.get("1724")),
            "front_left_window_position_percent": _safe_int(signal.get("3727")),
            "front_right_window_position_percent": _safe_int(signal.get("3728")),
            "rear_left_window_position_percent": _safe_int(signal.get("1879")),
            "rear_right_window_position_percent": _safe_int(signal.get("1880")),
            "climate_on": _one_is_on(signal.get("1938")),
            "climate_mode": _climate_mode(signal),
            "air_recirculation": _not_zero(signal.get("1943")),
            "fast_cooling_active": _two_is_on(signal.get("2669")),
            "fast_heating_active": _two_is_on(signal.get("2681")),
            "windshield_defrosting": _two_is_on(signal.get("1945")),
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
            "fully_charged": _one_is_on(signal.get("3736")),
            "speed_limit_enabled": _one_is_on(signal.get("12054")),
            "speed_limit_kmh": _safe_int(signal.get("6048")),
            "speed_limit_unit": signal.get("6047"),
            "tire_pressure_alarm_front_left": _safe_int(signal.get("2641")),
            "tire_pressure_alarm_front_right": _safe_int(signal.get("2648")),
            "tire_pressure_alarm_rear_left": _safe_int(signal.get("2655")),
            "tire_pressure_alarm_rear_right": _safe_int(signal.get("2662")),
            "raw_signal_1010": signal.get("1010"),
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
        },
        "raw_updated_at": time.time(),
    }


def _vehicle_status_car_type_path(car_type: str | None) -> str:
    """Return the backend status path segment for a vehicle model."""
    normalized = str(car_type or "C10").strip().lower()
    if normalized == "b10":
        # The international backend reports carType=B10 in the vehicle list,
        # but the status endpoint is shared with C10.
        return "c10"
    return normalized or "c10"


def _tire_pressures_bar(car_type: str | None, signal: dict[str, Any]) -> dict[str, float | None]:
    """Return model-specific tire-pressure slot mapping."""
    if str(car_type or "").strip().upper() == "B10":
        return {
            "tire_pressure_front_left_bar": _to_bar(signal.get("2646")),
            "tire_pressure_front_right_bar": _to_bar(signal.get("2653")),
            "tire_pressure_rear_left_bar": _to_bar(signal.get("2660")),
            "tire_pressure_rear_right_bar": _to_bar(signal.get("2667")),
        }
    return {
        "tire_pressure_front_left_bar": _to_bar(signal.get("2667")),
        "tire_pressure_front_right_bar": _to_bar(signal.get("2653")),
        "tire_pressure_rear_left_bar": _to_bar(signal.get("2646")),
        "tire_pressure_rear_right_bar": _to_bar(signal.get("2660")),
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
    """Return the movement state independent from charging state."""
    drive_status = _safe_int(signal.get("1941"))
    vehicle_state = _safe_int(signal.get("1944"))
    if drive_status in (1, 2, 4, 7) or vehicle_state in (1, 2, 4):
        return "parked"
    if drive_status in (3, 5) or vehicle_state in (5,):
        return "driving"

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
    if charging_current_a is not None and remaining_charge_minutes is not None:
        # Confirmed charging sessions show a clearly non-zero current
        # (typically negative while energy flows into the pack). Treat
        # sub-1A noise as idle / just plugged in.
        return abs(charging_current_a) >= 1.0

    charging_power_kw = _charging_power_kw(signal)
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


def _charging_connection_state(signal: dict[str, Any]) -> str | None:
    """Return the observed charge-connection state."""
    if _is_charging(signal):
        return "charging"
    connection_status = _safe_int(signal.get("1149"))
    if connection_status == 0:
        return "unplugged"
    if connection_status == 1:
        return "plugged_in"
    if connection_status == 2:
        return "charging"
    return None


def _climate_mode(signal: dict[str, Any]) -> str | None:
    mode = _safe_int(signal.get("3713"))
    return {
        0: "off",
        1: "fast_cool",
        3: "fast_heat",
        4: "quick_ventilation",
    }.get(mode)


def _charging_power_kw(signal: dict[str, Any]) -> float | None:
    """Return charging power without using GPS longitude-like signal 2191."""
    current = _safe_float(signal.get("1178"))
    voltage = _safe_float(signal.get("1177"))
    if current is None or voltage is None:
        return None
    # The C10 plugged-idle snapshot shows about 1.5A without active charging.
    if abs(current) < 3.0:
        return None
    return round(abs(current * voltage) / 1000.0, 3)
