"""Read-only CN Leapmotor gateway client."""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import math
import secrets
import time
from typing import Any
from urllib.parse import urlsplit

import requests

from .models import LeapmotorApiError, LeapmotorAuthError, Vehicle
from .transport import create_session

GATEWAY = "https://app-gw-global-master.leapmotor.com"
SIGNAL_NAMES = {
    "1204": "soc", "100003": "preciseSoc", "2188": "liveRemainingRange",
    "3257": "electricRangeStandard", "1318": "totalMileage", "1319": "speed",
    "1349": "interiorTemp", "1177": "batteryVoltage", "1178": "batteryCurrent",
    "1182": "minBatteryTemp", "1200": "chargeRemainTime", "1149": "chargeState",
    "1298": "driverDoorLockStatus", "3260": "expectedMileage",
    "1277": "lbcmDriverDoorStatus", "1278": "rbcmDriverDoorStatus",
    "1279": "lbcmLeftRearDoorStatus", "1280": "rbcmRightRearDoorStatus",
    "2183": "acSetting", "3636": "sentryMode",
    "1480": "parkingBrakeState", "47": "acInputSlowCharge", "1197": "dcInputFastCharge",
    "3736": "chargeCompleted", "48": "healthyChargeEnabled",
    "2646": "leftFrontTirePressure", "2653": "rightFrontTirePressure",
    "2660": "leftRearTirePressure", "2667": "rightRearTirePressure",
}


class CNReadOnlyApiClient:
    """Use an imported CN gateway session for read-only polling only."""

    def __init__(self, session_data: dict[str, Any], proxy_url: str = "") -> None:
        if not isinstance(session_data, dict) or not isinstance(session_data.get("gateway"), dict):
            raise LeapmotorAuthError("CN session must contain a gateway object.")
        self._state = dict(session_data)
        self._gateway = dict(session_data.get("gateway") or {})
        self._key = self._decode_key()
        self.user_id = str(self._gateway.get("accountId") or self._state.get("accountId") or "")
        if not self.user_id or type(self._gateway.get("accountId") or self._state.get("accountId")) not in (str, int):
            raise LeapmotorAuthError("CN session has no valid account identity.")
        self.session = create_session(proxy_url)
        self.session_pending_save = False

    def session_snapshot(self) -> dict[str, Any]:
        """Return only fields required to resume the gateway session."""
        return {
            "deviceId": self._state["deviceId"],
            "accountId": self.user_id,
            "gateway": {
                field: self._gateway[field]
                for field in ("accountId", "accessToken", "refreshToken", "signKeyBase64", "expiresAt")
                if field in self._gateway
            },
        }

    def refresh_session(self) -> None:
        """Rotate gateway credentials once; never request SMS or legacy login."""
        if self.session_pending_save:
            raise LeapmotorApiError("CN credentials must be saved before another renewal.")
        refresh_token = self._gateway.get("refreshToken")
        if not isinstance(refresh_token, str) or not refresh_token:
            raise LeapmotorAuthError("CN gateway refresh token is missing.")
        body = self._request("POST", GATEWAY + "/base/base-user/token/v1/refresh",
                             {"refreshToken": refresh_token})
        self._gateway, self._key = self.decode_gateway_response(body, self.user_id)
        self.session_pending_save = True

    @staticmethod
    def decode_gateway_response(body: dict[str, Any], account_id: str) -> tuple[dict[str, Any], bytes]:
        """Decode the common login and renewal gateway credential envelope."""
        try:
            data = body["data"]
            access_token = data["accessToken"]
            renewed_refresh_token = data["refreshToken"]
            if not isinstance(renewed_refresh_token, str) or not renewed_refresh_token:
                raise ValueError
            parts = access_token.split(".")
            if len(parts) != 3:
                raise ValueError
            signature = base64.b64decode(parts[2] + "=" * (-len(parts[2]) % 4), altchars=b"-_", validate=True)
            component_two = base64.b64decode(data["signParam"]["r2"], validate=True)
            component_three = base64.b64decode(data["signParam"]["r3"], validate=True)
            if not signature or not len(signature) == len(component_two) == len(component_three):
                raise ValueError
            key = bytes(first ^ second ^ third for first, second, third in zip(signature, component_two, component_three))
            claims = json.loads(base64.b64decode(parts[1] + "=" * (-len(parts[1]) % 4), altchars=b"-_", validate=True))
            expiry = float(claims["exp"])
            if not math.isfinite(expiry) or expiry <= time.time():
                raise ValueError
        except (KeyError, TypeError, ValueError, AttributeError):
            raise LeapmotorApiError("CN gateway returned invalid credentials.") from None
        gateway = {
            "accountId": account_id,
            "accessToken": access_token,
            "refreshToken": renewed_refresh_token,
            "signKeyBase64": base64.b64encode(key).decode(),
            "expiresAt": expiry,
        }
        return gateway, key

    def close(self) -> None:
        self.session.close()

    def _decode_key(self) -> bytes:
        try:
            key = base64.b64decode(self._gateway["signKeyBase64"], validate=True)
            if not key or any(
                not isinstance(value, str) or not value.strip()
                for value in (self._gateway["accessToken"], self._state["deviceId"])
            ):
                raise ValueError
            return key
        except (KeyError, TypeError, ValueError):
            raise LeapmotorAuthError("CN session is incomplete; import a current saved session.") from None

    def fetch_data(self) -> dict[str, Any]:
        if self.session_pending_save:
            raise LeapmotorApiError("CN credentials must be saved before polling.")
        vehicles = self.get_vehicle_list()
        result: dict[str, Any] = {"user_id": self.user_id, "vehicles": {}}
        for vehicle in vehicles:
            result["vehicles"][vehicle.vin] = self.get_vehicle_status(vehicle)
        return result

    def get_vehicle_list(self) -> list[Vehicle]:
        body = self._request("GET", GATEWAY + "/app/app-global-service/v1/vehicle/list")
        data = body.get("data") or {}
        vehicles: list[Vehicle] = []
        seen = set()
        for bucket, is_shared in (("bindcars", False), ("sharedcars", True)):
            items = data.get(bucket, [])
            if not isinstance(items, list):
                raise LeapmotorApiError("CN vehicle list has an invalid structure.")
            for item in items:
                if isinstance(item, dict) and item.get("vin"):
                    if str(item["vin"]) in seen:
                        continue
                    seen.add(str(item["vin"]))
                    model_param = item.get("modelParam") if isinstance(item.get("modelParam"), dict) else {}
                    car_type = item.get("carType") or item.get("cartype") or model_param.get("carType") or item.get("model") or "unknown"
                    abilities = item.get("abilities")
                    capabilities = tuple(value for value in abilities if isinstance(value, str) and value.strip()) if isinstance(abilities, list) else ()
                    vehicles.append(Vehicle(vin=str(item["vin"]), car_id=str(item["carId"]) if item.get("carId") is not None else None,
                                            car_type=str(car_type), nickname=item.get("nickName"), is_shared=is_shared,
                                            model_year=_optional_year(item.get("year") or item.get("modelYear") or item.get("modelyear")),
                                            capabilities=capabilities))
        return vehicles

    def get_vehicle_status(self, vehicle: Vehicle) -> dict[str, Any]:
        route = self._request("GET", GATEWAY + "/app/app-global-service/v1/vehicle/getCarRoute", {"vin": vehicle.vin}, vehicle)
        origin = _route_origin(((route.get("data") or {}).get("appRegion")))
        payload = {"vin": vehicle.vin, "appVersion": "1.22.87", "isMainApp": "1", "osType": "iOS"}
        body = self._request("POST", origin + "/app/app-signal-service/signal/info/query", payload, vehicle)
        data = body.get("data") or {}
        raw = data.get("signalMap") or {}
        if not isinstance(raw, dict):
            raise LeapmotorApiError("CN signal response has an invalid structure.")
        named = {SIGNAL_NAMES.get(str(key), str(key)): value for key, value in raw.items()}
        return {"vehicle": {"vin": vehicle.vin, "user_id": self.user_id, "car_id": vehicle.car_id, "car_type": vehicle.car_type, "model_year": vehicle.model_year, "nickname": vehicle.nickname, "is_shared": vehicle.is_shared},
                "status": {
                    "battery_percent": named.get("soc"),
                    "precise_battery_percent": named.get("preciseSoc"),
                    "remaining_range_km": named.get("expectedMileage" if "T03" in vehicle.car_type.upper() else "liveRemainingRange"),
                    "standard_range_km": named.get("electricRangeStandard"),
                    "odometer_km": named.get("totalMileage"),
                    "speed_kmh": named.get("speed"),
                    "interior_temp_c": named.get("interiorTemp"),
                    "battery_voltage_v": named.get("batteryVoltage"),
                    "battery_current_a": named.get("batteryCurrent"),
                    "minimum_battery_temp_c": named.get("minBatteryTemp"),
                    "remaining_charge_time_min": named.get("chargeRemainTime"),
                    "climate_set_temp_left_c": named.get("acSetting"),
                    "charge_state": _charge_state(named.get("chargeState")),
                    "sentry_enabled": _explicit_bool(named.get("sentryMode")),
                    "driver_door_open": _explicit_bool(named.get("lbcmDriverDoorStatus")),
                    "passenger_door_open": _explicit_bool(named.get("rbcmDriverDoorStatus")),
                    "left_rear_door_open": _explicit_bool(named.get("lbcmLeftRearDoorStatus")),
                    "right_rear_door_open": _explicit_bool(named.get("rbcmRightRearDoorStatus")),
                    "left_front_tire_pressure_kpa": named.get("leftFrontTirePressure"),
                    "right_front_tire_pressure_kpa": named.get("rightFrontTirePressure"),
                    "left_rear_tire_pressure_kpa": named.get("leftRearTirePressure"),
                    "right_rear_tire_pressure_kpa": named.get("rightRearTirePressure"),
                    "raw_charge_status_code": named.get("chargeState"),
                    "last_vehicle_timestamp": data.get("collectTime"),
                },
                # AC/DC inputs are connection flags, not proof of charging.
                # Completion comes from proven chargeState=2; the name of
                # signal 3736 alone does not establish its code semantics.
                "charging": {
                    "ac_input": _explicit_bool(named.get("acInputSlowCharge")),
                    "dc_input": _explicit_bool(named.get("dcInputFastCharge")),
                    "charge_completed": _charge_completed(named.get("chargeState")),
                },
                "diagnostics": {}, "location": {}, "cn_raw_signal_names": sorted(named), "raw_updated_at": time.time()}

    def _request(self, method: str, url: str, params: dict[str, Any] | None = None, vehicle: Vehicle | None = None) -> dict[str, Any]:
        params = params or {}
        try:
            response = self.session.request(method, url, headers=self._headers(params, vehicle), params=params if method == "GET" else None, json=params if method == "POST" else None, timeout=25, allow_redirects=False)
        except requests.RequestException:
            raise LeapmotorApiError("CN gateway connection failed.") from None
        if response.status_code == 401:
            raise LeapmotorAuthError("CN session was rejected; reauthentication is required.")
        if response.status_code != 200:
            raise LeapmotorApiError(f"CN gateway returned HTTP {response.status_code}.")
        try:
            body = response.json()
        except ValueError:
            raise LeapmotorApiError("CN gateway returned non-JSON data.") from None
        if isinstance(body, dict) and body.get("code") == 302002004:
            raise LeapmotorAuthError("CN gateway token has expired.")
        if not isinstance(body, dict) or type(body.get("code")) is not int or body["code"] != 0:
            raise LeapmotorApiError("CN gateway returned an unsuccessful or invalid response.")
        if not isinstance(body.get("data"), dict):
            raise LeapmotorApiError("CN gateway returned an invalid data object.")
        return body

    def _headers(self, params: dict[str, Any], vehicle: Vehicle | None) -> dict[str, str]:
        signed = {"acceptLanguage": "zh-CN", "channel": "1", "deviceId": str(self._state["deviceId"]), "deviceType": "ios", "nonce": str(secrets.randbelow(9_990_000) + 10_000), "source": "leapmotor", "timestamp": str(int(time.time() * 1000)), "version": "1.22.87"}
        canonical = "".join(str({**signed, **params}[key]) for key in sorted({**signed, **params}))
        return {**signed, "sign": hmac.new(self._key, canonical.encode(), hashlib.sha256).hexdigest(), "digest": "", "userId": self.user_id, "carvin": vehicle.vin if vehicle else "", "cartype": vehicle.car_type if vehicle else "", "token": str(self._gateway["accessToken"]), "x-region": "CN", "x-api-signature-version": "2.0", "x-subversion": "3.19.2-2"}


def _route_origin(route: Any) -> str:
    if not isinstance(route, str) or any(character.isspace() for character in route):
        raise LeapmotorApiError("CN vehicle route is not recognized.")
    try:
        parsed = urlsplit(route)
        valid = (
            parsed.scheme == "https"
            and parsed.path in ("", "/")
            and parsed.hostname
            and (parsed.hostname.endswith(".leapmotor.com") or parsed.hostname.endswith(".leapmotor.cn"))
            and parsed.netloc.lower() == parsed.hostname
            and not parsed.query
            and not parsed.fragment
            and "?" not in route
            and "#" not in route
        )
    except ValueError:
        valid = False
    if not valid:
        raise LeapmotorApiError("CN vehicle route is not recognized.")
    return "https://" + parsed.hostname


def _optional_year(value: Any) -> int | None:
    """Accept only a conventional model year; malformed values stay unknown."""
    if isinstance(value, bool):
        return None
    try:
        year = int(str(value).strip())
    except (TypeError, ValueError):
        return None
    return year if 1900 <= year <= 2100 else None


CHARGE_STATES = {
    0: "unplugged", 1: "charging", 2: "completed", 3: "fault",
    4: "scheduled_waiting", 6: "paused",
}


def _charge_state(value: Any) -> str | None:
    if isinstance(value, bool) or not isinstance(value, (str, int, float)):
        return None
    if isinstance(value, str):
        return {str(code): state for code, state in CHARGE_STATES.items()}.get(value.strip())
    return CHARGE_STATES.get(value)


def _charge_completed(value: Any) -> bool | None:
    state = _charge_state(value)
    return None if state is None else state == "completed"


def _explicit_bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if type(value) in (int, float):
        return {0: False, 1: True}.get(value)
    if isinstance(value, str):
        return {"0": False, "false": False, "1": True, "true": True}.get(value.strip().lower())
    return None
