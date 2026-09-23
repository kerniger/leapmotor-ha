"""Explicit phone/SMS authentication using the observed CN iOS protocol."""
from __future__ import annotations

import base64
import hashlib
import json
import re
import secrets
import time
from typing import Any

import requests
from cryptography.hazmat.primitives.asymmetric.padding import PKCS1v15
from cryptography.hazmat.primitives.serialization import load_der_public_key

from .models import LeapmotorApiError, LeapmotorAuthError
from .cn_api import CNReadOnlyApiClient, GATEWAY
from .transport import create_session, validate_proxy

LEGACY = "https://appuser.leapmotor.cn/app-user/applogin/"
PHONE_KEY = (
    "MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQDHUIQKhkwNqJFTZPe98mC1lmpbY9r/"
    "+7PEWZg8ebqYXT3sumKRaQ0zcoTx42x0iybmCRXy4CcZrgGAbwKzwqwNw0rFquJ6c7mgQA6"
    "k3lZU3p96qBlzK7DSkoFR6mO9pjcd2hlJ8wH+IwI5b8IWWZhwVN/4cM7npG0S0zeRn3soEwIDAQAB"
)


def normalize_phone(value: str) -> str:
    """Accept mainland numbers with an optional +86 or 0086 prefix."""
    value = value.strip().replace(" ", "").replace("-", "")
    for prefix in ("+86", "0086"):
        if value.startswith(prefix):
            value = value[len(prefix):]
            break
    if not re.fullmatch(r"1[0-9]{10}", value):
        raise ValueError("Invalid mainland phone number")
    return value


class CNPhoneLogin:
    """Keep credentials only in flow memory until the gateway session is saved."""

    def __init__(self, phone: str, proxy_url: str = "") -> None:
        self.phone = normalize_phone(phone)
        self._proxy_url = validate_proxy(proxy_url)
        self.device_id = secrets.token_hex(16)
        self._account: dict[str, Any] | None = None
        self._session: dict[str, Any] | None = None
        self._last_sms_attempt: float | None = None

    def _cipher(self) -> str:
        public_key = load_der_public_key(base64.b64decode(PHONE_KEY))
        return base64.urlsafe_b64encode(public_key.encrypt(self.phone.encode(), PKCS1v15())).decode().rstrip("=")

    def _legacy_headers(self) -> dict[str, str]:
        return {"APPPlatform": "iOS", "APPVersion": "1.22.87", "APPImei": self.device_id,
                "C-VERSIONS": "APP", "XFX-CDN-VRS": "v4",
                "User-Agent": "LeapControl/1789193213 CFNetwork/3896.100.1.2.1 Darwin/27.0.0"}

    def _request(self, method: str, url: str, **kwargs: Any) -> dict[str, Any]:
        try:
            with create_session(self._proxy_url) as session:
                response = session.request(method, url, timeout=25, allow_redirects=False, **kwargs)
            if response.status_code == 401:
                raise LeapmotorAuthError("CN authentication rejected.")
            if response.status_code != 200:
                raise LeapmotorApiError("CN authentication service unavailable.")
            body = response.json()
        except (requests.RequestException, ValueError):
            raise LeapmotorApiError("CN authentication connection or response failed.") from None
        if not isinstance(body, dict) or not any(field in body for field in ("code", "result", "status")):
            raise LeapmotorApiError("CN authentication response is invalid.")
        if body.get("success") is False or any(
            str(body[field]) not in ("0", "200") for field in ("code", "result", "status") if field in body
        ):
            raise LeapmotorAuthError("CN authentication rejected; no automatic retry.")
        return body

    def send_sms(self) -> None:
        """Called only after the separate explicit confirmation form is submitted."""
        now = time.monotonic()
        if self._last_sms_attempt is not None and now - self._last_sms_attempt < 60:
            raise LeapmotorApiError("Wait before explicitly requesting another SMS.")
        self._last_sms_attempt = now
        self._request("GET", LEGACY + "compliance/sendmessagecode",
                      headers=self._legacy_headers(), params={"phoneNo": self._cipher()})

    def login(self, code: str) -> dict[str, Any]:
        """Verify once, then exchange the legacy account token without sending SMS."""
        if self._session is not None:
            return self._session
        if self._account is None:
            if not re.fullmatch(r"[0-9]{4,8}", code):
                raise LeapmotorAuthError("Invalid SMS code format.")
            body = self._request("POST", LEGACY + "loginwithphone", headers=self._legacy_headers(),
                                 data={"phoneNoCiphertext": self._cipher(), "smsCode": code, "deviceID": self.device_id})
            self._account = self._find_account(body)
            if self._account is None:
                raise LeapmotorAuthError("CN login returned no account credentials.")
        payload = {"identifier": str(self._account["accountId"]), "identifierType": "1", "security": self._account["token"]}
        signed = {"acceptLanguage": "zh-CN", "channel": "1", "deviceId": self.device_id,
                  "deviceType": "ios", "nonce": str(secrets.randbelow(9_990_000) + 10_000),
                  "source": "leapmotor", "timestamp": str(int(time.time() * 1000)), "version": "1.22.87"}
        values = {**signed, **payload}
        signature = hashlib.sha256("".join(str(values[field]) for field in sorted(values)).encode()).hexdigest()
        headers = {**signed, "sign": signature, "digest": "", "userId": payload["identifier"],
                   "carvin": "", "cartype": "", "x-region": "CN", "x-api-signature-version": "2.0", "x-subversion": "3.19.2-2"}
        body = self._request("POST", GATEWAY + "/base/base-user/account/v1/login", headers=headers, json=payload)
        gateway, _ = CNReadOnlyApiClient.decode_gateway_response(body, payload["identifier"])
        encoded_claims = gateway["accessToken"].split(".")[1]
        claims = json.loads(base64.urlsafe_b64decode(encoded_claims + "=" * (-len(encoded_claims) % 4)))
        match = re.search(r"deviceId:([a-zA-Z0-9_-]+)", str(claims.get("user_name", "")))
        device_id = match[1] if match else self.device_id
        self._session = {"deviceId": device_id, "accountId": payload["identifier"], "gateway": gateway}
        return self._session

    @classmethod
    def _find_account(cls, value: Any) -> dict[str, Any] | None:
        if isinstance(value, dict):
            if isinstance(value.get("accountId"), (str, int)) and not isinstance(value.get("accountId"), bool) and value.get("accountId") and isinstance(value.get("token"), str) and value["token"]:
                return {"accountId": str(value["accountId"]), "token": value["token"]}
            for child in value.values():
                found = cls._find_account(child)
                if found is not None:
                    return found
        elif isinstance(value, list):
            for child in value:
                found = cls._find_account(child)
                if found is not None:
                    return found
        return None
