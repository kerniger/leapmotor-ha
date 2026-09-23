"""Offline tests of the HA CN client without importing the HA runtime."""
import importlib.util
import base64
import json
import time
from pathlib import Path
import sys
import types
import unittest
from unittest.mock import AsyncMock, Mock, patch

import requests


ROOT = Path(__file__).resolve().parents[1] / "custom_components" / "leapmotor" / "cn"
PACKAGE = "_cn_client_tests"
package = types.ModuleType(PACKAGE)
package.__path__ = [str(ROOT)]
sys.modules[PACKAGE] = package
spec = importlib.util.spec_from_file_location(f"{PACKAGE}.cn_api", ROOT / "cn_api.py")
client_module = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = client_module
spec.loader.exec_module(client_module)


def renewal_body():
    claims = base64.urlsafe_b64encode(json.dumps({"exp": time.time() + 3600}).encode()).decode().rstrip("=")
    return {"code": 0, "data": {
        "accessToken": f"header.{claims}.dGVzdA",
        "refreshToken": "new-refresh",
        "signParam": {"r2": "AAAAAA==", "r3": "AAAAAA=="},
    }}


class CNClientTests(unittest.TestCase):
    def setUp(self):
        self.client = client_module.CNReadOnlyApiClient({
            "deviceId": "test-device", "accountId": "test-account",
            "gateway": {"accessToken": "test-token", "signKeyBase64": "dGVzdA=="},
        })
        self.addCleanup(self.client.close)
        self.request = Mock()
        self.client.session.request = self.request

    def test_invalid_sessions(self):
        for state in (None, [], {}, {"gateway": []}, {"gateway": {}}):
            with self.subTest(state=state), self.assertRaises(client_module.LeapmotorAuthError):
                client_module.CNReadOnlyApiClient(state)

    def test_route_validation(self):
        self.assertEqual(client_module._route_origin("https://appgateway.leapmotor.com/"),
                         "https://appgateway.leapmotor.com")
        for route in (None, "http://appgateway.leapmotor.com", "https://evil.example",
                      "https://user@appgateway.leapmotor.com", "https://appgateway.leapmotor.com:443",
                      "https://appgateway.leapmotor.com?", "https://appgateway.leapmotor.com#token",
                      "https://appgateway.leapmotor.com/path", "https://[", "https://appgateway.\nleapmotor.com"):
            with self.subTest(route=route), self.assertRaises(client_module.LeapmotorApiError):
                client_module._route_origin(route)

    def test_transport_error_is_sanitized(self):
        self.request.side_effect = requests.ConnectionError("private-url-token")
        with self.assertRaises(client_module.LeapmotorApiError) as caught:
            self.client._request("GET", client_module.GATEWAY)
        self.assertNotIn("private-url-token", str(caught.exception))
        self.assertTrue(caught.exception.__suppress_context__)

    def test_http_errors_are_not_all_auth_failures(self):
        for status in (302, 403, 429, 500, 503):
            self.request.return_value = Mock(status_code=status)
            with self.subTest(status=status), self.assertRaises(client_module.LeapmotorApiError) as caught:
                self.client._request("GET", client_module.GATEWAY)
            self.assertNotIsInstance(caught.exception, client_module.LeapmotorAuthError)
        self.request.return_value = Mock(status_code=401)
        with self.assertRaises(client_module.LeapmotorAuthError):
            self.client._request("GET", client_module.GATEWAY)

    def test_response_schema(self):
        for body in ([], {}, {"code": False, "data": {}}, {"code": 0, "data": []}, {"code": 123}):
            self.request.return_value = Mock(status_code=200, json=Mock(return_value=body))
            with self.subTest(body=body), self.assertRaises(client_module.LeapmotorApiError):
                self.client._request("GET", client_module.GATEWAY)

    def test_success_disables_redirects(self):
        body = {"code": 0, "data": {}}
        self.request.return_value = Mock(status_code=200, json=Mock(return_value=body))
        self.assertEqual(self.client._request("GET", client_module.GATEWAY), body)
        self.assertFalse(self.request.call_args.kwargs["allow_redirects"])

    def test_expiry_response_is_auth_failure(self):
        self.request.return_value = Mock(status_code=200, json=Mock(return_value={"code": 302002004}))
        with self.assertRaises(client_module.LeapmotorAuthError):
            self.client.fetch_data()
        self.assertEqual(self.request.call_count, 1)

    def test_renewal_requires_save_before_reads(self):
        self.client._gateway["refreshToken"] = "old-refresh"
        self.request.return_value = Mock(status_code=200, json=Mock(return_value=renewal_body()))
        self.client.refresh_session()
        self.assertTrue(self.client.session_pending_save)
        snapshot = self.client.session_snapshot()
        self.assertEqual(snapshot["gateway"]["refreshToken"], "new-refresh")
        self.assertEqual(snapshot["gateway"]["signKeyBase64"], "dGVzdA==")
        self.assertEqual(self.request.call_args.args[1], client_module.GATEWAY + "/base/base-user/token/v1/refresh")
        with self.assertRaises(client_module.LeapmotorApiError):
            self.client.fetch_data()
        with self.assertRaises(client_module.LeapmotorApiError):
            self.client.refresh_session()
        self.assertEqual(self.request.call_count, 1)
        restored = client_module.CNReadOnlyApiClient(snapshot)
        self.addCleanup(restored.close)
        self.assertEqual(restored._key, self.client._key)

    def test_missing_refresh_token_never_requests_login(self):
        with self.assertRaises(client_module.LeapmotorAuthError):
            self.client.refresh_session()
        self.request.assert_not_called()

    def test_malformed_renewal_preserves_previous_state(self):
        self.client._gateway["refreshToken"] = "old-refresh"
        original = self.client.session_snapshot()
        for data in ({}, {"accessToken": "bad", "refreshToken": "new"}):
            self.request.return_value = Mock(status_code=200, json=Mock(return_value={"code": 0, "data": data}))
            with self.assertRaises(client_module.LeapmotorApiError):
                self.client.refresh_session()
            self.assertEqual(self.client.session_snapshot(), original)


class CNCoordinatorTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        core = types.ModuleType("homeassistant.core")
        core.HomeAssistant = object
        exceptions = types.ModuleType("homeassistant.exceptions")
        exceptions.ConfigEntryAuthFailed = type("ConfigEntryAuthFailed", (Exception,), {})
        update = types.ModuleType("homeassistant.helpers.update_coordinator")
        update.DataUpdateCoordinator = type("DataUpdateCoordinator", (), {
            "__class_getitem__": classmethod(lambda cls, item: cls),
        })
        update.UpdateFailed = type("UpdateFailed", (Exception,), {})
        spec = importlib.util.spec_from_file_location(f"{PACKAGE}.coordinator", ROOT / "coordinator.py")
        module = importlib.util.module_from_spec(spec)
        with patch.dict(sys.modules, {"homeassistant.core": core,
                                    "homeassistant.exceptions": exceptions,
                                    "homeassistant.helpers.update_coordinator": update}):
            spec.loader.exec_module(module)
        self.update_failed = module.UpdateFailed
        self.coordinator = module.CNCoordinator.__new__(module.CNCoordinator)
        self.client = client_module.CNReadOnlyApiClient({
            "deviceId": "test-device", "accountId": "test-account",
            "gateway": {"accessToken": "test", "refreshToken": "old", "signKeyBase64": "dGVzdA=="},
        })
        self.addCleanup(self.client.close)
        self.coordinator.client = self.client
        self.events = []

        async def execute(function):
            return function()

        async def save(snapshot):
            self.events.append("save")

        self.coordinator.hass = Mock(async_add_executor_job=execute)
        self.coordinator.store = Mock(async_save=AsyncMock(side_effect=save))

    async def test_refresh_persist_read_order(self):
        def fetch():
            self.events.append("read")
            if self.events == ["read"]:
                raise client_module.LeapmotorAuthError("expired")
            return {"vehicles": {}}

        def renew():
            self.events.append("renew")
            self.client.session_pending_save = True

        self.client.fetch_data = Mock(side_effect=fetch)
        self.client.refresh_session = Mock(side_effect=renew)
        await self.coordinator._async_update_data()
        self.assertEqual(self.events, ["read", "renew", "save", "read"])

    async def test_storage_failure_pauses_reads_and_retries_save(self):
        self.client.session_pending_save = True
        self.client.fetch_data = Mock(return_value={})
        self.client.refresh_session = Mock()
        self.coordinator.store.async_save.side_effect = OSError("disk failure")
        with self.assertRaises(self.update_failed):
            await self.coordinator._async_update_data()
        self.assertTrue(self.client.session_pending_save)
        self.client.fetch_data.assert_not_called()
        self.coordinator.store.async_save.side_effect = None
        await self.coordinator._async_update_data()
        self.assertFalse(self.client.session_pending_save)
        self.client.refresh_session.assert_not_called()

    async def test_network_failure_never_rotates(self):
        self.client.fetch_data = Mock(side_effect=client_module.LeapmotorApiError("offline"))
        self.client.refresh_session = Mock()
        with self.assertRaises(Exception):
            await self.coordinator._async_update_data()
        self.client.refresh_session.assert_not_called()

    async def test_only_one_rotation_per_poll(self):
        self.client.fetch_data = Mock(side_effect=client_module.LeapmotorAuthError("expired"))
        self.client.refresh_session = Mock()
        with self.assertRaises(Exception):
            await self.coordinator._async_update_data()
        self.client.refresh_session.assert_called_once()


if __name__ == "__main__":
    unittest.main()
