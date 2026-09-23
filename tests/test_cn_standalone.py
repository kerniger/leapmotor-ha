"""Offline checks for the isolated CN backend in the unified integration."""
import ast
import importlib
import json
from pathlib import Path
import sys
import types
import unittest
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[1] / "custom_components/leapmotor/cn"
PACKAGE = "_standalone_cn_tests"
package = types.ModuleType(PACKAGE)
package.__path__ = [str(ROOT)]
sys.modules[PACKAGE] = package
client_module = importlib.import_module(f"{PACKAGE}.cn_api")


class StandaloneTests(unittest.TestCase):
    def test_vehicle_list_preserves_model_metadata_without_exposing_capabilities(self):
        client = client_module.CNReadOnlyApiClient({"deviceId": "test-device", "gateway": {
            "accountId": "test-account", "accessToken": "test-token", "signKeyBase64": "dGVzdA==",
        }})
        self.addCleanup(client.close)
        client._request = Mock(return_value={"data": {"bindcars": [{
            "vin": "SYNTHETIC", "modelParam": {"carType": "C11"}, "modelYear": "2025",
            "abilities": ["read_status", 7, "", "feature_x"],
        }], "sharedcars": []}})
        vehicle = client.get_vehicle_list()[0]
        self.assertEqual((vehicle.car_type, vehicle.model_year), ("C11", 2025))
        self.assertEqual(vehicle.capabilities, ("read_status", "feature_x"))

    def test_b05_expansion_signal_map_is_explicit(self):
        expected = {
            "100003": "preciseSoc", "3257": "electricRangeStandard", "1319": "speed",
            "1177": "batteryVoltage", "1178": "batteryCurrent", "1182": "minBatteryTemp",
            "1200": "chargeRemainTime", "2646": "leftFrontTirePressure",
            "2653": "rightFrontTirePressure", "2660": "leftRearTirePressure",
            "2667": "rightRearTirePressure",
        }
        self.assertEqual({key: client_module.SIGNAL_NAMES[key] for key in expected}, expected)

    def test_charge_input_and_completion_decoders_are_strict(self):
        self.assertEqual(client_module._explicit_bool(True), True)
        self.assertEqual(client_module._explicit_bool(0), False)
        self.assertEqual(client_module._explicit_bool("true"), True)
        for value in (2, -1, "yes", None):
            self.assertIsNone(client_module._explicit_bool(value))

    def test_charge_completion_uses_only_proven_charge_state(self):
        for value in (1, 1.0, " 1 "):
            self.assertEqual(client_module._charge_state(value), "charging")
            self.assertIs(client_module._charge_completed(value), False)
        for value in (2, 2.0, "2"):
            self.assertEqual(client_module._charge_state(value), "completed")
            self.assertIs(client_module._charge_completed(value), True)
        for code, state in ((0, "unplugged"), (3, "fault"), (4, "scheduled_waiting"), (6, "paused")):
            for value in (code, str(code)):
                self.assertEqual(client_module._charge_state(value), state)
                self.assertIs(client_module._charge_completed(value), False)
        for value in (True, False, None, [], {}, 5, 7, -1, 1.5, "charging", float("nan"), float("inf")):
            with self.subTest(value=value):
                self.assertIsNone(client_module._charge_state(value))
                self.assertIsNone(client_module._charge_completed(value))

    def test_status_model_range_doors_and_independent_completion(self):
        client = client_module.CNReadOnlyApiClient({"deviceId": "test-device", "gateway": {
            "accountId": "test-account", "accessToken": "test-token", "signKeyBase64": "dGVzdA==",
        }})
        self.addCleanup(client.close)
        for model, expected in (("T03", 123), ("C11", 234), ("B05", 234)):
            client._request = Mock(side_effect=[
                {"data": {"appRegion": "https://appgateway.leapmotor.com"}},
                {"data": {"collectTime": 1700000000000, "signalMap": {
                    "3260": 123, "2188": 234, "1277": 1, "1278": 0,
                    "1279": 2, "1280": None, "1149": 2, "3736": False,
                }}},
            ])
            result = client.get_vehicle_status(client_module.Vehicle(
                vin="SYNTHETIC", car_type=model, car_id=None, nickname=None, is_shared=False,
            ))
            status = result["status"]
            self.assertEqual(status["remaining_range_km"], expected)
            self.assertIs(status["driver_door_open"], True)
            self.assertIs(status["passenger_door_open"], False)
            self.assertIsNone(status["left_rear_door_open"])
            self.assertIsNone(status["right_rear_door_open"])
            self.assertIs(result["charging"]["charge_completed"], True)
            self.assertEqual(result["location"], {})

    def test_timestamp_formats_and_freshness_boundaries(self):
        freshness = importlib.import_module(f"{PACKAGE}.freshness")
        now = 1700000000
        for value in (now, str(now), now * 1000, "2023-11-14T22:13:20+00:00"):
            self.assertEqual(freshness.timestamp_seconds(value), now)
            self.assertTrue(freshness.is_fresh(value, now=now))
        for value in (True, None, [], {}, "nan", "inf", 0, -1, "2023-11-14T22:13:20", 10**1000):
            self.assertIsNone(freshness.timestamp_seconds(value))
        self.assertTrue(freshness.is_fresh(now - 900, now=now))
        self.assertFalse(freshness.is_fresh(now - 901, now=now))
        self.assertTrue(freshness.is_fresh(now + 60, now=now))
        self.assertFalse(freshness.is_fresh(now + 61, now=now))

    def test_proxy_failure_has_no_direct_retry_and_no_secret_in_error(self):
        proxy = "http://user:private-password@proxy.invalid:3128"
        transport = importlib.import_module(f"{PACKAGE}.transport")
        with patch.dict("os.environ", {"HTTPS_PROXY": "http://wrong.invalid:80", "NO_PROXY": "*"}):
            client = client_module.CNReadOnlyApiClient({"deviceId": "test-device", "gateway": {
                "accountId": "test-account", "accessToken": "test-token", "signKeyBase64": "dGVzdA==",
            }}, proxy)
        self.addCleanup(client.close)
        self.assertFalse(client.session.trust_env)
        settings = client.session.merge_environment_settings("https://appgateway.leapmotor.com", {}, None, None, None)
        self.assertEqual(settings["proxies"]["https"], proxy)
        with patch.object(client.session, "send", side_effect=client_module.requests.exceptions.ProxyError(proxy)) as send:
            with self.assertRaises(client_module.LeapmotorApiError) as error:
                client.fetch_data()
            send.assert_called_once()
        self.assertNotIn("private-password", str(error.exception))
        for value in ("socks5://proxy.invalid", "http://proxy.invalid:bad", "http://proxy.invalid/path", "http://bad host"):
            with self.assertRaises(ValueError):
                transport.create_session(value)

    def test_phone_transport_uses_same_proxy(self):
        auth = importlib.import_module(f"{PACKAGE}.cn_auth")
        proxy = "http://user:password@proxy.invalid:3128"
        captured = []
        def send(session, request, **kwargs):
            captured.append(kwargs["proxies"])
            return Mock(status_code=200, json=Mock(return_value={"result": 0}))
        with patch.object(auth.requests.Session, "send", send):
            auth.CNPhoneLogin("13800000000", proxy).send_sms()
        self.assertEqual(captured, [{"http": proxy, "https": proxy}])

    def test_shared_manifest_and_isolated_cn_imports(self):
        manifest = json.loads((ROOT.parent / "manifest.json").read_text())
        self.assertEqual(manifest["domain"], "leapmotor")
        self.assertEqual(manifest["name"], "Leapmotor")
        for path in ROOT.glob("*.py"):
            tree = ast.parse(path.read_text())
            for node in ast.walk(tree):
                if isinstance(node, ast.ImportFrom):
                    self.assertNotIn("custom_components.leapmotor.", node.module or "")
                    if node.level and node.module:
                        self.assertTrue((ROOT / (node.module.split(".")[0] + ".py")).exists())
        self.assertNotIn("control", (ROOT / "__init__.py").read_text())

    def test_session_snapshot_drops_unrelated_private_data(self):
        client = client_module.CNReadOnlyApiClient({
            "deviceId": "synthetic-device", "phone": "synthetic-phone", "smsCode": "synthetic-code",
            "gateway": {"accountId": "synthetic-account", "accessToken": "synthetic-token", "signKeyBase64": "dGVzdA==", "unrelated": "private"},
        })
        self.addCleanup(client.close)
        snapshot = client.session_snapshot()
        self.assertNotIn("phone", snapshot)
        self.assertNotIn("smsCode", snapshot)
        self.assertNotIn("unrelated", snapshot["gateway"])

    def test_missing_account_rejected(self):
        with self.assertRaises(client_module.LeapmotorAuthError):
            client_module.CNReadOnlyApiClient({"deviceId": "test-device", "gateway": {
                "accessToken": "test-token", "signKeyBase64": "dGVzdA==",
            }})

    def test_polling_reads_only(self):
        client = client_module.CNReadOnlyApiClient({"deviceId": "test-device", "gateway": {
            "accountId": "synthetic-account", "accessToken": "test-token", "signKeyBase64": "dGVzdA==",
        }})
        self.addCleanup(client.close)
        client.session.request = Mock(return_value=Mock(status_code=200, json=Mock(return_value={"code": 0, "data": {"bindcars": [], "sharedcars": []}})))
        self.assertEqual(client.fetch_data()["vehicles"], {})
        self.assertTrue(client.session.request.call_args.args[1].endswith("/vehicle/list"))


class DiagnosticsTests(unittest.IsolatedAsyncioTestCase):
    async def test_no_session_or_vehicle_identifiers(self):
        diagnostics = importlib.import_module(f"{PACKAGE}.diagnostics")
        coordinator = Mock(last_update_success=True, data={"vehicles": {"private-vin": {"token": "private-token"}}})
        hass = Mock(data={"leapmotor_cn": {"test-entry": coordinator}})
        result = await diagnostics.async_get_config_entry_diagnostics(hass, Mock(entry_id="test-entry"))
        self.assertEqual(result["vehicle_count"], 1)
        self.assertNotIn("private-vin", json.dumps(result))
        self.assertNotIn("private-token", json.dumps(result))


if __name__ == "__main__":
    unittest.main()
