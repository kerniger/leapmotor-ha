"""Offline API regressions for reported charging and history faults."""
import json
import unittest
from unittest.mock import Mock

from test_status_path_fallback import api


class ChargingTests(unittest.TestCase):
    def setUp(self):
        self.client = api.LeapmotorApiClient(username="synthetic", password="synthetic")
        self.addCleanup(self.client.close)
        self.client._find_vehicle_by_vin = Mock(
            return_value=api.Vehicle("SYNTHETIC", None, "T03", None, False)
        )
        self.client.get_vehicle_status = Mock()
        self.client.get_charge_schedule = Mock(return_value={})
        self.client._remote_control_raw = Mock(return_value={"code": 0})

    def test_t03_writes_rejected_before_status_or_command(self):
        for model in ("T03", " t03 "):
            self.client._find_vehicle_by_vin.return_value.car_type = model
            for method, value in ((self.client.set_charge_limit, 80),
                                  (self.client.set_charging_plan_enabled, True),
                                  (self.client.set_charging_plan_enabled, False)):
                with self.subTest(model=model, method=method.__name__, value=value):
                    with self.assertRaisesRegex(api.LeapmotorApiError, "not supported.*T03"):
                        method("SYNTHETIC", value)
        self.client.get_vehicle_status.assert_not_called()
        self.client.get_charge_schedule.assert_not_called()
        self.client._remote_control_raw.assert_not_called()

    def test_complete_plan_preserved_for_supported_models(self):
        plan = {"isEnable": "0", "percent": 95, "beginTime": "00:00",
                "endTime": "06:00", "cycles": "1,3,5", "circulation": 1, "recharge": 1}
        self.client.get_vehicle_status.return_value = {"data": {"config": {"3": plan}}}
        for model in ("B10", "C10", "B05"):
            self.client._find_vehicle_by_vin.return_value.car_type = model
            self.client.set_charge_limit("SYNTHETIC", 80)
            payload = json.loads(self.client._remote_control_raw.call_args.kwargs["cmd_content"])
            self.assertEqual(payload, {"chargeEnable": 0, "chargesoc": 80,
                "starttime": "00:00", "endtime": "06:00", "cycles": "1,3,5",
                "circulation": 1, "recharge": 1})
            self.client.set_charging_plan_enabled("SYNTHETIC", True)
            payload = json.loads(self.client._remote_control_raw.call_args.kwargs["cmd_content"])
            self.assertEqual((payload["chargeEnable"], payload["chargesoc"]), (1, 95))
        self.client.get_charge_schedule.assert_not_called()

    def test_incomplete_plan_never_invents_schedule(self):
        self.client._find_vehicle_by_vin.return_value.car_type = "B10"
        complete = {"isEnable": 0, "percent": 95, "beginTime": "00:00",
                    "endTime": "06:00", "cycles": "1,3,5"}
        for missing in complete:
            plan = {key: value for key, value in complete.items() if key != missing}
            self.client.get_vehicle_status.return_value = {"data": {"config": {"3": plan}}}
            with self.subTest(missing=missing):
                for method, value in ((self.client.set_charge_limit, 80),
                                      (self.client.set_charging_plan_enabled, True)):
                    with self.assertRaisesRegex(api.LeapmotorApiError, "incomplete"):
                        method("SYNTHETIC", value)
        self.client._remote_control_raw.assert_not_called()

    def test_appointment_fills_missing_fields(self):
        self.client._find_vehicle_by_vin.return_value.car_type = "C10"
        self.client.get_vehicle_status.return_value = {"data": {"config": {"3": {"percent": 90}}}}
        self.client.get_charge_schedule.return_value = {"chargeEnable": 1, "chargesoc": 85,
            "starttime": "00:00", "endtime": "08:00", "cycles": "1,2", "circulation": 1}
        self.client.set_charging_plan_enabled("SYNTHETIC", False)
        payload = json.loads(self.client._remote_control_raw.call_args.kwargs["cmd_content"])
        self.assertEqual((payload["chargeEnable"], payload["chargesoc"], payload["starttime"]), (0, 90, "00:00"))


class NormalizationTests(unittest.TestCase):
    def normalize(self, model, data, **kwargs):
        return api.normalize_vehicle(api.Vehicle("SYNTHETIC", None, model, None, False),
                                     {"data": data}, None, **kwargs)

    def test_flat_t03_status_preserves_limit_without_claiming_schedule(self):
        result = self.normalize("T03", {"chargesocSetting": 90, "chargeTimeSetting": "00:00"})
        self.assertEqual(result["charging"]["charge_limit_percent"], 90)
        self.assertIsNone(result["charging"]["charging_planned_enabled"])
        self.assertIsNone(result["charging"]["charging_planned_start"])
        self.assertEqual(result["diagnostics"]["charging_plan_start_raw"], "00:00")

    def test_midnight_is_valid_when_enabled_state_exists(self):
        for enabled in (0, 1, "0", "1"):
            result = self.normalize("B10", {"config": {"3": {"isEnable": enabled, "beginTime": "00:00"}}})
            self.assertEqual(result["charging"]["charging_planned_enabled"], int(enabled))
            self.assertEqual(result["charging"]["charging_planned_start"], "00:00")

    def test_t03_daily_energy_uses_model_specific_wh_conversion(self):
        for value, distance in ((2744, 22), (11912, 96), (0, 0)):
            mileage = {"data": {"totalAccumulatedMileage": distance, "detail": [
                {"accumulatedMileage": distance, "accumulatedEnergyConsume": value}]}}
            history = self.normalize(" t03 ", {}, mileage_json=mileage)["history"]
            self.assertTrue(history["last_7_days_energy_complete"])
            self.assertEqual(history["last_7_days_energy_kwh"], round(value / 1000, 1))
            self.assertIsNone(history["last_7_days_energy_unavailable_reason"])
            self.assertEqual(history["last_7_days_energy_unit"], "kWh")
            self.assertEqual(history["last_7_days_energy_source_unit"], "Wh")
            self.assertEqual(history["last_7_days_detail"][0]["energy_raw"], value)
            self.assertEqual(history["last_7_days_detail"][0]["energy_kwh"], value / 1000)
            self.assertEqual(history["last_7_days_detail"][0]["driving_energy_kwh"], value / 1000)
            self.assertEqual(history["last_7_days_mileage_km"], distance)

    def test_b10_energy_keeps_original_scale_and_precision(self):
        history = self.normalize("B10", {}, mileage_json={"data": {
            "totalAccumulatedMileage": 22, "detail": [
                {"accumulatedMileage": 22, "accumulatedEnergyConsume": 2.75}]}})["history"]
        self.assertEqual(history["last_7_days_detail"][0]["energy_kwh"], 2.75)
        self.assertEqual(history["last_7_days_energy_unit"], "kWh")

    def test_real_zero_soc_is_not_discarded(self):
        result = self.normalize("T03", {"signal": {"1204": 0}})
        self.assertEqual(result["status"]["battery_percent"], 0)

    def test_ac_mode_only_decodes_known_values(self):
        for value, expected in ((0, "auto"), (1, "manual"), (17, None), (None, None)):
            result = self.normalize("B10", {"signal": {"1939": value}})
            self.assertEqual(result["diagnostics"]["ac_operation_mode"], expected)


if __name__ == "__main__":
    unittest.main()


class OctoberChargingTests(unittest.TestCase):
    def normalize(self, model="T03", **signals):
        return api.normalize_vehicle(api.Vehicle("SYNTHETIC", None, model, None, False),
                                     {"data": {"signal": signals}}, None)["charging"]

    def test_t03_wallbox_pause_is_connected_idle_without_claiming_dc(self):
        result = self.normalize(**{"47": 0, "1149": 0, "1178": 0, "1197": 1, "1200": 0})
        self.assertTrue(result["is_plugged_in"])
        self.assertEqual(result["connection_state"], "plugged_in")
        self.assertFalse(result["is_charging"])
        self.assertIsNone(result["dc_cable_connected"])

    def test_pause_exception_requires_every_reported_field(self):
        baseline = {"1149": 0, "1178": 0, "1197": 1, "1200": 0}
        for key, bad_value in (("1197", 0), ("1197", 2), ("1178", 4), ("1200", 10),
                               ("1010", 1), ("1319", 50)):
            with self.subTest(key=key, value=bad_value):
                self.assertFalse(self.normalize(**{**baseline, key: bad_value})["is_plugged_in"])
        for key in baseline:
            with self.subTest(missing=key):
                self.assertIsNot(self.normalize(**{k: v for k, v in baseline.items() if k != key})["is_plugged_in"], True)

    def test_other_models_keep_authoritative_connection_and_dc_state(self):
        for model in ("B10", "C10", "B05"):
            result = self.normalize(model, **{"1149": 0, "1178": 0, "1197": 1, "1200": 0})
            self.assertFalse(result["is_plugged_in"])
            self.assertEqual(result["connection_state"], "unplugged")
            self.assertTrue(result["dc_cable_connected"])

    def test_t03_unplug_pause_resume_sequence(self):
        for signals, expected in (
            ({"1149": 0, "1197": 0, "1178": 0, "1200": 0}, (False, False, "unplugged")),
            ({"1149": 2, "1197": 0, "1178": -8, "1200": 120}, (True, True, "charging")),
            ({"1149": 0, "1197": 1, "1178": 0, "1200": 0}, (True, False, "plugged_in")),
            ({"1149": 2, "1197": 0, "1178": -8, "1200": 60}, (True, True, "charging")),
            ({"1149": 0, "1197": 0, "1178": 0, "1200": 0}, (False, False, "unplugged")),
        ):
            result = self.normalize(**signals)
            self.assertEqual(tuple(result[k] for k in ("is_plugged_in", "is_charging", "connection_state")), expected)

    def test_login_service_errors_do_not_ask_for_a_new_password(self):
        client = api.LeapmotorApiClient(username="synthetic", password="synthetic")
        self.addCleanup(client.close)
        for http, code in ((503, 302010108), (429, 1), (200, 17)):
            with self.subTest(http=http, code=code):
                with self.assertRaises(api.LeapmotorApiError) as raised:
                    client._parse_api_body(http, json.dumps({"code": code}), "login")
                self.assertNotIsInstance(raised.exception, api.LeapmotorAuthError)
        with self.assertRaises(api.LeapmotorAuthError):
            client._parse_api_body(200, json.dumps({"code": 302010108}), "login")


class OctoberHistoryIntegrationTests(unittest.TestCase):
    def test_client_passes_configured_zone_to_normalization(self):
        client = api.LeapmotorApiClient(username="synthetic", password="synthetic",
                                       history_timezone="America/Los_Angeles")
        self.addCleanup(client.close)
        vehicle = api.Vehicle("SYNTHETIC", None, "T03", None, False)
        client.get_vehicle_list = Mock(return_value=[vehicle])
        client.get_vehicle_status = Mock(return_value={"data": {}})
        client._fetch_account_notifications = Mock(return_value=[])
        client._fetch_optional_read = Mock(side_effect=lambda label, *args: (
            {"data": {"totalAccumulatedMileage": 13, "detail": [{
                "xDay": 1790146800000, "day": "source-date", "accumulatedMileage": 13,
                "accumulatedEnergyConsume": 1250}]}} if label == "mileage energy detail" else None))
        result = client._fetch_authenticated_data()["vehicles"]["SYNTHETIC"]["history"]
        row = result["last_7_days_detail"][0]
        self.assertEqual(row["date"], "2026-09-23")
        self.assertEqual(row["source_date"], "source-date")
        self.assertEqual(row["energy_kwh"], 1.25)

    def test_charge_limit_provenance_preserves_raw_values(self):
        vehicle = api.Vehicle("SYNTHETIC", None, "T03", None, False)
        result = api.normalize_vehicle(vehicle, {"data": {
            "config": {"3": {"percent": 90}}, "chargesocSetting": 80,
            "soc": 0, "dumpEnergy": 32690, "signal": {"1204": 85}}}, None)
        self.assertEqual(result["charging"]["charge_limit_percent"], 90)
        self.assertEqual(result["charging"]["charge_limit_source"], "config_3")
        self.assertEqual(result["charging"]["charge_limit_reliability"], "unverified")
        diagnostics = result["diagnostics"]
        self.assertEqual(diagnostics["charge_limit_plan_raw"], 90)
        self.assertEqual(diagnostics["charge_limit_named_status_raw"], 80)
        self.assertEqual(diagnostics["battery_soc_named_raw"], 0)
        self.assertEqual(diagnostics["battery_soc_signal_raw"], 85)
        self.assertEqual(diagnostics["available_energy_raw_wh"], 32690)
