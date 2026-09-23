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

    def test_t03_unverified_energy_unit_does_not_pollute_kwh(self):
        for value, distance in ((2744, 22), (11912, 96), (0, 0)):
            mileage = {"data": {"totalAccumulatedMileage": distance, "detail": [
                {"accumulatedMileage": distance, "accumulatedEnergyConsume": value}]}}
            history = self.normalize(" t03 ", {}, mileage_json=mileage)["history"]
            self.assertTrue(history["last_7_days_energy_complete"])
            self.assertIsNone(history["last_7_days_energy_kwh"])
            self.assertEqual(history["last_7_days_energy_unavailable_reason"], "unverified_unit")
            self.assertEqual(history["last_7_days_detail"][0]["energy_raw"], value)
            self.assertIsNone(history["last_7_days_detail"][0]["energy_kwh"])
            self.assertIsNone(history["last_7_days_detail"][0]["driving_energy_kwh"])
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
