"""Tests for model-aware vehicle status endpoint fallback."""

from __future__ import annotations

import importlib
from pathlib import Path
import sys
import types
import unittest
from unittest.mock import Mock

ROOT = Path(__file__).parents[1]
if "custom_components" not in sys.modules:
    custom_components = types.ModuleType("custom_components")
    custom_components.__path__ = [str(ROOT / "custom_components")]
    sys.modules["custom_components"] = custom_components
if "custom_components.leapmotor" not in sys.modules:
    leapmotor = types.ModuleType("custom_components.leapmotor")
    leapmotor.__path__ = [str(ROOT / "custom_components" / "leapmotor")]
    sys.modules["custom_components.leapmotor"] = leapmotor

api = importlib.import_module("custom_components.leapmotor.api")


class VehicleStatusFallbackTests(unittest.TestCase):
    """Exercise fallback and reuse through the public status method."""

    def test_two_vehicle_reads_and_pins_stay_isolated(self) -> None:
        client = api.LeapmotorApiClient(username="test", password="test")
        self.addCleanup(client.close)
        client.set_vehicle_operation_passwords({"VIN-A": "1111", "VIN-B": "2222"})
        first = api.Vehicle("VIN-A", None, "T03", None, False)
        second = api.Vehicle("VIN-B", None, "B10", None, False)
        reader = Mock(side_effect=[{"data": {}}, {"data": {}}, {"data": {}}])
        client._get_vehicle_status_raw = reader
        for vehicle in (first, second, first):
            client.get_vehicle_status(vehicle)
        self.assertEqual(
            [(call.args[0].vin, call.kwargs["car_type_path"], call.kwargs["body"])
             for call in reader.call_args_list],
            [("VIN-A", "t03", "vin=VIN-A"), ("VIN-B", "c10", "vin=VIN-B"),
             ("VIN-A", "t03", "vin=VIN-A")],
        )
        self.assertEqual(client.operation_password_for_vin(first.vin), "1111")
        self.assertEqual(client.operation_password_for_vin(second.vin), "2222")
        self.assertEqual((first.car_type, second.car_type), ("T03", "B10"))

    def test_successful_c10_fallback_is_reused(self) -> None:
        client = api.LeapmotorApiClient(username="test", password="test")
        self.addCleanup(client.close)
        vehicle = api.Vehicle(
            vin="TEST-VIN",
            car_id=None,
            car_type="C16",
            nickname=None,
            is_shared=False,
        )
        paths: list[str] = []

        def get_status(_vehicle, *, car_type_path, body, label):
            paths.append(car_type_path)
            if car_type_path == "c16":
                client.last_api_results["vehicle status"] = {"http_status": 404}
                raise api.LeapmotorApiError("unsupported status path")
            return {"data": {"signal": {"1": 1}}}

        client._get_vehicle_status_raw = get_status

        first = client.get_vehicle_status(vehicle)
        second = client.get_vehicle_status(vehicle)

        self.assertEqual(paths, ["c16", "c10", "c10"])
        self.assertEqual(first["_status_endpoint_path"], "c10")
        self.assertEqual(second["_status_endpoint_path"], "c10")


if __name__ == "__main__":
    unittest.main()
