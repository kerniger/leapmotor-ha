"""Tests for commands that differ between vehicle models."""

from __future__ import annotations

import importlib.util
from pathlib import Path
import sys
import unittest

MODULE_PATH = (
    Path(__file__).parents[1]
    / "custom_components"
    / "leapmotor"
    / "model_helpers.py"
)
SPEC = importlib.util.spec_from_file_location("leapmotor_model_helpers", MODULE_PATH)
assert SPEC and SPEC.loader
models = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = models
SPEC.loader.exec_module(models)


class ClimateOffPayloadTests(unittest.TestCase):
    """Keep the verified T03 command isolated from other models."""

    def test_t03_uses_full_payload_with_operate_off(self) -> None:
        payload = models.climate_off_payload("T03")
        self.assertIsNotNone(payload)
        assert payload is not None
        self.assertEqual(payload["operate"], "off")
        self.assertEqual(
            set(payload),
            {
                "circle",
                "mode",
                "operate",
                "position",
                "temperature",
                "windlevel",
                "wshld",
            },
        )

    def test_other_models_keep_standard_command(self) -> None:
        for car_type in ("B10", "C10", "B05", ""):
            with self.subTest(car_type=car_type):
                self.assertIsNone(models.climate_off_payload(car_type))


class WindowPositionTests(unittest.TestCase):
    """Cover model-specific native window command scales."""

    def test_default_open_position_uses_each_models_native_scale(self) -> None:
        self.assertEqual(models.native_window_open_position("T03", None), 20)
        for car_type in ("B05", "B10", "C10"):
            with self.subTest(car_type=car_type):
                self.assertEqual(models.native_window_open_position(car_type, None), 2)

    def test_leap_platform_converts_percentage_to_native_scale(self) -> None:
        for car_type in ("B05", "B10", "C10"):
            with self.subTest(car_type=car_type):
                self.assertEqual(models.native_window_position(car_type, 20), 2)
                self.assertEqual(models.native_window_position(car_type, 50), 5)
                self.assertEqual(models.native_window_position(car_type, 100), 10)

    def test_t03_and_unknown_models_keep_percentage_scale(self) -> None:
        for car_type in ("T03", "ZX9", ""):
            with self.subTest(car_type=car_type):
                self.assertEqual(
                    models.native_window_open_position(
                        car_type,
                        None,
                    ),
                    20,
                )
                self.assertEqual(models.native_window_position(car_type, 50), 50)


class VehicleStatusPathResolverTests(unittest.TestCase):
    """Cover VIN-scoped status endpoint selection and fallback memory."""

    def test_known_b_series_models_use_c10_directly(self) -> None:
        resolver = models.VehicleStatusPathResolver()
        for car_type in ("B05", "B10", "B11"):
            with self.subTest(car_type=car_type):
                self.assertEqual(resolver.path_for("VIN", car_type), "c10")

    def test_successful_fallback_is_reused_for_only_that_vin(self) -> None:
        resolver = models.VehicleStatusPathResolver()
        self.assertTrue(resolver.should_try_c10_fallback("VIN-A", "c16", 404))
        resolver.remember("VIN-A", "c10")

        self.assertEqual(resolver.path_for("VIN-A", "C16"), "c10")
        self.assertEqual(resolver.path_for("VIN-B", "C16"), "c16")

    def test_failed_fallback_is_not_repeated(self) -> None:
        resolver = models.VehicleStatusPathResolver()
        self.assertTrue(resolver.should_try_c10_fallback("VIN", "c16", 404))
        self.assertFalse(resolver.should_try_c10_fallback("VIN", "c16", 404))

    def test_only_unsupported_paths_trigger_fallback(self) -> None:
        resolver = models.VehicleStatusPathResolver()
        self.assertFalse(resolver.should_try_c10_fallback("VIN-A", "c16", 500))
        self.assertFalse(resolver.should_try_c10_fallback("VIN-B", "c10", 404))


if __name__ == "__main__":
    unittest.main()
