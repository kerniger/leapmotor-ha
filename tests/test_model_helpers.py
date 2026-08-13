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
    """Cover the B10-only 0-10 command scale."""

    def test_b10_converts_percentage_to_native_scale(self) -> None:
        self.assertEqual(models.native_window_position("B10", 50), 5)

    def test_c10_and_t03_keep_percentage_scale(self) -> None:
        for car_type in ("C10", "T03"):
            with self.subTest(car_type=car_type):
                self.assertEqual(models.native_window_position(car_type, 50), 50)


if __name__ == "__main__":
    unittest.main()
