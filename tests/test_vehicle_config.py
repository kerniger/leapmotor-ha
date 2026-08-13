"""Tests for VIN-scoped multi-vehicle settings."""

from __future__ import annotations

import importlib.util
from pathlib import Path
from types import SimpleNamespace
import unittest

MODULE_PATH = (
    Path(__file__).parents[1]
    / "custom_components"
    / "leapmotor"
    / "vehicle_config.py"
)
SPEC = importlib.util.spec_from_file_location("leapmotor_vehicle_config", MODULE_PATH)
assert SPEC and SPEC.loader
vehicle_config = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(vehicle_config)


def _subentry(vin: str, **settings: object) -> SimpleNamespace:
    return SimpleNamespace(
        subentry_id=f"subentry-{vin}",
        subentry_type="vehicle",
        data={"vin": vin, **settings},
    )


def _entry(
    *subentries: SimpleNamespace,
    data: dict[str, object] | None = None,
    options: dict[str, object] | None = None,
) -> SimpleNamespace:
    return SimpleNamespace(
        subentries={item.subentry_id: item for item in subentries},
        data=data or {},
        options=options or {},
    )


class VehicleConfigTests(unittest.TestCase):
    """Keep credentials and telemetry targets isolated by VIN."""

    def test_vehicle_pin_overrides_legacy_pin(self) -> None:
        entry = _entry(
            _subentry("VIN-A", operation_password="1111"),
            _subentry("VIN-B", operation_password="2222"),
            data={"operation_password": "legacy"},
        )
        self.assertEqual(
            vehicle_config.operation_password_for_vehicle(entry, "VIN-A"),
            "1111",
        )
        self.assertEqual(
            vehicle_config.operation_password_for_vehicle(entry, "VIN-B"),
            "2222",
        )

    def test_blank_vehicle_pin_disables_legacy_fallback(self) -> None:
        entry = _entry(
            _subentry("VIN-A", operation_password=""),
            data={"operation_password": "legacy"},
        )
        self.assertIsNone(
            vehicle_config.operation_password_for_vehicle(entry, "VIN-A")
        )

    def test_legacy_abrp_token_is_never_shared_across_multiple_cars(self) -> None:
        entry = _entry(data={"abrp_enabled": True, "abrp_token": "legacy-token"})
        self.assertEqual(
            vehicle_config.abrp_config_for_vehicle(
                entry,
                "VIN-A",
                vehicle_count=2,
            ),
            (False, ""),
        )

    def test_abrp_tokens_are_resolved_per_vehicle(self) -> None:
        entry = _entry(
            _subentry("VIN-A", abrp_enabled=True, abrp_token="token-a"),
            _subentry("VIN-B", abrp_enabled=True, abrp_token="token-b"),
        )
        self.assertEqual(
            vehicle_config.abrp_config_for_vehicle(
                entry,
                "VIN-A",
                vehicle_count=2,
            ),
            (True, "token-a"),
        )
        self.assertEqual(
            vehicle_config.abrp_config_for_vehicle(
                entry,
                "VIN-B",
                vehicle_count=2,
            ),
            (True, "token-b"),
        )


if __name__ == "__main__":
    unittest.main()
