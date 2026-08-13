"""Tests for inconsistent model-specific status signals."""

from __future__ import annotations

import importlib.util
from pathlib import Path
import sys
import unittest

MODULE_PATH = (
    Path(__file__).parents[1]
    / "custom_components"
    / "leapmotor"
    / "signal_helpers.py"
)
SPEC = importlib.util.spec_from_file_location("leapmotor_signal_helpers", MODULE_PATH)
assert SPEC and SPEC.loader
signals = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = signals
SPEC.loader.exec_module(signals)


class WindowOpenStateTests(unittest.TestCase):
    """Cover T03 position fallback without changing other models."""

    def test_t03_open_position_overrides_dead_closed_flag(self) -> None:
        self.assertIs(
            signals.window_open_state(0, 20, use_position=True),
            True,
        )

    def test_t03_zero_position_and_flag_are_closed(self) -> None:
        self.assertIs(
            signals.window_open_state(0, 0, use_position=True),
            False,
        )

    def test_other_models_ignore_untrusted_position(self) -> None:
        self.assertIs(
            signals.window_open_state(0, 50, use_position=False),
            False,
        )

    def test_nonbinary_status_is_open(self) -> None:
        self.assertIs(
            signals.window_open_state(2, None, use_position=False),
            True,
        )

    def test_missing_status_and_position_are_unknown(self) -> None:
        self.assertIs(
            signals.window_open_state(None, None, use_position=True),
            None,
        )

    def test_decimal_zero_string_is_closed(self) -> None:
        self.assertIs(signals.nonzero_state("0.0"), False)


class ChargingPlausibilityTests(unittest.TestCase):
    """Cover the B10 READY/unplugged phantom-charge sequence."""

    def test_physical_ready_precludes_charging_in_park(self) -> None:
        self.assertIs(
            signals.vehicle_precludes_charging(
                {"1010": 0, "1319": 0, "1258": 1}
            ),
            True,
        )

    def test_parked_vehicle_with_ready_off_can_charge(self) -> None:
        self.assertIs(
            signals.vehicle_precludes_charging(
                {"1010": 0, "1319": 0, "1258": 0}
            ),
            False,
        )

    def test_explicit_unplugged_state_rejects_charging(self) -> None:
        self.assertIs(signals.charge_connection_allows_charging(0, 1), False)

    def test_deferred_state_keeps_connection_plausible(self) -> None:
        self.assertIs(signals.charge_connection_allows_charging(4, 1), True)

    def test_drive_time_state_rejects_charging(self) -> None:
        self.assertIs(signals.charge_connection_allows_charging(5, 1), False)

    def test_connected_states_allow_charging_checks(self) -> None:
        for state in (1, 2, 3):
            with self.subTest(state=state):
                self.assertIs(
                    signals.charge_connection_allows_charging(state, 0),
                    True,
                )

    def test_legacy_explicit_unplugged_state_rejects_charging(self) -> None:
        self.assertIs(signals.charge_connection_allows_charging(None, 0), False)

    def test_missing_connection_state_keeps_legacy_current_fallback(self) -> None:
        self.assertIs(signals.charge_connection_allows_charging(None, None), True)


if __name__ == "__main__":
    unittest.main()
