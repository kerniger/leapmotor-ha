"""Tests for mileage and energy history validation."""

from __future__ import annotations

import importlib.util
from pathlib import Path
import sys
import unittest
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

MODULE_PATH = (
    Path(__file__).parents[1]
    / "custom_components"
    / "leapmotor"
    / "history_helpers.py"
)
SPEC = importlib.util.spec_from_file_location("leapmotor_history_helpers", MODULE_PATH)
assert SPEC and SPEC.loader
history = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = history
SPEC.loader.exec_module(history)


class HistoryWindowTests(unittest.TestCase):
    def test_seven_calendar_days_including_today_across_dst(self) -> None:
        tz = ZoneInfo("Europe/Berlin")
        for now in (datetime(2026, 9, 11, 12, tzinfo=tz),
                    datetime(2026, 3, 30, 12, tzinfo=tz),
                    datetime(2026, 10, 26, 12, tzinfo=tz)):
            with self.subTest(now=now):
                start_ms, end_ms = history.seven_day_window_ms(now)
                start = datetime.fromtimestamp(start_ms / 1000, tz)
                end = datetime.fromtimestamp(end_ms / 1000, tz)
                self.assertEqual(start.date(), now.date() - timedelta(days=6))
                self.assertEqual(end.date(), now.date())
                self.assertEqual((start.hour, start.minute, start.second), (0, 0, 0))
                self.assertEqual((end.hour, end.minute, end.second), (23, 59, 59))

    def test_weekly_rates_normalized_without_losing_other_fields(self) -> None:
        rows = [{"weekStart": "2026-08-24", "hundredKmEC": 14.2,
                 "hundredMiKwhEC": "4.3", "extra": 10},
                {"hundredKmEC": 0, "hundredMiKwhEC": 0.0},
                {"hundredKmEC": "NaN", "hundredMiKwhEC": "invalid"}]
        result = history.normalize_weekly_consumption(rows)
        self.assertEqual(result[0]["hundredMiKwhEC"], 4.3)
        self.assertEqual(result[0]["extra"], 10)
        self.assertEqual(result[1]["hundredMiKwhEC"], 0.0)
        self.assertIsNone(result[2]["hundredKmEC"])
        self.assertIsNone(result[2]["hundredMiKwhEC"])
        self.assertEqual(rows[0]["hundredMiKwhEC"], "4.3")


class MileageEnergyDetailTests(unittest.TestCase):
    """Only publish energy totals whose distance coverage is complete."""

    def test_complete_detail_returns_energy_and_normalized_days(self) -> None:
        result = history.summarize_mileage_energy_detail(
            [
                {
                    "day": "2026-08-29",
                    "xDay": 1787961600000,
                    "currentMileage": 12010,
                    "accumulatedMileage": 10,
                    "accumulatedMileageMile": "6.2",
                    "accumulatedEnergyConsume": 2,
                },
                {
                    "day": "2026-08-30",
                    "xDay": 1788048000000,
                    "currentMileage": 12084,
                    "accumulatedMileage": 74,
                    "accumulatedMileageMile": "46.0",
                    "accumulatedEnergyConsume": "14.5",
                },
            ],
            84,
        )

        self.assertIs(result["energy_complete"], True)
        self.assertEqual(result["energy_kwh"], 16.5)
        self.assertEqual(result["detail_mileage_km"], 84.0)
        self.assertEqual(result["covered_mileage_km"], 84.0)
        self.assertEqual(result["detail_days"], 2)
        self.assertEqual(result["daily_detail"][0]["date"], "2026-08-29")
        self.assertEqual(result["daily_detail"][0]["driving_energy_kwh"], 2.0)
        self.assertEqual(result["daily_detail"][1]["driving_energy_kwh"], 14.5)
        for day in result["daily_detail"]:
            self.assertEqual(day["driving_energy_kwh"], day["energy_kwh"])

    def test_missing_energy_makes_total_unavailable(self) -> None:
        result = history.summarize_mileage_energy_detail(
            [
                {"day": "2026-08-29", "accumulatedMileage": 20},
                {
                    "day": "2026-08-30",
                    "accumulatedMileage": 64,
                    "accumulatedEnergyConsume": 12,
                },
            ],
            84,
        )

        self.assertIs(result["energy_complete"], False)
        self.assertIsNone(result["energy_kwh"])
        self.assertEqual(result["detail_mileage_km"], 84.0)
        self.assertEqual(result["covered_mileage_km"], 64.0)

    def test_partial_detail_mileage_makes_total_unavailable(self) -> None:
        result = history.summarize_mileage_energy_detail(
            [
                {
                    "day": "2026-08-30",
                    "accumulatedMileage": 20,
                    "accumulatedEnergyConsume": 4,
                }
            ],
            84,
        )

        self.assertIs(result["energy_complete"], False)
        self.assertIsNone(result["energy_kwh"])
        self.assertEqual(result["detail_mileage_km"], 20.0)
        self.assertEqual(result["covered_mileage_km"], 20.0)

    def test_missing_detail_is_unknown_not_zero(self) -> None:
        result = history.summarize_mileage_energy_detail(None, 0)

        self.assertIs(result["energy_complete"], False)
        self.assertIsNone(result["energy_kwh"])
        self.assertEqual(result["daily_detail"], [])


if __name__ == "__main__":
    unittest.main()
