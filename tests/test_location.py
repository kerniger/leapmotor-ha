"""Tests for GPS coordinate sign handling."""

from __future__ import annotations

import importlib.util
from pathlib import Path
import sys
import unittest

MODULE_PATH = (
    Path(__file__).parents[1]
    / "custom_components"
    / "leapmotor"
    / "location.py"
)
SPEC = importlib.util.spec_from_file_location("leapmotor_location", MODULE_PATH)
assert SPEC and SPEC.loader
location = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = location
SPEC.loader.exec_module(location)


class ResolveCoordinateTests(unittest.TestCase):
    """Cover missing signs and guarded hemisphere changes."""

    def test_unsigned_fallback_uses_remembered_western_sign(self) -> None:
        result = location.resolve_coordinate(
            signed_value=None,
            unsigned_value=9.14,
            remembered_sign=-1,
        )
        self.assertEqual(result.value, -9.14)
        self.assertEqual(result.source, "unsigned_signal_with_memory")

    def test_single_positive_signed_poll_does_not_poison_sign(self) -> None:
        result = location.resolve_coordinate(
            signed_value=9.14,
            unsigned_value=9.14,
            remembered_sign=-1,
        )
        self.assertEqual(result.value, -9.14)
        self.assertEqual(result.sign, -1)
        self.assertEqual(result.pending_flip_count, 1)

    def test_negative_signed_poll_is_immediately_authoritative(self) -> None:
        result = location.resolve_coordinate(
            signed_value=-9.14,
            unsigned_value=9.14,
            remembered_sign=1,
        )
        self.assertEqual(result.value, -9.14)
        self.assertEqual(result.sign, -1)

    def test_positive_sign_flip_requires_ten_polls(self) -> None:
        result = None
        for pending in range(10):
            result = location.resolve_coordinate(
                signed_value=9.14,
                unsigned_value=9.14,
                remembered_sign=-1,
                pending_flip_count=pending,
            )
        assert result
        self.assertEqual(result.value, 9.14)
        self.assertEqual(result.sign, 1)
        self.assertEqual(result.pending_flip_count, 0)

    def test_crossing_near_meridian_is_immediate(self) -> None:
        result = location.resolve_coordinate(
            signed_value=0.5,
            unsigned_value=0.5,
            remembered_sign=-1,
        )
        self.assertEqual(result.value, 0.5)
        self.assertEqual(result.sign, 1)


if __name__ == "__main__":
    unittest.main()
