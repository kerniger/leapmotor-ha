"""GPS coordinate normalization helpers."""

from __future__ import annotations

from dataclasses import dataclass

MERIDIAN_CROSSING_DEGREES = 1.0
SIGN_FLIP_CONFIRMATIONS = 10


@dataclass(frozen=True, slots=True)
class CoordinateResolution:
    """Result of resolving one signed or unsigned coordinate axis."""

    value: float | None
    sign: int | None
    pending_flip_count: int
    source: str


def resolve_coordinate(
    *,
    signed_value: object,
    unsigned_value: object,
    remembered_sign: int | None,
    pending_flip_count: int = 0,
) -> CoordinateResolution:
    """Resolve one coordinate while guarding against lost minus signs."""
    signed = _safe_float(signed_value)
    if signed not in (None, 0.0):
        proposed_sign = -1 if signed < 0 else 1
        sign_is_authoritative = (
            remembered_sign is None
            or remembered_sign == proposed_sign
            or signed < 0
            or abs(signed) <= MERIDIAN_CROSSING_DEGREES
        )
        if sign_is_authoritative:
            return CoordinateResolution(signed, proposed_sign, 0, "signed_signal")

        confirmations = pending_flip_count + 1
        if confirmations >= SIGN_FLIP_CONFIRMATIONS:
            return CoordinateResolution(
                signed, proposed_sign, 0, "confirmed_hemisphere_crossing"
            )
        return CoordinateResolution(
            abs(signed) * remembered_sign,
            remembered_sign,
            confirmations,
            "remembered_sign_guard",
        )

    unsigned = _safe_float(unsigned_value)
    if unsigned is None:
        return CoordinateResolution(None, remembered_sign, 0, "unavailable")

    sign = remembered_sign or 1
    return CoordinateResolution(
        abs(unsigned) * sign,
        remembered_sign,
        0,
        "unsigned_signal_with_memory" if remembered_sign else "unsigned_signal",
    )


def _safe_float(value: object) -> float | None:
    """Return an API coordinate as float or None."""
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
