"""Helpers for interpreting inconsistent vehicle status signals."""

from __future__ import annotations


def nonzero_state(raw: object) -> bool | None:
    """Return whether a numeric or boolean-like status value is non-zero."""
    if raw is None:
        return None
    if isinstance(raw, bool):
        return raw
    try:
        return float(raw) != 0
    except (TypeError, ValueError):
        normalized = str(raw).strip().lower()
        if normalized in {"true", "yes", "on"}:
            return True
        if normalized in {"false", "no", "off"}:
            return False
        return None


def window_open_state(
    status_value: object,
    position_percent: object,
    *,
    use_position: bool,
) -> bool | None:
    """Return window state from its flag and, where reliable, position."""
    status_open = nonzero_state(status_value)
    position_open = nonzero_state(position_percent) if use_position else None
    if status_open is None and position_open is None:
        return None
    return bool(status_open or position_open)


def vehicle_precludes_charging(signals: dict[str, object]) -> bool:
    """Return whether motion or physical READY makes charging impossible."""
    gear = _safe_int(signals.get("1010"))
    speed = _safe_float(signals.get("1319"))
    vehicle_ready = nonzero_state(signals.get("1258"))
    return (
        gear in {1, 2, 3}
        or (speed is not None and speed > 0)
        or vehicle_ready is True
    )


def charge_connection_allows_charging(
    connection_status: object,
    legacy_plug_status: object,
) -> bool:
    """Reject explicit unplugged and drive-time cable states."""
    connection = _safe_int(connection_status)
    if connection is not None:
        # State 4 means connected but waiting for the configured schedule. It
        # does not prove charging, but real current may begin before the state
        # changes, so let the current/time checks make the final decision.
        return connection in {1, 2, 3, 4}
    legacy_plugged = nonzero_state(legacy_plug_status)
    return legacy_plugged is not False


def _safe_int(raw: object) -> int | None:
    """Return a status value as int or None."""
    if raw is None:
        return None
    try:
        return int(float(raw))
    except (TypeError, ValueError):
        return None


def _safe_float(raw: object) -> float | None:
    """Return a status value as float or None."""
    if raw is None:
        return None
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None
