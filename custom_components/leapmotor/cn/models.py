"""CN client types, with no dependency on EU authentication code."""
from dataclasses import dataclass


class LeapmotorApiError(Exception):
    """CN request failed."""


class LeapmotorAuthError(LeapmotorApiError):
    """CN authentication failed."""


@dataclass(slots=True)
class Vehicle:
    """Minimal vehicle identity."""

    vin: str
    car_id: str | None
    car_type: str
    nickname: str | None
    is_shared: bool
    model_year: int | None = None
    capabilities: tuple[str, ...] = ()
