"""Internal Leapmotor API layer.

This package contains Home Assistant independent API primitives. The public HA
integration still imports the compatibility client from ``custom_components``.
"""

from .exceptions import (
    LeapmotorAccountCertError,
    LeapmotorApiError,
    LeapmotorAuthError,
    LeapmotorMissingAppCertError,
    LeapmotorNoVehicleError,
)
from .crypto import derive_operate_password, derive_session_device_id
from .models import Vehicle
from .remote import REMOTE_ACTION_SPECS, RemoteActionSpec, build_seat_comfort_payload
from .transport import CurlTransport

__all__ = [
    "LeapmotorAccountCertError",
    "LeapmotorApiError",
    "LeapmotorAuthError",
    "LeapmotorMissingAppCertError",
    "LeapmotorNoVehicleError",
    "REMOTE_ACTION_SPECS",
    "RemoteActionSpec",
    "build_seat_comfort_payload",
    "CurlTransport",
    "Vehicle",
    "derive_operate_password",
    "derive_session_device_id",
]
