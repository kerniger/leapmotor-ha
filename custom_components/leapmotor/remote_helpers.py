"""Helpers for Leapmotor remote-control actions and service resolution."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from functools import partial
from typing import Any

from homeassistant.exceptions import HomeAssistantError

from .api import LeapmotorApiError, LeapmotorMissingAppCertError
from .coordinator import LeapmotorDataUpdateCoordinator


@dataclass(frozen=True, slots=True)
class RemoteActionSpec:
    """Description of one exposed remote-control action."""

    action: str
    translation_key: str
    icon: str
    method_name: str
    service_name: str


def resolve_target_vin(
    coordinator: LeapmotorDataUpdateCoordinator,
    vin: str | None = None,
) -> str:
    """Resolve a single target VIN from service input."""
    vehicles = coordinator.data.get("vehicles") or {}
    if vin:
        if vin not in vehicles:
            raise HomeAssistantError(f"Unknown VIN for this account: {vin}")
        return vin

    if len(vehicles) == 1:
        return next(iter(vehicles))

    raise HomeAssistantError(
        "Multiple vehicles are available. Specify a VIN when calling this service."
    )


def format_remote_error(exc: Exception) -> str:
    """Translate raw API errors into clearer Home Assistant error text."""
    message = str(exc).strip() or exc.__class__.__name__
    lowered = message.lower()

    if isinstance(exc, LeapmotorMissingAppCertError):
        return (
            "App certificate or private key is missing. "
            "Login requires both app_cert.pem and app_key.pem."
        )
    if "betriebspasswort" in lowered or "operatepassword" in lowered:
        return (
            "Vehicle PIN verification failed. Check the configured vehicle PIN; "
            "read-only data can still work without it."
        )
    if "account_cert_error" in lowered or "konto-zertifikat" in lowered:
        return "The Leapmotor account certificate could not be opened for this login."
    if "missing local app certificate material" in lowered:
        return (
            "Local app certificate material is missing. "
            "Upload the app certificate and app key in the integration options."
        )
    if "anmeldung fehlgeschlagen" in lowered or "login" in lowered and "failed" in lowered:
        return "Leapmotor login failed. Check username, password and certificate material."
    if "shared" in lowered and "right" in lowered:
        return "This shared-car account does not have the required right for this action."
    return message


async def async_execute_remote_action(
    coordinator: LeapmotorDataUpdateCoordinator,
    vin: str,
    spec: RemoteActionSpec,
    kwargs: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Execute one remote action with shared cooldown, error and refresh handling."""
    cooldown = coordinator.remote_action_cooldown_remaining(vin)
    if cooldown:
        raise HomeAssistantError(
            f"Remote action cooldown active. Try again in {cooldown} seconds."
        )
    if not coordinator.client.operation_password:
        raise HomeAssistantError(
            "Vehicle PIN is not configured. Read-only data works without a PIN, "
            "but remote-control actions require it."
        )

    method = getattr(coordinator.client, spec.method_name)
    callable_ = partial(method, vin, **kwargs) if kwargs else partial(method, vin)
    try:
        result = await coordinator.hass.async_add_executor_job(callable_)
    except LeapmotorApiError as exc:
        coordinator.record_remote_action(
            vin,
            spec.action,
            success=False,
            error=format_remote_error(exc),
        )
        raise HomeAssistantError(format_remote_error(exc)) from exc

    coordinator.record_remote_action(
        vin,
        spec.action,
        success=True,
        result=result,
    )
    if spec.action == "lock":
        coordinator.set_lock_state_override(vin, True, ttl_seconds=15)
    elif spec.action == "unlock":
        coordinator.set_lock_state_override(vin, False, ttl_seconds=15)
    await coordinator.async_request_refresh()
    if spec.action in {"lock", "unlock"}:
        await asyncio.sleep(4)
        await coordinator.async_request_refresh()
    return result
