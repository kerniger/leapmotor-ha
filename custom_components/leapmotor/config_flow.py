"""Config flow for the Leapmotor integration."""

from __future__ import annotations

import os
import logging
from pathlib import Path
from typing import Any
import uuid

import voluptuous as vol

from homeassistant.components.file_upload import process_uploaded_file
from homeassistant import config_entries
from homeassistant.const import CONF_PASSWORD, CONF_USERNAME
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers import selector

from .api import (
    LeapmotorApiClient,
)
from .leap_api import (
    LeapmotorAccountCertError,
    LeapmotorApiError,
    LeapmotorAuthError,
    LeapmotorMissingAppCertError,
    LeapmotorNoVehicleError,
)
from .const import (
    CONF_ABRP_ENABLED,
    CONF_ABRP_TOKEN,
    CONF_APP_CERT_FILE,
    CONF_APP_CERT_PEM,
    CONF_APP_KEY_FILE,
    CONF_APP_KEY_PEM,
    CONF_DEVICE_ID,
    CONF_ECO_POLLING_ENABLED,
    CONF_ECO_SCAN_INTERVAL,
    CONF_OPERATION_PASSWORD,
    CONF_SCAN_INTERVAL,
    CONF_VIN,
    DEFAULT_ECO_SCAN_INTERVAL_MINUTES,
    DEFAULT_SCAN_INTERVAL_MINUTES,
    DOMAIN,
    STATIC_APP_CERT,
    STATIC_APP_KEY,
    STATIC_CERT_STORAGE_DIR,
    SUBENTRY_TYPE_VEHICLE,
)
from .vehicle_config import vehicle_subentries

_LOGGER = logging.getLogger(__name__)


STEP_USER_DATA_SCHEMA = vol.Schema(
    {
        vol.Required(CONF_USERNAME): str,
        vol.Required(CONF_PASSWORD): str,
        vol.Optional(CONF_OPERATION_PASSWORD, default=""): str,
        vol.Optional(CONF_SCAN_INTERVAL, default=DEFAULT_SCAN_INTERVAL_MINUTES): vol.All(
            vol.Coerce(int),
            vol.Range(min=1, max=120),
        ),
        vol.Optional(CONF_ECO_POLLING_ENABLED, default=False): bool,
        vol.Optional(CONF_ECO_SCAN_INTERVAL, default=DEFAULT_ECO_SCAN_INTERVAL_MINUTES): vol.All(
            vol.Coerce(int),
            vol.Range(min=5, max=240),
        ),
        vol.Optional(CONF_ABRP_ENABLED, default=False): bool,
        vol.Optional(CONF_ABRP_TOKEN, default=""): str,
    }
)


CERTIFICATE_DATA_SCHEMA = vol.Schema(
    {
        vol.Optional(CONF_APP_CERT_FILE): selector.FileSelector(
            selector.FileSelectorConfig(accept=".pem,.crt,.cert")
        ),
        vol.Optional(CONF_APP_KEY_FILE): selector.FileSelector(
            selector.FileSelectorConfig(accept=".pem,.key")
        ),
        vol.Optional(CONF_APP_CERT_PEM, default=""): selector.TextSelector(
            selector.TextSelectorConfig(multiline=True)
        ),
        vol.Optional(CONF_APP_KEY_PEM, default=""): selector.TextSelector(
            selector.TextSelectorConfig(multiline=True)
        ),
    }
)


def app_certificate_dir(hass: HomeAssistant) -> Path:
    """Return the persistent directory for user-provided app certificate files."""
    return Path(hass.config.path(STATIC_CERT_STORAGE_DIR))


def migrate_legacy_app_certificate_material(hass: HomeAssistant) -> None:
    """Copy pre-0.5.11 cert files out of the HACS-managed integration folder."""
    cert_dir = app_certificate_dir(hass)
    cert_dir.mkdir(parents=True, exist_ok=True)
    component_dir = Path(__file__).resolve().parent
    for file_name in (STATIC_APP_CERT, STATIC_APP_KEY):
        legacy_path = component_dir / file_name
        target_path = cert_dir / file_name
        if legacy_path.exists() and not target_path.exists():
            target_path.write_bytes(legacy_path.read_bytes())
            os.chmod(target_path, 0o600 if file_name == STATIC_APP_KEY else 0o644)


def has_app_certificate_material(hass: HomeAssistant) -> bool:
    """Return whether the required local app certificate files exist."""
    migrate_legacy_app_certificate_material(hass)
    cert_dir = app_certificate_dir(hass)
    return (cert_dir / STATIC_APP_CERT).exists() and (cert_dir / STATIC_APP_KEY).exists()


def _write_pem_if_provided(path: Path, value: str, marker: str, mode: int) -> None:
    pem = value.strip().replace("\\n", "\n")
    if not pem:
        return
    if marker not in pem:
        raise ValueError(f"Invalid PEM content for {path.name}.")
    path.write_text(pem + "\n", encoding="utf-8")
    os.chmod(path, mode)


def _write_uploaded_pem_if_provided(
    hass: HomeAssistant,
    path: Path,
    file_id: str | None,
    marker: str,
    mode: int,
) -> None:
    if not file_id:
        return
    with process_uploaded_file(hass, file_id) as uploaded_path:
        _write_pem_if_provided(
            path,
            uploaded_path.read_text(encoding="utf-8"),
            marker,
            mode,
        )


def save_app_certificate_material(hass: HomeAssistant, data: dict[str, Any]) -> None:
    """Persist user-provided app certificate material locally, if supplied."""
    cert_dir = app_certificate_dir(hass)
    cert_dir.mkdir(parents=True, exist_ok=True)
    _write_uploaded_pem_if_provided(
        hass,
        cert_dir / STATIC_APP_CERT,
        data.get(CONF_APP_CERT_FILE),
        "BEGIN CERTIFICATE",
        0o644,
    )
    _write_uploaded_pem_if_provided(
        hass,
        cert_dir / STATIC_APP_KEY,
        data.get(CONF_APP_KEY_FILE),
        "PRIVATE KEY",
        0o600,
    )
    _write_pem_if_provided(
        cert_dir / STATIC_APP_CERT,
        str(data.get(CONF_APP_CERT_PEM) or ""),
        "BEGIN CERTIFICATE",
        0o644,
    )
    _write_pem_if_provided(
        cert_dir / STATIC_APP_KEY,
        str(data.get(CONF_APP_KEY_PEM) or ""),
        "PRIVATE KEY",
        0o600,
    )


async def validate_input(hass: HomeAssistant, data: dict[str, Any]) -> dict[str, Any]:
    """Validate the user input by doing one read-only data refresh."""
    await hass.async_add_executor_job(save_app_certificate_material, hass, data)
    client = LeapmotorApiClient(
        username=data[CONF_USERNAME],
        password=data[CONF_PASSWORD],
        operation_password=data.get(CONF_OPERATION_PASSWORD) or None,
        device_id=data.get(CONF_DEVICE_ID),
        static_cert_dir=app_certificate_dir(hass),
    )
    try:
        try:
            result = await hass.async_add_executor_job(client.fetch_data)
        except LeapmotorApiError as exc:
            _LOGGER.warning(
                "Leapmotor setup validation failed after login: %s; last API results: %s",
                exc,
                client.last_api_results,
            )
            raise
    finally:
        await hass.async_add_executor_job(client.close)

    vehicles = result.get("vehicles") or {}
    if not vehicles:
        raise LeapmotorNoVehicleError("No vehicle linked to this account.")
    return {
        "title": f"Leapmotor ({data[CONF_USERNAME]})",
        "vehicles": len(vehicles),
    }


class LeapmotorConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    """Handle a config flow for Leapmotor."""

    VERSION = 1
    MINOR_VERSION = 1

    @classmethod
    @callback
    def async_get_supported_subentry_types(
        cls,
        config_entry: config_entries.ConfigEntry,
    ) -> dict[str, type[config_entries.ConfigSubentryFlow]]:
        """Return the VIN-scoped configurations supported by this account."""
        return {SUBENTRY_TYPE_VEHICLE: LeapmotorVehicleSubentryFlow}

    @staticmethod
    def async_get_options_flow(
        config_entry: config_entries.ConfigEntry,
    ) -> config_entries.OptionsFlow:
        """Create the options flow."""
        return LeapmotorOptionsFlow(config_entry)

    async def async_step_user(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> config_entries.ConfigFlowResult:
        """Start setup with certificate material if it is not installed yet."""
        if await self.hass.async_add_executor_job(has_app_certificate_material, self.hass):
            return await self.async_step_account(user_input)
        return await self.async_step_certificates()

    async def async_step_certificates(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> config_entries.ConfigFlowResult:
        """Collect required app certificate material before account login."""
        errors: dict[str, str] = {}
        if user_input is not None:
            try:
                await self.hass.async_add_executor_job(
                    save_app_certificate_material,
                    self.hass,
                    user_input,
                )
            except ValueError:
                errors["base"] = "certificate_import_error"
            else:
                if await self.hass.async_add_executor_job(has_app_certificate_material, self.hass):
                    return await self.async_step_account()
                errors["base"] = "missing_app_cert"

        return self.async_show_form(
            step_id="certificates",
            data_schema=CERTIFICATE_DATA_SCHEMA,
            errors=errors,
        )

    async def async_step_account(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> config_entries.ConfigFlowResult:
        """Handle account credentials after certificate material is available."""
        return await self._async_handle_account_step(user_input, step_id="account")

    async def _async_handle_account_step(
        self,
        user_input: dict[str, Any] | None,
        *,
        step_id: str,
    ) -> config_entries.ConfigFlowResult:
        """Handle account validation and entry creation."""
        errors: dict[str, str] = {}

        if user_input is not None:
            data = dict(user_input)
            data.setdefault(CONF_DEVICE_ID, uuid.uuid4().hex)
            try:
                info = await validate_input(self.hass, data)
            except LeapmotorMissingAppCertError:
                errors["base"] = "missing_app_cert"
            except LeapmotorAccountCertError:
                errors["base"] = "account_cert_error"
            except LeapmotorNoVehicleError:
                errors["base"] = "no_vehicle"
            except ValueError:
                errors["base"] = "certificate_import_error"
            except LeapmotorAuthError:
                errors["base"] = "invalid_auth"
            except LeapmotorApiError:
                errors["base"] = "api_refresh_failed"
            except Exception:  # noqa: BLE001
                errors["base"] = "unknown"
            else:
                await self.async_set_unique_id(user_input[CONF_USERNAME].lower())
                self._abort_if_unique_id_configured()
                entry_data = {
                    CONF_USERNAME: data[CONF_USERNAME],
                    CONF_PASSWORD: data[CONF_PASSWORD],
                    CONF_DEVICE_ID: data[CONF_DEVICE_ID],
                    CONF_OPERATION_PASSWORD: data.get(CONF_OPERATION_PASSWORD) or "",
                    CONF_SCAN_INTERVAL: data[CONF_SCAN_INTERVAL],
                    CONF_ECO_POLLING_ENABLED: bool(data.get(CONF_ECO_POLLING_ENABLED)),
                    CONF_ECO_SCAN_INTERVAL: data[CONF_ECO_SCAN_INTERVAL],
                    CONF_ABRP_ENABLED: bool(data.get(CONF_ABRP_ENABLED)),
                    CONF_ABRP_TOKEN: data.get(CONF_ABRP_TOKEN) or "",
                }

                return self.async_create_entry(title=info["title"], data=entry_data)

        return self.async_show_form(
            step_id=step_id,
            data_schema=STEP_USER_DATA_SCHEMA,
            errors=errors,
        )


class LeapmotorOptionsFlow(config_entries.OptionsFlow):
    """Handle Leapmotor options."""

    def __init__(self, config_entry: config_entries.ConfigEntry) -> None:
        self._config_entry = config_entry

    async def async_step_init(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> config_entries.ConfigFlowResult:
        """Manage configurable runtime options."""
        if user_input is not None:
            try:
                await self.hass.async_add_executor_job(
                    save_app_certificate_material,
                    self.hass,
                    user_input,
                )
            except ValueError:
                return self.async_show_form(
                    step_id="init",
                    data_schema=self._options_schema(),
                    errors={"base": "certificate_import_error"},
                )
            return self.async_create_entry(
                title="",
                data={
                    CONF_SCAN_INTERVAL: user_input[CONF_SCAN_INTERVAL],
                    CONF_ECO_POLLING_ENABLED: bool(user_input.get(CONF_ECO_POLLING_ENABLED)),
                    CONF_ECO_SCAN_INTERVAL: user_input[CONF_ECO_SCAN_INTERVAL],
                },
            )

        return self.async_show_form(
            step_id="init",
            data_schema=self._options_schema(),
        )

    def _options_schema(self) -> vol.Schema:
        """Build the options schema."""
        scan_interval = self._config_entry.options.get(
            CONF_SCAN_INTERVAL,
            self._config_entry.data.get(CONF_SCAN_INTERVAL, DEFAULT_SCAN_INTERVAL_MINUTES),
        )
        eco_scan_interval = self._config_entry.options.get(
            CONF_ECO_SCAN_INTERVAL,
            self._config_entry.data.get(CONF_ECO_SCAN_INTERVAL, DEFAULT_ECO_SCAN_INTERVAL_MINUTES),
        )
        return vol.Schema(
            {
                vol.Optional(CONF_APP_CERT_FILE): selector.FileSelector(
                    selector.FileSelectorConfig(accept=".pem,.crt,.cert")
                ),
                vol.Optional(CONF_APP_KEY_FILE): selector.FileSelector(
                    selector.FileSelectorConfig(accept=".pem,.key")
                ),
                vol.Optional(CONF_APP_CERT_PEM, default=""): selector.TextSelector(
                    selector.TextSelectorConfig(multiline=True)
                ),
                vol.Optional(CONF_APP_KEY_PEM, default=""): selector.TextSelector(
                    selector.TextSelectorConfig(multiline=True)
                ),
                vol.Optional(CONF_SCAN_INTERVAL, default=scan_interval): vol.All(
                    vol.Coerce(int),
                    vol.Range(min=1, max=120),
                ),
                vol.Optional(
                    CONF_ECO_POLLING_ENABLED,
                    default=bool(
                        self._config_entry.options.get(
                            CONF_ECO_POLLING_ENABLED,
                            self._config_entry.data.get(CONF_ECO_POLLING_ENABLED, False),
                        )
                    ),
                ): bool,
                vol.Optional(CONF_ECO_SCAN_INTERVAL, default=eco_scan_interval): vol.All(
                    vol.Coerce(int),
                    vol.Range(min=5, max=240),
                ),
            }
        )


class LeapmotorVehicleSubentryFlow(config_entries.ConfigSubentryFlow):
    """Configure one vehicle belonging to a Leapmotor account."""

    def __init__(self) -> None:
        self._vin: str | None = None

    async def async_step_user(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> config_entries.SubentryFlowResult:
        """Select an unconfigured vehicle from the loaded account."""
        entry = self._get_entry()
        coordinator = self.hass.data.get(DOMAIN, {}).get(entry.entry_id)
        vehicles = (coordinator.data.get("vehicles") or {}) if coordinator else {}
        configured = vehicle_subentries(entry)
        available = {
            vin: _vehicle_title(vin, vehicle_data)
            for vin, vehicle_data in vehicles.items()
            if vin not in configured
        }
        if not available:
            return self.async_abort(reason="all_vehicles_configured")
        if user_input is not None:
            self._vin = str(user_input[CONF_VIN])
            return await self.async_step_vehicle()
        return self.async_show_form(
            step_id="user",
            data_schema=vol.Schema(
                {
                    vol.Required(CONF_VIN): selector.SelectSelector(
                        selector.SelectSelectorConfig(
                            options=[
                                {"value": vin, "label": title}
                                for vin, title in available.items()
                            ]
                        )
                    )
                }
            ),
        )

    async def async_step_vehicle(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> config_entries.SubentryFlowResult:
        """Collect settings for the selected vehicle."""
        assert self._vin is not None
        if user_input is not None:
            data = _vehicle_settings_data(self._vin, user_input)
            coordinator = self.hass.data[DOMAIN][self._entry_id]
            title = _vehicle_title(
                self._vin,
                coordinator.data["vehicles"][self._vin],
            )
            return self.async_create_entry(
                title=title,
                data=data,
                unique_id=self._vin,
            )
        return self.async_show_form(
            step_id="vehicle",
            data_schema=_vehicle_settings_schema(),
        )

    async def async_step_reconfigure(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> config_entries.SubentryFlowResult:
        """Update PIN and ABRP settings for one vehicle."""
        entry = self._get_entry()
        subentry = self._get_reconfigure_subentry()
        vin = str(subentry.data[CONF_VIN])
        if user_input is not None:
            return self.async_update_and_abort(
                entry,
                subentry,
                data=_vehicle_settings_data(vin, user_input),
            )
        return self.async_show_form(
            step_id="reconfigure",
            data_schema=_vehicle_settings_schema(subentry.data),
            description_placeholders={"vehicle": subentry.title},
        )


def _vehicle_settings_schema(data: dict[str, Any] | None = None) -> vol.Schema:
    """Build the VIN-scoped vehicle settings schema."""
    current = data or {}
    return vol.Schema(
        {
            vol.Optional(
                CONF_OPERATION_PASSWORD,
                default=str(current.get(CONF_OPERATION_PASSWORD) or ""),
            ): str,
            vol.Optional(
                CONF_ABRP_ENABLED,
                default=bool(current.get(CONF_ABRP_ENABLED, False)),
            ): bool,
            vol.Optional(
                CONF_ABRP_TOKEN,
                default=str(current.get(CONF_ABRP_TOKEN) or ""),
            ): str,
        }
    )


def _vehicle_settings_data(vin: str, data: dict[str, Any]) -> dict[str, Any]:
    """Normalize one vehicle subentry payload."""
    return {
        CONF_VIN: vin,
        CONF_OPERATION_PASSWORD: str(data.get(CONF_OPERATION_PASSWORD) or "").strip(),
        CONF_ABRP_ENABLED: bool(data.get(CONF_ABRP_ENABLED)),
        CONF_ABRP_TOKEN: str(data.get(CONF_ABRP_TOKEN) or "").strip(),
    }


def _vehicle_title(vin: str, vehicle_data: dict[str, Any]) -> str:
    """Return a useful, privacy-conscious subentry title."""
    vehicle = vehicle_data.get("vehicle") or {}
    model = str(vehicle.get("car_type") or "Leapmotor")
    nickname = str(vehicle.get("nickname") or "").strip()
    return f"{nickname or model} (...{vin[-4:]})"
