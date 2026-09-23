"""CN-only onboarding; independent of the EU integration."""
from __future__ import annotations

import json
from typing import Any
import voluptuous as vol
from homeassistant import config_entries
from homeassistant.core import callback
from homeassistant.helpers.selector import TextSelector, TextSelectorConfig, TextSelectorType
from homeassistant.helpers.storage import Store

from .models import LeapmotorApiError, LeapmotorAuthError
from .cn_api import CNReadOnlyApiClient
from .cn_auth import CNPhoneLogin
from .const import CONF_CN_SESSION, CONF_REGION, CONF_SCAN_INTERVAL, DEFAULT_SCAN_INTERVAL_MINUTES, DOMAIN_CN, REGION_CN

from .const import CONF_PROXY_URL
from .transport import validate_proxy

PROXY_SELECTOR = TextSelector(TextSelectorConfig(type=TextSelectorType.PASSWORD))

CN_SESSION_SCHEMA = vol.Schema({
    vol.Required(CONF_CN_SESSION): str,
    vol.Optional(CONF_PROXY_URL, default=""): PROXY_SELECTOR,
    vol.Optional(CONF_SCAN_INTERVAL, default=DEFAULT_SCAN_INTERVAL_MINUTES): vol.All(vol.Coerce(int), vol.Range(min=1, max=120)),
})


class LeapmotorCNConfigFlowMixin:
    """Phone/SMS onboarding and expert session import."""

    def __init__(self) -> None:
        """Initialize CN mixin state cooperatively."""
        super().__init__()
        self._cn_login = None
        self._cn_interval = 5
        self._cn_sms_sent = False
        self._cn_reauth_entry = None
        self._proxy_url = ""

    async def async_step_cn(self, user_input: dict[str, Any] | None = None) -> config_entries.ConfigFlowResult:
        """Choose normal phone onboarding or the expert import path."""
        return self.async_show_menu(step_id="cn", menu_options=["cn_phone", "cn_import"])

    async def async_step_reauth(self, entry_data: dict[str, Any]) -> config_entries.ConfigFlowResult:
        """Require a fresh explicit SMS confirmation for CN reauthentication."""
        if entry_data.get(CONF_REGION) != REGION_CN:
            return self.async_abort(reason="reauth_not_supported")
        self._cn_reauth_entry = self.hass.config_entries.async_get_entry(self.context["entry_id"])
        self._proxy_url = self._cn_reauth_entry.options.get(CONF_PROXY_URL, entry_data.get(CONF_PROXY_URL, ""))
        return await self.async_step_cn_phone()

    async def async_step_cn_phone(self, user_input: dict[str, Any] | None = None) -> config_entries.ConfigFlowResult:
        """Collect the phone number without sending anything."""
        errors = {}
        if user_input is not None:
            try:
                self._proxy_url = validate_proxy(user_input.get(CONF_PROXY_URL, self._proxy_url))
            except ValueError:
                errors[CONF_PROXY_URL] = "invalid_proxy"
            try:
                if not errors:
                    self._cn_login = CNPhoneLogin(user_input["phone"], self._proxy_url)
            except ValueError:
                errors["phone"] = "invalid_phone"
            else:
                if errors:
                    return self.async_show_form(step_id="cn_phone", errors=errors, data_schema=self._phone_schema())
                self._cn_interval = user_input.get(CONF_SCAN_INTERVAL, DEFAULT_SCAN_INTERVAL_MINUTES)
                self._cn_sms_sent = False
                return await self.async_step_cn_send_sms()
        return self.async_show_form(step_id="cn_phone", errors=errors, data_schema=self._phone_schema())

    def _phone_schema(self):
        return vol.Schema({
            vol.Required("phone"): str,
            vol.Optional(CONF_PROXY_URL, default=self._proxy_url): PROXY_SELECTOR,
            vol.Optional(CONF_SCAN_INTERVAL, default=DEFAULT_SCAN_INTERVAL_MINUTES): vol.All(vol.Coerce(int), vol.Range(min=1, max=120)),
        })

    async def async_step_cn_send_sms(self, user_input: dict[str, Any] | None = None) -> config_entries.ConfigFlowResult:
        """Only an explicitly checked confirmation can send an SMS."""
        if self._cn_login is None:
            return await self.async_step_cn_phone()
        if self._cn_sms_sent:
            return await self.async_step_cn_code()
        errors = {}
        if user_input is not None and user_input.get("send_sms") is True:
            try:
                await self.hass.async_add_executor_job(self._cn_login.send_sms)
            except LeapmotorAuthError:
                errors["base"] = "invalid_auth"
            except LeapmotorApiError:
                errors["base"] = "sms_request_failed"
            else:
                self._cn_sms_sent = True
                return await self.async_step_cn_code()
        return self.async_show_form(step_id="cn_send_sms", errors=errors,
                                    data_schema=vol.Schema({vol.Required("send_sms", default=False): bool}))

    async def async_step_cn_code(self, user_input: dict[str, Any] | None = None) -> config_entries.ConfigFlowResult:
        """Save authentication before setup polls vehicles; never resend SMS here."""
        if self._cn_login is None or not self._cn_sms_sent:
            return await self.async_step_cn_phone()
        errors = {}
        if user_input is not None:
            try:
                session_data = await self.hass.async_add_executor_job(self._cn_login.login, user_input["sms_code"])
            except LeapmotorAuthError:
                errors["base"] = "invalid_auth"
            except LeapmotorApiError:
                errors["base"] = "cannot_connect"
            else:
                account_id = session_data["accountId"]
                if self._cn_reauth_entry is not None:
                    entry = self._cn_reauth_entry
                    if entry.unique_id != f"cn-{account_id}":
                        return self.async_abort(reason="wrong_account")
                    try:
                        await Store(self.hass, 1, f"{DOMAIN_CN}.cn_session.{entry.entry_id}", private=True).async_save(session_data)
                    except Exception:
                        return self.async_show_form(step_id="cn_code", errors={"base": "session_save_failed"},
                                                    data_schema=vol.Schema({vol.Required("sms_code"): str}))
                    self._cn_login = None
                    return self.async_update_reload_and_abort(entry, data_updates={CONF_CN_SESSION: session_data, CONF_PROXY_URL: self._proxy_url},
                                                              options={**entry.options, CONF_PROXY_URL: self._proxy_url})
                await self.async_set_unique_id(f"cn-{account_id}")
                self._abort_if_unique_id_configured()
                self._cn_login = None
                return self.async_create_entry(title=f"Leapmotor CN ({account_id})", data={
                    CONF_REGION: REGION_CN, CONF_CN_SESSION: session_data, CONF_SCAN_INTERVAL: self._cn_interval, CONF_PROXY_URL: self._proxy_url,
                })
        return self.async_show_form(step_id="cn_code", errors=errors,
                                    data_schema=vol.Schema({vol.Required("sms_code"): str}))

    async def async_step_cn_import(self, user_input: dict[str, Any] | None = None) -> config_entries.ConfigFlowResult:
        """Import an already authorized CN gateway session; never requests SMS."""
        errors: dict[str, str] = {}
        if user_input is not None:
            try:
                try:
                    self._proxy_url = validate_proxy(user_input.get(CONF_PROXY_URL, ""))
                except ValueError:
                    return self.async_show_form(step_id="cn_import", data_schema=CN_SESSION_SCHEMA, errors={CONF_PROXY_URL: "invalid_proxy"})
                session_data = json.loads(user_input[CONF_CN_SESSION])
                client = CNReadOnlyApiClient(session_data, self._proxy_url)
                try:
                    result = await self.hass.async_add_executor_job(client.fetch_data)
                    session_data = client.session_snapshot()
                finally:
                    await self.hass.async_add_executor_job(client.close)
            except (json.JSONDecodeError, LeapmotorAuthError):
                errors["base"] = "invalid_auth"
            except LeapmotorApiError:
                errors["base"] = "api_refresh_failed"
            except Exception:  # noqa: BLE001
                errors["base"] = "unknown"
            else:
                account_id = str(session_data.get("accountId") or (session_data.get("gateway") or {}).get("accountId") or "cn")
                await self.async_set_unique_id(f"cn-{account_id}")
                self._abort_if_unique_id_configured()
                return self.async_create_entry(title=f"Leapmotor CN ({account_id})", data={CONF_REGION: REGION_CN, CONF_CN_SESSION: session_data, CONF_SCAN_INTERVAL: user_input[CONF_SCAN_INTERVAL], CONF_PROXY_URL: self._proxy_url})
        return self.async_show_form(step_id="cn_import", data_schema=CN_SESSION_SCHEMA, errors=errors)


    @staticmethod
    @callback
    def async_get_options_flow(config_entry):
        return CNOptionsFlow()


class CNOptionsFlow(config_entries.OptionsFlow):
    async def async_step_init(self, user_input=None):
        return await self.async_step_cn_init(user_input)

    async def async_step_cn_init(self, user_input=None):
        errors = {}
        if user_input is not None:
            try:
                proxy = validate_proxy(user_input.get(CONF_PROXY_URL, ""))
            except ValueError:
                errors[CONF_PROXY_URL] = "invalid_proxy"
            else:
                return self.async_create_entry(title="", data={**self.config_entry.options, CONF_PROXY_URL: proxy})
        current = self.config_entry.options.get(CONF_PROXY_URL, self.config_entry.data.get(CONF_PROXY_URL, ""))
        return self.async_show_form(step_id="cn_init", errors=errors, data_schema=vol.Schema({
            vol.Optional(CONF_PROXY_URL, default=current): PROXY_SELECTOR,
        }))
