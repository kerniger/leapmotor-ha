"""Offline phone authentication and config-flow safety checks; no live requests."""
import importlib
import importlib.util
import sys
import types
import unittest
from unittest.mock import AsyncMock, Mock, patch

from test_cn_ha_client import PACKAGE, ROOT, renewal_body

auth = importlib.import_module(f"{PACKAGE}.cn_auth")


class CNAuthenticationTests(unittest.TestCase):
    def test_number_formats(self):
        for number in ("13800000000", "+8613800000000", "008613800000000"):
            self.assertEqual(auth.normalize_phone(number), "13800000000")
        for number in ("+4913800000000", "123", "1380000000x"):
            with self.assertRaises(ValueError):
                auth.normalize_phone(number)

    @patch.object(auth.requests.Session, "request")
    def test_construction_never_sends_sms(self, request):
        auth.CNPhoneLogin("13800000000")
        request.assert_not_called()

    @patch.object(auth.requests.Session, "request")
    def test_sms_is_explicit_encrypted_and_throttled(self, request):
        request.return_value = Mock(status_code=200, json=Mock(return_value={"result": 0}))
        login = auth.CNPhoneLogin("13800000000")
        login.send_sms()
        self.assertNotEqual(request.call_args.kwargs["params"]["phoneNo"], login.phone)
        self.assertFalse(request.call_args.kwargs["allow_redirects"])
        with self.assertRaises(auth.LeapmotorApiError):
            login.send_sms()
        request.assert_called_once()

    @patch.object(auth.requests.Session, "request")
    def test_form_login_and_gateway_without_sms(self, request):
        request.side_effect = [
            Mock(status_code=200, json=Mock(return_value={"result": 0, "data": {"accountId": "test-account", "token": "legacy"}})),
            Mock(status_code=200, json=Mock(return_value=renewal_body())),
        ]
        login = auth.CNPhoneLogin("13800000000")
        state = login.login("001234")
        self.assertEqual(state["accountId"], "test-account")
        self.assertEqual(request.call_args_list[0].kwargs["data"]["smsCode"], "001234")
        self.assertNotIn("phone", state)
        self.assertNotIn("smsCode", state)
        self.assertIs(login.login("001234"), state)
        self.assertEqual(request.call_count, 2)
        self.assertTrue(all("sendmessagecode" not in call.args[1] for call in request.call_args_list))

    @patch.object(auth.requests.Session, "request")
    def test_invalid_code_never_requests(self, request):
        with self.assertRaises(auth.LeapmotorAuthError):
            auth.CNPhoneLogin("13800000000").login("invalid")
        request.assert_not_called()


class FlowBase:
    def __init_subclass__(cls, **kwargs):
        pass

    def async_show_form(self, **kwargs):
        return {"type": "form", **kwargs}

    def async_show_menu(self, **kwargs):
        return {"type": "menu", **kwargs}

    async def async_set_unique_id(self, unique_id):
        self.unique_id = unique_id

    def _abort_if_unique_id_configured(self):
        pass

    def async_create_entry(self, **kwargs):
        return {"type": "create_entry", **kwargs}

    def async_abort(self, **kwargs):
        return {"type": "abort", **kwargs}


class CNConfigFlowTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        homeassistant = types.ModuleType("homeassistant")
        entries = types.ModuleType("homeassistant.config_entries")
        entries.ConfigFlow = FlowBase
        entries.OptionsFlow = FlowBase
        homeassistant.config_entries = entries
        const = types.ModuleType("homeassistant.const")
        const.CONF_PASSWORD = "password"
        const.CONF_USERNAME = "username"
        core = types.ModuleType("homeassistant.core")
        core.HomeAssistant = object
        core.callback = lambda f: f
        selector = types.ModuleType("homeassistant.helpers.selector")
        selector.TextSelector = lambda config: str
        selector.TextSelectorConfig = dict
        selector.TextSelectorType = types.SimpleNamespace(PASSWORD="password")
        storage = types.ModuleType("homeassistant.helpers.storage")
        storage.Store = Mock()
        spec = importlib.util.spec_from_file_location(f"{PACKAGE}.config_flow", ROOT / "config_flow.py")
        self.module = importlib.util.module_from_spec(spec)
        with patch.dict(sys.modules, {"homeassistant": homeassistant, "homeassistant.config_entries": entries,
                                    "homeassistant.const": const, "homeassistant.core": core,
                                    "homeassistant.helpers.selector": selector, "homeassistant.helpers.storage": storage}):
            spec.loader.exec_module(self.module)
        self.flow = type("TestCNFlow", (self.module.LeapmotorCNConfigFlowMixin, FlowBase), {})()

        async def execute(function, *args):
            return function(*args)

        self.flow.hass = Mock(async_add_executor_job=execute)
        self.network = patch.object(auth.requests.Session, "request", side_effect=AssertionError("Unexpected network request"))
        self.network.start()
        self.addCleanup(self.network.stop)

    async def test_forms_and_unchecked_confirmation_never_send(self):
        self.assertEqual((await self.flow.async_step_cn())["type"], "menu")
        result = await self.flow.async_step_cn_phone({"phone": "13800000000"})
        self.assertEqual(result["step_id"], "cn_send_sms")
        await self.flow.async_step_cn_send_sms()
        await self.flow.async_step_cn_send_sms({"send_sms": False})

    async def test_explicit_confirmation_once(self):
        await self.flow.async_step_cn_phone({"phone": "13800000000"})
        self.flow._cn_login.send_sms = Mock()
        result = await self.flow.async_step_cn_send_sms({"send_sms": True})
        self.assertEqual(result["step_id"], "cn_code")
        await self.flow.async_step_cn_send_sms({"send_sms": True})
        self.flow._cn_login.send_sms.assert_called_once()

    async def test_failed_sms_does_not_retry_on_display(self):
        await self.flow.async_step_cn_phone({"phone": "13800000000"})
        self.flow._cn_login.send_sms = Mock(side_effect=auth.LeapmotorApiError("timeout"))
        result = await self.flow.async_step_cn_send_sms({"send_sms": True})
        self.assertEqual(result["errors"]["base"], "sms_request_failed")
        await self.flow.async_step_cn_send_sms()
        self.flow._cn_login.send_sms.assert_called_once()

    async def test_success_stores_session_not_phone_or_code(self):
        await self.flow.async_step_cn_phone({"phone": "13800000000"})
        self.flow._cn_sms_sent = True
        self.flow._cn_login.login = Mock(return_value={"accountId": "test-account", "gateway": {}})
        result = await self.flow.async_step_cn_code({"sms_code": "001234"})
        self.assertEqual(result["type"], "create_entry")
        self.assertEqual(self.flow.unique_id, "cn-test-account")
        self.assertNotIn("phone", result["data"])
        self.assertNotIn("sms_code", result["data"])

    async def test_reauth_wrong_account_is_rejected(self):
        await self.flow.async_step_cn_phone({"phone": "13800000000"})
        self.flow._cn_sms_sent = True
        self.flow._cn_reauth_entry = Mock(unique_id="cn-original")
        self.flow._cn_login.login = Mock(return_value={"accountId": "different", "gateway": {}})
        result = await self.flow.async_step_cn_code({"sms_code": "001234"})
        self.assertEqual(result["reason"], "wrong_account")


if __name__ == "__main__":
    unittest.main()
