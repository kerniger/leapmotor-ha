"""Real HA lifecycle tests with synthetic transport; run separately from stub tests.

Requires homeassistant==2025.12.5 on Python 3.13. No cloud calls or credentials.
Run: python tools/ha_cn_runtime.py -v
"""
import asyncio
import base64
import json
from pathlib import Path
import shutil
import sys
import tempfile
import time
import types
import unittest
from unittest.mock import AsyncMock, Mock, patch

from homeassistant import config_entries, loader
from homeassistant.core import HomeAssistant
from homeassistant.helpers import area_registry, device_registry, entity_registry
from homeassistant.setup import async_setup_component

ROOT = Path(__file__).resolve().parents[1]


class RuntimeTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix="cn-ha-runtime-")
        self.addCleanup(self.directory.cleanup)
        target = Path(self.directory.name) / "custom_components" / "leapmotor"
        shutil.copytree(ROOT / "custom_components/leapmotor", target,
                        ignore=shutil.ignore_patterns("__pycache__"))
        package = types.ModuleType("custom_components")
        package.__path__ = [str(target.parent)]
        modules = {name: module for name, module in sys.modules.items()
                   if name == "custom_components" or name.startswith("custom_components.")}
        for name in modules:
            del sys.modules[name]
        sys.modules["custom_components"] = package
        def restore_modules():
            for name in list(sys.modules):
                if name == "custom_components" or name.startswith("custom_components."):
                    del sys.modules[name]
            sys.modules.update(modules)
        self.addCleanup(restore_modules)
        self.hass = HomeAssistant(self.directory.name)
        self.hass.config.skip_pip = True
        loader.async_setup(self.hass)
        self.hass.config_entries = config_entries.ConfigEntries(self.hass, {})
        await self.hass.config_entries.async_initialize()
        await asyncio.gather(area_registry.async_load(self.hass),
                             device_registry.async_load(self.hass),
                             entity_registry.async_load(self.hass))
        await async_setup_component(self.hass, "persistent_notification", {})
        self.calls = []
        self.expired = False
        self.car_type = "B05"
        self.collect_time = None
        self.signal_map = {
            "1204": 75, "100003": 74.8, "2188": 320, "3257": 400,
            "1318": 1234, "1319": 0, "1349": 22, "1177": 430.6,
            "1178": -12.8, "1182": 28, "1200": 90, "2646": 247,
            "2653": 242, "2660": 247, "2667": 244, "2183": 25,
            "1149": 1, "3636": 0,
            "47": 1, "1197": 0, "3736": False,
        }
        self.transport = patch("requests.sessions.Session.request", side_effect=self.request)
        self.transport.start()
        self.addCleanup(self.transport.stop)

    async def asyncTearDown(self):
        await self.hass.async_stop(force=True)

    def request(self, method, url, **kwargs):
        self.calls.append(url)
        if url.endswith("/token/v1/refresh"):
            claims = base64.urlsafe_b64encode(json.dumps({"exp": time.time() + 3600}).encode()).decode().rstrip("=")
            data = {"accessToken": f"header.{claims}.dGVzdA", "refreshToken": "rotated-refresh",
                    "signParam": {"r2": "AAAAAA==", "r3": "AAAAAA=="}}
            self.expired = False
        elif self.expired:
            return Mock(status_code=200, json=Mock(return_value={"code": 302002004}))
        elif url.endswith("/vehicle/list"):
            data = {"bindcars": [{"vin": "SYNTHETIC-B05", "carType": self.car_type}], "sharedcars": []}
        elif url.endswith("/vehicle/getCarRoute"):
            data = {"appRegion": "https://appgateway.leapmotor.com"}
        elif url.endswith("/signal/info/query"):
            data = {"signalMap": self.signal_map, "collectTime": self.collect_time}
        else:
            raise AssertionError(f"Unexpected endpoint: {url}")
        return Mock(status_code=200, json=Mock(return_value={"code": 0, "data": data}))

    async def import_entry(self, proxy=""):
        result = await self.hass.config_entries.flow.async_init("leapmotor", context={"source": "user"})
        self.assertEqual(result["type"], "menu")
        result = await self.hass.config_entries.flow.async_configure(result["flow_id"], {"next_step_id": "cn"})
        self.assertEqual(result["type"], "menu")
        result = await self.hass.config_entries.flow.async_configure(result["flow_id"], {"next_step_id": "cn_import"})
        session = {"deviceId": "synthetic-device", "gateway": {"accountId": "synthetic-account",
                   "accessToken": "initial-token", "refreshToken": "initial-refresh", "signKeyBase64": "dGVzdA=="}}
        result = await self.hass.config_entries.flow.async_configure(result["flow_id"],
                         {"cn_session": json.dumps(session), "scan_interval": 5, "proxy_url": proxy})
        self.assertEqual(result["type"], "create_entry", result)
        await self.hass.async_block_till_done()
        entry = result["result"]
        self.assertEqual(entry.state, config_entries.ConfigEntryState.LOADED)
        return entry

    async def test_import_sensors_renewal_reload_remove(self):
        entry = await self.import_entry()
        states = self.hass.states.async_all("sensor")
        self.assertEqual(len(states), 17)
        self.assertEqual(sorted(float(s.state) for s in states if s.state != "charging"), [
            -12.8, 0, 22, 25, 28, 74.8, 75, 90, 242, 244, 247, 247, 320, 400,
            430.6, 1234,
        ])
        binary_states = self.hass.states.async_all("binary_sensor")
        self.assertEqual(len(binary_states), 4)
        self.assertEqual(sorted(state.state for state in binary_states), ["off", "off", "off", "on"])
        coordinator = self.hass.data["leapmotor_cn"][entry.entry_id]
        self.expired = True
        await coordinator.async_refresh()
        self.assertTrue(coordinator.last_update_success)
        saved = await coordinator.store.async_load()
        self.assertEqual(saved["gateway"]["refreshToken"], "rotated-refresh")
        store_file = Path(self.directory.name) / ".storage" / f"leapmotor_cn.cn_session.{entry.entry_id}"
        self.assertEqual(store_file.stat().st_mode & 0o777, 0o600)
        self.assertTrue(await self.hass.config_entries.async_reload(entry.entry_id))
        await self.hass.async_block_till_done()
        renewed = self.hass.data["leapmotor_cn"][entry.entry_id]
        self.assertIsNot(renewed, coordinator)
        self.assertEqual(renewed.client.session_snapshot()["gateway"]["refreshToken"], "rotated-refresh")
        self.assertEqual(sum(url.endswith("/token/v1/refresh") for url in self.calls), 1)
        await self.hass.config_entries.async_remove(entry.entry_id)
        await self.hass.async_block_till_done()
        self.assertFalse(store_file.exists())
        self.assertNotIn(entry.entry_id, self.hass.data["leapmotor_cn"])

    async def test_non_b05_creates_only_confirmed_signal_entities(self):
        self.car_type = "C11"
        self.signal_map = {"1204": 64, "1318": 9876, "999999": 1}
        await self.import_entry()
        states = self.hass.states.async_all("sensor")
        self.assertEqual(len(states), 2)
        self.assertEqual(sorted(float(state.state) for state in states), [64, 9876])
        device = device_registry.async_get(self.hass).async_get_device(
            identifiers={("leapmotor_cn", "SYNTHETIC-B05")})
        self.assertIsNotNone(device)
        self.assertEqual(device.model, "C11")

    async def test_proxy_persists_and_options_reload_only_cn(self):
        proxy = "http://synthetic-user:synthetic-password@proxy.invalid:3128"
        entry = await self.import_entry(proxy)
        coordinator = self.hass.data["leapmotor_cn"][entry.entry_id]
        self.assertEqual(coordinator.client.session.proxies, {"http": proxy, "https": proxy})
        self.assertFalse(coordinator.client.session.trust_env)
        flow = await self.hass.config_entries.options.async_init(entry.entry_id)
        bad = await self.hass.config_entries.options.async_configure(flow["flow_id"], {"proxy_url": "socks5://invalid"})
        self.assertEqual(bad["errors"], {"proxy_url": "invalid_proxy"})
        changed = "http://second.invalid:8080"
        result = await self.hass.config_entries.options.async_configure(flow["flow_id"], {"proxy_url": changed})
        self.assertEqual(result["type"], "create_entry")
        await self.hass.async_block_till_done()
        self.assertEqual(self.hass.data["leapmotor_cn"][entry.entry_id].client.session.proxies["https"], changed)
        flow = await self.hass.config_entries.options.async_init(entry.entry_id)
        await self.hass.config_entries.options.async_configure(flow["flow_id"], {"proxy_url": ""})
        await self.hass.async_block_till_done()
        self.assertEqual(self.hass.data["leapmotor_cn"][entry.entry_id].client.session.proxies, {})

    async def test_failed_storage_blocks_reads_until_credentials_saved(self):
        entry = await self.import_entry()
        coordinator = self.hass.data["leapmotor_cn"][entry.entry_id]
        self.expired = True
        with patch.object(coordinator.store, "async_save", AsyncMock(side_effect=OSError("disk full"))):
            await coordinator.async_refresh()
            self.assertFalse(coordinator.last_update_success)
            self.assertTrue(coordinator.client.session_pending_save)
            self.assertTrue(self.calls[-1].endswith("/token/v1/refresh"))
            count = len(self.calls)
            await coordinator.async_refresh()
            self.assertEqual(len(self.calls), count)
        await coordinator.async_refresh()
        self.assertTrue(coordinator.last_update_success)
        self.assertFalse(coordinator.client.session_pending_save)
        self.assertEqual(sum(url.endswith("/token/v1/refresh") for url in self.calls), 1)

    async def test_phone_forms_never_send_without_confirmation(self):
        result = await self.hass.config_entries.flow.async_init("leapmotor", context={"source": "user"})
        result = await self.hass.config_entries.flow.async_configure(result["flow_id"], {"next_step_id": "cn"})
        result = await self.hass.config_entries.flow.async_configure(result["flow_id"], {"next_step_id": "cn_phone"})
        result = await self.hass.config_entries.flow.async_configure(result["flow_id"], {"phone": "13800000000", "scan_interval": 5})
        self.assertEqual(result["step_id"], "cn_send_sms")
        result = await self.hass.config_entries.flow.async_configure(result["flow_id"], {"send_sms": False})
        self.assertEqual(result["step_id"], "cn_send_sms")
        self.assertEqual(self.calls, [])

    async def test_missing_and_invalid_values_become_unknown(self):
        entry = await self.import_entry()
        coordinator = self.hass.data["leapmotor_cn"][entry.entry_id]
        data = coordinator.data
        data["vehicles"]["SYNTHETIC-B05"]["status"] = {
            "battery_percent": 101, "remaining_range_km": "nan", "odometer_km": True}
        coordinator.async_set_updated_data(data)
        await self.hass.async_block_till_done()
        self.assertEqual([s.state for s in self.hass.states.async_all("sensor")], ["unknown"] * 17)
        coordinator.async_set_updated_data({"vehicles": {}})
        await self.hass.async_block_till_done()
        self.assertEqual([s.state for s in self.hass.states.async_all("sensor")], ["unavailable"] * 17)

    async def test_charge_binaries_are_strict_and_signal_gated(self):
        entry = await self.import_entry()
        coordinator = self.hass.data["leapmotor_cn"][entry.entry_id]
        data = coordinator.data
        data["vehicles"]["SYNTHETIC-B05"]["charging"] = {
            "ac_input": None, "dc_input": True, "charge_completed": None,
        }
        coordinator.async_set_updated_data(data)
        await self.hass.async_block_till_done()
        states = {state.attributes["friendly_name"]: state.state for state in self.hass.states.async_all("binary_sensor")}
        self.assertEqual(states["Leapmotor China B05 AC charging input"], "unknown")
        self.assertEqual(states["Leapmotor China B05 DC charging input"], "on")
        self.assertEqual(states["Leapmotor China B05 Charge completed"], "unknown")

    async def test_late_signals_share_device_and_survive_reload_without_duplicates(self):
        self.car_type = "T03"
        self.signal_map = {"1204": 64, "2188": 999}
        entry = await self.import_entry()
        self.assertEqual(len(self.hass.states.async_all("sensor")), 1)
        coordinator = self.hass.data["leapmotor_cn"][entry.entry_id]
        self.collect_time = time.time() * 1000
        self.signal_map.update({"3260": 123, "1149": 2, "1277": 1, "47": 0})
        await coordinator.async_refresh()
        await self.hass.async_block_till_done()
        states = self.hass.states.async_all()
        ids = {s.entity_id for s in states if s.entity_id.startswith(("sensor.", "binary_sensor.", "button."))}
        self.assertEqual(len(ids), 7)
        self.assertEqual(self.hass.states.get("sensor.leapmotor_china_t03_range").state, "123.0")
        registry = entity_registry.async_get(self.hass)
        device_ids = {registry.async_get(entity_id).device_id for entity_id in ids}
        self.assertEqual(len(device_ids), 1)
        self.assertNotIn(None, device_ids)
        for _ in range(2):
            await coordinator.async_refresh()
            await self.hass.async_block_till_done()
        self.assertEqual(len(entity_registry.async_entries_for_config_entry(registry, entry.entry_id)), 7)
        self.assertTrue(await self.hass.config_entries.async_reload(entry.entry_id))
        await self.hass.async_block_till_done()
        self.assertFalse(coordinator._listeners)
        self.assertEqual({e.entity_id for e in entity_registry.async_entries_for_config_entry(registry, entry.entry_id)}, ids)

    async def test_all_proven_charge_codes_reach_ha(self):
        entry = await self.import_entry()
        coordinator = self.hass.data["leapmotor_cn"][entry.entry_id]
        for code, state in ((0, "unplugged"), (1, "charging"), (2, "completed"),
                            (3, "fault"), (4, "scheduled_waiting"), (6, "paused"),
                            (5, "unknown"), (True, "unknown"), ([], "unknown")):
            self.signal_map["1149"] = code
            await coordinator.async_refresh()
            self.assertEqual(self.hass.states.get("sensor.leapmotor_china_b05_charge_state").state, state)
            completed = "unknown" if state == "unknown" else "on" if code == 2 else "off"
            self.assertEqual(self.hass.states.get("binary_sensor.leapmotor_china_b05_charge_completed").state, completed)

    async def test_door_freshness_expiry_missing_vehicle_and_timer_cleanup(self):
        self.signal_map.update({"1277": 1, "1278": 0, "1279": 2, "1280": True})
        self.collect_time = time.time()
        timers = []
        def schedule(hass, delay, action):
            cancel = Mock()
            timers.append((delay, action, cancel))
            return cancel
        # Import the platform before setup so the expiry scheduler can be observed.
        from custom_components.leapmotor.cn import binary_sensor as platform
        with patch.object(platform, "async_call_later", side_effect=schedule):
            entry = await self.import_entry()
            coordinator = self.hass.data["leapmotor_cn"][entry.entry_id]
            door_id = "binary_sensor.leapmotor_china_b05_driver_door"
            self.assertEqual(self.hass.states.get(door_id).state, "on")
            self.assertEqual(self.hass.states.get("binary_sensor.leapmotor_china_b05_passenger_door").state, "off")
            self.assertEqual(self.hass.states.get("binary_sensor.leapmotor_china_b05_left_rear_door").state, "unknown")
            self.assertEqual(len(timers), 4)
            self.assertTrue(all(890 < delay <= 901 for delay, _, _ in timers))
            with patch("custom_components.leapmotor.cn.freshness.time.time", return_value=self.collect_time + 901):
                for _, action, _ in timers:
                    action(None)
                self.assertEqual(self.hass.states.get(door_id).state, "unknown")
            self.collect_time = None
            await coordinator.async_refresh()
            self.assertEqual(self.hass.states.get(door_id).state, "unknown")
            self.collect_time = time.time()
            await coordinator.async_refresh()
            self.assertEqual(self.hass.states.get(door_id).state, "on")
            latest = timers[-4:]
            coordinator.async_set_updated_data({"vehicles": {}})
            self.assertEqual(self.hass.states.get(door_id).state, "unavailable")
            self.assertTrue(all(cancel.called for _, _, cancel in latest))
            await coordinator.async_refresh()
            latest = timers[-4:]
            self.assertTrue(await self.hass.config_entries.async_unload(entry.entry_id))
            self.assertTrue(all(cancel.called for _, _, cancel in latest))

    async def test_manual_refresh_button(self):
        entry = await self.import_entry()
        button_id = "button.leapmotor_china_b05_refresh"
        self.assertIsNotNone(self.hass.states.get(button_id))
        coordinator = self.hass.data["leapmotor_cn"][entry.entry_id]

        initial_calls = len(self.calls)
        await self.hass.services.async_call("button", "press", {"entity_id": button_id}, blocking=True)
        await self.hass.async_block_till_done()

        # Verify a refresh was triggered (i.e. more calls to the API)
        self.assertGreater(len(self.calls), initial_calls)
        self.assertTrue(coordinator.last_update_success)


if __name__ == "__main__":
    unittest.main()
