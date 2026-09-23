"""Offline HA regressions for diagnostics, entity defaults and service boundaries.

Run separately from unit tests: python tools/ha_issue_runtime.py -v
Requires Python 3.13 and homeassistant==2025.12.5; all vehicle data is synthetic.
"""
import asyncio
from copy import deepcopy
import json
from pathlib import Path
import shutil
import sys
import tempfile
import types
import unittest
from unittest.mock import Mock, patch

from homeassistant import auth, config_entries, loader
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers import area_registry, device_registry, entity_registry
from homeassistant.setup import async_setup_component

ROOT = Path(__file__).resolve().parents[1]
VIN = "SYNTHETIC-VIN-123456"


class IssueRuntimeTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix="leapmotor-issues-")
        self.addCleanup(self.directory.cleanup)
        target = Path(self.directory.name) / "custom_components" / "leapmotor"
        shutil.copytree(ROOT / "custom_components/leapmotor", target,
                        ignore=shutil.ignore_patterns("__pycache__"))
        package = types.ModuleType("custom_components")
        package.__path__ = [str(target.parent)]
        for name in list(sys.modules):
            if name == "custom_components" or name.startswith("custom_components."):
                del sys.modules[name]
        sys.modules["custom_components"] = package
        self.hass = HomeAssistant(self.directory.name)
        self.hass.config.skip_pip = True
        loader.async_setup(self.hass)
        self.hass.config_entries = config_entries.ConfigEntries(self.hass, {})
        await self.hass.config_entries.async_initialize()
        await asyncio.gather(area_registry.async_load(self.hass),
                             device_registry.async_load(self.hass),
                             entity_registry.async_load(self.hass))
        self.hass.auth = await auth.auth_manager_from_config(self.hass, [], [])
        await async_setup_component(self.hass, "persistent_notification", {})
        # No cloud/network access is needed by this regression suite.
        self.network = patch("socket.socket.connect", side_effect=AssertionError("Unexpected network access"))
        self.network.start()
        self.addCleanup(self.network.stop)
        from custom_components.leapmotor import api
        self.api = api
        self.client = api.LeapmotorApiClient(username="synthetic", password="synthetic")
        self.addCleanup(self.client.close)
        self.client._remote_control_raw = Mock(return_value={"code": 0})
        self.client._find_vehicle_by_vin = Mock(return_value=api.Vehicle(VIN, None, "T03", "Private Owner", False))
        self.client.get_vehicle_status = Mock()
        self.client.last_api_results = {"remote_control": {"remote_ctl_id": "private-remote-session"}}
        self.client.user_id = "private-user-123456"
        self.client.fetch_data = Mock()
        self.factory = patch("custom_components.leapmotor.LeapmotorApiClient", return_value=self.client)
        self.factory.start()
        self.addCleanup(self.factory.stop)

    async def asyncTearDown(self):
        await self.hass.async_stop(force=True)

    async def load_entry(self, model="T03"):
        vehicle = self.api.Vehicle(VIN, "private-car-123456", model, "Private Owner", False)
        self.client._find_vehicle_by_vin.return_value = vehicle
        result = self.api.normalize_vehicle(vehicle, {"data": {
            "signal": {"1204": 75, "1939": 0}, "ptcState": 2,
            "chargesocSetting": 95, "chargeTimeSetting": "00:00"}}, None)
        result["diagnostics"].update(available_energy_kwh=36.13, climate_air_direction=0)
        self.client.fetch_data.return_value = {"vehicles": {VIN: result}}
        entry = config_entries.ConfigEntry(
            domain="leapmotor", title="Private Account Title", version=1, minor_version=1,
            data={"username": "synthetic", "password": "private-password", "device_id": "synthetic",
                  "operation_password": "1234"}, options={}, unique_id="synthetic",
            source="user", discovery_keys=types.MappingProxyType({}), subentries_data=None,
        )
        await self.hass.config_entries.async_add(entry)
        await self.hass.async_block_till_done()
        self.assertEqual(entry.state, config_entries.ConfigEntryState.LOADED)
        return entry

    def assert_private_values_absent(self, result):
        encoded = json.dumps(result)
        for value in (VIN, "Private Owner", "Private Account Title", "private-remote-session",
                      "private-password", "private-car-123456", "private-user-123456"):
            self.assertNotIn(value, encoded)
        self.assertIn("***123456", encoded)

    async def test_both_diagnostics_exports_redact_nested_values_without_mutation(self):
        entry = await self.load_entry()
        from custom_components.leapmotor.diagnostics import async_get_config_entry_diagnostics
        coordinator = self.hass.data["leapmotor"][entry.entry_id]
        nested = {"items": [{"VIN": VIN, "nickname": "Private Owner",
                             "remoteCtlId": "private-remote-session"}]}
        coordinator.data["vehicles"][VIN]["history"]["nested"] = nested
        coordinator.client.last_api_results["nested"] = nested
        before = deepcopy(coordinator.data)
        result = await async_get_config_entry_diagnostics(self.hass, entry)
        self.assert_private_values_absent(result)
        self.assertEqual(result["entry"]["vehicle_subentries"][0]["title"], "**REDACTED**")
        self.assertEqual(result["entry"]["vehicle_subentries"][0]["data"]["vin"], "***123456")
        await self.hass.services.async_call("leapmotor", "export_diagnostics",
                                          {"filename": "issue-regression.json"}, blocking=True)
        exported = json.loads((Path(self.directory.name) / "leapmotor/issue-regression.json").read_text())
        self.assert_private_values_absent(exported)
        self.assertEqual(coordinator.data, before)

    async def test_t03_controls_absent_and_service_rejected(self):
        entry = await self.load_entry()
        registry = entity_registry.async_get(self.hass)
        self.assertIsNone(registry.async_get_entity_id("number", "leapmotor", f"{VIN}_charge_limit_setting"))
        self.assertIsNone(registry.async_get_entity_id("switch", "leapmotor", f"{VIN}_charging_schedule"))
        limit_id = registry.async_get_entity_id("sensor", "leapmotor", f"{VIN}_charge_limit_percent")
        self.assertEqual(self.hass.states.get(limit_id).state, "95")
        with self.assertRaisesRegex(HomeAssistantError, "not supported.*T03"):
            await self.hass.services.async_call("leapmotor", "set_charge_limit",
                {"vin": VIN, "charge_limit_percent": 80}, blocking=True)
        self.client.get_vehicle_status.assert_not_called()
        self.client._remote_control_raw.assert_not_called()

    async def test_other_models_keep_controls(self):
        await self.load_entry("B10")
        registry = entity_registry.async_get(self.hass)
        self.assertIsNotNone(registry.async_get_entity_id("number", "leapmotor", f"{VIN}_charge_limit_setting"))
        self.assertIsNotNone(registry.async_get_entity_id("switch", "leapmotor", f"{VIN}_charging_schedule"))

    async def test_sensor_classes_raw_defaults_and_user_override_survive_reload(self):
        entry = await self.load_entry()
        from homeassistant.components.sensor.const import DEVICE_CLASS_STATE_CLASSES, SensorDeviceClass, SensorStateClass
        self.assertIn(SensorStateClass.MEASUREMENT, DEVICE_CLASS_STATE_CLASSES[SensorDeviceClass.ENERGY_STORAGE])
        registry = entity_registry.async_get(self.hass)
        energy_id = registry.async_get_entity_id("sensor", "leapmotor", f"{VIN}_available_energy_kwh")
        energy = self.hass.states.get(energy_id)
        self.assertEqual(energy.state, "36.13")
        self.assertEqual(energy.attributes["device_class"], "energy_storage")
        self.assertEqual(energy.attributes["state_class"], "measurement")
        for suffix in ("ptc_state", "climate_air_direction"):
            entity_id = registry.async_get_entity_id("sensor", "leapmotor", f"{VIN}_{suffix}")
            self.assertEqual(registry.async_get(entity_id).disabled_by, entity_registry.RegistryEntryDisabler.INTEGRATION)
            self.assertIsNone(self.hass.states.get(entity_id))
        ptc_id = registry.async_get_entity_id("sensor", "leapmotor", f"{VIN}_ptc_state")
        registry.async_update_entity(ptc_id, disabled_by=None)
        await self.hass.config_entries.async_reload(entry.entry_id)
        await self.hass.async_block_till_done()
        self.assertIsNone(registry.async_get(ptc_id).disabled_by)
        self.assertEqual(self.hass.states.get(ptc_id).state, "2")
        mode_id = registry.async_get_entity_id("sensor", "leapmotor", f"{VIN}_ac_operation_mode")
        mode = self.hass.states.get(mode_id)
        self.assertEqual(mode.state, "auto")
        self.assertEqual(mode.attributes["device_class"], "enum")
        self.assertEqual(mode.attributes["options"], ["auto", "manual"])


if __name__ == "__main__":
    unittest.main()
