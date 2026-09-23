"""Synthetic HA tests for the shared region entry and control boundary."""
import json
import types
import unittest
from unittest.mock import AsyncMock, Mock, patch

from homeassistant import config_entries
from homeassistant.exceptions import HomeAssistantError
import ha_cn_runtime as runtime


class RegionTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = runtime.RuntimeTests.asyncSetUp
    asyncTearDown = runtime.RuntimeTests.asyncTearDown
    request = runtime.RuntimeTests.request
    import_entry = runtime.RuntimeTests.import_entry

    async def test_region_menu_and_existing_oversea_onboarding(self):
        result = await self.hass.config_entries.flow.async_init('leapmotor', context={'source': 'user'})
        self.assertEqual(set(result['menu_options']), {'eu', 'cn'})
        with patch('custom_components.leapmotor.config_flow.has_app_certificate_material', return_value=False):
            result = await self.hass.config_entries.flow.async_configure(result['flow_id'], {'next_step_id': 'eu'})
        self.assertEqual(result['step_id'], 'certificates')
        self.assertEqual(self.calls, [])

    async def test_diagnostics_never_exports_cn_credentials(self):
        entry = await self.import_entry('http://synthetic-user:synthetic-password@proxy.invalid:8080')
        from custom_components.leapmotor.diagnostics import async_get_config_entry_diagnostics
        result = await async_get_config_entry_diagnostics(self.hass, entry)
        self.assertEqual(set(result), {'integration', 'read_only', 'last_update_success', 'vehicle_count'})
        encoded = json.dumps(result)
        for secret in ('initial-token', 'initial-refresh', 'synthetic-account', 'synthetic-password', 'proxy.invalid', 'dGVzdA=='):
            self.assertNotIn(secret, encoded)
        self.assertTrue(result['read_only'])

    async def test_all_remote_services_reject_cn_targets_without_eu_fallback(self):
        entry = await self.import_entry()
        self.assertFalse(self.hass.services.has_service('leapmotor', 'lock'))
        from custom_components import leapmotor
        eu_client = Mock()
        eu = types.SimpleNamespace(data={'vehicles': {'SYNTHETIC-EU': {}}}, client=eu_client)
        self.hass.data['leapmotor'] = {'synthetic-eu-entry': eu}
        await leapmotor._async_register_services(self.hass)
        services = dict(self.hass.services.async_services()['leapmotor'])
        extras = {
            'set_charge_limit': {'charge_limit_percent': 80},
            'set_climate': {'mode': 'cold'},
            'set_climate_schedule': {'start_time': '08:00'},
            'set_prepare_car_schedule': {'start_time': '08:00'},
            'send_destination': {'name': 'Synthetic', 'latitude': 0, 'longitude': 0},
        }
        with patch.object(leapmotor, 'async_execute_remote_action', new_callable=AsyncMock) as execute:
            for service in services:
                if service == 'export_diagnostics':
                    continue
                for target in ({'entity_id': 'sensor.leapmotor_china_b05_battery'}, {'vin': 'SYNTHETIC-B05'}):
                    with self.subTest(service=service, target=next(iter(target))):
                        with self.assertRaisesRegex(HomeAssistantError, 'China'):
                            await self.hass.services.async_call('leapmotor', service, {**extras.get(service, {}), **target}, blocking=True)
            execute.assert_not_awaited()
        self.assertEqual(eu_client.mock_calls, [])
        # Existing EU default targeting still executes against the EU coordinator.
        with patch.object(leapmotor, 'async_execute_remote_action', new_callable=AsyncMock) as execute:
            await self.hass.services.async_call('leapmotor', 'lock', {}, blocking=True)
            self.assertIs(execute.await_args.args[0], eu)
            self.assertEqual(execute.await_args.args[1], 'SYNTHETIC-EU')
        leapmotor._async_unregister_services(self.hass)

    async def test_entry_without_region_uses_existing_eu_lifecycle(self):
        from custom_components import leapmotor
        entry = Mock(entry_id='synthetic-eu', data={'username': 'synthetic', 'password': 'synthetic', 'device_id': 'synthetic'}, options={})
        coordinator = Mock(data={'vehicles': {}})
        coordinator.async_load_location_signs = AsyncMock()
        coordinator.async_config_entry_first_refresh = AsyncMock()
        coordinator.async_flush_location_signs = AsyncMock()
        with (
            patch.object(leapmotor, 'LeapmotorApiClient') as client,
            patch.object(leapmotor, 'LeapmotorDataUpdateCoordinator', return_value=coordinator),
            patch.object(leapmotor, '_async_ensure_vehicle_subentries'),
            patch.object(leapmotor, 'async_remove_obsolete_entities', new_callable=AsyncMock),
            patch.object(leapmotor, '_async_register_services', new_callable=AsyncMock),
            patch.object(config_entries.ConfigEntries, 'async_forward_entry_setups', new_callable=AsyncMock),
            patch.object(config_entries.ConfigEntries, 'async_unload_platforms', new_callable=AsyncMock, return_value=True),
        ):
            self.assertTrue(await leapmotor.async_setup_entry(self.hass, entry))
            client.assert_called_once()
            self.assertIs(self.hass.data['leapmotor'][entry.entry_id], coordinator)
            self.assertNotIn(entry.entry_id, self.hass.data.get('leapmotor_cn', {}))
            self.assertTrue(await leapmotor.async_unload_entry(self.hass, entry))
        self.assertEqual(self.calls, [])


if __name__ == '__main__':
    unittest.main()
