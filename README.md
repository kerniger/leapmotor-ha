# leapmotor-ha

[![HACS Custom](https://img.shields.io/badge/HACS-Custom-orange.svg)](https://hacs.xyz/)
[![Open your Home Assistant instance and add this repository to HACS](https://my.home-assistant.io/badges/hacs_repository.svg)](https://my.home-assistant.io/redirect/hacs_repository/?owner=kerniger&repository=leapmotor-ha&category=integration)

Unofficial Home Assistant integration for Leapmotor vehicles.

This repository is the cleaned public version of the project. It does not
contain client certificates, private keys, captured tokens, account data,
research logs, or reverse-engineering workfiles.

## Features

- Vehicle state, battery, range, odometer, charging, doors, windows, lock, GPS
  tracker, tire pressure, diagnostics, and mileage/energy history.
- Native Home Assistant entities for sensors, binary sensors, lock, buttons,
  number, image, and device tracker.
- Remote-control services for supported actions such as lock/unlock, charger
  unlock, climate, windows, trunk, sunshade, charge limit, and send destination
  to navigation.
- Optional ABRP Generic Telemetry push and EVCC helper sensors.
- Multi-vehicle support for main-account and shared vehicles.
- Redacted diagnostics export for support.
- Native HA unit metadata for standard measurements; EV consumption is exposed
  as `kWh/100 km` plus optional `mi/kWh` when the API provides it.
- HACS-ready release ZIP, translations, and brand assets.

## Important

- This is an unofficial project and is not affiliated with or endorsed by
  Leapmotor.
- Use at your own risk. No liability is accepted for account restrictions, API
  changes, failed commands, vehicle side effects, or any other consequence.
- Remote-control actions should only be used deliberately and in a safe vehicle
  state.
- Leapmotor can change the API at any time.

## Requirements

The current login path requires Leapmotor app client certificate material:

- `/config/leapmotor/app_cert.pem`
- `/config/leapmotor/app_key.pem`

These files are not included in this repository. Upload/paste them during setup
or place them in `/config/leapmotor` before setup. Uploaded/pasted files are
stored outside `custom_components`, so HACS updates do not remove them.

The certificate files are app/client-level material, not user-specific account
credentials. Your normal Leapmotor username/password are still required. A
Vehicle PIN is optional and only needed for remote-control actions.

Recommended account setup: create a second Leapmotor account, share the vehicle
to it in the official app, and use that shared account in Home Assistant. Using
the same account in Home Assistant and the mobile app can log the app out.

## Installation

### HACS

Fast path:

[![Open your Home Assistant instance and add this repository to HACS](https://my.home-assistant.io/badges/hacs_repository.svg)](https://my.home-assistant.io/redirect/hacs_repository/?owner=kerniger&repository=leapmotor-ha&category=integration)

Manual path:

1. Open HACS in Home Assistant.
2. Open `Custom repositories`.
3. Add `https://github.com/kerniger/leapmotor-ha` as type `Integration`.
4. Install `Leapmotor`.
5. Restart Home Assistant.
6. Add the `Leapmotor` integration from `Settings -> Devices & services`.
7. Upload/paste `app_cert.pem` and `app_key.pem`, or place them in
   `/config/leapmotor` before setup.

### Manual

Copy `custom_components/leapmotor` to
`/config/custom_components/leapmotor`, restart Home Assistant, then add the
integration from `Settings -> Devices & services`.

## Configuration

- Email/password are required.
- App certificate/private key are required.
- Vehicle PIN is optional for setup; without it the integration stays read-only.
- ABRP live data is optional and only needs the ABRP Generic Token.
- Update interval is configurable.
- Optional eco polling can slow cloud polling when every vehicle is clearly
  locked, parked, and unplugged.
- If multiple vehicles are available, entities are created per VIN and services
  can target a vehicle by `vin` or a Leapmotor `entity_id`.

## Notes

- The backend is polled, not streamed. Values can lag behind the real vehicle
  state; use the `Last refresh` sensor and diagnostic age attributes for
  automation decisions.
- Entity IDs are intentionally stable English slugs. Display names follow the
  Home Assistant language where translations are available.
- Existing unavailable entities from older versions are not removed
  automatically. Remove/re-add the integration if you want a clean entity set.
- GPS sign issues are corrected only when the mirrored coordinate is clearly
  closer to the configured Home Assistant home location. This covers known
  southern-latitude and western-longitude backend sign issues.
- Optional eco polling never wakes or controls the vehicle. It only changes how
  often the cloud backend is polled while the current vehicle state is clearly
  quiet.

## Services

The integration exposes services under `leapmotor.*`. Vehicle-targeted services
accept either `vin` or an existing Leapmotor `entity_id`. Services that
physically control the vehicle require the configured Vehicle PIN. Availability
can still depend on model, vehicle state, account rights, and shared-car
permissions.

| Service | PIN required | Purpose |
|---|---:|---|
| `leapmotor.lock` / `leapmotor.unlock` | yes | Lock or unlock vehicle |
| `leapmotor.unlock_charger` | yes | Unlock the charging connector before unplugging |
| `leapmotor.trunk_open` / `leapmotor.trunk_close` | yes | Open or close trunk |
| `leapmotor.find_car` | yes | Find vehicle |
| `leapmotor.windows_open` / `leapmotor.windows_close` | yes | Open or close windows |
| `leapmotor.sunshade_open` / `leapmotor.sunshade_close` | yes | Open or close sunshade |
| `leapmotor.ac_switch` | yes | Climate off |
| `leapmotor.quick_cool` / `leapmotor.quick_heat` | yes | Climate quick profiles |
| `leapmotor.windshield_defrost` | yes | Windshield defrost |
| `leapmotor.battery_preheat` | yes | Battery preheat |
| `leapmotor.set_charge_limit` | yes | Set charge limit |
| `leapmotor.send_destination` | no | Send destination to navigation |
| `leapmotor.export_diagnostics` | no | Write redacted support JSON |

`windows_open` / `windows_close` accept optional `value` from `0` to `100`.
`sunshade_open` / `sunshade_close` accept optional `value` from `0` to `10`.
`set_charge_limit` requires `charge_limit_percent`. `send_destination` requires
`name`, `latitude`, and `longitude`; `address` is optional.

Unlock the charging connector:

```yaml
action: leapmotor.unlock_charger
data:
  entity_id: sensor.c10_battery
```

Send a destination:

```yaml
action: leapmotor.send_destination
data:
  entity_id: device_tracker.c10_location
  name: Bern Bahnhof
  address: Bahnhofplatz, Bern, Schweiz
  latitude: 46.94809
  longitude: 7.43914
```

## ABRP And EVCC

ABRP telemetry is optional and disabled by default. Enable `ABRP live data`
during setup and enter the ABRP Generic Token from the ABRP vehicle live-data
setup. Users do not need to request or enter an ABRP API key.

EVCC can consume the Leapmotor data through Home Assistant entities. Useful
entities include SOC, range, plug state, active charging, `evcc_status`,
charging power/current/voltage, odometer, and `charging_finish_time`.

## Troubleshooting

- HACS install issues: add this repository as type `Integration`, then restart
  Home Assistant.
- Certificate/login issues: upload/paste both PEM files or place them as
  `/config/leapmotor/app_cert.pem` and `/config/leapmotor/app_key.pem`.
- App logout issues: use a second Leapmotor account with shared vehicle access.
- Stale or wrong values: press `Refresh data`, check `Last refresh`, then run
  `leapmotor.export_diagnostics`.
- Remote-control issues: configure the Vehicle PIN and check account/shared-car
  permissions.

### Debug Logging

For troubleshooting, enable Home Assistant debug logging for the integration:

```yaml
logger:
  default: info
  logs:
    custom_components.leapmotor: debug
```

Restart Home Assistant after changing `configuration.yaml`. Debug logs are
intended to show sanitized API status codes, polling mode changes, update
reasons, and integration flow details. Do not publish full Home Assistant logs
without checking them for account data, VINs, locations, tokens, or certificate
material first. For public issues, prefer `leapmotor.export_diagnostics`.

## Screenshots

Installation screenshots are stored in `docs/screenshots`.

## Special Thanks

Special thanks to [sbstn-0x2a](https://github.com/sbstn-0x2a) for validating and
sharing additional Leapmotor raw-signal mappings across charging, doors,
climate, heating, seating, range, and diagnostics.

Special thanks to [markoceri](https://github.com/markoceri) for the public
Leapmotor API and certificate research that helped cross-check this integration.

## Legal

See [LEGAL.md](LEGAL.md) and [SECURITY.md](SECURITY.md) before publishing logs,
diagnostics, or modified builds.

## License

MIT. See [LICENSE](LICENSE).
