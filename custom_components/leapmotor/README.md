# Leapmotor Home Assistant Integration

[![HACS Custom](https://img.shields.io/badge/HACS-Custom-orange.svg)](https://hacs.xyz/)
[![Open your Home Assistant instance and add this repository to HACS](https://my.home-assistant.io/badges/hacs_repository.svg)](https://my.home-assistant.io/redirect/hacs_repository/?owner=kerniger&repository=leapmotor-ha&category=integration)

Custom integration for Leapmotor vehicle data and the verified remote-control path.

## Disclaimer

This is an unofficial reverse-engineered integration. Use it at your own risk.
There is no warranty or liability for account restrictions, API changes, failed
commands, vehicle side effects, or any other consequence of using the
integration. Remote-control actions should only be tested deliberately and with
the vehicle in a safe state.

## Current Scope

This integration creates read-only entities plus remote-control buttons based on
the verified `operatePassword` flow.

Available data includes:

- Battery percentage
- Remaining range
- Odometer
- Static vehicle image from the official picture package (`carpic_for_tripsum.png`)
- Total mileage summary from the mileage/energy endpoint
- Consumption-screen data from the official app flow:
  cumulative energy, last-7-days mileage/energy, six-week average consumption,
  and last-week driving/climate/other energy split
- Vehicle lock as a native Home Assistant lock entity for remote lock/unlock
  actions
- Vehicle state as a readable status sensor
- Vehicle READY/ON3 diagnostic based on signal `1258`
- Charge-cable plugged-in state and active-charging state
- Door and trunk open states
- Window, skylight, gear, speed, battery temperature, PTC power, and range-mode
  diagnostics from APK-verified status signals
- Interior and climate target temperatures
- Climate mode and climate/heating activity diagnostics
- Seat heating/ventilation levels and mirror heating diagnostics
- Charge limit
- Scheduled charging window and recurrence details
- Scheduled charging flag as a read-only binary sensor
- Charging schedule switch for enabling/disabling the existing schedule while
  preserving start time, end time, recurrence, and charge limit
- One-touch vehicle preparation services for immediate and scheduled climate,
  front-seat comfort, steering-wheel heat, mirror heat, and optional navigation
  sync on supported vehicles
- Tire pressures
- GPS location as a Home Assistant device tracker
- Remote-control buttons for:
  - Charger unlock
  - Trunk
  - Find vehicle
  - Sunshade
  - Battery preheat
  - A/C switch
  - Quick cool
  - Quick heat
  - Windshield defrost
  - Window action
- Native Home Assistant service `leapmotor.send_destination` for sending a
  latitude/longitude destination to the vehicle navigation.
- Optional ABRP Generic Telemetry live-data push after successful vehicle polls.
- Native HA unit metadata for standard measurements; EV consumption is exposed
  as `kWh/100 km` plus optional `mi/kWh` when the API provides it.

## Install

### HACS

Fast path:

[![Open your Home Assistant instance and add this repository to HACS](https://my.home-assistant.io/badges/hacs_repository.svg)](https://my.home-assistant.io/redirect/hacs_repository/?owner=kerniger&repository=leapmotor-ha&category=integration)

Manual HACS path:

1. Open HACS in Home Assistant.
2. Open the three-dot menu and choose `Custom repositories`.
3. Add this repository URL:

   ```text
   https://github.com/kerniger/leapmotor-ha
   ```

4. Select `Integration` as the repository type and add it.
5. Search for `Leapmotor` in HACS and install it.
6. Restart Home Assistant.
7. Add the integration from:

   ```text
   Settings -> Devices & services -> Add integration -> Leapmotor
   ```

8. During setup, upload/paste the required certificate/key material. Uploaded
   or pasted values are stored as `/config/leapmotor/app_cert.pem` and
   `/config/leapmotor/app_key.pem`. Alternatively, place both files there
   before setup.

### Manual

Copy the complete folder:

```text
custom_components/leapmotor
```

into your Home Assistant config directory:

```text
config/custom_components/leapmotor
```

Restart Home Assistant, then add the integration from:

```text
Settings -> Devices & services -> Add integration -> Leapmotor
```

## Public Auth Model

This public repository does not ship app certificate material.

Normal setup path:

- install the Custom Component
- preferably create a second Leapmotor account and share the vehicle to it,
  because using the same account in Home Assistant and the official app can log
  the app out when the integration authenticates
- provide local files as `/config/leapmotor/app_cert.pem` and
  `/config/leapmotor/app_key.pem`, or upload/paste them during setup/options
- optionally enable ABRP live data and enter your ABRP Generic token
- configure the integration in Home Assistant

The component itself contains both parts needed at runtime: the Home Assistant
entity layer and the backend/auth layer. No separate add-on is required for the
normal user install path.

## Setup Fields

- Username / email: your Leapmotor account email
- Password: your Leapmotor account password
- App certificate file: required for login; upload it here or leave the field
  empty only if `/config/leapmotor/app_cert.pem` already exists.
- App private key file: required for login; upload it here or leave the field
  empty only if `/config/leapmotor/app_key.pem` already exists.
- App certificate/private key PEM: fallback paste fields if file upload is not
  convenient.
- Vehicle PIN: optional; leave empty for read-only mode. Remote-control actions
  usually require this PIN and stay unavailable without it. Sending a
  destination does not require the PIN, matching the observed app flow.
- Update interval: default `5` minutes
- Eco polling: optional; when enabled, polling switches to the slower eco
  interval only while every vehicle is locked, parked, and unplugged.
- Eco update interval: default `15` minutes
- ABRP live data: optional
- ABRP Generic token: optional; only required when ABRP live data is enabled

After setup, the Vehicle PIN, update interval, eco polling, and ABRP options can
be changed from the integration options without recreating the entry.

## State Freshness

- Lock state is treated conservatively. If the cloud vehicle timestamp is too
  old, the lock falls back to `unknown` instead of showing a stale unlocked
  state for hours.
- Lock state follows the app home-screen state using validated signal `1298`
  (`1=locked`, `0=unlocked`). Signal `47` is not used for lock state.
- Diagnostic lock-state sensors expose the source, age, and raw signal code so
  stale cloud data and temporary remote-control overrides are visible without
  opening the entity attributes.
- The `Lädt` binary sensor represents active charging only. Plugged-in but
  stopped/idle sessions stay `off`; use the separate charge-cable sensor for
  plugged-in state. The charging-connection sensor can additionally report
  `finished` when the backend still marks the cable as connected after charging
  has completed.
- Vehicle state and GPS location expose freshness metadata in entity
  attributes and diagnostics so stale backend data is easier to identify.
- Each vehicle also gets a `Refresh data` button to trigger an immediate poll
  outside the normal 5-minute interval.
- Optional eco polling can switch to a slower interval when all vehicles are
  clearly locked, parked, and unplugged. It switches back to the normal interval
  as soon as a vehicle is not clearly quiet, for example when plugged in,
  charging, unlocked, or moving.
- Home Assistant's own `x minutes ago` text often reflects the last state
  change, not the last successful poll. Use the `Last refresh` sensor to see
  when the integration actually fetched data successfully.

## ABRP Live Data

ABRP telemetry is optional and disabled by default. If enabled during setup or
in the integration options, the integration sends one Generic Telemetry update
after each successful Leapmotor poll.

Required ABRP options:

- ABRP Generic token from the ABRP vehicle live-data setup

The integration includes a default ABRP API key. Normal users only need their
vehicle-specific ABRP Generic token.

The submitted telemetry includes state of charge, estimated range, charging
state, odometer, speed fallback `0`, and GPS coordinates when available and not
marked stale.

## evcc

evcc can consume the Leapmotor data through Home Assistant entities. No
Leapmotor-specific evcc adapter is required for the basic vehicle data.

Recommended entities:

- `sensor.<vehicle>_battery`: state of charge
- `sensor.<vehicle>_range` or `sensor.<vehicle>_live_range`: remaining range
- `binary_sensor.<vehicle>_charge_cable_plugged_in`: cable/plug state
- `binary_sensor.<vehicle>_charging`: active charging only
- `sensor.<vehicle>_evcc_status`: IEC 61851 `A`/`B`/`C`
- `sensor.<vehicle>_charging_connection`: `unplugged`, `plugged_in`,
  `charging`, or `finished`
- `sensor.<vehicle>_charging_finish_time`: target-time helper while
  actively charging
- `sensor.<vehicle>_charging_power`: charging power
- `sensor.<vehicle>_charging_current` and
  `sensor.<vehicle>_charging_voltage`: raw electrical values
- `sensor.<vehicle>_odometer`: mileage

For automation logic:

- Charging `on` means confirmed active charging.
- Cable plugged in `on` plus charging `off` means plugged in but idle/stopped.
- Charging connection `finished` means the cable is still connected and the
  vehicle/backend reports completed or idle charging.
- Check the `Last refresh` sensor when decisions depend on freshness.

The Leapmotor backend is cloud-polled. For fast load control, use evcc or the
wallbox as the real-time electrical source and use Leapmotor mainly for SOC,
range, plug state, and vehicle diagnostics.

## Service Automation Examples

All vehicle-targeted services accept `vin` or a Leapmotor `entity_id`. Services
that physically control the vehicle require the Vehicle PIN except
`leapmotor.send_destination`, which follows the observed app flow without the
PIN.

Send a navigation destination:

```yaml
action: leapmotor.send_destination
data:
  entity_id: device_tracker.c10_location
  name: Bern Bahnhof
  address: Bahnhofplatz, Bern, Schweiz
  latitude: 46.94809
  longitude: 7.43914
```

Set charge limit:

```yaml
action: leapmotor.set_charge_limit
data:
  entity_id: number.c10_set_charge_limit
  charge_limit_percent: 85
```

Disable the charging schedule:

```yaml
action: switch.turn_off
target:
  entity_id: switch.c10_charging_schedule
```

Unlock the charger before unplugging:

```yaml
action: leapmotor.unlock_charger
data:
  entity_id: sensor.c10_battery
```

This requires the configured Vehicle PIN and uses the app-verified dedicated
charger-unlock command, not the normal vehicle door-unlock command.

Start quick climate heating:

```yaml
action: leapmotor.quick_heat
data:
  entity_id: sensor.c10_battery
```

Prepare the car immediately with cooling and seat ventilation:

```yaml
action: leapmotor.prepare_car
data:
  entity_id: sensor.c10_battery
  mode: cold
  temperature: 18
  fan_speed: 7
  recirculate: true
  driver_seat: ventilation
  driver_seat_level: 3
  passenger_seat: ventilation
  passenger_seat_level: 3
```

Partially open windows:

```yaml
action: leapmotor.windows_open
data:
  entity_id: sensor.c10_battery
  value: 30
```

Lock the vehicle from an automation:

```yaml
action: leapmotor.lock
data:
  entity_id: lock.c10_lock
```

Export redacted diagnostics:

```yaml
action: leapmotor.export_diagnostics
data:
  filename: leapmotor-diagnostics.json
```

## Runtime Notes

- Requires `curl` on the Home Assistant host/container.
- Requires Python packages from `manifest.json`: `cryptography` and `requests`.
- The integration uses the same API path verified by the reverse-engineered client.
- Most remote-control actions use the verified `operatePassword` flow and
  require a configured Vehicle PIN. `leapmotor.send_destination` uses the
  observed app flow without `operatePassword`.
- `leapmotor.windows_open` and `leapmotor.windows_close` accept optional
  `value` from `0` to `100` for partial window positioning.
- `leapmotor.sunshade_open` and `leapmotor.sunshade_close` accept optional
  `value` from `0` to `10` for partial sunshade positioning.
- The account certificate password is derived internally and is no longer asked
  during setup. Known captured values remain as fallback only.
- The integration exposes Home Assistant diagnostics with secrets redacted:
  account state, anonymized vehicle metadata, raw status codes, raw APK signal
  values, mileage summary, vehicle picture availability, last API codes, and
  last remote-control result.
- `leapmotor.export_diagnostics` writes the same anonymized support data to a
  JSON file under `/config/leapmotor`.
- Diagnostics include a compact redacted `support_summary` with vehicle count,
  model, endpoint path, signal count, charging connection state, polling mode,
  and last API result labels.
- The internal API primitives now live under `custom_components/leapmotor/leap_api`
  so Home Assistant entities are less coupled to auth, crypto, transport, and
  remote-command payload definitions.
- The vehicle image no longer uses the generic `shareBindUrl` CDN fallback. It
  now downloads the signed vehicle picture package once per picture key and
  serves the extracted static image from the local Home Assistant cache.
- A `Last vehicle action` sensor and action attributes show the latest
  remote-control status per VIN.
- The repository now includes HACS brand assets in `brand/icon.png` and
  `brand/logo.png`, plus local integration brand assets in
  `custom_components/leapmotor/brand/`. Home Assistant and browser caches can
  keep old brand assets for a while after installation or update.
- The current proof set is strongest on the C10. Main-account and shared-car
  handling are both implemented; feature availability may still vary by model,
  especially for climate, sunshade, trunk, and window actions.

## Known Limitations

- Unofficial cloud integration; Leapmotor can change authentication, endpoints,
  rate limits, or signal meanings at any time.
- User-provided app certificate material is required. The integration does not
  generate, bundle, fetch, or download backend-trusted certificates.
- The backend is polled, not streamed. Use `Last refresh` and diagnostic age
  attributes when automations depend on freshness.
- A second shared Leapmotor account is recommended because using the same
  account in Home Assistant and the official app can log the app out.
- Remote-control support depends on model, account rights, shared-car
  permissions, vehicle state, and backend policy.

## Related Projects

- [leapmotor-mate](https://github.com/ProtossBlaster/leapmotor-mate) is a
  self-hosted companion with trip tracking, charge logging, pricing, exports,
  schedules, and its own web UI. Use it when you want TeslaMate-style history;
  this integration stays focused on native Home Assistant entities and
  services.

## Troubleshooting

- HACS install: add the repository as type `Integration`, install `Leapmotor`,
  restart Home Assistant, then add the integration from Devices & services.
- HACS `No manifest.json`: verify the custom repository URL is
  `https://github.com/kerniger/leapmotor-ha` and not a subfolder or extracted
  ZIP path.
- Certificate errors: upload/paste both `app_cert.pem` and `app_key.pem`, or
  store them as `/config/leapmotor/app_cert.pem` and
  `/config/leapmotor/app_key.pem`.
- Login `code 21` / incorrect account or password: if the same credentials work
  in the official app, log out fully and change/reset the password once in the
  official Leapmotor app, then retry Home Assistant.
- Do not rely on certificate files inside `custom_components/leapmotor`; HACS
  can replace that folder during updates.
- App logout: use a second Leapmotor account and share the vehicle to it.
- Stale or unavailable entities: press `Refresh data`, check `Last refresh`,
  then run `leapmotor.export_diagnostics` and inspect the JSON under
  `/config/leapmotor`.

## Debug Logging

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
material first. For public issues, prefer `leapmotor.export_diagnostics`, which
redacts secrets and includes a compact `support_summary`.

## Certificate Retrieval

The integration cannot generate or retrieve backend-trusted app certificate
material and does not include or download it automatically. Users must provide
legitimate certificate/key material themselves, either as
`/config/leapmotor/app_cert.pem` and `/config/leapmotor/app_key.pem` or via the
setup/options upload fields. Without those files, login cannot complete
reliably. Community resources may discuss compatible PEM certificate/key
material, but users must review and decide themselves whether using such
material is acceptable for their setup.

## Special Thanks

Special thanks to [sbstn-0x2a](https://github.com/sbstn-0x2a) for validating and
sharing additional Leapmotor raw-signal mappings across charging, doors,
climate, heating, seating, range, and diagnostics.

Special thanks to [markoceri](https://github.com/markoceri) for the public
Leapmotor API and certificate research that helped cross-check this integration.

## Legal

See [LEGAL.md](../../LEGAL.md) and [SECURITY.md](../../SECURITY.md) before
publishing logs, diagnostics, or modified builds.

## License

MIT. See [LICENSE](../../LICENSE).
