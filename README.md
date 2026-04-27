# leapmotor-ha

Unofficial Home Assistant integration for Leapmotor vehicles.

## Status

This repository is the cleaned public version of the project.

It intentionally does not contain:

- client certificates
- private keys
- captured tokens
- account data
- research logs
- reverse-engineering workfiles

## Features

- Home Assistant custom integration
- Read-only vehicle entities
- Static vehicle image entity from the official vehicle picture package
- Mileage/energy-history summary sensors
- Native Home Assistant lock entity
- Device tracker from vehicle GPS position
- Remote-control buttons for supported actions
- Native Home Assistant services for supported remote actions
- Send destination to vehicle navigation via Home Assistant service
- Optional ABRP Generic Telemetry live-data push
- Options flow for vehicle PIN and update interval
- Redacted diagnostics export
- Multi-language translations
- Multi-vehicle support for main-account and shared-car vehicles
- Single Custom Component package with entity layer and backend/auth layer

## Important

- Unofficial project
- Use at your own risk
- No liability for account restrictions, API changes, failed commands, vehicle side effects, or any other consequence
- Remote-control actions should only be used deliberately and in a safe vehicle state

## Security

For legal and security reasons, this repository does not include the static app
certificate material required by the current login path.

The public install target is one Home Assistant Custom Component:

- `custom_components/leapmotor`

Internal architecture:

- the Home Assistant integration manages entities, config flow, and UI
- the backend/auth layer manages login, session, certificate, vehicle data, and command calls

Current reality:

- the integration code still supports direct login with local certificate files
- the internal backend path covers login, vehicle list, status, and remote commands
- additional read-only calls cover total mileage and charging-plan details

Expected local files inside `custom_components/leapmotor/`:

- `app_cert.pem`
- `app_key.pem`

They can be copied manually or pasted into the setup/options form if you already
have legitimate certificate material. The setup/options form also supports
uploading the certificate/key files directly.

Without these files, direct authentication fails by design.

## Installation

1. Copy `custom_components/leapmotor` into your Home Assistant config directory under `config/custom_components/leapmotor`.
2. Provide the required local `app_cert.pem` and `app_key.pem` files in that directory, or upload/paste them during setup.
3. Restart Home Assistant.
4. Add the `Leapmotor` integration from `Settings -> Devices & services`.

## Configuration

- Email and password are required
- App certificate and app private key are required, but are not included in this repository
- Vehicle PIN is optional for setup
- Without the Vehicle PIN, the integration works in read-only mode
- Most remote-control actions stay unavailable until a Vehicle PIN is configured
- `send_destination` does not require the Vehicle PIN, matching the observed app flow
- Remote-control actions have a short cooldown to reduce accidental duplicate commands
- If multiple vehicles are available, entities are created per VIN and services can target a vehicle by `vin` or a Leapmotor `entity_id`

## Services

The integration exposes these Home Assistant services under `leapmotor.*`:

- `lock`
- `unlock`
- `trunk_open`
- `trunk_close`
- `find_car`
- `sunshade_open`
- `sunshade_close`
- `battery_preheat`
- `windows_open`
- `windows_close`
- `ac_switch` (climate off)
- `quick_cool`
- `quick_heat`
- `windshield_defrost`
- `send_destination`

Each service accepts:

- `vin` for direct vehicle targeting
- `entity_id` as an optional shortcut to resolve the target vehicle from an existing Leapmotor entity

`send_destination` additionally requires `name`, `latitude`, and `longitude`.
`address` is optional and defaults to `name`.

## Diagnostics

In addition to the regular vehicle entities, the integration exposes:

- redacted config-entry diagnostics for support/export
- last remote-action status and error details
- last API update status and error classification
- optional raw candidate status signals for future mapping work

## FAQ

### Why are app_cert.pem and app_key.pem not included?

They are app-level client certificate material required by the current login
path. Publishing them would expose reusable authentication material. Users must
provide legitimate local certificate material themselves.

### Can the integration generate these certificates?

No. Self-generated certificates are not useful unless the Leapmotor backend
trusts them. The integration can import existing PEM files, but it cannot create
valid backend-trusted app certificates.

### Why do range and odometer not show `.00`?

Whole kilometer values are exposed as integers so Home Assistant displays
`123 km` instead of `123.00 km`. Values with real decimals are preserved.

## Repository Layout

- `custom_components/leapmotor` - Home Assistant custom integration and backend/auth layer

## Roadmap

- Keep improving status mapping across more Leapmotor models.
- Keep investigating a future login path that does not need local app certificate material.
- Add more write features only after the exact app request, safety model, and permission behavior are verified.

## Legal

This project is not affiliated with or endorsed by Leapmotor.
See [LEGAL.md](LEGAL.md) and [SECURITY.md](SECURITY.md) before publishing logs,
diagnostics, or modified builds.

## License

MIT. See [LICENSE](LICENSE).
