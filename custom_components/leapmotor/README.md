# Leapmotor Home Assistant Integration

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
- Vehicle lock as a native Home Assistant lock entity
- Vehicle state as a readable status sensor
- Interior and climate target temperatures
- Charge limit
- Scheduled charging window and recurrence details
- Scheduled charging flag as a read-only binary sensor
- Tire pressures
- GPS location as a Home Assistant device tracker
- Remote-control buttons for:
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

## Install

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
- provide local `app_cert.pem` and `app_key.pem` in `config/custom_components/leapmotor`, or upload/paste them during setup/options
- configure the integration in Home Assistant

The component itself contains both parts needed at runtime: the Home Assistant
entity layer and the backend/auth layer. No separate add-on is required for the
normal user install path.

## Setup Fields

- Username / email: your Leapmotor account email
- Password: your Leapmotor account password
- App certificate file: required for login; upload it here or leave the field
  empty only if `app_cert.pem` already exists locally.
- App private key file: required for login; upload it here or leave the field
  empty only if `app_key.pem` already exists locally.
- App certificate/private key PEM: fallback paste fields if file upload is not
  convenient.
- Vehicle PIN: optional; leave empty for read-only mode. Remote-control actions
  usually require this PIN and stay unavailable without it. Sending a
  destination does not require the PIN, matching the observed app flow.
- Update interval: default `5` minutes

After setup, the Vehicle PIN and update interval can be changed from the
integration options without recreating the entry.

## State Freshness

- Lock state is treated conservatively. If the cloud vehicle timestamp is too
  old, the lock falls back to `unknown` instead of showing a stale unlocked
  state for hours.
- Vehicle state and GPS location expose freshness metadata in entity
  attributes and diagnostics so stale backend data is easier to identify.
- Each vehicle also gets a `Refresh data` button to trigger an immediate poll
  outside the normal 5-minute interval.
- Home Assistant's own `x minutes ago` text often reflects the last state
  change, not the last successful poll. Use the `Last refresh` sensor to see
  when the integration actually fetched data successfully.

## ABRP Live Data

ABRP telemetry is optional and disabled by default. If enabled in the
integration options, the integration sends one Generic Telemetry update after
each successful Leapmotor poll.

Required ABRP options:

- ABRP API key
- ABRP Generic token from the ABRP vehicle live-data setup

The submitted telemetry includes state of charge, estimated range, charging
state, odometer, speed fallback `0`, and GPS coordinates when available and not
marked stale.

## Runtime Notes

- Requires `curl` on the Home Assistant host/container.
- Requires Python packages from `manifest.json`: `cryptography` and `requests`.
- The integration uses the same API path verified by the reverse-engineered client.
- Most remote-control actions use the verified `operatePassword` flow and
  require a configured Vehicle PIN. `leapmotor.send_destination` uses the
  observed app flow without `operatePassword`.
- The account certificate password is derived internally and is no longer asked
  during setup. Known captured values remain as fallback only.
- The integration exposes Home Assistant diagnostics with secrets redacted:
  account state, vehicle metadata, raw status codes, mileage summary, vehicle
  picture availability, last API codes, and last remote-control result.
- The vehicle image no longer uses the generic `shareBindUrl` CDN fallback. It
  now downloads the signed vehicle picture package once per picture key and
  serves the extracted static image from the local Home Assistant cache.
- A `Last vehicle action` sensor and action attributes show the latest
  remote-control status per VIN.
- The repository now includes HACS brand assets in `brand/icon.png` and
  `brand/logo.png`. Home Assistant still does not automatically use arbitrary
  local integration images in every UI location, so logo visibility depends on
  the consumer (for example HACS branding vs. Devices & Services UI).
- The current proof set is strongest on the C10. Main-account and shared-car
  handling are both implemented; feature availability may still vary by model,
  especially for climate, sunshade, trunk, and window actions.
