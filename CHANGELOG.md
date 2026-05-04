# Changelog

## 0.5.28 - 2026-05-04

- Add account notification sensors from PR #5: unread message count, latest
  message title, and latest message timestamp.
- Fetch notification data once per coordinator refresh and keep vehicle updates
  working if the message endpoints are unavailable.

## 0.5.27 - 2026-05-04

- Hotfix entity display names after `0.5.26`: restore explicit entity names so
  Home Assistant no longer shows only the vehicle/device name.

## 0.5.26 - 2026-05-04

- Keep English entity IDs from the registry migration, but restore localized
  friendly names by switching sensor and binary-sensor entities to
  `translation_key` based names.
- Complete English/German entity translations for all sensor and binary-sensor
  entities.
- Document the recommended setup with a second Leapmotor account and shared
  vehicle to avoid logging the official app out.

## 0.5.25 - 2026-05-04

- Add an automatic Entity Registry migration for existing Leapmotor entries:
  known German-generated entity IDs are renamed to English IDs on startup after
  a HACS update/restart.
- This intentionally changes entity IDs such as `sensor.c10_batterie` to
  `sensor.c10_battery`; existing dashboards and automations may need to be
  updated.

## 0.5.24 - 2026-05-04

- Switch hardcoded default entity names for sensors, binary sensors, and the
  device tracker from German to English.
- Add a diagnostic `Regenerative braking` binary sensor derived from confirmed
  charging current while no external charge cable is plugged in.
- Complete the Dutch setup/options certificate wording based on community PR
  feedback while preserving the current `/config/leapmotor/app_cert.pem` and
  `/config/leapmotor/app_key.pem` guidance.

## 0.5.23 - 2026-05-04

- Fix charge-complete handling when the backend keeps signal `1149=2` but
  reports zero charging current and no remaining charge time; Home Assistant now
  shows plugged-in instead of active charging.
- Add optional `value` support for `windows_open`, `windows_close`,
  `sunshade_open`, and `sunshade_close` services to support partial
  positioning while preserving the previous full open/close default.
- Clarify certificate setup labels and documentation: uploaded/pasted material
  is stored as `/config/leapmotor/app_cert.pem` and
  `/config/leapmotor/app_key.pem`; certificates are still not bundled or
  downloaded automatically.

## 0.5.22 - 2026-05-04

- Add APK-verified read-only signal mappings for gear, speed, battery
  temperature, PTC power, DC charge cable, window/skylight state, parking
  camera, sentinel/parking-photo flags, range mode, live range, and tire
  pressure alarms.
- Correct speed-limit switch mapping to signal `12054`; signal `6047` is kept
  as raw speed-limit unit metadata.
- Treat signal `1939` as A/C fan mode metadata and stop using it as a charging
  fallback.
- Rename the `3257` diagnostic range display to CLTC remaining range while
  keeping the existing entity key for compatibility.

## 0.5.21 - 2026-05-04

- Fix active-charging detection when signal `1149` remains `1` during a real
  charging session; current, power, and remaining charge time now take
  precedence.
- Show `Ladeverbindung` as `charging` for confirmed active charging instead of
  `plugged_in`/`connecting`.

## 0.5.20 - 2026-05-04

- Restore app-correlated lock state using validated signal `1298`
  (`1=locked`, `0=unlocked`) instead of signal `47`.
- Keep vehicle movement state derived from `1941`/`1944` so lock state and
  parked/driving state are no longer coupled.

## 0.5.19 - 2026-05-04

- Correct signal `47` to charge-cable plugged-in state instead of lock state.
- Add newly validated door, trunk, charging, climate, heating, seat, mirror,
  speed-limit, and precise SOC/range signal entities.
- Update vehicle-state and active-charging derivation from the latest validated
  raw signal mapping.
- Add community credit for Toxo666's signal-mapping work.

## 0.5.18 - 2026-05-01

- Fix the rolling 7-day energy sensor metadata so Home Assistant no longer
  warns about an invalid `device_class=energy` / `state_class=measurement`
  combination.

## 0.5.17 - 2026-05-01

- Add B10 support by routing `carType=B10` status requests through the
  backend-compatible `c10` status endpoint.
- Fix B10 tire-pressure slot mapping based on app Vehicle Health verification.
- Expose additional B10 raw signal candidates in diagnostics for further
  community validation.
- Add verified consumption-screen read-only data from the official app flow:
  cumulative energy, last-7-days mileage/energy, six-week kWh/100 km average,
  and last-week driving/climate/other energy split.

## 0.5.16 - 2026-04-29

- Restore app-aligned vehicle-state precedence: `1298` is the primary
  parked/driving signal, while `1941` and `1944` remain fallback diagnostics.
- Fix the `0.5.15` regression where fresh `1941=3` / `1944=2` could show
  `driving` even when the app and `1298=1` showed the vehicle as parked.

## 0.5.15 - 2026-04-29

- Fix vehicle-state mapping so confirmed fresh `1941=3` / `1944=2` drive
  states are shown as `driving` even when the older parked flag remains set.
- Hide stale vehicle-state values instead of showing outdated `parked` or
  `driving` states as current, while keeping raw status codes in attributes.

## 0.5.14 - 2026-04-27

- Make the approved logo/icon artwork transparent outside the vehicle graphic.

## 0.5.13 - 2026-04-27

- Replace the rejected generated logo/icon with the cleaned user-provided
  artwork, scaled to 512x512 for Home Assistant and HACS.

## 0.5.12 - 2026-04-27

- Replace logo/icon PNG assets with clean transparent-background images and
  opaque vehicle windows so checkerboard artifacts are not visible.

## 0.5.11 - 2026-04-27

- Fix vehicle-state mapping so app-correlated parked states are no longer shown
  as driving.
- Store user-provided `app_cert.pem` and `app_key.pem` under
  `/config/leapmotor/` so HACS updates do not remove them.
- Automatically copy legacy certificate files from
  `/config/custom_components/leapmotor/` to `/config/leapmotor/` when present.

## 0.5.10 - 2026-04-27

- Fix charging-power mapping by deriving it from voltage/current instead of the
  GPS-like `2191` signal.
- Keep `Lädt` false for plugged-in but stopped/idle charging sessions.
- Document that the currently observed Leapmotor API signals do not reliably
  distinguish plugged-in from unplugged when the vehicle is not charging.

## 0.5.9 - 2026-04-27

- Ask for optional ABRP live-data settings during the initial setup flow, not
  only in the integration options.
- Use an internal default ABRP API key so users only need their vehicle-specific
  ABRP Generic token.
- Move local brand images into the Home Assistant custom-integration brand
  folder so the integration logo/icon can be discovered by newer HA versions.

## 0.5.8 - 2026-04-27

- Add Home Assistant service `leapmotor.send_destination`.
- Add `cmdId=180` send-destination support using the verified app flow.
- Add optional ABRP Generic Telemetry push after successful Leapmotor polls.
- Add manual refresh button and explicit last-refresh status.
- Improve stale-state handling for lock, vehicle state, and GPS data.
- Add charge-limit write service while preserving the current charging schedule.
- Add local static vehicle image support from the signed vehicle picture package.
- Add mileage/energy history summary sensors.
- Improve diagnostics with redacted API state, raw status candidates, and last action status.
- Improve setup/options flow for app certificate and private key provisioning.
- Add HACS brand assets.

## 0.5.7

- Public supportability baseline with read-only vehicle data and verified remote-control actions.
- Keep app certificate material out of the repository.
