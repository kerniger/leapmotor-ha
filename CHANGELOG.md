# Changelog

## 0.6.15 - 2026-06-08

- Add B10-verified one-touch vehicle preparation services:
  `leapmotor.prepare_car`, `leapmotor.set_prepare_car_schedule`, and
  `leapmotor.cancel_prepare_car_schedule` using cmdIds `360` and `361`.

## 0.6.14 - 2026-06-08

- Add a `Vehicle READY` diagnostic binary sensor based on signal `1258`
  (`bcmKeyPositionOn3`), matching B10 READY/ON3 behavior.
- Schedule a short follow-up refresh after remote-control commands so delayed
  comfort, climate, charge-limit, and schedule status changes surface faster
  without increasing normal polling.
- Replace the deprecated Home Assistant device-tracker `TrackerEntity` import.
- Add a README reference to `leapmotor-mate` for TeslaMate-style trip and charge
  history.

## 0.6.13 - 2026-06-08

- Add B10-verified climate pre-conditioning schedule services using `cmdId=171`:
  `leapmotor.set_climate_schedule` writes one future schedule entry and
  `leapmotor.cancel_climate_schedule` sends an empty schedule list.

## 0.6.12 - 2026-06-06

- Refresh expired access tokens through `/acct/v1/token/refresh` before falling
  back to a full account login.
- Read the current charging plan through the dedicated `getAppointment`
  endpoint when vehicle status does not contain a complete plan, fixing charge
  limit and schedule updates on affected vehicles such as T03.
- Route B11 vehicle status requests through the shared C10 status endpoint,
  matching the international backend behavior.

## 0.6.11 - 2026-06-06

- Correct steering-wheel and exterior-mirror heating payloads from live C10
  captures of the international app.
- Correct driver/passenger seat heating and ventilation payloads and restore
  the `0-3` Home Assistant level controls. Passenger ventilation level 2 was
  additionally verified by sending it to a C10 through the API.
- Turn climate control fully off with `{"operate":"off"}` instead of the
  incomplete `close` operation reported in issue #42.

## 0.6.10 - 2026-05-28

- Add comfort controls based on `markoceri/leapmotor-api`: steering wheel
  heating and mirror heating switches, plus driver/passenger seat heating and
  ventilation level controls.

## 0.6.9 - 2026-05-28

- Retry vehicle-list resolution once after a fresh login when a remote action
  fails before command execution with `Token is invalid`.

## 0.6.8 - 2026-05-17

- Add a parameterised `leapmotor.set_climate` service for cooling, heating,
  ventilation, temperature, fan speed, recirculation, and windshield defrost.
- Replace the one-shot battery preheat button with a stateful Home Assistant
  switch and add the matching battery preheat off command.
- Add an optional diagnostic sensor for the last charging-session energy from
  the charging daily detail endpoint.
- Add an optional `is_driving` binary sensor derived from the existing vehicle
  state logic.
- Reduce duplicated visible controls and range entities by keeping secondary
  range variants disabled by default.

## 0.6.7 - 2026-05-13

- Add a controllable charging schedule switch that toggles the existing
  charging plan while preserving start time, end time, recurrence, and charge
  limit.

## 0.6.6 - 2026-05-12

- Use `en-GB` as the default signed API language to better match the standalone
  API client and international app behavior.
- Generate and persist a unique login device ID per Home Assistant config entry
  instead of reusing the same static device ID for every installation.

## 0.6.5 - 2026-05-11

- Improve setup failure diagnostics by recording transport-level API errors
  before a parsed API response exists.
- Use the `markoceri` GitHub handle consistently in README credits.
- Stop exposing signal `1939` as a raw charge status and rename the raw
  vehicle-state metadata to `raw_ac_operation_mode_code`; charge connection
  metadata remains based on signal `1149`.
- Stop using signal `1941` for vehicle parked/driving state. Movement state is
  now derived from gear (`1010`), speed (`1319`), and ON3 (`1258`), while
  `1941` is exposed as `raw_ac_fan_speed_code`.

## 0.6.4 - 2026-05-10

- Use the APK tire-pressure slot mapping for all models after live C10
  verification showed the previous non-B10/default mapping displayed rear right
  pressure as front left.

## 0.6.3 - 2026-05-10

- Add setup-time warning logs with compact last API result metadata when login
  succeeds but the first vehicle-data refresh fails. This helps diagnose setup
  failures before the config entry exists and before diagnostics services are
  available.

## 0.6.2 - 2026-05-09

- Remove the energy device class from the rolling 7-day energy sensor because
  Home Assistant does not allow `device_class: energy` with
  `state_class: measurement`.

## 0.6.1 - 2026-05-09

- Correct positive longitude values for western-hemisphere Home Assistant
  installations when the mirrored negative longitude is clearly closer to the
  configured Home Assistant home location. This fixes GPS placement for regions
  west of Greenwich without hard-coding countries.
- Expose longitude correction diagnostics on the device tracker via
  `raw_longitude`, `longitude_corrected`, and `longitude_correction_source`.

## 0.6.0 - 2026-05-09

- Start the 0.6 internal API-layer split by moving API exceptions, vehicle
  models, remote-command payloads, operatePassword crypto helpers, and the
  curl transport into `custom_components/leapmotor/leap_api`.
- Add optional eco polling. When enabled, the integration switches to a slower
  polling interval only while every vehicle is clearly locked, parked, and
  unplugged; otherwise it keeps the normal interval.
- Expose polling mode and normal/eco intervals in integration diagnostics.
- Add a compact redacted `support_summary` to diagnostics exports for easier
  issue triage without sharing the full payload.
- Tighten unit metadata for energy sensors and expose the API-provided
  `mi/kWh` average consumption value as an optional diagnostic sensor for
  imperial-unit users.

## 0.5.39 - 2026-05-09

- Add a dedicated charger-unlock button and `leapmotor.unlock_charger`
  service using the live-verified app command `cmdId=192` with
  `{"operation":"unlock"}`.
- Document the charger-unlock service example for Home Assistant automations.

## 0.5.38 - 2026-05-08

- Correct positive latitude values for southern-hemisphere Home Assistant
  installations when the mirrored negative latitude is clearly closer to the
  configured HA home location. This fixes backend GPS sign issues reported by
  Australian users without hard-coding a country list.
- Expose GPS correction diagnostics on the device tracker via `raw_latitude`,
  `latitude_corrected`, and `latitude_correction_source`.
- Update community credit from `Toxo666` to `sbstn-0x2a`.

## 0.5.37 - 2026-05-06

- Create optional diagnostic entities only when the first vehicle payload
  actually contains the corresponding raw value. Existing entity-registry
  entries are not removed automatically.
- Add EVCC helper sensors: `evcc_status` with IEC 61851 `A`/`B`/`C` mapping
  and `charging_finish_time` from the remaining charge time.
- Return the charge-limit number as an integer so Home Assistant accepts values
  like `90` without requiring `90.0`.
- Keep the English entity-ID migration enabled; localized display names remain
  language-specific.

## 0.5.36 - 2026-05-06

- Improve T03 named-payload coverage using fields also modeled by LeapConnect:
  available energy, outdoor temperature, climate fan/air-direction raw values,
  PTC state/settings, sunshade position, Bluetooth/hotspot state, door-control
  permission, and window remote-control support.
- Add dedicated diagnostic entities for these T03 fields instead of mapping
  them onto unrelated C10/B10-specific signals.
- Report charging power as `0.0 kW` when current and voltage are present but
  current is below the active-charging threshold.

## 0.5.35 - 2026-05-06

- Add `collectTime` / `collectTimeMs` as a timestamp fallback for T03 status
  payloads without numeric signal timestamps.
- Expand diagnostics exports with the complete normalized `status` and
  `charging` sections so remaining T03 gaps can be identified from support
  exports.

## 0.5.34 - 2026-05-06

- Add a T03 compatibility fallback for status payloads that expose named fields
  directly under `data` instead of the newer numeric `data.signal` map.
- Map safe T03 named fields for SOC, range, mileage, speed, gear, location,
  charge state, charge current/voltage/time, DC charge flag, lock, doors,
  windows, climate setpoint, and tire pressure.
- Derive parked/driving state from speed or gear when model-specific drive-state
  signals are absent.

## 0.5.33 - 2026-05-05

- Fix the HACS release archive layout. The release ZIP now contains the
  integration files directly at the archive root, so HACS installs
  `manifest.json` to `/config/custom_components/leapmotor/manifest.json`
  instead of nesting the repository inside the integration directory.

## 0.5.32 - 2026-05-05

- Improve low-current AC charging detection for B10 vehicles that charge around
  2.5 A, while keeping C10 plugged-idle snapshots around 1.5 A from being
  reported as active charging.
- Keep completed charging detection tied to true idle current so plugged-in
  vehicles with residual current are shown as connected rather than finished.

## 0.5.31 - 2026-05-05

- Fix HACS release metadata so HACS downloads `leapmotor-ha.zip` from GitHub
  releases instead of trying to infer the integration path from the repository.
- Fix the GitHub validation workflow so documentation that explains PEM marker
  text is not treated as a leaked certificate.

## 0.5.30 - 2026-05-05

- Add `leapmotor.export_diagnostics`, which writes an anonymized support JSON
  file with interpreted state and raw APK signal values to `/config/leapmotor`.
- Improve charge-complete detection: a plugged-in vehicle with completed/idle
  charging now reports `finished` instead of falling back to active charging.
- Add diagnostic lock-state sensors for source, age, and raw lock signal so
  stale cloud state and remote-control overrides are easier to identify.
- Document evcc usage through the Home Assistant entities, including SOC,
  range, plug state, active charging, and charging-connection interpretation.
- Expand service documentation with PIN requirements, targeting rules, and
  Home Assistant automation examples for navigation, climate, windows, charge
  limit, lock, and diagnostics export.
- Complete translation coverage for buttons, locks, numbers, image/location
  entities, and the setup/options config flow in all bundled languages.
- Improve release/HACS documentation with badges, screenshot checklist, known
  limitations, and troubleshooting for certificates, HACS setup, stale data,
  remote actions, and second-account usage.
- Add a read-only shared-car status fallback: if a shared vehicle returns an
  empty live-signal payload with `vin` only, retry once with `vin` and `carId`
  before treating the live status as unavailable.

## 0.5.29 - 2026-05-05

- Restore localized sensor and binary-sensor display names while keeping the
  English entity IDs introduced in `0.5.25`.
- Fix the previous translation-key regression by assigning localized entity
  names from the bundled translation files with English fallback.
- Complete translation entries for all current sensor and binary-sensor keys so
  missing translations no longer collapse to the vehicle name only.

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
