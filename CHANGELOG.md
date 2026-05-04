# Changelog

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
