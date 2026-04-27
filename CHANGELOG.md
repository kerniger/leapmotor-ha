# Changelog

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
