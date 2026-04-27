# Changelog

## 0.5.9 - Planned

- Ask for optional ABRP live-data settings during the initial setup flow, not
  only in the integration options.
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
