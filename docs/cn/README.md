# Leapmotor China beta (0.8.0b2)

The unified Home Assistant integration uses the existing `leapmotor` domain.
Existing entries without a region continue to use the EU backend.

## Install the prerelease

Create a Home Assistant backup first. In HACS, open Leapmotor, choose
**Redownload**, enable prerelease versions if needed, and select **0.8.0b2**.
Restart Home Assistant after installation. For manual installation, extract
the release asset `leapmotor-ha.zip` into `custom_components/leapmotor` and
restart Home Assistant.

Existing EU entries keep their configuration and entity identities. The former
standalone `leapmotor_cn` beta is not migrated automatically. Do not run both
integrations against the same CN account; disable the standalone entry before
setting up China in the shared integration. Do not import an old session over
newer credentials. To roll back, restore your backup or reinstall the previous
stable release; the stable release does not support the new CN entries.

## Setup

Add **Leapmotor**, then select **China** (or **Rest of the World** for EU setup).
Choose phone/SMS login or expert session import. Entering a phone number does not
send an SMS: sending requires a separate checked confirmation. Session JSON
contains credentials and should not be shared.

An optional HTTP(S) proxy applies only to that CN entry. Configure it during setup
or through the entry options. Empty means direct access; a failed proxy request
does not fall back to a direct connection. Session renewal is saved before polling
resumes. Manual refresh only reads vehicle data.

## Limitations

- CN is read-only, with sensors, binary sensors and a manual refresh button.
- Entity discovery depends on available signals. Missing or invalid values stay
  unknown; stale door status expires and missing vehicles become unavailable.
- B05 and T03 behavior is covered by synthetic tests. Live vehicle validation of
  this combined beta remains outstanding.
- Phone/SMS onboarding and automatic renewal still need live validation of this
  combined release. An earlier standalone test encountered a gateway connection
  failure; its cause remains unresolved. Cloud access may depend on the account,
  region and proxy. A successful installation is not a connectivity guarantee.
- CN vehicle controls, lock status, windows, climate control, navigation, tracker,
  history, ABRP, EVCC and image platforms are unavailable.
- CN targets are rejected by shared remote services in mixed CN/EU installations.
  This beta does not claim feature parity with the EU backend.

For feedback, report the vehicle model/year, Home Assistant version, whether
you use a proxy (without its URL or credentials), the failing setup step and
which entities are missing or unknown. Never attach session JSON, tokens,
phone numbers, VINs or raw vehicle locations.

## Local validation

Unit tests require Python 3.12+, requests, cryptography and voluptuous:

```sh
python -m unittest discover -s tests
```

Run the Home Assistant lifecycle tests separately with Python 3.13 and
`homeassistant==2025.12.5`; they replace the transport with synthetic responses:

```sh
python tools/ha_cn_runtime.py -v
python tools/ha_region_runtime.py -v
```

Build the HACS release archive locally:

```sh
bash scripts/build_release_zip.sh /tmp/leapmotor-ha-0.8.0b2.zip
```
