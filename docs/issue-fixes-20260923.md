## Fixes and remaining investigations — 0.7.3 / 0.8.0b2

- **Diagnostics (#69):** Both the HA diagnostics download and
  `leapmotor.export_diagnostics` mask nested VINs, account/vehicle titles,
  nicknames and remote-control session IDs. Raw history and API metadata pass
  through the same redactor. Existing exports are not rewritten; create a new
  export after updating.
- **Stored battery energy (#71):** Available energy uses `energy_storage` with
  `measurement`, retaining its entity ID, values and kWh unit. This combination
  is supported by the declared minimum HA version, 2025.3.0.
- **T03 charge controls (#70/#72):** The integration no longer creates the
  charge-limit number or charging-schedule switch for T03. Its current command
  190 path has no verified effect on the reported T03, even while awake.
  Direct service/API writes also fail clearly before sending a command. This
  restriction does not claim that every T03 firmware lacks the capability.
  Set the limit in the vehicle; the read-only charge-limit sensor remains.
  Old registry entries can remain unavailable after upgrading and may be
  removed from HA's entity settings. Other models retain their controls, but
  writes require a complete current plan (including enabled state) rather than
  inventing missing schedule settings.
- **Incomplete schedule readback (#72):** A start time without an enabled-state
  field is not shown as a confirmed schedule. The diagnostic field
  `charging_plan_start_raw` preserves the reported time. A valid midnight start
  with an enabled-state field is retained; missing state never means off.
- **T03 daily energy (#67, mitigation):** Samples suggest Wh but do not establish
  the API unit. T03 daily/rolling kWh values are therefore unknown until that
  contract is verified. `daily_detail.energy_raw` preserves the source values,
  `energy_unit` is null, and `energy_unavailable_reason` is `unverified_unit`.
  Mileage remains available. `energy_complete` still describes field/mileage
  coverage only. Other models keep their existing energy scale. No automatic
  repair of older HA statistics is attempted.
- **Energy scope remains unconfirmed (#67):** The B10 driving-only interpretation
  is still provisional; later short-trip data do not fully reconcile it.
  Raw T03 responses with explicit units or matching app values for the same
  dates are needed before conversion. This issue is not fully resolved.
- **Raw climate codes (#73):** PTC state and air direction are labelled as raw
  codes and disabled by default for newly registered entities. Existing user
  registry choices remain intact; disable these entities manually if desired.
  AC operation mode is a translated `auto`/`manual` enum; unknown codes stay
  unknown rather than being assigned an invented meaning.
- **Windows (#68):** The model-scaled 20% ventilation command from 0.7.2 is
  retained. A second T03 user confirmed opening and closing; no additional
  command change is needed.

Wake-up SOC transients reported in #70 still need a timestamped raw status
sequence to distinguish invalid telemetry from a real 0% reading. This release
does not hide all zero SOC values. API code 137 and cloud acceptance alone do
not prove whether a command reached or changed the vehicle.

Validation uses synthetic data and offline HA lifecycle tests. It does not
replace live confirmation on affected vehicles. CN connectivity/SMS/session
renewal limitations from 0.8.0b1 remain in the new beta.
