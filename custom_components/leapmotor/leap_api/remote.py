"""Verified Leapmotor remote-control command payloads."""

from __future__ import annotations

from dataclasses import dataclass

from ..const import (
    REMOTE_CTL_AC_OFF,
    REMOTE_CTL_AC_ON,
    REMOTE_CTL_AC_SWITCH,
    REMOTE_CTL_BATTERY_PREHEAT,
    REMOTE_CTL_BATTERY_PREHEAT_OFF,
    REMOTE_CTL_FIND_CAR,
    REMOTE_CTL_LOCK,
    REMOTE_CTL_QUICK_COOL,
    REMOTE_CTL_QUICK_HEAT,
    REMOTE_CTL_SUNSHADE,
    REMOTE_CTL_SUNSHADE_CLOSE,
    REMOTE_CTL_SUNSHADE_OPEN,
    REMOTE_CTL_TRUNK,
    REMOTE_CTL_TRUNK_CLOSE,
    REMOTE_CTL_TRUNK_OPEN,
    REMOTE_CTL_UNLOCK,
    REMOTE_CTL_UNLOCK_CHARGER,
    REMOTE_CTL_WINDSHIELD_DEFROST,
    REMOTE_CTL_WINDOWS,
    REMOTE_CTL_WINDOWS_CLOSE,
    REMOTE_CTL_WINDOWS_OPEN,
)


@dataclass(frozen=True, slots=True)
class RemoteActionSpec:
    """Verified remote-control action payload."""

    cmd_id: str
    cmd_content: str


REMOTE_ACTION_SPECS: dict[str, RemoteActionSpec] = {
    REMOTE_CTL_UNLOCK: RemoteActionSpec(cmd_id="110", cmd_content='{"value":"unlock"}'),
    REMOTE_CTL_LOCK: RemoteActionSpec(cmd_id="110", cmd_content='{"value":"lock"}'),
    REMOTE_CTL_UNLOCK_CHARGER: RemoteActionSpec(
        cmd_id="192",
        cmd_content='{"operation":"unlock"}',
    ),
    REMOTE_CTL_TRUNK: RemoteActionSpec(cmd_id="130", cmd_content='{"value":"true"}'),
    REMOTE_CTL_TRUNK_OPEN: RemoteActionSpec(cmd_id="130", cmd_content='{"value":"true"}'),
    REMOTE_CTL_TRUNK_CLOSE: RemoteActionSpec(cmd_id="130", cmd_content='{"value":"false"}'),
    REMOTE_CTL_FIND_CAR: RemoteActionSpec(cmd_id="120", cmd_content='{"value":"true"}'),
    REMOTE_CTL_SUNSHADE: RemoteActionSpec(cmd_id="240", cmd_content='{"value":"10"}'),
    REMOTE_CTL_SUNSHADE_OPEN: RemoteActionSpec(cmd_id="240", cmd_content='{"value":"10"}'),
    REMOTE_CTL_SUNSHADE_CLOSE: RemoteActionSpec(cmd_id="240", cmd_content='{"value":"0"}'),
    REMOTE_CTL_BATTERY_PREHEAT: RemoteActionSpec(cmd_id="160", cmd_content='{"value":"ptcon"}'),
    REMOTE_CTL_BATTERY_PREHEAT_OFF: RemoteActionSpec(cmd_id="160", cmd_content='{"value":"ptcoff"}'),
    REMOTE_CTL_WINDOWS: RemoteActionSpec(cmd_id="230", cmd_content='{"value":"2"}'),
    REMOTE_CTL_WINDOWS_OPEN: RemoteActionSpec(cmd_id="230", cmd_content='{"value":"2"}'),
    REMOTE_CTL_WINDOWS_CLOSE: RemoteActionSpec(cmd_id="230", cmd_content='{"value":"0"}'),
    REMOTE_CTL_AC_SWITCH: RemoteActionSpec(
        cmd_id="170",
        cmd_content='{"circle":"out","mode":"wind","operate":"close","position":"all","temperature":"26","windlevel":"3","wshld":"0"}',
    ),
    REMOTE_CTL_AC_ON: RemoteActionSpec(
        cmd_id="170",
        cmd_content='{"circle":"out","mode":"wind","operate":"manual","position":"all","temperature":"26","windlevel":"3","wshld":"0"}',
    ),
    REMOTE_CTL_AC_OFF: RemoteActionSpec(
        cmd_id="170",
        cmd_content='{"circle":"out","mode":"wind","operate":"close","position":"all","temperature":"26","windlevel":"3","wshld":"0"}',
    ),
    REMOTE_CTL_QUICK_COOL: RemoteActionSpec(
        cmd_id="170",
        cmd_content='{"circle":"in","mode":"cold","operate":"manual","position":"all","temperature":"18","windlevel":"7","wshld":"0"}',
    ),
    REMOTE_CTL_QUICK_HEAT: RemoteActionSpec(
        cmd_id="170",
        cmd_content='{"circle":"in","mode":"hot","operate":"manual","position":"all","temperature":"32","windlevel":"7","wshld":"0"}',
    ),
    REMOTE_CTL_WINDSHIELD_DEFROST: RemoteActionSpec(
        cmd_id="170",
        cmd_content='{"circle":"in","mode":"hot","operate":"manual","position":"all","temperature":"32","windlevel":"7","wshld":"1"}',
    ),
}
