"""pyATS/Genie operational snapshots and before/after comparisons."""

from __future__ import annotations

import json
import os
import re
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Pattern, Sequence, Set, Tuple

from src.state import StateStore


CORE_FEATURES = (
    "interface",
    "platform",
    "vlan",
    "stp",
    "lag",
    "lldp",
)

OPTIONAL_FEATURES = (
    "routing",
    "ospf",
    "bgp",
    "arp",
)

DEFAULT_CRITICAL_FEATURES = (
    "interface",
    "platform",
    "vlan",
    "stp",
    "lag",
    "lldp",
)

# Keys that only ever describe the Genie Ops *wrapper* rather than the learned
# operational state. ``capture`` normally persists only ``Ops.info``, but a
# model that does not expose ``info`` falls back to the whole object, so these
# are pruned defensively for every feature.
#
# ``connections``/``device``/``context_manager`` are the dangerous ones: they
# serialise to ``repr`` strings that embed a per-process memory address, so a
# before/after comparison across two captures can never match.
WRAPPER_EXCLUDES: Tuple[str, ...] = (
    "_exclude",
    "attributes",
    "callables",
    "commands",
    "connections",
    "context_manager",
    "device",
    "diff_ignore",
    "maker",
    "raw_data",
    "raw_output",
)

# Keep topology and operational state while removing counters, timers, and
# values expected to change as part of the upgrade itself.
#
# Entries are matched against dictionary keys at ANY depth of the learned
# snapshot. An entry containing regular-expression metacharacters is treated as
# a regex and matched with ``fullmatch``; every other entry is an exact,
# case-sensitive key name. Names must match the keys produced by the Genie Ops
# model - a name that never appears in the snapshot silently has no effect.
#
# Genie's own per-feature exclusion list (``Ops._exclude``) is recorded at
# capture time and unioned with these at comparison time, so Cisco's maintained
# list is authoritative and this table only extends it.
FEATURE_EXCLUDES: Dict[str, Sequence[str]] = {
    "interface": (
        "maker",
        "in_discards",
        "in_octets",
        "in_pkts",
        "out_octets",
        "out_pkts",
        "in_rate",
        "out_rate",
        "in_errors",
        "in_crc_errors",
        "in_rate_pkts",
        "out_rate_pkts",
        "in_broadcast_pkts",
        "out_broadcast_pkts",
        "in_multicast_pkts",
        "out_multicast_pkts",
        "in_unicast_pkts",
        "out_unicast_pkts",
        "last_clear",
        "last_change",
        "chars_out",
        "chars_in",
        "pkts_out",
        "pkts_in",
        "mac_address",
        "phys_address",
        "accounting",
        "counters",
        "statistics",
        "(Null.*)",
    ),
    "platform": (
        "maker",
        "rp_uptime",
        "uptime",
        "disk_free_space",
        "disk_used_space",
        "disk_total_space",
        "total_free_bytes",
        "main_mem",
        "date",
        "index",
        "switchover_reason",
        "version",
        "version_short",
        "image",
        "rp_boot_image",
        "boot_image",
        "installed_packages",
    ),
    "vlan": ("maker",),
    "stp": (
        "maker",
        "topology_changes",
        "time_since_topology_change",
        "last_topology_change",
        "bpdu_sent",
        "bpdu_received",
        "forward_transitions",
        "hold_count",
        "counters",
        "timers",
    ),
    "lag": (
        "maker",
        "age",
        "lacp_in_pkts",
        "lacp_out_pkts",
        "counters",
        "system_priority",
    ),
    "lldp": (
        "maker",
        "frame_in",
        "frame_out",
        "tlv_discard",
        "tlv_unknown",
        "frame_discard",
        "hold_time",
        "age",
        "counters",
    ),
    "routing": ("maker", "updated", "route_age"),
    "ospf": (
        "maker",
        "age",
        "uptime",
        "last_change",
        "cksum",
        "seq",
        "dead_timer",
        "hello_timer",
        "checksum",
        "seq_num",
        "statistics",
        "lsas",
        "last_state_change",
        "bdr_ip_addr",
        "dr_ip_addr",
        "bdr_router_id",
        "dr_router_id",
        "area_scope_lsa_cksum_sum",
    ),
    "bgp": (
        "maker",
        "keepalives",
        "last_reset",
        "reset_reason",
        "foreign_port",
        "local_port",
        "msg_rcvd",
        "msg_sent",
        "up_down",
        "bgp_table_version",
        "routing_table_version",
        "tbl_ver",
        "table_version",
        "memory_usage",
        "updates",
        "mss",
        "total_bytes",
        "up_time",
        "bgp_negotiated_keepalive_timers",
        "hold_time",
        "keepalive_interval",
        "sent",
        "received",
        "status_codes",
        "holdtime",
        "connections_dropped",
        "connections_established",
    ),
    "arp": (
        "maker",
        "in_requests_pkts",
        "in_replies_pkts",
        "out_replies_pkts",
        "out_requests_pkts",
        "in_drops",
        "out_total",
        "age",
        "statistics",
    ),
}


class GenieUnavailableError(RuntimeError):
    """Raised when Genie is required but unavailable on the current runtime."""


class SnapshotError(RuntimeError):
    """Raised when required operational data cannot be captured."""


def _looks_like_regex(entry: str) -> bool:
    """Return True when an exclude entry should be compiled as a regex."""
    return re.escape(entry) != entry


def compile_excludes(
    entries: Iterable[str],
) -> Tuple[Set[str], Tuple[Pattern[str], ...]]:
    """Split exclude entries into literal key names and compiled regexes."""
    literals: Set[str] = set()
    patterns: List[Pattern[str]] = []
    for entry in entries:
        if not isinstance(entry, str) or not entry:
            continue
        if _looks_like_regex(entry):
            try:
                patterns.append(re.compile(entry))
            except re.error:
                literals.add(entry)
        else:
            literals.add(entry)
    return literals, tuple(patterns)


def prune_excluded(
    value: Any,
    literals: Set[str],
    patterns: Sequence[Pattern[str]],
) -> Any:
    """Recursively drop excluded dictionary keys at every depth."""
    if isinstance(value, dict):
        pruned: Dict[Any, Any] = {}
        for key, inner in value.items():
            text = str(key)
            if text in literals:
                continue
            if any(pattern.fullmatch(text) for pattern in patterns):
                continue
            pruned[key] = prune_excluded(inner, literals, patterns)
        return pruned
    if isinstance(value, (list, tuple)):
        return [prune_excluded(item, literals, patterns) for item in value]
    return value


@dataclass(frozen=True)
class FeatureComparison:
    """Comparison result for one learned feature."""

    feature: str
    critical: bool
    changed: bool
    missing_before: bool
    missing_after: bool
    diff: str


@dataclass(frozen=True)
class ComparisonReport:
    """Health-gate result for all learned features."""

    passed: bool
    comparisons: List[FeatureComparison]
    report_path: str

    @property
    def blocking_features(self) -> List[str]:
        return [
            result.feature
            for result in self.comparisons
            if _is_blocking_comparison(result)
        ]


def _is_blocking_comparison(result: FeatureComparison) -> bool:
    return (
        result.critical
        and (result.changed or result.missing_before or result.missing_after)
    ) or (not result.missing_before and result.missing_after)


class GenieSnapshotManager:
    """Capture and compare Genie Ops snapshots for an IOS-XE device."""

    def __init__(
        self,
        snapshot_directory: str | Path,
        *,
        state_store: Optional[StateStore] = None,
        diff_factory: Optional[Callable[..., Any]] = None,
    ):
        self.snapshot_directory = Path(snapshot_directory)
        self.snapshot_directory.mkdir(parents=True, exist_ok=True)
        self.state_store = state_store
        self._diff_factory = diff_factory

    @staticmethod
    def validate_runtime() -> None:
        """Fail early when a Genie-enabled run is not on Linux/WSL/macOS."""
        if os.name == "nt":
            raise GenieUnavailableError(
                "pyATS/Genie is not supported on native Windows. "
                "Run the orchestrator in Linux or WSL."
            )
        try:
            import genie  # noqa: F401
            import pyats  # noqa: F401
            import unicon  # noqa: F401
        except ImportError as error:
            raise GenieUnavailableError(
                "pyATS/Genie dependencies are missing. Install "
                "requirements-genie.txt in the Linux/WSL environment."
            ) from error

    def capture(
        self,
        *,
        job_id: str,
        hostname: str,
        address: str,
        port: int,
        username: str,
        password: str,
        enable_password: Optional[str],
        phase: str,
        core_features: Iterable[str] = CORE_FEATURES,
        optional_features: Iterable[str] = OPTIONAL_FEATURES,
        required_features: Iterable[str] = CORE_FEATURES,
        connect_timeout: int = 120,
    ) -> Dict[str, Any]:
        """Learn configured Ops features and persist JSON snapshots."""
        self.validate_runtime()
        from pyats.topology import loader

        features = list(dict.fromkeys([*core_features, *optional_features]))
        required = set(required_features)
        testbed = loader.load(
            self._testbed_data(
                hostname=hostname,
                address=address,
                port=port,
                username=username,
                password=password,
                enable_password=enable_password,
                connect_timeout=connect_timeout,
            )
        )
        device = testbed.devices[hostname]
        phase_directory = self._phase_directory(job_id, hostname, phase)
        self._clear_phase_directory(phase_directory)
        results: Dict[str, Any] = {}
        errors: Dict[str, str] = {}
        genie_excludes: Dict[str, List[str]] = {}

        try:
            device.connect(
                log_stdout=False,
                learn_hostname=True,
                init_exec_commands=["terminal length 0", "terminal width 511"],
                init_config_commands=[],
            )
            for feature in features:
                try:
                    learned = device.learn(feature)
                    data = self._ops_payload(learned)
                    genie_excludes[feature] = self._ops_excludes(learned)
                    path = phase_directory / f"{feature}.json"
                    self._write_json(path, data)
                    results[feature] = data
                    self._record_snapshot(
                        job_id,
                        hostname,
                        phase,
                        feature,
                        str(path),
                        True,
                    )
                except Exception as error:
                    errors[feature] = str(error)
                    self._record_snapshot(
                        job_id,
                        hostname,
                        phase,
                        feature,
                        None,
                        False,
                        str(error),
                    )

            self._capture_command(
                device=device,
                command="show version",
                name="_show_version",
                output_directory=phase_directory,
                results=results,
            )
            self._capture_command(
                device=device,
                command="show install summary",
                name="_show_install_summary",
                output_directory=phase_directory,
                results=results,
            )
        except SnapshotError:
            raise
        except Exception as error:
            raise SnapshotError(
                f"Genie capture failed for {hostname}: {error}"
            ) from error
        finally:
            try:
                if device.is_connected():
                    device.disconnect()
            except Exception:  # pragma: no cover - best-effort teardown
                pass

        required_failures = sorted(required.intersection(errors))
        manifest = {
            "job_id": job_id,
            "hostname": hostname,
            "phase": phase,
            "captured_features": sorted(results),
            "errors": errors,
            "required_failures": required_failures,
            "genie_excludes": genie_excludes,
        }
        self._write_json(phase_directory / "manifest.json", manifest)
        if required_failures:
            raise SnapshotError(
                f"Required Genie learns failed for {hostname}: "
                f"{', '.join(required_failures)}"
            )
        return manifest

    def compare(
        self,
        *,
        job_id: str,
        hostname: str,
        before_phase: str = "before",
        after_phase: str = "after",
        critical_features: Iterable[str] = DEFAULT_CRITICAL_FEATURES,
        required_features: Iterable[str] = CORE_FEATURES,
    ) -> ComparisonReport:
        """Compare persisted snapshots and write a structured report."""
        critical = set(critical_features)
        required = set(required_features)
        before_directory = self._phase_directory(
            job_id, hostname, before_phase, create=False
        )
        after_directory = self._phase_directory(
            job_id, hostname, after_phase, create=False
        )
        before_manifest_path = before_directory / "manifest.json"
        after_manifest_path = after_directory / "manifest.json"
        if not before_manifest_path.exists() or not after_manifest_path.exists():
            raise SnapshotError(
                f"Current snapshot manifest is missing for {hostname}"
            )
        before_manifest = self._read_json(before_manifest_path)
        after_manifest = self._read_json(after_manifest_path)
        before_features = {
            feature
            for feature in before_manifest.get("captured_features", [])
            if not feature.startswith("_")
        }
        after_features = {
            feature
            for feature in after_manifest.get("captured_features", [])
            if not feature.startswith("_")
        }
        available_features = sorted(
            before_features | after_features | required
        )
        comparisons: List[FeatureComparison] = []

        for feature in available_features:
            before_path = before_directory / f"{feature}.json"
            after_path = after_directory / f"{feature}.json"
            missing_before = (
                feature not in before_features or not before_path.exists()
            )
            missing_after = (
                feature not in after_features or not after_path.exists()
            )
            diff_text = ""
            changed = False
            if not missing_before and not missing_after:
                entries = self._exclude_entries(
                    feature,
                    before_manifest,
                    after_manifest,
                )
                literals, patterns = compile_excludes(entries)
                before = prune_excluded(
                    self._read_json(before_path), literals, patterns
                )
                after = prune_excluded(
                    self._read_json(after_path), literals, patterns
                )
                # The health gate is decided on the pruned structures, not on
                # a Genie Diff attribute, so exclusions are guaranteed to be
                # honoured regardless of Genie's internal diff semantics.
                changed = before != after
                if changed:
                    diff_text = self._render_diff(before, after, entries)
            comparisons.append(
                FeatureComparison(
                    feature=feature,
                    critical=feature in critical or feature in required,
                    changed=changed,
                    missing_before=missing_before,
                    missing_after=missing_after,
                    diff=diff_text,
                )
            )

        report_directory = (
            self.snapshot_directory / job_id / hostname / "comparisons"
        )
        report_directory.mkdir(parents=True, exist_ok=True)
        report_path = report_directory / f"{before_phase}-to-{after_phase}.json"
        passed = not any(
            _is_blocking_comparison(result) for result in comparisons
        )
        payload = {
            "job_id": job_id,
            "hostname": hostname,
            "before_phase": before_phase,
            "after_phase": after_phase,
            "passed": passed,
            "blocking_features": [
                result.feature
                for result in comparisons
                if _is_blocking_comparison(result)
            ],
            "comparisons": [asdict(result) for result in comparisons],
        }
        self._write_json(report_path, payload)
        return ComparisonReport(
            passed=passed,
            comparisons=comparisons,
            report_path=str(report_path),
        )

    @staticmethod
    def _exclude_entries(
        feature: str,
        before_manifest: Dict[str, Any],
        after_manifest: Dict[str, Any],
    ) -> List[str]:
        """Union our exclude table with Genie's own per-feature list."""
        entries: List[str] = list(WRAPPER_EXCLUDES)
        entries.extend(FEATURE_EXCLUDES.get(feature, ()))
        for manifest in (before_manifest, after_manifest):
            recorded = manifest.get("genie_excludes", {}) or {}
            for entry in recorded.get(feature, []) or []:
                if isinstance(entry, str):
                    entries.append(entry)
        return list(dict.fromkeys(entries))

    def _render_diff(
        self,
        before: Dict[str, Any],
        after: Dict[str, Any],
        entries: Sequence[str],
    ) -> str:
        """Render a human-readable diff of already-pruned snapshots."""
        try:
            diff = self._new_diff(before, after, exclude=list(entries))
            diff.findDiff()
            rendered = str(diff)
        except Exception as error:  # pragma: no cover - rendering is advisory
            return f"<diff rendering failed: {error}>"
        return rendered or "<structural difference outside the rendered diff>"

    def _new_diff(self, before: Dict[str, Any], after: Dict[str, Any], **kwargs):
        if self._diff_factory is not None:
            return self._diff_factory(before, after, **kwargs)
        self.validate_runtime()
        from genie.utils.diff import Diff

        return Diff(before, after, **kwargs)

    @staticmethod
    def _ops_payload(learned: Any) -> Dict[str, Any]:
        """Return only the learned operational state of a Genie Ops object.

        ``device.learn()`` returns an Ops object whose operational data lives
        in ``.info``. Serialising the whole object also captures wrapper
        attributes (``connections``, ``device``, ``context_manager``,
        ``raw_output``) which either embed per-process object reprs or raw CLI
        text, and which therefore differ on every capture.
        """
        info = getattr(learned, "info", None)
        if isinstance(info, dict):
            return info
        if isinstance(learned, dict):
            nested = learned.get("info")
            return nested if isinstance(nested, dict) else learned
        to_dict = getattr(learned, "to_dict", None)
        if callable(to_dict):
            payload = to_dict()
            if isinstance(payload, dict):
                nested = payload.get("info")
                return nested if isinstance(nested, dict) else payload
        raise SnapshotError(
            f"Genie returned an unsupported learn result: {type(learned)!r}"
        )

    @staticmethod
    def _ops_excludes(learned: Any) -> List[str]:
        """Return Genie's own exclusion list for a learned Ops object."""
        raw = getattr(learned, "_exclude", None) or []
        try:
            return [entry for entry in raw if isinstance(entry, str)]
        except TypeError:
            return []

    def _phase_directory(
        self,
        job_id: str,
        hostname: str,
        phase: str,
        *,
        create: bool = True,
    ) -> Path:
        path = self.snapshot_directory / job_id / hostname / phase
        if create:
            path.mkdir(parents=True, exist_ok=True)
        return path

    def _record_snapshot(
        self,
        job_id: str,
        hostname: str,
        phase: str,
        feature: str,
        path: Optional[str],
        success: bool,
        error: Optional[str] = None,
    ) -> None:
        if self.state_store:
            self.state_store.record_snapshot(
                job_id=job_id,
                hostname=hostname,
                phase=phase,
                feature=feature,
                path=path,
                success=success,
                error=error,
            )

    @staticmethod
    def _clear_phase_directory(path: Path) -> None:
        for snapshot_file in path.glob("*.json"):
            snapshot_file.unlink()

    @staticmethod
    def _capture_command(
        *,
        device,
        command: str,
        name: str,
        output_directory: Path,
        results: Dict[str, Any],
    ) -> None:
        try:
            data = device.parse(command)
        except Exception:
            try:
                data = {"raw": device.execute(command)}
            except Exception as error:
                data = {"error": str(error)}
        path = output_directory / f"{name}.json"
        GenieSnapshotManager._write_json(path, data)
        results[name] = data

    @staticmethod
    def _testbed_data(
        *,
        hostname: str,
        address: str,
        port: int,
        username: str,
        password: str,
        enable_password: Optional[str],
        connect_timeout: int,
    ) -> Dict[str, Any]:
        credentials: Dict[str, Dict[str, str]] = {
            "default": {"username": username, "password": password}
        }
        if enable_password:
            credentials["enable"] = {"password": enable_password}
        return {
            "testbed": {"name": "iosxe-upgrade-orchestrator"},
            "devices": {
                hostname: {
                    "os": "iosxe",
                    "type": "switch",
                    "credentials": credentials,
                    "connections": {
                        "cli": {
                            "protocol": "ssh",
                            "ip": address,
                            "port": int(port),
                            "arguments": {
                                "connection_timeout": connect_timeout,
                                "prompt_recovery": True,
                            },
                            "settings": {
                                "EXEC_TIMEOUT": connect_timeout,
                            },
                        }
                    },
                }
            },
        }

    @staticmethod
    def _read_json(path: Path) -> Dict[str, Any]:
        with path.open("r", encoding="utf-8") as file:
            return json.load(file)

    @staticmethod
    def _write_json(path: Path, data: Any) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary_path = path.with_suffix(path.suffix + ".tmp")
        with temporary_path.open("w", encoding="utf-8") as file:
            json.dump(data, file, indent=2, sort_keys=True, default=str)
            file.write("\n")
        temporary_path.replace(path)
