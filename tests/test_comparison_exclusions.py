"""Regression tests for Genie snapshot exclusion handling.

These tests deliberately model ``genie.utils.diff.Diff`` as an engine that
matches exclude entries as *exact key names only* (no regex, no wildcards),
which is the documented behaviour. The health gate must therefore not depend
on the diff engine honouring anything more than that.
"""

import tempfile
import unittest
from pathlib import Path

from src.comparison import (
    FEATURE_EXCLUDES,
    WRAPPER_EXCLUDES,
    GenieSnapshotManager,
    compile_excludes,
    prune_excluded,
)


class LiteralOnlyDiff:
    """Stand-in for the real Genie Diff: literal key names, no regex."""

    def __init__(self, before, after, exclude=None):
        self._literals = {
            entry for entry in (exclude or []) if isinstance(entry, str)
        }
        self._before = self._strip(before)
        self._after = self._strip(after)
        self.diffs = []

    def _strip(self, value):
        if isinstance(value, dict):
            return {
                key: self._strip(inner)
                for key, inner in value.items()
                if str(key) not in self._literals
            }
        if isinstance(value, list):
            return [self._strip(item) for item in value]
        return value

    def findDiff(self):
        if self._before != self._after:
            self.diffs = ["changed"]

    def __str__(self):
        return "changed" if self.diffs else ""


class FakeOps:
    """Minimal stand-in for a Genie Ops object returned by device.learn()."""

    def __init__(self, info, exclude=None, wrapper_noise=None):
        self.info = info
        self._exclude = list(exclude or [])
        self._wrapper_noise = wrapper_noise or {}

    def to_dict(self):
        payload = {
            "_exclude": self._exclude,
            "attributes": None,
            "commands": None,
            "connections": None,
            "context_manager": {},
            "raw_data": False,
            "info": self.info,
        }
        payload.update(self._wrapper_noise)
        return payload


class ExclusionTests(unittest.TestCase):
    def setUp(self):
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.manager = GenieSnapshotManager(
            self.temporary_directory.name,
            diff_factory=LiteralOnlyDiff,
        )

    def tearDown(self):
        self.temporary_directory.cleanup()

    def _write(self, phase, feature, value, genie_excludes=None):
        path = (
            Path(self.temporary_directory.name)
            / "job"
            / "sw1"
            / phase
            / f"{feature}.json"
        )
        self.manager._write_json(path, value)
        manifest_path = path.parent / "manifest.json"
        manifest = {"captured_features": [], "errors": {}, "genie_excludes": {}}
        if manifest_path.exists():
            manifest = self.manager._read_json(manifest_path)
        manifest.setdefault("captured_features", [])
        manifest.setdefault("genie_excludes", {})
        if feature not in manifest["captured_features"]:
            manifest["captured_features"].append(feature)
        if genie_excludes:
            manifest["genie_excludes"][feature] = list(genie_excludes)
        self.manager._write_json(manifest_path, manifest)

    def _seed_required(self):
        for feature in ("interface", "platform"):
            self._write("before", feature, {"value": "up"})
            self._write("after", feature, {"value": "up"})

    def _compare(self, critical, required=("interface", "platform")):
        return self.manager.compare(
            job_id="job",
            hostname="sw1",
            required_features=list(required),
            critical_features=list(critical),
        )

    # -- payload extraction -------------------------------------------------

    def test_ops_payload_returns_info_not_the_wrapper(self):
        ops = FakeOps({"GigabitEthernet1/0/1": {"oper_status": "up"}})
        self.assertEqual(
            GenieSnapshotManager._ops_payload(ops),
            {"GigabitEthernet1/0/1": {"oper_status": "up"}},
        )

    def test_ops_payload_unwraps_a_serialised_ops_dict(self):
        ops = FakeOps({"a": 1})
        self.assertEqual(
            GenieSnapshotManager._ops_payload(ops.to_dict()), {"a": 1}
        )

    def test_ops_excludes_are_read_from_the_ops_object(self):
        ops = FakeOps({}, exclude=["in_octets", "last_clear"])
        self.assertEqual(
            GenieSnapshotManager._ops_excludes(ops),
            ["in_octets", "last_clear"],
        )

    # -- wrapper noise ------------------------------------------------------

    def test_wrapper_keys_never_reach_the_health_gate(self):
        # A snapshot that was persisted from the whole Ops object, including a
        # per-process repr that can never match between two captures.
        before = {
            "connections": "<Connection object at 0x7f9c11110000>",
            "context_manager": {},
            "info": {"Vlan10": {"oper_status": "up"}},
        }
        after = {
            "connections": "<Connection object at 0x7f9c22220000>",
            "context_manager": {},
            "info": {"Vlan10": {"oper_status": "up"}},
        }
        self._seed_required()
        self._write("before", "vlan", before)
        self._write("after", "vlan", after)

        report = self._compare(critical=("interface", "platform", "vlan"))

        vlan = next(r for r in report.comparisons if r.feature == "vlan")
        self.assertFalse(vlan.changed, vlan.diff)
        self.assertTrue(report.passed)

    # -- regex excludes -----------------------------------------------------

    def test_regex_exclude_applies_even_though_genie_has_no_regex_support(self):
        self.assertIn("(Null.*)", FEATURE_EXCLUDES["interface"])
        before = {
            "Null0": {"in_octets": 1, "counters": {"in_pkts": 1}},
            "GigabitEthernet1/0/1": {"oper_status": "up"},
        }
        after = {
            "Null0": {"in_octets": 999, "counters": {"in_pkts": 999}},
            "GigabitEthernet1/0/1": {"oper_status": "up"},
        }
        self._write("before", "platform", {"value": "x"})
        self._write("after", "platform", {"value": "x"})
        self._write("before", "interface", before)
        self._write("after", "interface", after)

        report = self._compare(critical=("interface", "platform"))

        interface = next(
            r for r in report.comparisons if r.feature == "interface"
        )
        self.assertFalse(interface.changed, interface.diff)
        self.assertTrue(report.passed)

    def test_counter_movement_under_a_dynamic_interface_key_is_ignored(self):
        before = {
            "GigabitEthernet1/0/1": {
                "oper_status": "up",
                "counters": {"in_octets": 10, "out_octets": 10},
                "last_change": "1w2d",
            }
        }
        after = {
            "GigabitEthernet1/0/1": {
                "oper_status": "up",
                "counters": {"in_octets": 0, "out_octets": 0},
                "last_change": "00:02:11",
            }
        }
        self._write("before", "platform", {"value": "x"})
        self._write("after", "platform", {"value": "x"})
        self._write("before", "interface", before)
        self._write("after", "interface", after)

        report = self._compare(critical=("interface", "platform"))
        interface = next(
            r for r in report.comparisons if r.feature == "interface"
        )
        self.assertFalse(interface.changed, interface.diff)

    def test_a_real_operational_regression_still_blocks(self):
        before = {"GigabitEthernet1/0/1": {"oper_status": "up"}}
        after = {"GigabitEthernet1/0/1": {"oper_status": "down"}}
        self._write("before", "platform", {"value": "x"})
        self._write("after", "platform", {"value": "x"})
        self._write("before", "interface", before)
        self._write("after", "interface", after)

        report = self._compare(critical=("interface", "platform"))
        self.assertFalse(report.passed)
        self.assertEqual(report.blocking_features, ["interface"])

    # -- genie's own exclude list ------------------------------------------

    def test_genie_recorded_excludes_from_the_manifest_are_honoured(self):
        self._seed_required()
        self._write(
            "before",
            "lldp",
            {"interfaces": {"Gi1/0/1": {"neighbour_ttl": 100}}},
            genie_excludes=["neighbour_ttl"],
        )
        self._write(
            "after",
            "lldp",
            {"interfaces": {"Gi1/0/1": {"neighbour_ttl": 7}}},
            genie_excludes=["neighbour_ttl"],
        )

        report = self._compare(critical=("interface", "platform", "lldp"))
        lldp = next(r for r in report.comparisons if r.feature == "lldp")
        self.assertFalse(lldp.changed, lldp.diff)

    # -- helpers ------------------------------------------------------------

    def test_compile_excludes_splits_literals_and_patterns(self):
        literals, patterns = compile_excludes(["in_octets", "(Null.*)"])
        self.assertEqual(literals, {"in_octets"})
        self.assertEqual(len(patterns), 1)
        self.assertTrue(patterns[0].fullmatch("Null0"))

    def test_prune_removes_keys_at_every_depth(self):
        literals, patterns = compile_excludes(["uptime"])
        pruned = prune_excluded(
            {"slot": {"R0": {"uptime": "1d", "state": "ok"}}},
            literals,
            patterns,
        )
        self.assertEqual(pruned, {"slot": {"R0": {"state": "ok"}}})

    def test_wrapper_excludes_cover_the_ops_serialisation_keys(self):
        for key in ("connections", "context_manager", "raw_output", "device"):
            self.assertIn(key, WRAPPER_EXCLUDES)


if __name__ == "__main__":
    unittest.main()
