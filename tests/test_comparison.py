import tempfile
import unittest
from pathlib import Path

from src.comparison import FEATURE_EXCLUDES, GenieSnapshotManager


class FakeDiff:
    def __init__(self, before, after, exclude=None):
        del exclude
        self.before = before
        self.after = after
        self.diffs = []

    def findDiff(self):
        if self.before != self.after:
            self.diffs = ["changed"]

    def __str__(self):
        return "changed" if self.diffs else ""


class ComparisonTests(unittest.TestCase):
    def setUp(self):
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.manager = GenieSnapshotManager(
            self.temporary_directory.name,
            diff_factory=FakeDiff,
        )

    def tearDown(self):
        self.temporary_directory.cleanup()

    def write_feature(self, phase, feature, value):
        path = (
            Path(self.temporary_directory.name)
            / "job"
            / "sw1"
            / phase
            / f"{feature}.json"
        )
        self.manager._write_json(path, value)
        manifest_path = path.parent / "manifest.json"
        captured = []
        if manifest_path.exists():
            captured = self.manager._read_json(manifest_path).get(
                "captured_features", []
            )
        if feature not in captured:
            captured.append(feature)
        self.manager._write_json(
            manifest_path,
            {"captured_features": captured, "errors": {}},
        )

    def test_critical_change_blocks_health_gate(self):
        for feature in ("interface", "platform"):
            self.write_feature("before", feature, {"value": "up"})
            self.write_feature(
                "after",
                feature,
                {"value": "down" if feature == "interface" else "up"},
            )

        report = self.manager.compare(
            job_id="job",
            hostname="sw1",
            required_features=["interface", "platform"],
            critical_features=["interface"],
        )

        self.assertFalse(report.passed)
        self.assertEqual(report.blocking_features, ["interface"])

    def test_noncritical_protocol_change_is_reported_but_not_blocking(self):
        for feature in ("interface", "platform", "routing"):
            self.write_feature("before", feature, {"value": "before"})
            self.write_feature(
                "after",
                feature,
                {"value": "after" if feature == "routing" else "before"},
            )

        report = self.manager.compare(
            job_id="job",
            hostname="sw1",
            required_features=["interface", "platform"],
            critical_features=["interface", "platform"],
        )

        self.assertTrue(report.passed)
        routing = next(
            result
            for result in report.comparisons
            if result.feature == "routing"
        )
        self.assertTrue(routing.changed)
        self.assertFalse(routing.critical)

    def test_stale_file_not_listed_in_current_manifest_is_missing(self):
        for feature in ("interface", "platform", "vlan"):
            self.write_feature("before", feature, {"value": "up"})
        self.write_feature("after", "interface", {"value": "up"})
        self.write_feature("after", "platform", {"value": "up"})
        stale_path = (
            Path(self.temporary_directory.name)
            / "job"
            / "sw1"
            / "after"
            / "vlan.json"
        )
        self.manager._write_json(stale_path, {"value": "up"})

        report = self.manager.compare(
            job_id="job",
            hostname="sw1",
            required_features=["interface", "platform"],
            critical_features=["interface", "platform", "vlan"],
        )

        self.assertFalse(report.passed)
        vlan = next(
            result
            for result in report.comparisons
            if result.feature == "vlan"
        )
        self.assertTrue(vlan.missing_after)

    def test_missing_optional_baseline_feature_blocks_health_gate(self):
        for feature in ("interface", "platform", "routing"):
            self.write_feature("before", feature, {"value": "up"})
        for feature in ("interface", "platform"):
            self.write_feature("after", feature, {"value": "up"})

        report = self.manager.compare(
            job_id="job",
            hostname="sw1",
            required_features=["interface", "platform"],
            critical_features=["interface", "platform"],
        )

        self.assertFalse(report.passed)
        self.assertIn("routing", report.blocking_features)

    def test_tunnel_interfaces_are_not_excluded_wholesale(self):
        self.assertNotIn("((t|T)unnel.*)", FEATURE_EXCLUDES["interface"])

    def test_stp_excludes_match_genie_ops_field_names(self):
        # Genie's STP Ops model produces these keys; the exclude list must
        # match them by name or the Diff engine will report legitimate
        # topology-change counter movement as a real diff and fail the health
        # gate after every reboot.
        stp_excludes = set(FEATURE_EXCLUDES["stp"])
        for key in (
            "topology_changes",
            "time_since_topology_change",
            "bpdu_sent",
            "bpdu_received",
            "forward_transitions",
            "maker",
        ):
            self.assertIn(key, stp_excludes)

    def test_stp_topology_change_diff_is_hidden_by_excludes(self):
        # Realistic slice of a Genie STP Ops snapshot before and after a
        # reboot: everything is stable except the counters that the exclude
        # list is supposed to drop.
        before = {
            "mstp": {
                "mst_instances": {
                    "0": {
                        "bridge_priority": 32768,
                        "topology_changes": 12,
                        "time_since_topology_change": "00:05:10",
                        "bpdu_sent": 100,
                        "bpdu_received": 90,
                        "forward_transitions": 5,
                        "interfaces": {"GigabitEthernet1/0/1": {"role": "root"}},
                    }
                }
            },
            "maker": {"host": "sw1", "timestamp": "2025-01-01T00:00:00"},
        }
        after = {
            "mstp": {
                "mst_instances": {
                    "0": {
                        "bridge_priority": 32768,
                        "topology_changes": 37,
                        "time_since_topology_change": "00:00:47",
                        "bpdu_sent": 250,
                        "bpdu_received": 240,
                        "forward_transitions": 11,
                        "interfaces": {"GigabitEthernet1/0/1": {"role": "root"}},
                    }
                }
            },
            "maker": {"host": "sw1", "timestamp": "2025-01-01T00:15:00"},
        }
        manager = GenieSnapshotManager(
            self.temporary_directory.name,
            diff_factory=RegexExcludeDiff,
        )
        self.write_feature_with_manager(manager, "before", "stp", before)
        self.write_feature_with_manager(manager, "after", "stp", after)
        # Also drop in interface/platform so the health gate has all required
        # features and only the STP result is under test.
        for feature in ("interface", "platform"):
            self.write_feature_with_manager(
                manager, "before", feature, {"value": "up"}
            )
            self.write_feature_with_manager(
                manager, "after", feature, {"value": "up"}
            )

        report = manager.compare(
            job_id="job",
            hostname="sw1",
            required_features=["interface", "platform", "stp"],
            critical_features=["interface", "platform", "stp"],
        )

        stp = next(r for r in report.comparisons if r.feature == "stp")
        self.assertFalse(
            stp.changed,
            f"STP diff should be fully filtered by excludes, got: {stp.diff!r}",
        )
        self.assertTrue(report.passed)

    def write_feature_with_manager(self, manager, phase, feature, value):
        path = (
            Path(self.temporary_directory.name)
            / "job"
            / "sw1"
            / phase
            / f"{feature}.json"
        )
        manager._write_json(path, value)
        manifest_path = path.parent / "manifest.json"
        captured = []
        if manifest_path.exists():
            captured = manager._read_json(manifest_path).get(
                "captured_features", []
            )
        if feature not in captured:
            captured.append(feature)
        manager._write_json(
            manifest_path,
            {"captured_features": captured, "errors": {}},
        )


class RegexExcludeDiff:
    """Minimal stand-in for ``genie.utils.diff.Diff``.

    Recursively compares two structures while dropping any dict key that
    matches one of the ``exclude`` regexes. This mirrors the actual Genie
    behaviour closely enough to prove that the exclude *names* used in
    ``FEATURE_EXCLUDES`` line up with real snapshot keys.
    """

    def __init__(self, before, after, exclude=None):
        import re

        self._patterns = [re.compile(pattern) for pattern in (exclude or [])]
        self._before = self._strip(before)
        self._after = self._strip(after)
        self.diffs = []

    def _strip(self, value):
        if isinstance(value, dict):
            return {
                key: self._strip(inner)
                for key, inner in value.items()
                if not self._is_excluded(key)
            }
        if isinstance(value, list):
            return [self._strip(item) for item in value]
        return value

    def _is_excluded(self, key):
        text = str(key)
        return any(pattern.fullmatch(text) for pattern in self._patterns)

    def findDiff(self):
        if self._before != self._after:
            self.diffs = ["changed"]

    def __str__(self):
        return "changed" if self.diffs else ""


if __name__ == "__main__":
    unittest.main()
