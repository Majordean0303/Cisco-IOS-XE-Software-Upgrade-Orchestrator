"""Regression tests for the install-mode safety fixes.

Covers the auto-abort rollback timer, install-command error scanning, retry
bounds on the post-upgrade health gate, install-state precedence, image path
parsing and the optional per-device SSH port.
"""

import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

import src.transport as transport
from src.credentials import CredentialManager
from src.device.iosxe import (
    evaluate_compatibility,
    image_family,
    image_file_name,
    parse_install_facts,
)
from src.orchestration.engine import UpgradeEngine, UpgradeOperationError
from src.state import DeviceState, StateStore


DEVICE = {
    "hostname": "sw1",
    "ip_address": "192.0.2.1",
    "platform": "catalyst9300",
    "target_version": "17.12.4",
    "image_path": "/images/cat9k_iosxe.17.12.04.SPA.bin",
    "image_md5": "0" * 32,
}


def build_outputs(*, running, install_summary):
    return {
        "show version": (
            f"Cisco IOS XE Software, Version {running}\n"
            'System image file is "flash:packages.conf"\n'
            "Model Number : C9300-48P\n"
            "Processor board ID ABC123"
        ),
        "show boot": (
            "BOOT path-list : flash:packages.conf\nManual Boot : no"
        ),
        "show switch": "*1 Active Ready",
        "show install summary": install_summary,
        "dir flash:": "11353194496 bytes total (5648769024 bytes free)",
    }


# ---------------------------------------------------------------------------
# transport: auto-abort timer and error scanning
# ---------------------------------------------------------------------------


class RecordingConnection:
    """Netmiko stand-in that replays scripted responses."""

    def __init__(self, responses, channel_reads=(), alive=True):
        self._responses = iter(responses)
        self._channel_reads = iter(channel_reads)
        self.sent = []
        self.alive = alive

    def check_enable_mode(self):
        return True

    def enable(self):  # pragma: no cover - never needed, enable mode is on
        pass

    def send_command_timing(self, command, **kwargs):
        del kwargs
        self.sent.append(command)
        response = next(self._responses)
        if isinstance(response, Exception):
            raise response
        return response

    def read_channel(self):
        return next(self._channel_reads, "")

    def is_alive(self):
        return self.alive


class FakeTask:
    def __init__(self, connection):
        self.host = type(
            "Host",
            (),
            {"get_connection": lambda _self, name, config: connection},
        )()
        self.nornir = type("Nornir", (), {"config": object()})()


class AutoAbortTimerTests(unittest.TestCase):
    def test_timer_is_clamped_to_the_range_iosxe_accepts(self):
        self.assertEqual(transport.normalize_auto_abort_timer(90), 90)
        self.assertEqual(transport.normalize_auto_abort_timer(5), 30)
        self.assertEqual(transport.normalize_auto_abort_timer(9000), 1200)

    def test_zero_or_unset_leaves_the_device_default_in_place(self):
        self.assertIsNone(transport.normalize_auto_abort_timer(0))
        self.assertIsNone(transport.normalize_auto_abort_timer(None))
        self.assertIsNone(transport.normalize_auto_abort_timer("not a number"))

    def test_activate_sends_an_explicit_auto_abort_timer(self):
        connection = RecordingConnection(
            [
                "This operation may require a reload of the system. "
                "Do you want to proceed? [y/n]",
                EOFError("device reloading"),
            ]
        )
        result = transport.execute_install_activate(
            FakeTask(connection),
            max_wait=5,
            auto_abort_timer_minutes=90,
        )
        self.assertEqual(
            connection.sent, ["install activate auto-abort-timer 90", "y"]
        )
        self.assertTrue(result.result["reload_initiated"])
        self.assertEqual(result.result["auto_abort_timer_minutes"], 90)

    def test_activate_without_a_timer_keeps_the_bare_command(self):
        connection = RecordingConnection(
            [
                "This operation may require a reload of the system. "
                "Do you want to proceed? [y/n]",
                EOFError("device reloading"),
            ]
        )
        transport.execute_install_activate(
            FakeTask(connection), max_wait=5, auto_abort_timer_minutes=None
        )
        self.assertEqual(connection.sent, ["install activate", "y"])

    def test_timer_stop_succeeds_on_a_clean_return_to_the_prompt(self):
        connection = RecordingConnection(
            ["\nAbort timer stopped\nswitch#"],
            channel_reads=[""],
        )
        result = transport.execute_install_auto_abort_timer_stop(
            FakeTask(connection), max_wait=5
        )
        self.assertEqual(connection.sent, ["install auto-abort-timer stop"])
        self.assertFalse(result.failed)
        self.assertTrue(result.result["success"])


class InstallErrorScanningTests(unittest.TestCase):
    def test_error_before_the_reload_confirmation_is_a_hard_failure(self):
        connection = RecordingConnection(
            ["% Invalid input detected at '^' marker.\nswitch#"],
            channel_reads=[""],
        )
        result = transport.execute_install_activate(
            FakeTask(connection), max_wait=5
        )
        self.assertTrue(result.failed)
        self.assertEqual(
            result.result["error"],
            "IOS-XE reported an install command failure",
        )
        self.assertFalse(result.result["reload_initiated"])

    def test_error_after_the_reload_confirmation_is_reconciled_not_failed(self):
        # The switch accepted the reload and then printed something that
        # matches an error pattern on its way down. Marking that a command
        # failure would strand a device that is in fact upgrading.
        connection = RecordingConnection(
            [
                "This operation may require a reload of the system. "
                "Do you want to proceed? [y/n]",
                "Building configuration...\n"
                "%Error: something noisy on the console\n",
            ],
            channel_reads=[""],
            alive=False,
        )
        result = transport.execute_install_activate(
            FakeTask(connection), max_wait=5
        )
        self.assertTrue(result.result["reload_initiated"])
        self.assertNotEqual(
            result.result.get("error"),
            "IOS-XE reported an install command failure",
        )
        self.assertIn("must be reconciled", result.result["error"])

    def test_only_unscanned_output_is_examined(self):
        scanned = transport._unscanned("abc% Invalid input" + "x" * 500, 300)
        self.assertNotIn("% Invalid input", scanned)
        self.assertIn("% Invalid input", transport._unscanned("x% Invalid input", 0))


# ---------------------------------------------------------------------------
# engine: commit deadline, retry bounds, install-state precedence, ssh port
# ---------------------------------------------------------------------------


class ActivationEngine(UpgradeEngine):
    """Engine whose device I/O is scripted, for activation-path assertions."""

    def __init__(self, *args, facts, activate_result, **kwargs):
        super().__init__(*args, **kwargs)
        self.facts = facts
        self.activate_result = activate_result
        self.reconciled = False

    def _collect_facts(self, hostname, **kwargs):
        del hostname, kwargs
        return self.facts

    def _run_host_task(self, hostname, task, **kwargs):
        del hostname, kwargs
        if task.__name__ == "execute_install_activate":
            return True, self.activate_result
        return False, {"hash_match": True}

    def _reconcile_activation(self, job_id, hostname):
        del job_id, hostname
        self.reconciled = True


class EngineHarness(UpgradeEngine):
    """Engine with device I/O replaced by scripted facts."""

    def __init__(self, *args, facts=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.facts = facts
        self.task_calls = []

    def _collect_facts(self, hostname, **kwargs):
        del hostname, kwargs
        return self.facts

    def _run_host_task(self, hostname, task, **kwargs):
        del hostname, kwargs
        self.task_calls.append(task.__name__)
        return False, {"success": True}


class DummySnapshots:
    def __init__(self, directory):
        self.snapshot_directory = Path(directory)


class EngineSafetyTests(unittest.TestCase):
    def setUp(self):
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.store = StateStore(
            Path(self.temporary_directory.name) / "state.db"
        )
        self.job_id = self.store.create_job(
            inventory_path="devices.xlsx",
            config={},
            devices=[DEVICE],
            mode="upgrade",
        )

    def tearDown(self):
        self.temporary_directory.cleanup()

    def make_engine(self, *, facts=None, config=None):
        engine = EngineHarness(
            config=config or {},
            state_store=self.store,
            credential_manager=CredentialManager({}),
            snapshot_manager=DummySnapshots(self.temporary_directory.name),
            facts=facts,
            sleeper=lambda _: None,
        )
        engine._inventory_devices["sw1"] = dict(DEVICE)
        return engine

    # -- deadline bookkeeping --------------------------------------------

    def test_naive_deadline_is_read_as_utc(self):
        engine = self.make_engine()
        deadline = engine._auto_abort_deadline(
            {"metadata": {"auto_abort_expires_at": "2030-01-01T00:00:00"}}
        )
        self.assertEqual(deadline.tzinfo, timezone.utc)

    def test_missing_or_unparseable_deadline_is_not_treated_as_expired(self):
        engine = self.make_engine()
        self.assertIsNone(engine._auto_abort_deadline({"metadata": {}}))
        self.assertFalse(engine._auto_abort_window_expired({"metadata": {}}))
        self.assertFalse(
            engine._auto_abort_window_expired(
                {"metadata": {"auto_abort_expires_at": "not a timestamp"}}
            )
        )

    def test_expired_window_is_detected(self):
        engine = self.make_engine()
        past = (
            datetime.now(timezone.utc) - timedelta(minutes=1)
        ).isoformat()
        self.assertTrue(
            engine._auto_abort_window_expired(
                {"metadata": {"auto_abort_expires_at": past}}
            )
        )

    def _prepare_commit_pending(self, *, expires_at):
        self.store.transition(
            self.job_id,
            "sw1",
            DeviceState.COMMIT_PENDING,
            "Awaiting commit",
            metadata={
                "auto_abort_expires_at": expires_at,
                "version_before": "17.9.4",
            },
        )
        return self.store.get_device(self.job_id, "sw1")

    def test_expired_timer_that_reverted_the_device_is_recorded_as_rolled_back(self):
        facts = parse_install_facts(
            build_outputs(running="17.9.4", install_summary="IMG C 17.9.4")
        )
        engine = self.make_engine(facts=facts)
        past = (datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat()
        device = self._prepare_commit_pending(expires_at=past)

        proceed = engine._reconcile_expired_auto_abort(
            self.job_id, "sw1", device
        )

        self.assertFalse(proceed)
        row = self.store.get_device(self.job_id, "sw1")
        self.assertEqual(row["state"], DeviceState.ROLLED_BACK.value)
        self.assertIn("auto-abort timer expired", row["message"])
        self.assertIn("reverted", row["message"])

    def test_expired_timer_with_the_target_still_uncommitted_still_commits(self):
        facts = parse_install_facts(
            build_outputs(
                running="17.12.4",
                install_summary="IMG U 17.12.4\nIMG C 17.9.4",
            )
        )
        engine = self.make_engine(facts=facts)
        past = (datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat()
        device = self._prepare_commit_pending(expires_at=past)

        self.assertTrue(
            engine._reconcile_expired_auto_abort(self.job_id, "sw1", device)
        )

    def test_expired_timer_on_an_already_committed_target_moves_to_cleanup(self):
        facts = parse_install_facts(
            build_outputs(running="17.12.4", install_summary="IMG C 17.12.4")
        )
        engine = self.make_engine(
            facts=facts, config={"cleanup": {"soak_seconds": 0}}
        )
        past = (datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat()
        device = self._prepare_commit_pending(expires_at=past)

        self.assertFalse(
            engine._reconcile_expired_auto_abort(self.job_id, "sw1", device)
        )
        self.assertEqual(
            self.store.get_device(self.job_id, "sw1")["state"],
            DeviceState.CLEANUP_PENDING.value,
        )

    def test_commit_refuses_to_run_blind_after_the_deadline(self):
        facts = parse_install_facts(
            build_outputs(running="17.9.4", install_summary="IMG C 17.9.4")
        )
        engine = self.make_engine(facts=facts)
        past = (datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat()
        self._prepare_commit_pending(expires_at=past)

        engine._commit(self.job_id, "sw1")

        self.assertEqual(engine.task_calls, [])
        self.assertEqual(
            self.store.get_device(self.job_id, "sw1")["state"],
            DeviceState.ROLLED_BACK.value,
        )

    # -- retry bounds -----------------------------------------------------

    def test_rollback_required_is_not_retried(self):
        engine = self.make_engine()
        self.store.transition(
            self.job_id,
            "sw1",
            DeviceState.ROLLBACK_REQUIRED,
            "Needs a decision",
        )

        moved = engine._reset_failed_state(
            self.job_id,
            "sw1",
            DeviceState.ROLLBACK_REQUIRED.value,
            retry_activation=True,
        )

        self.assertFalse(moved)
        self.assertEqual(
            self.store.get_device(self.job_id, "sw1")["state"],
            DeviceState.ROLLBACK_REQUIRED.value,
        )

    def test_post_check_retries_are_bounded(self):
        engine = self.make_engine(
            config={"post_checks": {"max_recheck_attempts": 1}}
        )
        self.store.transition(
            self.job_id, "sw1", DeviceState.POST_CHECK_FAILED, "Gate failed"
        )

        first = engine._reset_failed_state(
            self.job_id,
            "sw1",
            DeviceState.POST_CHECK_FAILED.value,
            retry_activation=False,
        )
        self.assertTrue(first)
        self.assertEqual(
            self.store.get_device(self.job_id, "sw1")["state"],
            DeviceState.POST_CHECKING.value,
        )

        self.store.transition(
            self.job_id, "sw1", DeviceState.POST_CHECK_FAILED, "Gate failed"
        )
        second = engine._reset_failed_state(
            self.job_id,
            "sw1",
            DeviceState.POST_CHECK_FAILED.value,
            retry_activation=False,
        )
        self.assertFalse(second)
        self.assertEqual(
            self.store.get_device(self.job_id, "sw1")["state"],
            DeviceState.POST_CHECK_FAILED.value,
        )

    def test_error_after_reload_confirmation_reconciles_instead_of_failing(self):
        # Pairs with the transport test above: the engine must send this result
        # to reconciliation, because the device is already reloading.
        facts = parse_install_facts(
            build_outputs(
                running="17.9.4",
                install_summary="IMG C 17.9.4\nIMG I 17.12.4",
            )
        )
        engine = ActivationEngine(
            config={
                "pre_checks": {"minimum_free_space_mb": 1},
                "upgrade": {"save_configuration_before_reload": False},
            },
            state_store=self.store,
            credential_manager=CredentialManager({}),
            snapshot_manager=DummySnapshots(self.temporary_directory.name),
            facts=facts,
            activate_result={
                "success": False,
                "reload_initiated": True,
                "error": (
                    "IOS-XE reported an error after the reload was confirmed; "
                    "device state must be reconciled"
                ),
            },
        )
        engine._inventory_devices["sw1"] = dict(DEVICE)
        self.store.transition(
            self.job_id,
            "sw1",
            DeviceState.ACTIVATION_PENDING,
            "Ready to activate",
            metadata={
                "facts_before": {"serial": "ABC123"},
                "device_image_path": "flash:cat9k_iosxe.17.12.04.SPA.bin",
            },
        )

        engine._activate_device(self.job_id, "sw1")

        self.assertTrue(engine.reconciled)
        self.assertNotEqual(
            self.store.get_device(self.job_id, "sw1")["state"],
            DeviceState.ACTIVATION_FAILED.value,
        )

    def test_activation_records_the_commit_deadline(self):
        facts = parse_install_facts(
            build_outputs(
                running="17.9.4",
                install_summary="IMG C 17.9.4\nIMG I 17.12.4",
            )
        )
        engine = ActivationEngine(
            config={
                "pre_checks": {"minimum_free_space_mb": 1},
                "upgrade": {
                    "save_configuration_before_reload": False,
                    "auto_abort_timer_minutes": 240,
                },
            },
            state_store=self.store,
            credential_manager=CredentialManager({}),
            snapshot_manager=DummySnapshots(self.temporary_directory.name),
            facts=facts,
            activate_result={"success": True, "reload_initiated": True},
        )
        engine._inventory_devices["sw1"] = dict(DEVICE)
        self.store.transition(
            self.job_id,
            "sw1",
            DeviceState.ACTIVATION_PENDING,
            "Ready to activate",
            metadata={
                "facts_before": {"serial": "ABC123"},
                "device_image_path": "flash:cat9k_iosxe.17.12.04.SPA.bin",
            },
        )

        engine._activate_device(self.job_id, "sw1")

        metadata = self.store.get_device(self.job_id, "sw1")["metadata"]
        self.assertEqual(metadata["auto_abort_timer_minutes"], 240)
        self.assertTrue(metadata["auto_abort_timer_explicit"])
        deadline = datetime.fromisoformat(metadata["auto_abort_expires_at"])
        self.assertGreater(deadline, datetime.now(timezone.utc))
        self.assertLess(
            deadline,
            datetime.now(timezone.utc) + timedelta(minutes=241),
        )

    def test_retries_write_to_their_own_snapshot_phase(self):
        self.assertEqual(UpgradeEngine._post_check_phase({}), "after")
        self.assertEqual(
            UpgradeEngine._post_check_phase({"post_check_attempts": 1}),
            "after-retry-1",
        )
        self.assertEqual(
            UpgradeEngine._post_check_phase({"post_check_attempts": 2}),
            "after-retry-2",
        )

    # -- install state precedence ----------------------------------------

    def test_committed_state_wins_over_inactive_for_the_same_version(self):
        facts = parse_install_facts(
            build_outputs(
                running="17.12.4",
                install_summary="IMG I 17.12.4\nIMG C 17.12.4",
            )
        )
        engine = self.make_engine(facts=facts)
        self.assertEqual(engine._state_for_version(facts, "17.12.4"), "C")

    def test_unknown_version_has_no_state(self):
        facts = parse_install_facts(
            build_outputs(running="17.9.4", install_summary="IMG C 17.9.4")
        )
        engine = self.make_engine(facts=facts)
        self.assertIsNone(engine._state_for_version(facts, "17.12.4"))

    # -- ssh port ---------------------------------------------------------

    def test_ssh_port_defaults_to_22(self):
        self.assertEqual(UpgradeEngine._device_ssh_port({}), 22)
        self.assertEqual(UpgradeEngine._device_ssh_port({"ssh_port": ""}), 22)
        self.assertEqual(UpgradeEngine._device_ssh_port({"ssh_port": None}), 22)

    def test_ssh_port_is_read_from_the_inventory(self):
        self.assertEqual(
            UpgradeEngine._device_ssh_port({"ssh_port": "2222"}), 2222
        )

    def test_invalid_ssh_port_is_rejected(self):
        for value in ("not-a-port", 0, 70000):
            with self.assertRaises(UpgradeOperationError):
                UpgradeEngine._device_ssh_port(
                    {"hostname": "sw1", "ssh_port": value}
                )

    # -- raw output stays out of the state database -----------------------

    def test_facts_dict_excludes_raw_cli_text_by_default(self):
        facts = parse_install_facts(
            build_outputs(running="17.9.4", install_summary="IMG C 17.9.4")
        )
        self.assertNotIn("raw", facts.to_dict())
        self.assertIn("raw", facts.to_dict(include_raw=True))
        self.assertIn("show version", facts.to_dict(include_raw=True)["raw"])

    def test_raw_output_is_written_beside_the_snapshots(self):
        facts = parse_install_facts(
            build_outputs(running="17.9.4", install_summary="IMG C 17.9.4")
        )
        engine = self.make_engine(facts=facts)
        path = engine._persist_facts_raw(
            self.job_id, "sw1", "validate", facts
        )
        self.assertTrue(Path(path).is_file())
        self.assertIn("dir flash:", Path(path).read_text(encoding="utf-8"))


# ---------------------------------------------------------------------------
# device layer: image path parsing
# ---------------------------------------------------------------------------


class ImagePathTests(unittest.TestCase):
    def test_windows_paths_are_split_on_a_posix_host(self):
        self.assertEqual(
            image_file_name(
                r"\\share\images\cat9k_iosxe.17.12.04.SPA.bin"
            ),
            "cat9k_iosxe.17.12.04.SPA.bin",
        )
        self.assertEqual(
            image_file_name("flash:cat9k_iosxe.17.12.04.SPA.bin"),
            "cat9k_iosxe.17.12.04.SPA.bin",
        )
        self.assertEqual(
            image_file_name("/images/cat9k_iosxe.17.12.04.SPA.bin"),
            "cat9k_iosxe.17.12.04.SPA.bin",
        )

    def test_image_family_still_reads_the_prefix(self):
        self.assertEqual(
            image_family(r"C:\images\cat9k_iosxe.17.12.04.SPA.bin"),
            "cat9k_iosxe",
        )

    def _report(self, image_path):
        outputs = build_outputs(
            running="17.9.4", install_summary="IMG C 17.9.4"
        )
        facts = parse_install_facts(outputs)
        device = dict(DEVICE, image_path=image_path)
        return evaluate_compatibility(
            device=device,
            facts=facts,
            image_size_bytes=None,
            config={"pre_checks": {"minimum_free_space_mb": 1}},
        )

    def _check(self, report, name):
        return next(check for check in report.checks if check.name == name)

    def test_a_version_shaped_directory_no_longer_decides_the_gate(self):
        report = self._report(
            r"\\share\images\17.03.05\cat9k_iosxe.17.12.04.SPA.bin"
        )
        self.assertTrue(self._check(report, "image_version").passed)

    def test_a_genuinely_mismatched_filename_still_fails(self):
        report = self._report("/images/cat9k_iosxe.17.06.05.SPA.bin")
        self.assertFalse(self._check(report, "image_version").passed)


if __name__ == "__main__":
    unittest.main()
