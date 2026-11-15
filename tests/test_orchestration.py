import tempfile
import threading
import unittest
from pathlib import Path

from src.credentials import CredentialManager
from src.device.iosxe import parse_install_facts
from src.orchestration.batching import (
    build_upgrade_batches,
    validate_batches,
)
from src.orchestration.engine import UpgradeEngine, UpgradeOperationError
from src.state import DeviceLockedError, DeviceState, StateStore


DEVICE = {
    "hostname": "sw1",
    "ip_address": "192.0.2.1",
    "platform": "catalyst9300",
    "target_version": "17.12.4",
    "image_path": "/images/cat9k_iosxe.17.12.04.SPA.bin",
    "image_md5": "0" * 32,
}


class DummySnapshots:
    pass


class ResumeEngine(UpgradeEngine):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.calls = []
        self.expected_versions = []

    def _wait_for_version(
        self,
        job_id,
        hostname,
        *,
        expected_version,
        success_state,
        success_message,
        failure_state=DeviceState.RECONNECT_TIMEOUT,
        wait_metadata=None,
    ):
        del failure_state, wait_metadata
        self.calls.append("reconnect")
        self.expected_versions.append(expected_version)
        self._transition(
            job_id,
            hostname,
            success_state,
            success_message,
        )
        return True

    def _reconcile_activation(self, job_id, hostname):
        self.calls.append("reconcile_activation")
        self._transition(
            job_id,
            hostname,
            DeviceState.REBOOTING,
            "Activation is being reconciled",
        )

    def _post_checks(self, job_id, hostname):
        self.calls.append("post_checks")
        self._transition(
            job_id,
            hostname,
            DeviceState.COMMIT_PENDING,
            "Post checks passed",
        )

    def _commit(self, job_id, hostname):
        self.calls.append("commit")
        self._transition(
            job_id,
            hostname,
            DeviceState.CLEANUP_PENDING,
            "Committed",
            metadata={"cleanup_eligible_at": "2000-01-01T00:00:00+00:00"},
        )

    def _cleanup(self, job_id, hostname, *, explicit):
        del explicit
        self.calls.append("cleanup")
        self._transition(
            job_id,
            hostname,
            DeviceState.COMPLETE,
            "Complete",
        )


class ActivationPreflightEngine(UpgradeEngine):
    def __init__(self, *args, facts, **kwargs):
        super().__init__(*args, **kwargs)
        self.facts = facts
        self.task_calls = []

    def _collect_facts(self, hostname, **kwargs):
        del hostname, kwargs
        return self.facts

    def _run_host_task(self, hostname, task, **kwargs):
        del hostname, kwargs
        self.task_calls.append(task.__name__)
        return False, {"verified": True}


class BatchingTests(unittest.TestCase):
    def test_canary_and_redundancy_groups_are_separated(self):
        devices = [
            {"hostname": "a", "redundancy_group": "pair-1"},
            {"hostname": "b", "redundancy_group": "pair-1"},
            {"hostname": "c", "redundancy_group": "pair-2"},
            {"hostname": "d"},
        ]

        batches = build_upgrade_batches(
            devices,
            batch_size=3,
            canary_count=1,
        )
        validate_batches(batches, devices)

        self.assertEqual(batches[0], ["a"])
        self.assertFalse(any({"a", "b"}.issubset(batch) for batch in batches))

    def test_duplicate_batch_targets_are_dispatched_once(self):
        class CountingEngine(ResumeEngine):
            def _run_device(self, job_id, hostname, **kwargs):
                del job_id, kwargs
                self.calls.append(hostname)

        with tempfile.TemporaryDirectory() as temporary_directory:
            store = StateStore(Path(temporary_directory) / "state.db")
            engine = CountingEngine(
                config={"workflow": {"max_parallel_devices": 2}},
                state_store=store,
                credential_manager=CredentialManager({}),
                snapshot_manager=DummySnapshots(),
            )

            engine._run_batch(
                "unused",
                ["sw1", "sw1"],
                run_mode="cleanup",
                retry_failed=True,
                retry_activation=False,
                dry_run=False,
                live_status=False,
            )

        self.assertEqual(engine.calls, ["sw1"])


class ResumeTests(unittest.TestCase):
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
        self.engine = ResumeEngine(
            config={"cleanup": {"auto_cleanup": True}},
            state_store=self.store,
            credential_manager=CredentialManager({}),
            snapshot_manager=DummySnapshots(),
            sleeper=lambda _: None,
        )
        self.store.acquire_job_execution_lock(
            self.job_id,
            self.engine.execution_owner,
        )

    def tearDown(self):
        self.store.release_job_execution_lock(
            self.job_id,
            self.engine.execution_owner,
        )
        self.store.release_job_execution_lock(
            self.job_id,
            "replacement-owner",
        )
        self.temporary_directory.cleanup()

    def test_resume_after_reboot_continues_without_reactivating(self):
        self.store.transition(
            self.job_id,
            "sw1",
            DeviceState.REBOOTING,
            "Reload was initiated before process exit",
        )

        self.engine._run_device(
            self.job_id,
            "sw1",
            run_mode="upgrade",
            retry_failed=False,
            retry_activation=False,
            dry_run=False,
        )

        self.assertEqual(
            self.store.get_device(self.job_id, "sw1")["state"],
            DeviceState.COMPLETE.value,
        )
        self.assertEqual(
            self.engine.calls,
            ["reconnect", "post_checks", "commit", "cleanup"],
        )

    def test_rollback_reboot_resumes_against_previous_version(self):
        self.store.transition(
            self.job_id,
            "sw1",
            DeviceState.REBOOTING,
            "Rollback reload initiated",
            metadata={
                "rollback_in_progress": True,
                "expected_version": "17.9.4a",
            },
        )

        self.engine._run_device(
            self.job_id,
            "sw1",
            run_mode="upgrade",
            retry_failed=False,
            retry_activation=False,
            dry_run=False,
        )

        self.assertEqual(
            self.store.get_device(self.job_id, "sw1")["state"],
            DeviceState.ROLLED_BACK.value,
        )
        self.assertEqual(self.engine.expected_versions, ["17.9.4a"])
        self.assertEqual(self.engine.calls, ["reconnect"])
        self.assertNotIn("activate", self.engine.calls)

    def test_interrupted_activation_is_reconciled_not_replayed(self):
        self.store.transition(
            self.job_id,
            "sw1",
            DeviceState.ACTIVATING,
            "Activation intent persisted",
        )

        self.engine._run_device(
            self.job_id,
            "sw1",
            run_mode="upgrade",
            retry_failed=False,
            retry_activation=False,
            dry_run=False,
        )

        self.assertEqual(
            self.engine.calls,
            [
                "reconcile_activation",
                "reconnect",
                "post_checks",
                "commit",
                "cleanup",
            ],
        )
        self.assertNotIn("activate", self.engine.calls)

    def test_cleanup_mode_refuses_staged_device(self):
        self.store.transition(
            self.job_id,
            "sw1",
            DeviceState.STAGED,
            "Image is staged",
        )

        with self.assertRaisesRegex(
            UpgradeOperationError, "Refusing cleanup dispatch"
        ):
            self.engine._run_device(
                self.job_id,
                "sw1",
                run_mode="cleanup",
                retry_failed=True,
                retry_activation=False,
                dry_run=False,
            )

    def test_stage_mode_refuses_activation_state_without_changing_it(self):
        self.store.transition(
            self.job_id,
            "sw1",
            DeviceState.ACTIVATION_PENDING,
            "Waiting for maintenance window",
        )

        with self.assertRaisesRegex(
            UpgradeOperationError, "Refusing stage-only dispatch"
        ):
            self.engine._run_device(
                self.job_id,
                "sw1",
                run_mode="stage",
                retry_failed=True,
                retry_activation=True,
                dry_run=False,
            )

        self.assertEqual(
            self.store.get_device(self.job_id, "sw1")["state"],
            DeviceState.ACTIVATION_PENDING.value,
        )

    def test_lost_execution_lease_fences_state_transition(self):
        self.store.acquire_device_lock(
            self.job_id,
            "sw1",
            self.engine.execution_owner,
        )
        lease_lost = threading.Event()
        lease_lost.set()
        self.engine._set_execution_context(
            self.job_id,
            "sw1",
            lease_lost,
        )
        try:
            with self.assertRaises(DeviceLockedError):
                self.engine._transition(
                    self.job_id,
                    "sw1",
                    DeviceState.VALIDATING,
                    "Must not be written",
                )
        finally:
            self.engine._clear_execution_context()
            self.store.release_execution_lock(
                self.job_id,
                "sw1",
                self.engine.execution_owner,
            )

        self.assertEqual(
            self.store.get_device(self.job_id, "sw1")["state"],
            DeviceState.PENDING.value,
        )

    def test_replaced_execution_owner_fences_state_transition(self):
        self.store.acquire_device_lock(
            self.job_id,
            "sw1",
            self.engine.execution_owner,
        )
        lease_lost = threading.Event()
        self.engine._set_execution_context(
            self.job_id,
            "sw1",
            lease_lost,
        )
        self.store.release_execution_lock(
            self.job_id,
            "sw1",
            self.engine.execution_owner,
        )
        self.store.release_job_execution_lock(
            self.job_id,
            self.engine.execution_owner,
        )
        self.store.acquire_job_execution_lock(
            self.job_id,
            "replacement-owner",
        )
        self.store.acquire_device_lock(
            self.job_id,
            "sw1",
            "replacement-owner",
        )
        try:
            with self.assertRaises(DeviceLockedError):
                self.engine._transition(
                    self.job_id,
                    "sw1",
                    DeviceState.VALIDATING,
                    "Must not be written",
                )
        finally:
            self.engine._clear_execution_context()
            self.store.release_execution_lock(
                self.job_id,
                "sw1",
                "replacement-owner",
            )

        self.assertEqual(
            self.store.get_device(self.job_id, "sw1")["state"],
            DeviceState.PENDING.value,
        )

    def test_activation_preflight_rejects_other_inactive_version(self):
        outputs = {
            "show version": (
                "Cisco IOS XE Software, Version 17.9.4\n"
                'System image file is "flash:packages.conf"\n'
                "Model Number : C9300-48P\n"
                "Processor board ID ABC123"
            ),
            "show boot": (
                "BOOT path-list : flash:packages.conf\n"
                "Manual Boot : no"
            ),
            "show switch": "*1 Active Ready",
            "show install summary": (
                "IMG C 17.9.4\n"
                "IMG I 17.12.4\n"
                "IMG I 17.6.6"
            ),
            "dir flash:": (
                "11353194496 bytes total (5648769024 bytes free)"
            ),
        }
        engine = ActivationPreflightEngine(
            config={
                "pre_checks": {"minimum_free_space_mb": 1},
                "upgrade": {"save_configuration_before_reload": False},
            },
            state_store=self.store,
            credential_manager=CredentialManager({}),
            snapshot_manager=DummySnapshots(),
            facts=parse_install_facts(outputs),
        )
        engine._inventory_devices["sw1"] = dict(DEVICE)
        self.store.transition(
            self.job_id,
            "sw1",
            DeviceState.ACTIVATION_PENDING,
            "Ready to activate",
            metadata={
                "facts_before": {"serial": "ABC123"},
                "device_image_path": (
                    "FLASH:cat9k_iosxe.17.12.04.SPA.bin"
                ),
            },
        )

        engine._activate_device(self.job_id, "sw1")

        device = self.store.get_device(self.job_id, "sw1")
        self.assertEqual(device["state"], DeviceState.ACTIVATION_FAILED.value)
        self.assertIn("Other inactive IOS-XE versions", device["message"])
        self.assertEqual(engine.task_calls, [])

    def test_cleanup_rejects_image_staged_during_soak(self):
        outputs = {
            "show version": (
                "Cisco IOS XE Software, Version 17.12.4\n"
                'System image file is "flash:packages.conf"\n'
                "Model Number : C9300-48P\n"
                "Processor board ID ABC123"
            ),
            "show boot": (
                "BOOT path-list : flash:packages.conf\n"
                "Manual Boot : no"
            ),
            "show switch": "*1 Active Ready",
            "show install summary": (
                "IMG C 17.12.4\n"
                "IMG I 17.9.4\n"
                "IMG I 17.15.1"
            ),
            "dir flash:": (
                "11353194496 bytes total (5648769024 bytes free)"
            ),
        }
        engine = ActivationPreflightEngine(
            config={"cleanup": {"command_timeout": 60}},
            state_store=self.store,
            credential_manager=CredentialManager({}),
            snapshot_manager=DummySnapshots(),
            facts=parse_install_facts(outputs),
        )
        self.store.transition(
            self.job_id,
            "sw1",
            DeviceState.CLEANUP_PENDING,
            "Soak complete",
            metadata={
                "cleanup_eligible_at": "2000-01-01T00:00:00+00:00",
                "facts_before": {"serial": "ABC123"},
                "facts_committed": {
                    "install_states": {"I": ["17.9.4"]}
                },
            },
        )

        engine._cleanup(self.job_id, "sw1", explicit=True)

        device = self.store.get_device(self.job_id, "sw1")
        self.assertEqual(device["state"], DeviceState.CLEANUP_FAILED.value)
        self.assertIn("Unexpected inactive packages", device["message"])
        self.assertEqual(engine.task_calls, [])

    def test_interrupted_activation_polls_before_allowing_replay(self):
        observed = {}

        def capture_wait(job_id, hostname, **kwargs):
            observed.update(
                {"job_id": job_id, "hostname": hostname, **kwargs}
            )
            return False

        self.engine._wait_for_version = capture_wait
        UpgradeEngine._reconcile_activation(
            self.engine,
            self.job_id,
            "sw1",
        )

        self.assertEqual(
            observed["failure_state"],
            DeviceState.ACTIVATION_UNCERTAIN,
        )
        self.assertEqual(observed["expected_version"], "17.12.4")

    def test_unfinished_canary_halts_batch_progression(self):
        self.store.transition(
            self.job_id,
            "sw1",
            DeviceState.ACTIVATION_PENDING,
            "Outside maintenance window",
        )

        self.assertTrue(
            self.engine._must_halt_after_batch(
                self.job_id,
                ["sw1"],
                run_mode="upgrade",
                is_canary=True,
            )
        )

    def test_resume_rejects_changed_safety_policy(self):
        changed_engine = UpgradeEngine(
            config={"safety": {"allow_stacks": True}},
            state_store=self.store,
            credential_manager=CredentialManager({}),
            snapshot_manager=DummySnapshots(),
        )
        job = self.store.get_job(self.job_id)

        with self.assertRaisesRegex(
            UpgradeOperationError,
            "does not match the policy persisted",
        ):
            changed_engine._bind_job_config(job)


if __name__ == "__main__":
    unittest.main()
