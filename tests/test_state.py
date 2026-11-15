import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from src.state import (
    DeviceLockedError,
    DeviceState,
    StateConflictError,
    StateStore,
)


DEVICE = {
    "hostname": "sw1",
    "ip_address": "192.0.2.1",
    "platform": "catalyst9300",
    "target_version": "17.12.4",
    "image_path": "/images/cat9k_iosxe.17.12.04.SPA.bin",
    "image_md5": "0" * 32,
}


class StateStoreTests(unittest.TestCase):
    def setUp(self):
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.database = Path(self.temporary_directory.name) / "state.db"
        self.store = StateStore(self.database)

    def tearDown(self):
        self.temporary_directory.cleanup()

    def create_job(self, hostname="sw1"):
        device = dict(DEVICE, hostname=hostname)
        return self.store.create_job(
            inventory_path="devices.xlsx",
            config={
                "credentials": {
                    "username": "operator",
                    "password": "do-not-store",
                    "enable_password": "also-secret",
                }
            },
            devices=[device],
            mode="upgrade",
        )

    def test_state_survives_reopening_database(self):
        job_id = self.create_job()
        self.store.transition(
            job_id,
            "sw1",
            DeviceState.REBOOTING,
            "Reload initiated",
            expected=[DeviceState.PENDING],
            metadata={"activation_requested": True},
        )

        reopened = StateStore(self.database)
        device = reopened.get_device(job_id, "sw1")

        self.assertEqual(device["state"], DeviceState.REBOOTING.value)
        self.assertTrue(device["metadata"]["activation_requested"])

    def test_config_secrets_are_redacted(self):
        job_id = self.create_job()

        config = self.store.get_job(job_id)["config"]

        self.assertEqual(config["credentials"]["username"], "operator")
        self.assertEqual(config["credentials"]["password"], "<redacted>")
        self.assertEqual(
            config["credentials"]["enable_password"], "<redacted>"
        )

    def test_transition_detects_stale_expected_state(self):
        job_id = self.create_job()

        with self.assertRaises(StateConflictError):
            self.store.transition(
                job_id,
                "sw1",
                DeviceState.READY,
                "Ready",
                expected=[DeviceState.VALIDATING],
            )

    def test_device_lock_blocks_another_job(self):
        first_job = self.create_job()
        second_job = self.store.create_job(
            inventory_path="devices.xlsx",
            config={},
            devices=[DEVICE],
            mode="upgrade",
        )
        self.store.acquire_job_execution_lock(first_job, "owner-1")
        self.store.acquire_job_execution_lock(second_job, "owner-2")
        self.store.acquire_device_lock(first_job, "sw1", "owner-1")

        with self.assertRaises(DeviceLockedError):
            self.store.acquire_device_lock(second_job, "sw1", "owner-2")

    def test_same_job_cannot_be_run_by_two_execution_owners(self):
        job_id = self.create_job()
        self.store.acquire_job_execution_lock(job_id, "owner-1")
        self.store.acquire_device_lock(job_id, "sw1", "owner-1")

        with self.assertRaises(DeviceLockedError):
            self.store.acquire_device_lock(job_id, "sw1", "owner-2")

    def test_execution_lock_is_not_reentrant_for_same_owner(self):
        job_id = self.create_job()
        self.store.acquire_job_execution_lock(job_id, "owner-1")
        self.store.acquire_device_lock(job_id, "sw1", "owner-1")

        with self.assertRaises(DeviceLockedError):
            self.store.acquire_device_lock(job_id, "sw1", "owner-1")

    def test_hostname_alias_in_another_job_uses_same_device_lock(self):
        first_job = self.create_job()
        alias = dict(DEVICE, hostname="sw1-alias")
        second_job = self.store.create_job(
            inventory_path="devices.xlsx",
            config={},
            devices=[alias],
            mode="upgrade",
        )
        self.store.acquire_job_execution_lock(first_job, "owner-1")
        self.store.acquire_job_execution_lock(second_job, "owner-2")
        self.store.acquire_device_lock(first_job, "sw1", "owner-1")

        with self.assertRaises(DeviceLockedError):
            self.store.acquire_device_lock(
                second_job,
                "sw1-alias",
                "owner-2",
            )

    def test_hostname_keyed_reservation_is_migrated_on_reopen(self):
        first_job = self.create_job()
        alias = dict(DEVICE, hostname="sw1-alias")
        second_job = self.store.create_job(
            inventory_path="devices.xlsx",
            config={},
            devices=[alias],
            mode="upgrade",
        )
        with self.store._connection(immediate=True) as connection:
            connection.execute(
                """
                INSERT INTO device_locks (hostname, job_id, acquired_at)
                VALUES ('sw1', ?, '2024-01-01T00:00:00+00:00')
                """,
                (first_job,),
            )

        reopened = StateStore(self.database)
        reopened.acquire_job_execution_lock(second_job, "owner-2")

        with self.assertRaises(DeviceLockedError):
            reopened.acquire_device_lock(
                second_job,
                "sw1-alias",
                "owner-2",
            )

    def test_stale_owner_cannot_release_device_reservation(self):
        job_id = self.create_job()
        self.store.acquire_job_execution_lock(job_id, "owner-1")
        self.store.acquire_device_lock(job_id, "sw1", "owner-1")

        with self.assertRaises(DeviceLockedError):
            self.store.release_device_lock(job_id, "sw1", "owner-2")

    def test_only_one_process_can_execute_a_job(self):
        job_id = self.create_job()
        self.store.acquire_job_execution_lock(job_id, "owner-1")

        with self.assertRaises(DeviceLockedError):
            self.store.acquire_job_execution_lock(job_id, "owner-2")

    def test_job_takeover_waits_for_device_leases_to_expire(self):
        job_id = self.create_job()
        self.store.acquire_job_execution_lock(job_id, "owner-1")
        self.store.acquire_device_lock(job_id, "sw1", "owner-1")
        past = (datetime.now(timezone.utc) - timedelta(seconds=1)).isoformat()
        with self.store._connection(immediate=True) as connection:
            connection.execute(
                """
                UPDATE job_execution_locks
                SET expires_at = ?
                WHERE job_id = ?
                """,
                (past, job_id),
            )

        with self.assertRaisesRegex(
            DeviceLockedError,
            "active device operations",
        ):
            self.store.acquire_job_execution_lock(job_id, "owner-2")
        with self.assertRaises(DeviceLockedError):
            self.store.renew_execution_lock(
                job_id,
                "sw1",
                "owner-1",
            )

        with self.store._connection(immediate=True) as connection:
            connection.execute(
                """
                UPDATE execution_locks
                SET expires_at = ?
                WHERE job_id = ?
                """,
                (past, job_id),
            )
        self.store.acquire_job_execution_lock(job_id, "owner-2")

    def test_lock_migration_preserves_active_alias_owner(self):
        stale_job = self.create_job()
        active_device = dict(DEVICE, hostname="sw1-alias")
        active_job = self.store.create_job(
            inventory_path="devices.xlsx",
            config={},
            devices=[active_device],
            mode="upgrade",
        )
        now = datetime.now(timezone.utc)
        past = (now - timedelta(minutes=5)).isoformat()
        future = (now + timedelta(minutes=5)).isoformat()
        with self.store._connection(immediate=True) as connection:
            connection.executemany(
                """
                INSERT INTO device_locks (hostname, job_id, acquired_at)
                VALUES (?, ?, ?)
                """,
                [
                    ("sw1", stale_job, past),
                    ("sw1-alias", active_job, now.isoformat()),
                ],
            )
            connection.executemany(
                """
                INSERT INTO execution_locks (
                    hostname, job_id, owner_token, acquired_at, expires_at
                ) VALUES (?, ?, ?, ?, ?)
                """,
                [
                    ("sw1", stale_job, "stale-owner", past, past),
                    (
                        "sw1-alias",
                        active_job,
                        "active-owner",
                        now.isoformat(),
                        future,
                    ),
                ],
            )

        reopened = StateStore(self.database)
        with reopened._connection() as connection:
            reservation = connection.execute(
                "SELECT hostname, job_id FROM device_locks"
            ).fetchone()
            execution = connection.execute(
                """
                SELECT hostname, job_id, owner_token
                FROM execution_locks
                """
            ).fetchone()

        self.assertEqual(reservation["hostname"], "ip:192.0.2.1")
        self.assertEqual(reservation["job_id"], active_job)
        self.assertEqual(execution["job_id"], active_job)
        self.assertEqual(execution["owner_token"], "active-owner")

    def test_transition_is_atomically_fenced_by_replacement_owner(self):
        job_id = self.create_job()
        self.store.acquire_job_execution_lock(job_id, "owner-1")
        self.store.acquire_device_lock(job_id, "sw1", "owner-1")
        self.store.release_execution_lock(job_id, "sw1", "owner-1")
        self.store.release_job_execution_lock(job_id, "owner-1")
        self.store.acquire_job_execution_lock(job_id, "owner-2")
        self.store.acquire_device_lock(job_id, "sw1", "owner-2")

        with self.assertRaises(DeviceLockedError):
            self.store.transition(
                job_id,
                "sw1",
                DeviceState.VALIDATING,
                "Stale transition",
                owner_token="owner-1",
                require_job_lock=True,
            )

        self.assertEqual(
            self.store.get_device(job_id, "sw1")["state"],
            DeviceState.PENDING.value,
        )

    def test_terminal_job_reservation_is_reclaimed(self):
        first_job = self.create_job()
        self.store.transition(
            first_job,
            "sw1",
            DeviceState.ROLLED_BACK,
            "Rollback completed",
        )
        self.store.acquire_job_execution_lock(first_job, "owner-1")
        self.store.acquire_device_lock(first_job, "sw1", "owner-1")
        self.store.release_execution_lock(first_job, "sw1", "owner-1")
        self.store.release_job_execution_lock(first_job, "owner-1")
        alias = dict(DEVICE, hostname="sw1-alias")
        second_job = self.store.create_job(
            inventory_path="devices.xlsx",
            config={},
            devices=[alias],
            mode="upgrade",
        )
        self.store.acquire_job_execution_lock(second_job, "owner-2")

        self.store.acquire_device_lock(
            second_job,
            "sw1-alias",
            "owner-2",
        )

    def test_lock_migration_rolls_back_as_one_transaction(self):
        job_id = self.create_job()
        with self.store._connection(immediate=True) as connection:
            connection.execute(
                """
                INSERT INTO device_locks (hostname, job_id, acquired_at)
                VALUES ('sw1', ?, '2024-01-01T00:00:00+00:00')
                """,
                (job_id,),
            )

        with patch.object(
            StateStore,
            "_canonicalize_stored_lock_key",
            side_effect=RuntimeError("migration interrupted"),
        ):
            with self.assertRaisesRegex(RuntimeError, "migration interrupted"):
                StateStore(self.database)

        with self.store._connection() as connection:
            row = connection.execute(
                "SELECT hostname FROM device_locks"
            ).fetchone()
        self.assertEqual(row["hostname"], "sw1")

    def test_job_and_batch_plan_are_persisted_together(self):
        second = dict(DEVICE, hostname="sw2", ip_address="192.0.2.2")
        job_id = self.store.create_job(
            inventory_path="devices.xlsx",
            config={},
            devices=[DEVICE, second],
            mode="upgrade",
            batches=[["sw1"], ["sw2"]],
        )

        reopened = StateStore(self.database)
        devices = reopened.get_devices(job_id)

        self.assertEqual(
            {device["hostname"]: device["batch_number"] for device in devices},
            {"sw1": 1, "sw2": 2},
        )

    def test_duplicate_management_addresses_are_rejected(self):
        alias = dict(DEVICE, hostname="sw1-alias")

        with self.assertRaisesRegex(
            ValueError, "duplicate management addresses"
        ):
            self.store.create_job(
                inventory_path="devices.xlsx",
                config={},
                devices=[DEVICE, alias],
                mode="upgrade",
            )

    def _leave_stuck_reservation(self, job_id, state):
        self.store.transition(job_id, "sw1", state, "Reached failure state")
        self.store.acquire_job_execution_lock(job_id, "owner-1")
        self.store.acquire_device_lock(job_id, "sw1", "owner-1")
        self.store.release_execution_lock(job_id, "sw1", "owner-1")
        self.store.release_job_execution_lock(job_id, "owner-1")

    def test_cancel_job_frees_reservation_for_a_later_job(self):
        first_job = self.create_job()
        self._leave_stuck_reservation(first_job, DeviceState.STAGING_FAILED)

        alias = dict(DEVICE, hostname="sw1-alias")
        second_job = self.store.create_job(
            inventory_path="devices.xlsx",
            config={},
            devices=[alias],
            mode="upgrade",
        )
        self.store.acquire_job_execution_lock(second_job, "owner-2")
        with self.assertRaises(DeviceLockedError):
            self.store.acquire_device_lock(second_job, "sw1-alias", "owner-2")

        summary = self.store.cancel_job(first_job, reason="image not found")

        self.assertEqual(summary["released"], 1)
        self.assertEqual(
            self.store.get_device(first_job, "sw1")["state"],
            DeviceState.FAILED.value,
        )
        self.assertEqual(self.store.get_job(first_job)["status"], "cancelled")
        # The reservation is gone, so the second job can now take the device.
        self.store.acquire_device_lock(second_job, "sw1-alias", "owner-2")

    def test_cancel_requires_force_after_activation(self):
        job_id = self.create_job()
        self._leave_stuck_reservation(job_id, DeviceState.REBOOTING)

        with self.assertRaisesRegex(StateConflictError, "mid-upgrade"):
            self.store.cancel_job(job_id)

        # The device is untouched until the operator forces the cancel.
        self.assertEqual(
            self.store.get_device(job_id, "sw1")["state"],
            DeviceState.REBOOTING.value,
        )
        summary = self.store.cancel_job(job_id, force=True)
        self.assertEqual(summary["released"], 1)
        self.assertEqual(
            self.store.get_device(job_id, "sw1")["state"],
            DeviceState.FAILED.value,
        )

    def test_cancel_refuses_while_execution_lease_is_active(self):
        job_id = self.create_job()
        self.store.transition(
            job_id, "sw1", DeviceState.STAGING_FAILED, "Failed"
        )
        self.store.acquire_job_execution_lock(job_id, "owner-1")
        self.store.acquire_device_lock(job_id, "sw1", "owner-1")

        with self.assertRaisesRegex(DeviceLockedError, "another process"):
            self.store.cancel_job(job_id)


if __name__ == "__main__":
    unittest.main()
