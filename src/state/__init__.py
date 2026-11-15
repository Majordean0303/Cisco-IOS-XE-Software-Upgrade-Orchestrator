"""Durable job and per-device state for upgrade orchestration."""

from __future__ import annotations

import json
import ipaddress
import sqlite3
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional, Sequence


def utc_now() -> str:
    """Return an ISO-8601 UTC timestamp."""
    return datetime.now(timezone.utc).isoformat()


class DeviceState(str, Enum):
    """Persisted states for a resumable per-device workflow."""

    PENDING = "pending"
    VALIDATING = "validating"
    READY = "ready"
    BACKING_UP = "backing_up"
    BASELINE_CAPTURE = "baseline_capture"
    STAGING = "staging"
    HASH_VERIFIED = "hash_verified"
    STAGED = "staged"
    ACTIVATION_PENDING = "activation_pending"
    ACTIVATING = "activating"
    REBOOTING = "rebooting"
    RECONNECTING = "reconnecting"
    POST_CHECKING = "post_checking"
    STABILIZING = "stabilizing"
    COMMIT_PENDING = "commit_pending"
    COMMITTING = "committing"
    CLEANUP_PENDING = "cleanup_pending"
    CLEANING = "cleaning"
    COMPLETE = "complete"
    INCOMPATIBLE = "incompatible"
    PRECHECK_FAILED = "precheck_failed"
    BACKUP_FAILED = "backup_failed"
    BASELINE_FAILED = "baseline_failed"
    STAGING_FAILED = "staging_failed"
    ACTIVATION_UNCERTAIN = "activation_uncertain"
    ACTIVATION_FAILED = "activation_failed"
    RECONNECT_TIMEOUT = "reconnect_timeout"
    POST_CHECK_FAILED = "post_check_failed"
    COMMIT_FAILED = "commit_failed"
    CLEANUP_FAILED = "cleanup_failed"
    EXECUTION_FAILED = "execution_failed"
    ROLLBACK_REQUIRED = "rollback_required"
    ROLLING_BACK = "rolling_back"
    ROLLED_BACK = "rolled_back"
    FAILED = "failed"


SUCCESS_STATES = {
    DeviceState.STAGED.value,
    DeviceState.CLEANUP_PENDING.value,
    DeviceState.COMPLETE.value,
}

FINAL_STATES = {
    DeviceState.COMPLETE.value,
    DeviceState.ROLLED_BACK.value,
    DeviceState.FAILED.value,
}

FAILURE_STATES = {
    DeviceState.INCOMPATIBLE.value,
    DeviceState.PRECHECK_FAILED.value,
    DeviceState.BACKUP_FAILED.value,
    DeviceState.BASELINE_FAILED.value,
    DeviceState.STAGING_FAILED.value,
    DeviceState.ACTIVATION_UNCERTAIN.value,
    DeviceState.ACTIVATION_FAILED.value,
    DeviceState.RECONNECT_TIMEOUT.value,
    DeviceState.POST_CHECK_FAILED.value,
    DeviceState.COMMIT_FAILED.value,
    DeviceState.CLEANUP_FAILED.value,
    DeviceState.EXECUTION_FAILED.value,
    DeviceState.ROLLBACK_REQUIRED.value,
    DeviceState.FAILED.value,
}


# States reached before `install activate` is sent. In all of these the device
# is still running its original committed image and nothing is mid-flight, so a
# reservation can be released and the device handed to another job safely.
PRE_ACTIVATION_STATES = {
    DeviceState.PENDING.value,
    DeviceState.VALIDATING.value,
    DeviceState.READY.value,
    DeviceState.BACKING_UP.value,
    DeviceState.BASELINE_CAPTURE.value,
    DeviceState.STAGING.value,
    DeviceState.HASH_VERIFIED.value,
    DeviceState.STAGED.value,
    DeviceState.ACTIVATION_PENDING.value,
    DeviceState.INCOMPATIBLE.value,
    DeviceState.PRECHECK_FAILED.value,
    DeviceState.BACKUP_FAILED.value,
    DeviceState.BASELINE_FAILED.value,
    DeviceState.STAGING_FAILED.value,
}


class StateConflictError(RuntimeError):
    """Raised when a transition no longer matches persisted state."""


class DeviceLockedError(RuntimeError):
    """Raised when another job owns a device lock."""


class StateStore:
    """SQLite-backed state store safe for threaded orchestration and restarts."""

    def __init__(self, database: str | Path):
        self.database = Path(database)
        self.database.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    @contextmanager
    def _connection(self, *, immediate: bool = False) -> Iterator[sqlite3.Connection]:
        connection = sqlite3.connect(
            self.database,
            timeout=30,
            isolation_level=None,
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 30000")
        try:
            connection.execute("BEGIN IMMEDIATE" if immediate else "BEGIN")
            yield connection
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def _initialize(self) -> None:
        connection = sqlite3.connect(self.database, timeout=30)
        try:
            connection.execute("PRAGMA journal_mode = WAL")
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS jobs (
                    job_id TEXT PRIMARY KEY,
                    inventory_path TEXT NOT NULL,
                    mode TEXT NOT NULL,
                    status TEXT NOT NULL,
                    config_json TEXT NOT NULL,
                    operator TEXT,
                    dry_run INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    started_at TEXT,
                    completed_at TEXT
                );

                CREATE TABLE IF NOT EXISTS device_runs (
                    job_id TEXT NOT NULL,
                    hostname TEXT NOT NULL,
                    ip_address TEXT NOT NULL,
                    platform TEXT NOT NULL,
                    target_version TEXT NOT NULL,
                    image_path TEXT NOT NULL,
                    image_md5 TEXT NOT NULL,
                    redundancy_group TEXT,
                    batch_number INTEGER,
                    state TEXT NOT NULL,
                    message TEXT NOT NULL DEFAULT '',
                    attempt INTEGER NOT NULL DEFAULT 0,
                    metadata_json TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (job_id, hostname),
                    FOREIGN KEY (job_id) REFERENCES jobs(job_id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS events (
                    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    job_id TEXT NOT NULL,
                    hostname TEXT,
                    level TEXT NOT NULL,
                    state TEXT,
                    message TEXT NOT NULL,
                    data_json TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT NOT NULL,
                    FOREIGN KEY (job_id) REFERENCES jobs(job_id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS snapshots (
                    job_id TEXT NOT NULL,
                    hostname TEXT NOT NULL,
                    phase TEXT NOT NULL,
                    feature TEXT NOT NULL,
                    path TEXT,
                    success INTEGER NOT NULL,
                    error TEXT,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY (job_id, hostname, phase, feature),
                    FOREIGN KEY (job_id, hostname)
                        REFERENCES device_runs(job_id, hostname) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS device_locks (
                    hostname TEXT PRIMARY KEY,
                    job_id TEXT NOT NULL,
                    acquired_at TEXT NOT NULL,
                    FOREIGN KEY (job_id) REFERENCES jobs(job_id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS execution_locks (
                    hostname TEXT PRIMARY KEY,
                    job_id TEXT NOT NULL,
                    owner_token TEXT NOT NULL,
                    acquired_at TEXT NOT NULL,
                    expires_at TEXT NOT NULL,
                    FOREIGN KEY (job_id) REFERENCES jobs(job_id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS job_execution_locks (
                    job_id TEXT PRIMARY KEY,
                    owner_token TEXT NOT NULL,
                    acquired_at TEXT NOT NULL,
                    expires_at TEXT NOT NULL,
                    FOREIGN KEY (job_id) REFERENCES jobs(job_id) ON DELETE CASCADE
                );

                CREATE INDEX IF NOT EXISTS idx_device_runs_state
                    ON device_runs(job_id, state);
                CREATE INDEX IF NOT EXISTS idx_events_job_device
                    ON events(job_id, hostname, event_id);
                """
            )
        finally:
            connection.close()
        with self._connection(immediate=True) as connection:
            self._migrate_lock_keys(connection)

    def _migrate_lock_keys(self, connection: sqlite3.Connection) -> None:
        """Convert hostname-keyed locks from earlier databases to IP keys."""
        reservation_rows = []
        for stored_row in connection.execute(
            "SELECT hostname, job_id, acquired_at FROM device_locks"
        ).fetchall():
            row = dict(stored_row)
            row["hostname"] = self._canonicalize_stored_lock_key(
                connection,
                row,
            )
            reservation_rows.append(row)

        execution_rows = []
        for stored_row in connection.execute(
            """
            SELECT hostname, job_id, owner_token, acquired_at, expires_at
            FROM execution_locks
            """
        ).fetchall():
            row = dict(stored_row)
            row["hostname"] = self._canonicalize_stored_lock_key(
                connection,
                row,
            )
            execution_rows.append(row)

        reservations: Dict[str, Dict[str, Any]] = {}
        executions: Dict[str, Dict[str, Any]] = {}
        keys = {
            row["hostname"] for row in [*reservation_rows, *execution_rows]
        }
        now = datetime.now(timezone.utc)
        for key in keys:
            matching_reservations = [
                row for row in reservation_rows if row["hostname"] == key
            ]
            matching_executions = [
                row for row in execution_rows if row["hostname"] == key
            ]
            active_executions = [
                row
                for row in matching_executions
                if datetime.fromisoformat(row["expires_at"]) > now
            ]
            active_owners = {
                (row["job_id"], row["owner_token"])
                for row in active_executions
            }
            if len(active_owners) > 1:
                raise StateConflictError(
                    f"Multiple active execution owners map to {key}"
                )
            if active_executions:
                active = max(
                    active_executions,
                    key=lambda row: row["expires_at"],
                )
                executions[key] = active
                owner_reservations = [
                    row
                    for row in matching_reservations
                    if row["job_id"] == active["job_id"]
                ]
                reservations[key] = (
                    min(
                        owner_reservations,
                        key=lambda row: row["acquired_at"],
                    )
                    if owner_reservations
                    else {
                        "hostname": key,
                        "job_id": active["job_id"],
                        "acquired_at": active["acquired_at"],
                    }
                )
                continue
            if matching_reservations:
                reservation = min(
                    matching_reservations,
                    key=lambda row: row["acquired_at"],
                )
                reservations[key] = reservation
                matching_job_executions = [
                    row
                    for row in matching_executions
                    if row["job_id"] == reservation["job_id"]
                ]
                if matching_job_executions:
                    executions[key] = max(
                        matching_job_executions,
                        key=lambda row: row["expires_at"],
                    )
            elif matching_executions:
                executions[key] = max(
                    matching_executions,
                    key=lambda row: row["expires_at"],
                )

        if reservation_rows:
            connection.execute("DELETE FROM device_locks")
            connection.executemany(
                """
                INSERT INTO device_locks (hostname, job_id, acquired_at)
                VALUES (:hostname, :job_id, :acquired_at)
                """,
                reservations.values(),
            )
        if execution_rows:
            connection.execute("DELETE FROM execution_locks")
            connection.executemany(
                """
                INSERT INTO execution_locks (
                    hostname, job_id, owner_token, acquired_at, expires_at
                ) VALUES (
                    :hostname, :job_id, :owner_token, :acquired_at, :expires_at
                )
                """,
                executions.values(),
            )

    @staticmethod
    def _canonicalize_stored_lock_key(
        connection: sqlite3.Connection,
        row: Dict[str, Any],
    ) -> str:
        stored = str(row["hostname"])
        if stored.startswith("ip:"):
            return stored
        device = connection.execute(
            """
            SELECT ip_address
            FROM device_runs
            WHERE job_id = ? AND hostname = ?
            """,
            (row["job_id"], stored),
        ).fetchone()
        if device is None:
            return stored
        return f"ip:{ipaddress.ip_address(str(device['ip_address']))}"

    def create_job(
        self,
        *,
        inventory_path: str,
        config: Dict[str, Any],
        devices: Iterable[Dict[str, Any]],
        mode: str,
        dry_run: bool = False,
        operator: Optional[str] = None,
        job_id: Optional[str] = None,
        batches: Optional[Sequence[Sequence[str]]] = None,
    ) -> str:
        """Create a job and its initial per-device rows atomically."""
        job_id = job_id or uuid.uuid4().hex[:12]
        now = utc_now()
        device_rows = list(devices)
        hostnames = [str(device["hostname"]) for device in device_rows]
        if len(hostnames) != len(set(hostnames)):
            raise ValueError("Inventory contains duplicate hostnames")
        canonical_addresses = [
            str(ipaddress.ip_address(str(device["ip_address"])))
            for device in device_rows
        ]
        if len(canonical_addresses) != len(set(canonical_addresses)):
            raise ValueError("Inventory contains duplicate management addresses")
        batch_by_hostname: Dict[str, int] = {}
        if batches is not None:
            for batch_number, batch in enumerate(batches, start=1):
                for hostname in batch:
                    if hostname in batch_by_hostname:
                        raise ValueError(
                            f"Device {hostname!r} appears in multiple batches"
                        )
                    batch_by_hostname[str(hostname)] = batch_number
            if set(batch_by_hostname) != set(hostnames):
                raise ValueError(
                    "Batch plan must cover every inventory device exactly once"
                )

        with self._connection(immediate=True) as connection:
            connection.execute(
                """
                INSERT INTO jobs (
                    job_id, inventory_path, mode, status, config_json, operator,
                    dry_run, created_at, updated_at
                ) VALUES (?, ?, ?, 'planned', ?, ?, ?, ?, ?)
                """,
                (
                    job_id,
                    str(Path(inventory_path).resolve()),
                    mode,
                    json.dumps(
                        self._redact_config(config),
                        sort_keys=True,
                        default=str,
                    ),
                    operator,
                    int(dry_run),
                    now,
                    now,
                ),
            )
            for device in device_rows:
                connection.execute(
                    """
                    INSERT INTO device_runs (
                        job_id, hostname, ip_address, platform, target_version,
                        image_path, image_md5, redundancy_group, batch_number,
                        state, message, metadata_json, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '{}', ?, ?)
                    """,
                    (
                        job_id,
                        str(device["hostname"]),
                        str(device["ip_address"]),
                        str(device["platform"]),
                        str(device["target_version"]),
                        str(device["image_path"]),
                        str(device["image_md5"]),
                        self._optional_text(device.get("redundancy_group")),
                        batch_by_hostname.get(
                            str(device["hostname"]),
                            device.get("batch_number"),
                        ),
                        DeviceState.PENDING.value,
                        "Job created",
                        now,
                        now,
                    ),
                )
            self._insert_event(
                connection,
                job_id=job_id,
                hostname=None,
                level="INFO",
                state=None,
                message=f"Created {mode} job for {len(device_rows)} device(s)",
            )
        return job_id

    def get_job(self, job_id: str) -> Dict[str, Any]:
        with self._connection() as connection:
            row = connection.execute(
                "SELECT * FROM jobs WHERE job_id = ?",
                (job_id,),
            ).fetchone()
        if row is None:
            raise KeyError(f"Unknown job: {job_id}")
        result = dict(row)
        result["config"] = json.loads(result.pop("config_json"))
        result["dry_run"] = bool(result["dry_run"])
        return result

    def latest_job(self) -> Optional[Dict[str, Any]]:
        with self._connection() as connection:
            row = connection.execute(
                "SELECT job_id FROM jobs ORDER BY created_at DESC LIMIT 1"
            ).fetchone()
        return self.get_job(row["job_id"]) if row else None

    def list_jobs(self, limit: int = 20) -> List[Dict[str, Any]]:
        with self._connection() as connection:
            rows = connection.execute(
                """
                SELECT job_id, inventory_path, mode, status, operator, dry_run,
                       created_at, updated_at, started_at, completed_at
                FROM jobs
                ORDER BY created_at DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [dict(row) for row in rows]

    def get_devices(
        self,
        job_id: str,
        states: Optional[Sequence[str | DeviceState]] = None,
    ) -> List[Dict[str, Any]]:
        parameters: List[Any] = [job_id]
        query = "SELECT * FROM device_runs WHERE job_id = ?"
        if states:
            normalized = [
                state.value if isinstance(state, DeviceState) else str(state)
                for state in states
            ]
            placeholders = ",".join("?" for _ in normalized)
            query += f" AND state IN ({placeholders})"
            parameters.extend(normalized)
        query += " ORDER BY COALESCE(batch_number, 2147483647), hostname"
        with self._connection() as connection:
            rows = connection.execute(query, parameters).fetchall()
        return [self._device_row(row) for row in rows]

    def get_device(self, job_id: str, hostname: str) -> Dict[str, Any]:
        with self._connection() as connection:
            row = connection.execute(
                """
                SELECT * FROM device_runs
                WHERE job_id = ? AND hostname = ?
                """,
                (job_id, hostname),
            ).fetchone()
        if row is None:
            raise KeyError(f"Unknown device {hostname!r} in job {job_id}")
        return self._device_row(row)

    def assign_batches(self, job_id: str, batches: Sequence[Sequence[str]]) -> None:
        with self._connection(immediate=True) as connection:
            for batch_number, hostnames in enumerate(batches, start=1):
                for hostname in hostnames:
                    cursor = connection.execute(
                        """
                        UPDATE device_runs
                        SET batch_number = ?, updated_at = ?
                        WHERE job_id = ? AND hostname = ?
                        """,
                        (batch_number, utc_now(), job_id, hostname),
                    )
                    if cursor.rowcount != 1:
                        raise KeyError(
                            f"Unknown device {hostname!r} in job {job_id}"
                        )

    def set_job_status(
        self,
        job_id: str,
        status: str,
        *,
        owner_token: Optional[str] = None,
    ) -> None:
        now = utc_now()
        started_at = now if status == "running" else None
        completed_at = now if status in {"completed", "failed", "cancelled"} else None
        with self._connection(immediate=True) as connection:
            if owner_token:
                self._assert_job_execution_lock_in_transaction(
                    connection,
                    job_id,
                    owner_token,
                )
            cursor = connection.execute(
                """
                UPDATE jobs
                SET status = ?,
                    updated_at = ?,
                    started_at = COALESCE(started_at, ?),
                    completed_at = COALESCE(?, completed_at)
                WHERE job_id = ?
                """,
                (status, now, started_at, completed_at, job_id),
            )
            if cursor.rowcount != 1:
                raise KeyError(f"Unknown job: {job_id}")
            self._insert_event(
                connection,
                job_id=job_id,
                hostname=None,
                level="INFO",
                state=None,
                message=f"Job status changed to {status}",
            )

    def set_job_mode(
        self,
        job_id: str,
        mode: str,
        *,
        owner_token: Optional[str] = None,
    ) -> None:
        with self._connection(immediate=True) as connection:
            if owner_token:
                self._assert_job_execution_lock_in_transaction(
                    connection,
                    job_id,
                    owner_token,
                )
            cursor = connection.execute(
                """
                UPDATE jobs
                SET mode = ?, updated_at = ?
                WHERE job_id = ?
                """,
                (mode, utc_now(), job_id),
            )
            if cursor.rowcount != 1:
                raise KeyError(f"Unknown job: {job_id}")
            self._insert_event(
                connection,
                job_id=job_id,
                hostname=None,
                level="INFO",
                state=None,
                message=f"Job mode changed to {mode}",
            )

    def transition(
        self,
        job_id: str,
        hostname: str,
        state: str | DeviceState,
        message: str,
        *,
        expected: Optional[Sequence[str | DeviceState]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        level: str = "INFO",
        increment_attempt: bool = False,
        owner_token: Optional[str] = None,
        require_job_lock: bool = False,
    ) -> Dict[str, Any]:
        """Persist a state transition and event in one transaction."""
        new_state = state.value if isinstance(state, DeviceState) else str(state)
        expected_values = (
            {
                item.value if isinstance(item, DeviceState) else str(item)
                for item in expected
            }
            if expected
            else None
        )
        now = utc_now()
        with self._connection(immediate=True) as connection:
            if owner_token:
                if require_job_lock:
                    self._assert_job_execution_lock_in_transaction(
                        connection,
                        job_id,
                        owner_token,
                    )
                self._assert_device_execution_lock_in_transaction(
                    connection,
                    job_id,
                    hostname,
                    owner_token,
                )
            row = connection.execute(
                """
                SELECT state, metadata_json, attempt
                FROM device_runs
                WHERE job_id = ? AND hostname = ?
                """,
                (job_id, hostname),
            ).fetchone()
            if row is None:
                raise KeyError(f"Unknown device {hostname!r} in job {job_id}")
            if expected_values is not None and row["state"] not in expected_values:
                raise StateConflictError(
                    f"{hostname} is {row['state']}, expected one of "
                    f"{sorted(expected_values)}"
                )
            merged_metadata = json.loads(row["metadata_json"] or "{}")
            if metadata:
                merged_metadata.update(metadata)
            attempt = row["attempt"] + (1 if increment_attempt else 0)
            connection.execute(
                """
                UPDATE device_runs
                SET state = ?, message = ?, metadata_json = ?, attempt = ?,
                    updated_at = ?
                WHERE job_id = ? AND hostname = ?
                """,
                (
                    new_state,
                    message,
                    json.dumps(merged_metadata, sort_keys=True, default=str),
                    attempt,
                    now,
                    job_id,
                    hostname,
                ),
            )
            self._insert_event(
                connection,
                job_id=job_id,
                hostname=hostname,
                level=level,
                state=new_state,
                message=message,
                data=metadata,
            )
        return self.get_device(job_id, hostname)

    def record_event(
        self,
        job_id: str,
        message: str,
        *,
        hostname: Optional[str] = None,
        level: str = "INFO",
        state: Optional[str] = None,
        data: Optional[Dict[str, Any]] = None,
    ) -> None:
        with self._connection(immediate=True) as connection:
            self._insert_event(
                connection,
                job_id=job_id,
                hostname=hostname,
                level=level,
                state=state,
                message=message,
                data=data,
            )

    def list_events(
        self,
        job_id: str,
        *,
        hostname: Optional[str] = None,
        limit: int = 200,
    ) -> List[Dict[str, Any]]:
        query = "SELECT * FROM events WHERE job_id = ?"
        parameters: List[Any] = [job_id]
        if hostname:
            query += " AND hostname = ?"
            parameters.append(hostname)
        query += " ORDER BY event_id DESC LIMIT ?"
        parameters.append(limit)
        with self._connection() as connection:
            rows = connection.execute(query, parameters).fetchall()
        results = []
        for row in rows:
            result = dict(row)
            result["data"] = json.loads(result.pop("data_json") or "{}")
            results.append(result)
        return results

    def record_snapshot(
        self,
        *,
        job_id: str,
        hostname: str,
        phase: str,
        feature: str,
        path: Optional[str],
        success: bool,
        error: Optional[str] = None,
    ) -> None:
        with self._connection(immediate=True) as connection:
            connection.execute(
                """
                INSERT INTO snapshots (
                    job_id, hostname, phase, feature, path, success, error,
                    created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(job_id, hostname, phase, feature) DO UPDATE SET
                    path = excluded.path,
                    success = excluded.success,
                    error = excluded.error,
                    created_at = excluded.created_at
                """,
                (
                    job_id,
                    hostname,
                    phase,
                    feature,
                    path,
                    int(success),
                    error,
                    utc_now(),
                ),
            )

    def get_snapshots(
        self,
        job_id: str,
        hostname: str,
        phase: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        query = (
            "SELECT * FROM snapshots WHERE job_id = ? AND hostname = ?"
        )
        parameters: List[Any] = [job_id, hostname]
        if phase:
            query += " AND phase = ?"
            parameters.append(phase)
        query += " ORDER BY phase, feature"
        with self._connection() as connection:
            rows = connection.execute(query, parameters).fetchall()
        results = [dict(row) for row in rows]
        for result in results:
            result["success"] = bool(result["success"])
        return results

    def acquire_job_execution_lock(
        self,
        job_id: str,
        owner_token: str,
        *,
        lease_seconds: int = 120,
    ) -> None:
        now = datetime.now(timezone.utc)
        expires_at = (now + timedelta(seconds=lease_seconds)).isoformat()
        with self._connection(immediate=True) as connection:
            existing = connection.execute(
                """
                SELECT owner_token, expires_at
                FROM job_execution_locks
                WHERE job_id = ?
                """,
                (job_id,),
            ).fetchone()
            if existing and datetime.fromisoformat(existing["expires_at"]) > now:
                raise DeviceLockedError(
                    f"Job {job_id} is actively operated by another process"
                )
            active_devices = connection.execute(
                """
                SELECT 1
                FROM execution_locks
                WHERE job_id = ? AND expires_at > ?
                LIMIT 1
                """,
                (job_id, now.isoformat()),
            ).fetchone()
            if active_devices:
                raise DeviceLockedError(
                    f"Job {job_id} still has active device operations; "
                    "wait for their leases to expire"
                )
            cursor = connection.execute(
                """
                INSERT INTO job_execution_locks (
                    job_id, owner_token, acquired_at, expires_at
                ) VALUES (?, ?, ?, ?)
                ON CONFLICT(job_id) DO UPDATE SET
                    owner_token = excluded.owner_token,
                    acquired_at = excluded.acquired_at,
                    expires_at = excluded.expires_at
                """,
                (job_id, owner_token, now.isoformat(), expires_at),
            )
            if cursor.rowcount != 1:
                raise KeyError(f"Unknown job: {job_id}")

    def renew_job_execution_lock(
        self,
        job_id: str,
        owner_token: str,
        *,
        lease_seconds: int = 120,
    ) -> None:
        expires_at = (
            datetime.now(timezone.utc) + timedelta(seconds=lease_seconds)
        ).isoformat()
        with self._connection(immediate=True) as connection:
            cursor = connection.execute(
                """
                UPDATE job_execution_locks
                SET expires_at = ?
                WHERE job_id = ? AND owner_token = ?
                """,
                (expires_at, job_id, owner_token),
            )
            if cursor.rowcount != 1:
                raise DeviceLockedError(
                    f"Execution lock for job {job_id} is no longer owned by "
                    "this process"
                )

    def assert_job_execution_lock(
        self,
        job_id: str,
        owner_token: str,
    ) -> None:
        with self._connection() as connection:
            self._assert_job_execution_lock_in_transaction(
                connection,
                job_id,
                owner_token,
            )

    def release_job_execution_lock(
        self,
        job_id: str,
        owner_token: str,
    ) -> None:
        with self._connection(immediate=True) as connection:
            connection.execute(
                """
                DELETE FROM job_execution_locks
                WHERE job_id = ? AND owner_token = ?
                """,
                (job_id, owner_token),
            )

    def acquire_device_lock(
        self,
        job_id: str,
        hostname: str,
        owner_token: str,
        *,
        lease_seconds: int = 120,
    ) -> None:
        now = datetime.now(timezone.utc)
        expires_at = now.timestamp() + lease_seconds
        expires_text = datetime.fromtimestamp(
            expires_at, timezone.utc
        ).isoformat()
        with self._connection(immediate=True) as connection:
            self._assert_job_execution_lock_in_transaction(
                connection,
                job_id,
                owner_token,
            )
            lock_key = self._device_lock_key(connection, job_id, hostname)
            execution = connection.execute(
                """
                SELECT job_id, owner_token, expires_at
                FROM execution_locks
                WHERE hostname = ?
                """,
                (lock_key,),
            ).fetchone()
            execution_active = bool(
                execution
                and datetime.fromisoformat(execution["expires_at"]) > now
            )
            existing = connection.execute(
                "SELECT job_id FROM device_locks WHERE hostname = ?",
                (lock_key,),
            ).fetchone()
            if (
                existing
                and existing["job_id"] != job_id
                and not execution_active
                and self._reservation_is_terminal(
                    connection,
                    lock_key,
                    existing["job_id"],
                )
            ):
                connection.execute(
                    "DELETE FROM device_locks WHERE hostname = ?",
                    (lock_key,),
                )
                existing = None
            if existing and existing["job_id"] != job_id:
                raise DeviceLockedError(
                    f"{hostname} is locked by job {existing['job_id']}"
                )
            connection.execute(
                """
                INSERT INTO device_locks (hostname, job_id, acquired_at)
                VALUES (?, ?, ?)
                ON CONFLICT(hostname) DO UPDATE SET
                    acquired_at = excluded.acquired_at
                WHERE device_locks.job_id = excluded.job_id
                """,
                (lock_key, job_id, utc_now()),
            )
            if execution_active:
                raise DeviceLockedError(
                    f"{hostname} is actively operated by another process "
                    f"for job {execution['job_id']}"
                )
            connection.execute(
                """
                INSERT INTO execution_locks (
                    hostname, job_id, owner_token, acquired_at, expires_at
                ) VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(hostname) DO UPDATE SET
                    job_id = excluded.job_id,
                    owner_token = excluded.owner_token,
                    acquired_at = excluded.acquired_at,
                    expires_at = excluded.expires_at
                """,
                (
                    lock_key,
                    job_id,
                    owner_token,
                    now.isoformat(),
                    expires_text,
                ),
            )

    def renew_execution_lock(
        self,
        job_id: str,
        hostname: str,
        owner_token: str,
        *,
        lease_seconds: int = 120,
    ) -> None:
        expires_at = (
            datetime.now(timezone.utc) + timedelta(seconds=lease_seconds)
        ).isoformat()
        with self._connection(immediate=True) as connection:
            self._assert_job_execution_lock_in_transaction(
                connection,
                job_id,
                owner_token,
            )
            lock_key = self._device_lock_key(connection, job_id, hostname)
            cursor = connection.execute(
                """
                UPDATE execution_locks
                SET expires_at = ?
                WHERE hostname = ? AND job_id = ? AND owner_token = ?
                """,
                (expires_at, lock_key, job_id, owner_token),
            )
            if cursor.rowcount != 1:
                raise DeviceLockedError(
                    f"Execution lock for {hostname} is no longer owned by "
                    "this process"
                )

    def assert_execution_lock(
        self,
        job_id: str,
        hostname: str,
        owner_token: str,
    ) -> None:
        """Require an unexpired execution lease owned by this process."""
        now = datetime.now(timezone.utc)
        with self._connection() as connection:
            self._assert_job_execution_lock_in_transaction(
                connection,
                job_id,
                owner_token,
            )
            lock_key = self._device_lock_key(connection, job_id, hostname)
            row = connection.execute(
                """
                SELECT job_id, owner_token, expires_at
                FROM execution_locks
                WHERE hostname = ?
                """,
                (lock_key,),
            ).fetchone()
        if (
            row is None
            or row["job_id"] != job_id
            or row["owner_token"] != owner_token
            or datetime.fromisoformat(row["expires_at"]) <= now
        ):
            raise DeviceLockedError(
                f"Execution lock for {hostname} is no longer owned by "
                "this process"
            )

    def release_execution_lock(
        self,
        job_id: str,
        hostname: str,
        owner_token: str,
    ) -> None:
        with self._connection(immediate=True) as connection:
            self._assert_job_execution_lock_in_transaction(
                connection,
                job_id,
                owner_token,
            )
            lock_key = self._device_lock_key(connection, job_id, hostname)
            connection.execute(
                """
                DELETE FROM execution_locks
                WHERE hostname = ? AND job_id = ? AND owner_token = ?
                """,
                (lock_key, job_id, owner_token),
            )

    def release_device_lock(
        self,
        job_id: str,
        hostname: str,
        owner_token: str,
    ) -> None:
        with self._connection(immediate=True) as connection:
            lock_key = self._device_lock_key(connection, job_id, hostname)
            execution = connection.execute(
                """
                SELECT owner_token, expires_at
                FROM execution_locks
                WHERE hostname = ? AND job_id = ?
                """,
                (lock_key, job_id),
            ).fetchone()
            if (
                execution is None
                or execution["owner_token"] != owner_token
                or datetime.fromisoformat(execution["expires_at"])
                <= datetime.now(timezone.utc)
            ):
                raise DeviceLockedError(
                    f"Cannot release the reservation for {hostname} without "
                    "its active execution lease"
                )
            connection.execute(
                """
                DELETE FROM device_locks
                WHERE hostname = ? AND job_id = ?
                """,
                (lock_key, job_id),
            )

    def release_job_locks(self, job_id: str) -> bool:
        with self._connection(immediate=True) as connection:
            now = utc_now()
            connection.execute(
                """
                DELETE FROM execution_locks
                WHERE job_id = ? AND expires_at <= ?
                """,
                (job_id, now),
            )
            active = connection.execute(
                "SELECT 1 FROM execution_locks WHERE job_id = ? LIMIT 1",
                (job_id,),
            ).fetchone()
            if active:
                return False
            connection.execute(
                "DELETE FROM device_locks WHERE job_id = ?",
                (job_id,),
            )
            return True

    def cancel_job(
        self,
        job_id: str,
        *,
        force: bool = False,
        reason: str = "",
    ) -> Dict[str, Any]:
        """Cancel a job, mark its unfinished devices failed, and free its locks.

        This is the administrative escape hatch for a job that has failed or
        stalled and is holding device reservations that block later jobs. It
        refuses when another process still holds an active execution lease. By
        default it also refuses when any device has moved past staging (where the
        box may be mid-upgrade); pass ``force=True`` to override after checking
        the device by hand.
        """
        self.get_job(job_id)
        now = datetime.now(timezone.utc)
        now_text = now.isoformat()
        released = 0
        with self._connection(immediate=True) as connection:
            connection.execute(
                "DELETE FROM execution_locks WHERE job_id = ? AND expires_at <= ?",
                (job_id, now_text),
            )
            active = connection.execute(
                """
                SELECT hostname FROM execution_locks
                WHERE job_id = ? AND expires_at > ?
                LIMIT 1
                """,
                (job_id, now_text),
            ).fetchone()
            if active:
                raise DeviceLockedError(
                    f"Job {job_id} is still being executed by another process; "
                    "stop that run before cancelling"
                )
            rows = connection.execute(
                """
                SELECT hostname, state, metadata_json
                FROM device_runs
                WHERE job_id = ?
                """,
                (job_id,),
            ).fetchall()
            cancellable: List[str] = []
            unsafe: List[str] = []
            for row in rows:
                state = row["state"]
                if state in FINAL_STATES:
                    continue
                cancellable.append(row["hostname"])
                retry_state = None
                if state == DeviceState.EXECUTION_FAILED.value:
                    metadata = json.loads(row["metadata_json"] or "{}")
                    retry_state = metadata.get("retry_state")
                if not self._cancel_state_is_safe(state, retry_state):
                    unsafe.append(f"{row['hostname']} ({state})")
            if unsafe and not force:
                raise StateConflictError(
                    f"Refusing to cancel job {job_id}: "
                    + ", ".join(sorted(unsafe))
                    + " may be mid-upgrade. Verify the device(s) first, then "
                    "cancel with force to override."
                )
            message = "Job cancelled by operator"
            if reason:
                message = f"{message}: {reason}"
            for hostname in cancellable:
                connection.execute(
                    """
                    UPDATE device_runs
                    SET state = ?, message = ?, updated_at = ?
                    WHERE job_id = ? AND hostname = ?
                    """,
                    (DeviceState.FAILED.value, message, now_text, job_id, hostname),
                )
                self._insert_event(
                    connection,
                    job_id=job_id,
                    hostname=hostname,
                    level="WARNING",
                    state=DeviceState.FAILED.value,
                    message=message,
                )
            released = len(cancellable)
            connection.execute(
                "DELETE FROM execution_locks WHERE job_id = ?",
                (job_id,),
            )
            connection.execute(
                "DELETE FROM device_locks WHERE job_id = ?",
                (job_id,),
            )
            connection.execute(
                "DELETE FROM job_execution_locks WHERE job_id = ?",
                (job_id,),
            )
            connection.execute(
                """
                UPDATE jobs
                SET status = 'cancelled',
                    updated_at = ?,
                    completed_at = COALESCE(completed_at, ?)
                WHERE job_id = ?
                """,
                (now_text, now_text, job_id),
            )
            self._insert_event(
                connection,
                job_id=job_id,
                hostname=None,
                level="WARNING",
                state=None,
                message=(
                    f"Job cancelled; released {released} device reservation(s)"
                ),
            )
        summary = self.summarize_job(job_id)
        summary["released"] = released
        return summary

    @staticmethod
    def _cancel_state_is_safe(
        state: str,
        retry_state: Optional[str],
    ) -> bool:
        """Return True when cancelling a device in ``state`` is non-destructive."""
        if state in PRE_ACTIVATION_STATES:
            return True
        if state == DeviceState.EXECUTION_FAILED.value:
            return retry_state in PRE_ACTIVATION_STATES
        return False

    @staticmethod
    def _assert_job_execution_lock_in_transaction(
        connection: sqlite3.Connection,
        job_id: str,
        owner_token: str,
    ) -> None:
        row = connection.execute(
            """
            SELECT owner_token, expires_at
            FROM job_execution_locks
            WHERE job_id = ?
            """,
            (job_id,),
        ).fetchone()
        if (
            row is None
            or row["owner_token"] != owner_token
            or datetime.fromisoformat(row["expires_at"])
            <= datetime.now(timezone.utc)
        ):
            raise DeviceLockedError(
                f"Execution lock for job {job_id} is no longer owned by "
                "this process"
            )

    def _assert_device_execution_lock_in_transaction(
        self,
        connection: sqlite3.Connection,
        job_id: str,
        hostname: str,
        owner_token: str,
    ) -> None:
        lock_key = self._device_lock_key(connection, job_id, hostname)
        row = connection.execute(
            """
            SELECT job_id, owner_token, expires_at
            FROM execution_locks
            WHERE hostname = ?
            """,
            (lock_key,),
        ).fetchone()
        if (
            row is None
            or row["job_id"] != job_id
            or row["owner_token"] != owner_token
            or datetime.fromisoformat(row["expires_at"])
            <= datetime.now(timezone.utc)
        ):
            raise DeviceLockedError(
                f"Execution lock for {hostname} is no longer owned by "
                "this process"
            )

    @staticmethod
    def _reservation_is_terminal(
        connection: sqlite3.Connection,
        lock_key: str,
        job_id: str,
    ) -> bool:
        rows = connection.execute(
            """
            SELECT ip_address, state
            FROM device_runs
            WHERE job_id = ?
            """,
            (job_id,),
        ).fetchall()
        matching_states = [
            row["state"]
            for row in rows
            if f"ip:{ipaddress.ip_address(str(row['ip_address']))}" == lock_key
        ]
        return bool(matching_states) and all(
            state in FINAL_STATES for state in matching_states
        )

    @staticmethod
    def _device_lock_key(
        connection: sqlite3.Connection,
        job_id: str,
        hostname: str,
    ) -> str:
        row = connection.execute(
            """
            SELECT ip_address
            FROM device_runs
            WHERE job_id = ? AND hostname = ?
            """,
            (job_id, hostname),
        ).fetchone()
        if row is None:
            raise KeyError(
                f"Device {hostname!r} is not part of job {job_id!r}"
            )
        address = str(ipaddress.ip_address(str(row["ip_address"])))
        return f"ip:{address}"

    def summarize_job(self, job_id: str) -> Dict[str, Any]:
        with self._connection() as connection:
            rows = connection.execute(
                """
                SELECT state, COUNT(*) AS count
                FROM device_runs
                WHERE job_id = ?
                GROUP BY state
                ORDER BY state
                """,
                (job_id,),
            ).fetchall()
        counts = {row["state"]: row["count"] for row in rows}
        return {
            "job_id": job_id,
            "total": sum(counts.values()),
            "states": counts,
            "failed": sum(
                count for state, count in counts.items() if state in FAILURE_STATES
            ),
            "complete": counts.get(DeviceState.COMPLETE.value, 0),
            "cleanup_pending": counts.get(
                DeviceState.CLEANUP_PENDING.value, 0
            ),
        }

    @staticmethod
    def _device_row(row: sqlite3.Row) -> Dict[str, Any]:
        result = dict(row)
        result["metadata"] = json.loads(result.pop("metadata_json") or "{}")
        return result

    @staticmethod
    def _optional_text(value: Any) -> Optional[str]:
        if value is None:
            return None
        text = str(value).strip()
        return text or None

    @classmethod
    def redact_config(cls, value: Any) -> Any:
        """Return the configuration shape persisted for a job."""
        return cls._redact_config(value)

    @classmethod
    def _redact_config(cls, value: Any) -> Any:
        if isinstance(value, dict):
            redacted = {}
            for key, item in value.items():
                normalized_key = str(key).lower()
                if any(
                    marker in normalized_key
                    for marker in ("password", "secret", "token", "api_key")
                ):
                    redacted[key] = "<redacted>" if item else ""
                else:
                    redacted[key] = cls._redact_config(item)
            return redacted
        if isinstance(value, list):
            return [cls._redact_config(item) for item in value]
        return value

    @staticmethod
    def _insert_event(
        connection: sqlite3.Connection,
        *,
        job_id: str,
        hostname: Optional[str],
        level: str,
        state: Optional[str],
        message: str,
        data: Optional[Dict[str, Any]] = None,
    ) -> None:
        connection.execute(
            """
            INSERT INTO events (
                job_id, hostname, level, state, message, data_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                job_id,
                hostname,
                level,
                state,
                message,
                json.dumps(data or {}, sort_keys=True, default=str),
                utc_now(),
            ),
        )
