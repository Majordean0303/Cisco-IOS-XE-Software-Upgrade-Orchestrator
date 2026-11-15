"""Resumable, topology-aware IOS-XE install-mode orchestration."""

from __future__ import annotations

import getpass
import json
import socket
import threading
import time
import uuid
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple

from nornir.core import Nornir
from nornir.core.configuration import Config, LoggingConfig, RunnerConfig
from nornir.core.inventory import (
    ConnectionOptions,
    Defaults,
    Host,
    Hosts,
    Inventory,
)
from nornir.core.plugins.connections import ConnectionPluginRegister
from nornir.core.state import GlobalState
from nornir.plugins.runners import ThreadedRunner
from rich.console import Console
from rich.live import Live
from rich.table import Table

import src.transport as transport
from src.comparison import (
    CORE_FEATURES,
    DEFAULT_CRITICAL_FEATURES,
    OPTIONAL_FEATURES,
    GenieSnapshotManager,
    SnapshotError,
)
from src.credentials import CredentialManager
from src.device.iosxe import (
    InstallFacts,
    evaluate_compatibility,
    image_file_name,
    normalize_device_path,
    parse_install_facts,
    target_version_matches,
)
from src.inventory import InventoryManager
from src.orchestration.batching import build_upgrade_batches, validate_batches
from src.state import (
    FAILURE_STATES,
    FINAL_STATES,
    DeviceLockedError,
    DeviceState,
    StateStore,
    utc_now,
)
from src.utils import calculate_file_hash


class UpgradeOperationError(RuntimeError):
    """Raised when one device operation fails."""


class UnsafeDispatchStateError(UpgradeOperationError):
    """Raised when a command mode is unsafe for the persisted state."""


class UpgradeEngine:
    """Coordinate durable per-device install-mode state machines."""

    def __init__(
        self,
        *,
        config: Dict[str, Any],
        state_store: StateStore,
        credential_manager: CredentialManager,
        snapshot_manager: GenieSnapshotManager,
        console: Optional[Console] = None,
        jump_host_manager=None,
        sleeper: Callable[[float], None] = time.sleep,
        monotonic: Callable[[], float] = time.monotonic,
    ):
        self.config = config
        self._startup_config = config
        self.store = state_store
        self.credentials = credential_manager
        self.snapshots = snapshot_manager
        self.console = console or Console()
        self.jump_host_manager = jump_host_manager
        self.sleeper = sleeper
        self.monotonic = monotonic
        self.inventory_manager: Optional[InventoryManager] = None
        self.nr: Optional[Nornir] = None
        self._inventory_devices: Dict[str, Dict[str, Any]] = {}
        self._connection_credentials: Dict[str, Dict[str, str]] = {}
        self._connection_endpoints: Dict[str, Tuple[str, int]] = {}
        self.execution_owner = uuid.uuid4().hex
        self.execution_lease_seconds = int(
            self.config.get("workflow", {}).get(
                "execution_lock_lease_seconds", 120
            )
        )
        self._execution_context = threading.local()
        self._job_execution_active = False
        self._job_lease_lost: Optional[threading.Event] = None
        self._job_execution_id: Optional[str] = None

    def create_job(
        self,
        *,
        inventory_file: str,
        mode: str,
        dry_run: bool = False,
        operator: Optional[str] = None,
    ) -> str:
        """Validate inventory, build safe batches, and persist a new job."""
        if mode not in {"plan", "stage", "upgrade"}:
            raise ValueError(f"Unsupported job mode: {mode}")
        manager = InventoryManager(inventory_file)
        devices = manager.load_inventory()
        validation = manager.validate_inventory()
        if not validation["valid"]:
            raise ValueError(
                "Inventory validation failed: "
                + "; ".join(validation["errors"])
            )
        if not devices:
            raise ValueError("Inventory contains no enabled devices")

        workflow = self.config.get("workflow", {})
        configured_size = int(workflow.get("batch_size", 5))
        if configured_size <= 0:
            configured_size = min(
                len(devices),
                int(workflow.get("max_parallel_devices", 5)),
            )
        batches = build_upgrade_batches(
            devices,
            batch_size=configured_size,
            canary_count=int(workflow.get("canary_count", 1)),
        )
        validate_batches(batches, devices)
        job_id = self.store.create_job(
            inventory_path=inventory_file,
            config=self.config,
            devices=devices,
            mode=mode,
            dry_run=dry_run,
            operator=operator or getpass.getuser(),
            batches=batches,
        )
        return job_id

    def get_plan(self, job_id: str) -> Dict[str, Any]:
        """Return an approval-friendly plan without credentials."""
        job = self.store.get_job(job_id)
        job_config = job["config"]
        devices = self.store.get_devices(job_id)
        batches: Dict[int, List[str]] = {}
        for device in devices:
            batches.setdefault(int(device["batch_number"] or 0), []).append(
                device["hostname"]
            )
        return {
            "job_id": job_id,
            "mode": job["mode"],
            "inventory": job["inventory_path"],
            "dry_run": job["dry_run"],
            "batches": [batches[number] for number in sorted(batches)],
            "commands": [
                "verify /md5 flash:<image>",
                "install add file flash:<image>",
                self._planned_activate_command(job_config),
                "show version",
                "show install summary",
                "install commit",
                "install remove inactive",
            ],
            "genie_features": {
                "core": list(
                    job_config.get("comparison", {}).get(
                        "core_features", CORE_FEATURES
                    )
                ),
                "optional": list(
                    job_config.get("comparison", {}).get(
                        "optional_features", OPTIONAL_FEATURES
                    )
                ),
            },
        }

    @staticmethod
    def _planned_activate_command(job_config: Dict[str, Any]) -> str:
        """Show the activate command the plan will actually send."""
        minutes = transport.normalize_auto_abort_timer(
            job_config.get("upgrade", {}).get(
                "auto_abort_timer_minutes",
                transport.AUTO_ABORT_TIMER_DEFAULT_MINUTES,
            )
        )
        if minutes is None:
            return "install activate"
        return f"install activate auto-abort-timer {minutes}"

    def run(
        self,
        job_id: str,
        *,
        mode: Optional[str] = None,
        retry_failed: bool = False,
        retry_activation: bool = False,
        live_status: bool = True,
    ) -> Dict[str, Any]:
        """Run or resume a staged/upgrade job."""
        job = self.store.get_job(job_id)
        if mode:
            if mode not in {"stage", "upgrade"}:
                raise ValueError(f"Unsupported run mode: {mode}")
        run_mode = mode or job["mode"]
        if run_mode == "plan":
            return self.store.summarize_job(job_id)
        self._bind_job_config(job)
        job_lease = self._start_job_execution(job_id)
        try:
            if mode and mode != job["mode"]:
                self.store.set_job_mode(
                    job_id,
                    mode,
                    owner_token=self.execution_owner,
                )
                job["mode"] = mode
            return self._execute_run(
                job,
                run_mode=run_mode,
                retry_failed=retry_failed,
                retry_activation=retry_activation,
                live_status=live_status,
            )
        finally:
            self._stop_job_execution(job_id, job_lease)

    def _execute_run(
        self,
        job: Dict[str, Any],
        *,
        run_mode: str,
        retry_failed: bool,
        retry_activation: bool,
        live_status: bool,
    ) -> Dict[str, Any]:
        job_id = job["job_id"]
        self._prepare_runtime(job_id, job["inventory_path"])
        if self.config.get("comparison", {}).get("enabled", True):
            self.snapshots.validate_runtime()

        self._set_job_status(job_id, "running")
        try:
            batches = self._job_batches(job_id)
            for batch_index, hostnames in enumerate(batches):
                if batch_index > 0 and self._must_halt_after_batch(
                    job_id,
                    batches[batch_index - 1],
                    run_mode=run_mode,
                    is_canary=batch_index - 1 == 0,
                ):
                    self._set_job_status(job_id, "halted")
                    break
                pending = [
                    hostname
                    for hostname in hostnames
                    if self._needs_run(
                        job_id,
                        hostname,
                        run_mode,
                        retry_failed=retry_failed,
                    )
                ]
                if pending:
                    self.store.record_event(
                        job_id,
                        f"Starting batch {batch_index + 1}: {', '.join(pending)}",
                    )
                    self._run_batch(
                        job_id,
                        pending,
                        run_mode=run_mode,
                        retry_failed=retry_failed,
                        retry_activation=retry_activation,
                        dry_run=bool(job["dry_run"]),
                        live_status=live_status,
                    )
                if self._must_halt_after_batch(
                    job_id,
                    hostnames,
                    run_mode=run_mode,
                    is_canary=batch_index == 0,
                ):
                    self._set_job_status(job_id, "halted")
                    break
                pause = float(
                    self.config.get("workflow", {}).get(
                        "pause_between_batches", 0
                    )
                )
                if pause > 0 and batch_index < len(batches) - 1:
                    self.sleeper(pause)

            summary = self.store.summarize_job(job_id)
            self._set_final_job_status(job_id, run_mode, summary)
            return self.store.summarize_job(job_id)
        finally:
            if self.nr is not None:
                self.nr.close_connections()
            if self.jump_host_manager:
                self.jump_host_manager.close_all_tunnels()

    def cleanup(
        self,
        job_id: str,
        *,
        hostnames: Optional[Sequence[str]] = None,
        wait_for_soak: bool = False,
        live_status: bool = True,
    ) -> Dict[str, Any]:
        """Resume cleanup for healthy, committed devices."""
        job = self.store.get_job(job_id)
        self._bind_job_config(job)
        job_lease = self._start_job_execution(job_id)
        try:
            return self._execute_cleanup(
                job,
                hostnames=hostnames,
                wait_for_soak=wait_for_soak,
                live_status=live_status,
            )
        finally:
            self._stop_job_execution(job_id, job_lease)

    def _execute_cleanup(
        self,
        job: Dict[str, Any],
        *,
        hostnames: Optional[Sequence[str]],
        wait_for_soak: bool,
        live_status: bool,
    ) -> Dict[str, Any]:
        job_id = job["job_id"]
        self._prepare_runtime(job_id, job["inventory_path"])
        selected = list(
            dict.fromkeys(
                hostnames
                or [
                    device["hostname"]
                    for device in self.store.get_devices(
                        job_id,
                        states=[
                            DeviceState.CLEANUP_PENDING,
                            DeviceState.CLEANUP_FAILED,
                            DeviceState.CLEANING,
                        ],
                    )
                ]
            )
        )
        if not selected:
            return self.store.summarize_job(job_id)
        allowed_cleanup_states = {
            DeviceState.CLEANUP_PENDING.value,
            DeviceState.CLEANUP_FAILED.value,
            DeviceState.CLEANING.value,
        }
        invalid = [
            hostname
            for hostname in selected
            if self.store.get_device(job_id, hostname)["state"]
            not in allowed_cleanup_states
        ]
        if invalid:
            raise UpgradeOperationError(
                "Cleanup is only allowed for cleanup-pending devices: "
                + ", ".join(invalid)
            )

        if wait_for_soak:
            self._wait_until_cleanup_eligible(job_id, selected)
        self._set_job_status(job_id, "running")
        try:
            self._run_batch(
                job_id,
                list(selected),
                run_mode="cleanup",
                retry_failed=True,
                retry_activation=False,
                dry_run=False,
                live_status=live_status,
            )
            summary = self.store.summarize_job(job_id)
            self._set_final_job_status(job_id, "upgrade", summary)
            return self.store.summarize_job(job_id)
        finally:
            if self.nr is not None:
                self.nr.close_connections()
            if self.jump_host_manager:
                self.jump_host_manager.close_all_tunnels()

    def rollback(self, job_id: str, hostname: str) -> Dict[str, Any]:
        """Abort an activated, uncommitted image and verify the prior version."""
        job = self.store.get_job(job_id)
        self._bind_job_config(job)
        job_lease = self._start_job_execution(job_id)
        try:
            return self._execute_rollback(job, hostname)
        finally:
            self._stop_job_execution(job_id, job_lease)

    def cancel_job(
        self,
        job_id: str,
        *,
        force: bool = False,
        reason: str = "",
    ) -> Dict[str, Any]:
        """Cancel a failed or stalled job and release its device reservations."""
        return self.store.cancel_job(job_id, force=force, reason=reason)

    def _execute_rollback(
        self,
        job: Dict[str, Any],
        hostname: str,
    ) -> Dict[str, Any]:
        job_id = job["job_id"]
        self._prepare_runtime(job_id, job["inventory_path"])
        self.store.acquire_device_lock(
            job_id,
            hostname,
            self.execution_owner,
            lease_seconds=self.execution_lease_seconds,
        )
        heartbeat_stop = threading.Event()
        lease_lost = threading.Event()
        self._set_execution_context(job_id, hostname, lease_lost)
        heartbeat = threading.Thread(
            target=self._heartbeat_execution_lock,
            args=(job_id, hostname, heartbeat_stop, lease_lost),
            name=f"lock-heartbeat-{hostname}",
            daemon=True,
        )
        heartbeat.start()
        try:
            self._abort_uncommitted(job_id, hostname)
            return self.store.get_device(job_id, hostname)
        finally:
            heartbeat_stop.set()
            heartbeat.join(timeout=2)
            self.store.release_execution_lock(
                job_id,
                hostname,
                self.execution_owner,
            )
            self._clear_execution_context()
            if self.nr is not None:
                self.nr.close_connections()
            if self.jump_host_manager:
                self.jump_host_manager.close_all_tunnels()

    def status_table(self, job_id: str) -> Table:
        """Build a live per-device status table."""
        table = Table(title=f"Upgrade job {job_id}", expand=True)
        table.add_column("Batch", justify="right")
        table.add_column("Device")
        table.add_column("State")
        table.add_column("Message")
        table.add_column("Updated")
        for device in self.store.get_devices(job_id):
            state = device["state"]
            style = (
                "red"
                if state in FAILURE_STATES
                else "green"
                if state == DeviceState.COMPLETE.value
                else "yellow"
                if state
                in {
                    DeviceState.REBOOTING.value,
                    DeviceState.RECONNECTING.value,
                    DeviceState.ACTIVATING.value,
                }
                else "cyan"
            )
            table.add_row(
                str(device["batch_number"] or "-"),
                device["hostname"],
                f"[{style}]{state}[/{style}]",
                device["message"],
                device["updated_at"][11:19]
                if len(device["updated_at"]) >= 19
                else device["updated_at"],
            )
        return table

    def _bind_job_config(self, job: Dict[str, Any]) -> None:
        persisted = job["config"]
        current = self.store.redact_config(self._startup_config)
        if current != persisted:
            raise UpgradeOperationError(
                "Current configuration does not match the policy persisted "
                f"for job {job['job_id']}; use the original configuration"
            )
        self.config = persisted
        self.execution_lease_seconds = int(
            self.config.get("workflow", {}).get(
                "execution_lock_lease_seconds", 120
            )
        )

    def _start_job_execution(
        self,
        job_id: str,
    ) -> Tuple[threading.Event, threading.Event, threading.Thread]:
        self.store.acquire_job_execution_lock(
            job_id,
            self.execution_owner,
            lease_seconds=self.execution_lease_seconds,
        )
        stop = threading.Event()
        lease_lost = threading.Event()
        self._job_execution_active = True
        self._job_lease_lost = lease_lost
        self._job_execution_id = job_id
        heartbeat = threading.Thread(
            target=self._heartbeat_job_execution_lock,
            args=(job_id, stop, lease_lost),
            name=f"job-lock-heartbeat-{job_id}",
            daemon=True,
        )
        heartbeat.start()
        return stop, lease_lost, heartbeat

    def _stop_job_execution(
        self,
        job_id: str,
        lease: Tuple[threading.Event, threading.Event, threading.Thread],
    ) -> None:
        stop, _, heartbeat = lease
        stop.set()
        heartbeat.join(timeout=2)
        try:
            if self.nr is not None:
                self.nr.close_connections()
            if self.jump_host_manager:
                self.jump_host_manager.close_all_tunnels()
        finally:
            self.store.release_job_execution_lock(
                job_id,
                self.execution_owner,
            )
            self._job_execution_active = False
            self._job_lease_lost = None
            self._job_execution_id = None

    def _heartbeat_job_execution_lock(
        self,
        job_id: str,
        stop: threading.Event,
        lease_lost: threading.Event,
    ) -> None:
        interval = max(0.5, self.execution_lease_seconds / 3)
        while not stop.wait(interval):
            try:
                self.store.renew_job_execution_lock(
                    job_id,
                    self.execution_owner,
                    lease_seconds=self.execution_lease_seconds,
                )
            except DeviceLockedError:
                lease_lost.set()
                return

    def _assert_job_execution_lease(self, job_id: str) -> None:
        if not self._job_execution_active:
            return
        if self._job_execution_id != job_id:
            raise DeviceLockedError(
                f"Execution lease belongs to job {self._job_execution_id}"
            )
        if self._job_lease_lost and self._job_lease_lost.is_set():
            raise DeviceLockedError(
                f"Execution lease for job {job_id} was lost"
            )
        self.store.assert_job_execution_lock(
            job_id,
            self.execution_owner,
        )

    def _set_job_status(self, job_id: str, status: str) -> None:
        self._assert_job_execution_lease(job_id)
        self.store.set_job_status(
            job_id,
            status,
            owner_token=(
                self.execution_owner if self._job_execution_active else None
            ),
        )

    def _prepare_runtime(self, job_id: str, inventory_path: str) -> None:
        manager = InventoryManager(inventory_path)
        inventory_devices = manager.load_inventory()
        by_hostname = {
            str(device["hostname"]): device for device in inventory_devices
        }
        persisted = self.store.get_devices(job_id)
        missing = [
            device["hostname"]
            for device in persisted
            if device["hostname"] not in by_hostname
        ]
        if missing:
            raise ValueError(
                "Devices were removed from inventory after job creation: "
                + ", ".join(missing)
            )
        self.inventory_manager = manager
        self._inventory_devices = {}
        for device in persisted:
            merged = dict(by_hostname[device["hostname"]])
            for key in (
                "hostname",
                "ip_address",
                "platform",
                "target_version",
                "image_path",
                "image_md5",
                "redundancy_group",
            ):
                merged[key] = device[key]
            self._inventory_devices[device["hostname"]] = merged
        self.nr = self._build_nornir(self._inventory_devices.values())

    @staticmethod
    def _device_ssh_port(device: Dict[str, Any]) -> int:
        """Return the device's SSH port, defaulting to 22."""
        value = device.get("ssh_port")
        if value in (None, ""):
            return 22
        try:
            port = int(value)
        except (TypeError, ValueError) as error:
            raise UpgradeOperationError(
                f"Invalid ssh_port {value!r} for {device.get('hostname')}"
            ) from error
        if not 1 <= port <= 65535:
            raise UpgradeOperationError(
                f"Invalid ssh_port {port} for {device.get('hostname')}"
            )
        return port

    def _build_nornir(self, devices: Iterable[Dict[str, Any]]) -> Nornir:
        ConnectionPluginRegister.auto_register()
        hosts = Hosts()
        connection = self.config.get("connection", {})
        workers = int(
            self.config.get("workflow", {}).get(
                "max_parallel_devices",
                self.config.get("nornir", {})
                .get("runner", {})
                .get("options", {})
                .get("num_workers", 5),
            )
        )
        for device in devices:
            hostname = str(device["hostname"])
            credentials = self.credentials.get_device_credentials(hostname)
            if not credentials["username"] or not credentials["password"]:
                raise UpgradeOperationError(
                    f"Credentials were not found for {hostname}"
                )
            address = str(device["ip_address"])
            port = self._device_ssh_port(device)
            if self.jump_host_manager:
                tunnel = self.jump_host_manager.create_tunnel(address, port)
                if not tunnel:
                    raise UpgradeOperationError(
                        f"Could not establish jump-host tunnel for {hostname}"
                    )
                address, port = tunnel

            self._connection_credentials[hostname] = credentials
            self._connection_endpoints[hostname] = (address, int(port))
            extras = {
                "device_type": "cisco_xe",
                "secret": credentials.get("secret", ""),
                "conn_timeout": int(connection.get("timeout", 60)),
                "auth_timeout": int(connection.get("auth_timeout", 30)),
                "banner_timeout": int(connection.get("banner_timeout", 30)),
                "blocking_timeout": int(
                    connection.get("blocking_timeout", 30)
                ),
                "session_timeout": int(connection.get("session_timeout", 300)),
                "keepalive": int(connection.get("keepalive", 30)),
                "fast_cli": False,
            }
            hosts[hostname] = Host(
                name=hostname,
                hostname=address,
                port=int(port),
                platform="cisco_xe",
                username=credentials["username"],
                password=credentials["password"],
                data=dict(device),
                connection_options={
                    "netmiko": ConnectionOptions(
                        hostname=address,
                        port=int(port),
                        username=credentials["username"],
                        password=credentials["password"],
                        platform="cisco_xe",
                        extras=extras,
                    )
                },
            )

        inventory = Inventory(hosts=hosts, defaults=Defaults())
        config = Config(
            logging=LoggingConfig(enabled=False),
            runner=RunnerConfig(
                plugin="threaded",
                options={"num_workers": max(1, workers)},
            ),
        )
        return Nornir(
            inventory=inventory,
            config=config,
            data=GlobalState(),
            runner=ThreadedRunner(num_workers=max(1, workers)),
        )

    def _run_batch(
        self,
        job_id: str,
        hostnames: List[str],
        *,
        run_mode: str,
        retry_failed: bool,
        retry_activation: bool,
        dry_run: bool,
        live_status: bool,
    ) -> None:
        hostnames = list(dict.fromkeys(hostnames))
        if not hostnames:
            return
        workers = min(
            len(hostnames),
            max(
                1,
                int(
                    self.config.get("workflow", {}).get(
                        "max_parallel_devices", 5
                    )
                ),
            ),
        )
        with ThreadPoolExecutor(
            max_workers=workers,
            thread_name_prefix="iosxe-upgrade",
        ) as executor:
            futures = {
                executor.submit(
                    self._run_device,
                    job_id,
                    hostname,
                    run_mode=run_mode,
                    retry_failed=retry_failed,
                    retry_activation=retry_activation,
                    dry_run=dry_run,
                ): hostname
                for hostname in hostnames
            }
            if live_status:
                with Live(
                    self.status_table(job_id),
                    console=self.console,
                    refresh_per_second=2,
                ) as live:
                    remaining = set(futures)
                    while remaining:
                        done, remaining = wait(
                            remaining,
                            timeout=0.5,
                            return_when=FIRST_COMPLETED,
                        )
                        self._record_future_errors(job_id, futures, done)
                        live.update(self.status_table(job_id))
            else:
                done, _ = wait(futures)
                self._record_future_errors(job_id, futures, done)

    def _record_future_errors(self, job_id, futures, completed) -> None:
        for future in completed:
            hostname = futures[future]
            error = future.exception()
            if not error:
                continue
            if isinstance(error, DeviceLockedError):
                self.store.record_event(
                    job_id,
                    str(error),
                    hostname=hostname,
                    level="ERROR",
                    state=self.store.get_device(job_id, hostname)["state"],
                )
                continue
            self.store.record_event(
                job_id,
                f"Worker terminated: {error}",
                hostname=hostname,
                level="ERROR",
                state=self.store.get_device(job_id, hostname)["state"],
            )

    def _run_device(
        self,
        job_id: str,
        hostname: str,
        *,
        run_mode: str,
        retry_failed: bool,
        retry_activation: bool,
        dry_run: bool,
    ) -> None:
        self.store.acquire_device_lock(
            job_id,
            hostname,
            self.execution_owner,
            lease_seconds=self.execution_lease_seconds,
        )
        heartbeat_stop = threading.Event()
        lease_lost = threading.Event()
        self._set_execution_context(job_id, hostname, lease_lost)
        heartbeat = threading.Thread(
            target=self._heartbeat_execution_lock,
            args=(job_id, hostname, heartbeat_stop, lease_lost),
            name=f"lock-heartbeat-{hostname}",
            daemon=True,
        )
        heartbeat.start()
        try:
            try:
                self._run_device_locked(
                    job_id,
                    hostname,
                    run_mode=run_mode,
                    retry_failed=retry_failed,
                    retry_activation=retry_activation,
                    dry_run=dry_run,
                )
            except (DeviceLockedError, UnsafeDispatchStateError):
                raise
            except Exception as error:
                device = self.store.get_device(job_id, hostname)
                if device["state"] not in FAILURE_STATES:
                    self._transition(
                        job_id,
                        hostname,
                        DeviceState.EXECUTION_FAILED,
                        str(error),
                        metadata={"retry_state": device["state"]},
                        level="ERROR",
                    )
                raise
        finally:
            heartbeat_stop.set()
            heartbeat.join(timeout=2)
            self.store.release_execution_lock(
                job_id,
                hostname,
                self.execution_owner,
            )
            self._clear_execution_context()

    def _heartbeat_execution_lock(
        self,
        job_id: str,
        hostname: str,
        stop: threading.Event,
        lease_lost: threading.Event,
    ) -> None:
        interval = max(0.5, self.execution_lease_seconds / 3)
        while not stop.wait(interval):
            try:
                self.store.renew_execution_lock(
                    job_id,
                    hostname,
                    self.execution_owner,
                    lease_seconds=self.execution_lease_seconds,
                )
            except DeviceLockedError:
                lease_lost.set()
                return

    def _run_device_locked(
        self,
        job_id: str,
        hostname: str,
        *,
        run_mode: str,
        retry_failed: bool,
        retry_activation: bool,
        dry_run: bool,
    ) -> None:
        stage_states = {
            DeviceState.PENDING.value,
            DeviceState.VALIDATING.value,
            DeviceState.READY.value,
            DeviceState.BACKING_UP.value,
            DeviceState.BASELINE_CAPTURE.value,
            DeviceState.STAGING.value,
            DeviceState.HASH_VERIFIED.value,
            DeviceState.STAGED.value,
            DeviceState.INCOMPATIBLE.value,
            DeviceState.PRECHECK_FAILED.value,
            DeviceState.BACKUP_FAILED.value,
            DeviceState.BASELINE_FAILED.value,
            DeviceState.STAGING_FAILED.value,
        }
        for _ in range(30):
            device = self.store.get_device(job_id, hostname)
            state = device["state"]
            if run_mode == "cleanup" and state not in {
                DeviceState.CLEANUP_PENDING.value,
                DeviceState.CLEANUP_FAILED.value,
                DeviceState.CLEANING.value,
            }:
                raise UnsafeDispatchStateError(
                    f"Refusing cleanup dispatch from state {state}"
                )
            if run_mode == "stage":
                retry_state = device["metadata"].get("retry_state")
                execution_failure_is_safe = (
                    state == DeviceState.EXECUTION_FAILED.value
                    and retry_state in stage_states
                )
                if (
                    state not in stage_states
                    and state not in FINAL_STATES
                    and not execution_failure_is_safe
                ):
                    raise UnsafeDispatchStateError(
                        f"Refusing stage-only dispatch from state {state}"
                    )
            if state in FINAL_STATES:
                self.store.release_device_lock(
                    job_id,
                    hostname,
                    self.execution_owner,
                )
                return
            if run_mode == "stage" and state == DeviceState.STAGED.value:
                return
            if state == DeviceState.CLEANUP_PENDING.value:
                if run_mode == "cleanup":
                    self._cleanup(job_id, hostname, explicit=True)
                elif self.config.get("cleanup", {}).get("auto_cleanup", True):
                    self._cleanup(job_id, hostname, explicit=False)
                return
            if state in FAILURE_STATES:
                if not retry_failed:
                    return
                if state in {
                    DeviceState.ACTIVATION_FAILED.value,
                    DeviceState.ACTIVATION_UNCERTAIN.value,
                } and not retry_activation:
                    return
                if not self._reset_failed_state(
                    job_id,
                    hostname,
                    state,
                    retry_activation=retry_activation,
                ):
                    # Nothing moved, so looping would just re-run the same
                    # checks until the transition limit trips.
                    return
                continue

            if state in {
                DeviceState.PENDING.value,
                DeviceState.VALIDATING.value,
            }:
                self._validate_device(job_id, hostname, dry_run=dry_run)
            elif state in {
                DeviceState.READY.value,
                DeviceState.BACKING_UP.value,
            }:
                self._backup_device(job_id, hostname)
            elif state == DeviceState.BASELINE_CAPTURE.value:
                self._capture_baseline(job_id, hostname)
            elif state in {
                DeviceState.STAGING.value,
                DeviceState.HASH_VERIFIED.value,
            }:
                self._stage_device(job_id, hostname)
            elif state == DeviceState.STAGED.value:
                if run_mode == "stage":
                    return
                self._transition(
                    job_id,
                    hostname,
                    DeviceState.ACTIVATION_PENDING,
                    "Waiting to activate staged image",
                )
            elif state == DeviceState.ACTIVATION_PENDING.value:
                if not self._within_maintenance_window(hostname):
                    self.store.record_event(
                        job_id,
                        "Outside the configured maintenance window",
                        hostname=hostname,
                        level="WARNING",
                        state=state,
                    )
                    return
                self._activate_device(job_id, hostname)
            elif state == DeviceState.ACTIVATING.value:
                self._reconcile_activation(job_id, hostname)
            elif state == DeviceState.ROLLING_BACK.value:
                self._reconcile_rollback(job_id, hostname)
            elif state in {
                DeviceState.REBOOTING.value,
                DeviceState.RECONNECTING.value,
            }:
                persisted = self.store.get_device(job_id, hostname)
                rollback_in_progress = persisted["metadata"].get(
                    "rollback_in_progress", False
                )
                expected_version = (
                    persisted["metadata"].get("expected_version")
                    if rollback_in_progress
                    else persisted["target_version"]
                )
                success_state = (
                    DeviceState.ROLLED_BACK
                    if rollback_in_progress
                    else DeviceState.POST_CHECKING
                )
                failure_state = (
                    DeviceState.ROLLBACK_REQUIRED
                    if rollback_in_progress
                    else DeviceState.ACTIVATION_UNCERTAIN
                    if persisted["metadata"].get("activation_reconciliation")
                    else DeviceState.RECONNECT_TIMEOUT
                )
                if not expected_version:
                    raise UpgradeOperationError(
                        f"No expected version is recorded for {hostname}"
                    )
                if not self._wait_for_version(
                    job_id,
                    hostname,
                    expected_version=expected_version,
                    success_state=success_state,
                    success_message=(
                        f"Rolled back to IOS-XE {expected_version}"
                        if rollback_in_progress
                        else f"Device returned on IOS-XE {expected_version}"
                    ),
                    failure_state=failure_state,
                ):
                    return
                if rollback_in_progress:
                    self.store.release_device_lock(
                        job_id,
                        hostname,
                        self.execution_owner,
                    )
            elif state in {
                DeviceState.POST_CHECKING.value,
                DeviceState.STABILIZING.value,
            }:
                self._post_checks(job_id, hostname)
            elif state in {
                DeviceState.COMMIT_PENDING.value,
                DeviceState.COMMITTING.value,
            }:
                self._commit(job_id, hostname)
            elif state in {
                DeviceState.CLEANING.value,
                DeviceState.CLEANUP_FAILED.value,
            }:
                self._cleanup(job_id, hostname, explicit=run_mode == "cleanup")
                return
            else:
                raise UpgradeOperationError(
                    f"Unsupported persisted state for {hostname}: {state}"
                )
        raise UpgradeOperationError(
            f"State machine exceeded its transition limit for {hostname}"
        )

    def _validate_device(
        self,
        job_id: str,
        hostname: str,
        *,
        dry_run: bool,
    ) -> None:
        self._transition(
            job_id,
            hostname,
            DeviceState.VALIDATING,
            "Discovering IOS-XE compatibility and install state",
        )
        device = self._inventory_devices[hostname]
        image_path = str(device["image_path"])
        image_size: Optional[int] = None
        local_hash: Optional[str] = None
        if not image_path.lower().startswith("flash:"):
            source = Path(image_path)
            if not source.is_file():
                self._transition(
                    job_id,
                    hostname,
                    DeviceState.INCOMPATIBLE,
                    f"Image file does not exist: {source}",
                    level="ERROR",
                )
                return
            image_size = source.stat().st_size
            local_hash = calculate_file_hash(str(source), "md5").lower()
            if local_hash != str(device["image_md5"]).lower():
                self._transition(
                    job_id,
                    hostname,
                    DeviceState.INCOMPATIBLE,
                    "Local image MD5 does not match inventory",
                    metadata={"local_image_md5": local_hash},
                    level="ERROR",
                )
                return

        facts = self._collect_facts(
            hostname,
            job_id=job_id,
            label="validate",
        )
        target = str(device["target_version"])
        target_state = self._state_for_version(facts, target)
        if target_version_matches(facts.current_version, target) and target_state == "C":
            self._transition(
                job_id,
                hostname,
                DeviceState.COMPLETE,
                f"Already running committed IOS-XE {target}",
                metadata={"facts": facts.to_dict(), "already_target": True},
            )
            return

        report = evaluate_compatibility(
            device=device,
            facts=facts,
            image_size_bytes=image_size,
            config=self.config,
        )
        metadata = {
            "facts_before": facts.to_dict(),
            "version_before": facts.current_version,
            "compatibility": report.to_dict(),
            "local_image_md5": local_hash,
            "device_image_path": normalize_device_path(image_path),
            "facts_raw_directory": str(
                self._facts_raw_directory(job_id, hostname)
            ),
        }
        if not report.passed:
            self._transition(
                job_id,
                hostname,
                DeviceState.INCOMPATIBLE,
                "; ".join(report.failures),
                metadata=metadata,
                level="ERROR",
            )
            return
        if dry_run:
            self._transition(
                job_id,
                hostname,
                DeviceState.COMPLETE,
                "Dry-run compatibility checks passed; no changes made",
                metadata=metadata,
            )
            return
        self._transition(
            job_id,
            hostname,
            DeviceState.READY,
            "Compatibility checks passed",
            metadata=metadata,
        )

    def _backup_device(self, job_id: str, hostname: str) -> None:
        if not self.config.get("backup", {}).get("enabled", True):
            self._transition(
                job_id,
                hostname,
                DeviceState.BASELINE_CAPTURE,
                "Configuration backup disabled by policy",
                level="WARNING",
            )
            return
        self._transition(
            job_id,
            hostname,
            DeviceState.BACKING_UP,
            "Backing up configuration and device facts",
        )
        failed, result = self._run_host_task(
            hostname,
            transport.backup_configuration,
            backup_dir=self.config.get("backup", {}).get(
                "directory", "backups"
            ),
        )
        if failed:
            self._transition(
                job_id,
                hostname,
                DeviceState.BACKUP_FAILED,
                self._operation_error(result, "Configuration backup failed"),
                level="ERROR",
            )
            return
        self._transition(
            job_id,
            hostname,
            DeviceState.BASELINE_CAPTURE,
            "Configuration backup completed",
            metadata={"backup": result},
        )

    def _capture_baseline(self, job_id: str, hostname: str) -> None:
        comparison = self.config.get("comparison", {})
        if not comparison.get("enabled", True):
            self._transition(
                job_id,
                hostname,
                DeviceState.STAGING,
                "Genie comparison disabled",
                level="WARNING",
            )
            return
        self._transition(
            job_id,
            hostname,
            DeviceState.BASELINE_CAPTURE,
            "Learning pre-upgrade Genie operational state",
        )
        self._close_host_connections(hostname)
        address, port = self._connection_endpoints[hostname]
        credentials = self._connection_credentials[hostname]
        try:
            manifest = self.snapshots.capture(
                job_id=job_id,
                hostname=hostname,
                address=address,
                port=port,
                username=credentials["username"],
                password=credentials["password"],
                enable_password=credentials.get("secret"),
                phase="before",
                core_features=comparison.get("core_features", CORE_FEATURES),
                optional_features=comparison.get(
                    "optional_features", OPTIONAL_FEATURES
                ),
                required_features=comparison.get(
                    "required_features", ["interface", "platform"]
                ),
                connect_timeout=int(
                    self.config.get("connection", {}).get("timeout", 120)
                ),
            )
        except SnapshotError as error:
            self._transition(
                job_id,
                hostname,
                DeviceState.BASELINE_FAILED,
                str(error),
                level="ERROR",
            )
            return
        self._transition(
            job_id,
            hostname,
            DeviceState.STAGING,
            "Pre-upgrade Genie baseline captured",
            metadata={"before_snapshot": manifest},
        )

    def _stage_device(self, job_id: str, hostname: str) -> None:
        device_run = self.store.get_device(job_id, hostname)
        device = self._inventory_devices[hostname]
        state = device_run["state"]
        destination = normalize_device_path(
            str(
                device_run["metadata"].get(
                    "device_image_path",
                    device["image_path"],
                )
            )
        )
        expected_md5 = str(device["image_md5"])
        md5_read_timeout = int(
            self.config.get("image_transfer", {}).get("timeout", 1800)
        )

        if state == DeviceState.STAGING.value:
            failed, result = self._run_host_task(
                hostname,
                transport.verify_image_hash,
                image_name=destination.removeprefix("flash:"),
                expected_md5=expected_md5,
                read_timeout=md5_read_timeout,
            )
            if failed:
                source_path = str(device["image_path"])
                if source_path.lower().startswith("flash:"):
                    self._transition(
                        job_id,
                        hostname,
                        DeviceState.STAGING_FAILED,
                        self._operation_error(
                            result,
                            "Pre-staged device image failed MD5 verification",
                        ),
                        level="ERROR",
                    )
                    return
                failed, transfer_result = self._run_host_task(
                    hostname,
                    transport.transfer_image_scp,
                    source_file=source_path,
                    dest_file=image_file_name(source_path),
                    file_system="flash:",
                )
                if failed:
                    self._transition(
                        job_id,
                        hostname,
                        DeviceState.STAGING_FAILED,
                        self._operation_error(
                            transfer_result, "Image transfer failed"
                        ),
                        level="ERROR",
                    )
                    return
                failed, result = self._run_host_task(
                    hostname,
                    transport.verify_image_hash,
                    image_name=image_file_name(source_path),
                    expected_md5=expected_md5,
                    read_timeout=md5_read_timeout,
                )
                if failed:
                    self._transition(
                        job_id,
                        hostname,
                        DeviceState.STAGING_FAILED,
                        self._operation_error(
                            result, "Transferred image failed MD5 verification"
                        ),
                        level="ERROR",
                    )
                    return
            self._transition(
                job_id,
                hostname,
                DeviceState.HASH_VERIFIED,
                f"Verified device image {destination}",
                metadata={"device_image_path": destination},
            )
            return

        facts = self._collect_facts(
            hostname,
            job_id=job_id,
            label="stage",
        )
        target = str(device["target_version"])
        target_state = self._state_for_version(facts, target)
        if target_state in {"I", "U", "C"}:
            self._transition(
                job_id,
                hostname,
                DeviceState.STAGED,
                f"Target IOS-XE {target} is already added ({target_state})",
                metadata={"facts_staged": facts.to_dict()},
            )
            return
        failed, result = self._run_host_task(
            hostname,
            transport.execute_install_add,
            image_path=destination,
            max_wait=int(
                self.config.get("image_transfer", {}).get("install_add_timeout", 1800)
            ),
        )
        if failed:
            self._transition(
                job_id,
                hostname,
                DeviceState.STAGING_FAILED,
                self._operation_error(result, "install add failed"),
                level="ERROR",
            )
            return
        facts = self._collect_facts(
            hostname,
            job_id=job_id,
            label="stage-2",
        )
        target_state = self._state_for_version(facts, target)
        if target_state not in {"I", "U", "C"}:
            self._transition(
                job_id,
                hostname,
                DeviceState.STAGING_FAILED,
                "Target image is not present in show install summary after add",
                metadata={"install_add_output": result},
                level="ERROR",
            )
            return
        self._transition(
            job_id,
            hostname,
            DeviceState.STAGED,
            f"Added IOS-XE {target}; ready for activation",
            metadata={
                "facts_staged": facts.to_dict(),
                "install_add_output": result,
            },
        )

    def _activate_device(self, job_id: str, hostname: str) -> None:
        device_run = self.store.get_device(job_id, hostname)
        device = self._inventory_devices[hostname]
        target = str(device_run["target_version"])
        facts = self._collect_facts(
            hostname,
            job_id=job_id,
            label="activate-preflight",
        )
        target_state = self._state_for_version(facts, target)
        if target_version_matches(facts.current_version, target) and target_state in {
            "U",
            "C",
        }:
            self._transition(
                job_id,
                hostname,
                DeviceState.POST_CHECKING,
                f"Target IOS-XE {target} is already active; resuming checks",
                metadata={"activation_preflight": facts.to_dict()},
            )
            return

        report = evaluate_compatibility(
            device=device,
            facts=facts,
            image_size_bytes=None,
            config=self.config,
        )
        blocking_failures = [
            check.message
            for check in report.checks
            if not check.passed and check.name != "flash_space"
        ]
        baseline_serial = (
            device_run["metadata"].get("facts_before", {}).get("serial")
        )
        if (
            baseline_serial
            and baseline_serial != "Unknown"
            and facts.serial != baseline_serial
        ):
            blocking_failures.append(
                "Device serial changed after staging; refusing activation"
            )
        inactive_entries = facts.install_states.get("I", [])
        unexpected_inactive = [
            entry
            for entry in inactive_entries
            if not target_version_matches(entry, target)
        ]
        if target_state != "I":
            blocking_failures.append(
                f"Target IOS-XE {target} is not the inactive activation candidate"
            )
        if unexpected_inactive:
            blocking_failures.append(
                "Other inactive IOS-XE versions are present: "
                + ", ".join(unexpected_inactive)
            )
        if blocking_failures:
            self._transition(
                job_id,
                hostname,
                DeviceState.ACTIVATION_FAILED,
                "; ".join(blocking_failures),
                metadata={
                    "activation_preflight": facts.to_dict(),
                    "activation_compatibility": report.to_dict(),
                },
                level="ERROR",
            )
            return

        destination = normalize_device_path(
            str(
                device_run["metadata"].get(
                    "device_image_path",
                    device["image_path"],
                )
            )
        )
        failed, result = self._run_host_task(
            hostname,
            transport.verify_image_hash,
            image_name=destination.removeprefix("flash:"),
            expected_md5=str(device_run["image_md5"]),
            read_timeout=int(
                self.config.get("image_transfer", {}).get("timeout", 1800)
            ),
        )
        if failed:
            self._transition(
                job_id,
                hostname,
                DeviceState.ACTIVATION_FAILED,
                self._operation_error(
                    result,
                    "Staged image failed activation-time MD5 verification",
                ),
                metadata={"activation_preflight": facts.to_dict()},
                level="ERROR",
            )
            return

        if self.config.get("upgrade", {}).get(
            "save_configuration_before_reload", True
        ):
            failed, result = self._run_host_task(
                hostname,
                transport.save_running_configuration,
            )
            if failed:
                self._transition(
                    job_id,
                    hostname,
                    DeviceState.ACTIVATION_FAILED,
                    self._operation_error(
                        result, "Could not save running configuration"
                    ),
                    level="ERROR",
                )
                return
        auto_abort_minutes = transport.normalize_auto_abort_timer(
            self.config.get("upgrade", {}).get(
                "auto_abort_timer_minutes",
                transport.AUTO_ABORT_TIMER_DEFAULT_MINUTES,
            )
        )
        effective_abort_minutes = (
            auto_abort_minutes
            if auto_abort_minutes is not None
            else transport.AUTO_ABORT_TIMER_DEFAULT_MINUTES
        )
        # The rollback timer starts when the device accepts the command, so the
        # deadline is recorded before it is sent. Erring early is what we want:
        # a commit refused a minute too soon is recoverable, one attempted a
        # minute too late is not.
        auto_abort_expires_at = (
            datetime.now(timezone.utc)
            + timedelta(minutes=effective_abort_minutes)
        ).isoformat()
        self._transition(
            job_id,
            hostname,
            DeviceState.ACTIVATING,
            (
                "Executing install activate; reload expected. Commit deadline "
                f"{auto_abort_expires_at} ({effective_abort_minutes}m "
                "auto-abort timer)"
            ),
            increment_attempt=True,
            metadata={
                "activation_requested_at": utc_now(),
                "activation_preflight": facts.to_dict(),
                "auto_abort_timer_minutes": effective_abort_minutes,
                "auto_abort_timer_explicit": auto_abort_minutes is not None,
                "auto_abort_expires_at": auto_abort_expires_at,
            },
        )
        failed, result = self._run_host_task(
            hostname,
            transport.execute_install_activate,
            max_wait=int(
                self.config.get("upgrade", {}).get(
                    "activation_timeout", 1800
                )
            ),
            auto_abort_timer_minutes=auto_abort_minutes,
        )
        explicit_command_failure = (
            failed
            and isinstance(result, dict)
            and result.get("error")
            == "IOS-XE reported an install command failure"
        )
        if explicit_command_failure:
            self._transition(
                job_id,
                hostname,
                DeviceState.ACTIVATION_FAILED,
                self._operation_error(result, "install activate failed"),
                metadata={"activation_output": result},
                level="ERROR",
            )
            return
        if failed or not result.get("reload_initiated"):
            self._transition(
                job_id,
                hostname,
                DeviceState.ACTIVATING,
                "Activation command outcome is uncertain; reconciling device state",
                metadata={"activation_output": result},
                level="WARNING",
            )
            self._reconcile_activation(job_id, hostname)
            return
        self._transition(
            job_id,
            hostname,
            DeviceState.REBOOTING,
            "Activation accepted; waiting for reload and reconnect",
            metadata={
                "activation_output": result,
                "activated_at": utc_now(),
            },
        )
        self._close_host_connections(hostname)

    def _reconcile_activation(self, job_id: str, hostname: str) -> None:
        """Resolve an activation intent whose command outcome is unknown."""
        target = self.store.get_device(job_id, hostname)["target_version"]
        self._wait_for_version(
            job_id,
            hostname,
            expected_version=target,
            success_state=DeviceState.POST_CHECKING,
            success_message=(
                f"Interrupted activation completed on IOS-XE {target}"
            ),
            failure_state=DeviceState.ACTIVATION_UNCERTAIN,
            wait_metadata={"activation_reconciliation": True},
        )

    def _reconcile_rollback(self, job_id: str, hostname: str) -> None:
        """Resolve a rollback intent whose command outcome is unknown."""
        device = self.store.get_device(job_id, hostname)
        previous_version = device["metadata"].get(
            "expected_version",
            device["metadata"].get("version_before"),
        )
        if not previous_version:
            raise UpgradeOperationError(
                "Previous version is not recorded for rollback reconciliation"
            )
        if self._wait_for_version(
            job_id,
            hostname,
            expected_version=previous_version,
            success_state=DeviceState.ROLLED_BACK,
            success_message=(
                f"Rollback reconciled on IOS-XE {previous_version}"
            ),
            failure_state=DeviceState.ROLLBACK_REQUIRED,
            wait_metadata={
                "rollback_in_progress": True,
                "rollback_reconciliation": True,
                "expected_version": previous_version,
            },
        ):
            self.store.release_device_lock(
                job_id,
                hostname,
                self.execution_owner,
            )

    def _wait_for_version(
        self,
        job_id: str,
        hostname: str,
        *,
        expected_version: str,
        success_state: DeviceState,
        success_message: str,
        failure_state: DeviceState = DeviceState.RECONNECT_TIMEOUT,
        wait_metadata: Optional[Dict[str, Any]] = None,
    ) -> bool:
        self._transition(
            job_id,
            hostname,
            DeviceState.RECONNECTING,
            f"Waiting for IOS-XE {expected_version}",
            metadata=wait_metadata,
        )
        post_checks = self.config.get("post_checks", {})
        timeout = int(post_checks.get("max_reload_wait", 1800))
        interval = int(post_checks.get("reconnect_interval", 20))
        deadline = self.monotonic() + timeout
        last_observation = "SSH is unavailable"

        while self.monotonic() < deadline:
            self._close_host_connections(hostname)
            if self._tcp_reachable(hostname):
                try:
                    facts = self._collect_facts(hostname)
                    last_observation = (
                        f"Device reports IOS-XE {facts.current_version}"
                    )
                    if target_version_matches(
                        facts.current_version, expected_version
                    ):
                        self._transition(
                            job_id,
                            hostname,
                            success_state,
                            success_message,
                            metadata={"facts_after_reboot": facts.to_dict()},
                        )
                        return True
                except UpgradeOperationError as error:
                    last_observation = str(error)
            self.sleeper(interval)

        self._transition(
            job_id,
            hostname,
            failure_state,
            (
                f"Timed out after {timeout}s waiting for IOS-XE "
                f"{expected_version}: {last_observation}"
            ),
            level="ERROR",
        )
        return False

    def _post_checks(self, job_id: str, hostname: str) -> None:
        device = self.store.get_device(job_id, hostname)
        metadata = device["metadata"]
        if device["state"] == DeviceState.POST_CHECKING.value:
            stabilization_seconds = int(
                self.config.get("post_checks", {}).get(
                    "stabilization_seconds", 120
                )
            )
            stabilize_until = (
                datetime.now(timezone.utc)
                + timedelta(seconds=stabilization_seconds)
            ).isoformat()
            self._transition(
                job_id,
                hostname,
                DeviceState.STABILIZING,
                (
                    f"Waiting {stabilization_seconds}s for control-plane "
                    "stabilization"
                ),
                metadata={"stabilize_until": stabilize_until},
            )
            metadata["stabilize_until"] = stabilize_until

        stabilize_until_text = metadata.get("stabilize_until")
        if stabilize_until_text:
            stabilize_until = datetime.fromisoformat(stabilize_until_text)
            remaining = (
                stabilize_until - datetime.now(timezone.utc)
            ).total_seconds()
            if remaining > 0:
                self.sleeper(remaining)

        facts = self._collect_facts(
            hostname,
            job_id=job_id,
            label="post-check",
        )
        target = device["target_version"]
        target_state = self._state_for_version(facts, target)
        if (
            not target_version_matches(facts.current_version, target)
            or not facts.install_mode
            or target_state not in {"U", "C"}
        ):
            # The device is not where it should be, so this is not something a
            # repeated health check can resolve.
            self._post_check_failure(
                job_id,
                hostname,
                (
                    f"Post-check reconciliation failed: version="
                    f"{facts.current_version}, install_state={target_state}, "
                    f"install_mode={facts.install_mode}"
                ),
                metadata={"facts_post_check": facts.to_dict()},
                state=DeviceState.ROLLBACK_REQUIRED,
            )
            return

        after_phase = self._post_check_phase(metadata)
        comparison = self.config.get("comparison", {})
        comparison_report = None
        if comparison.get("enabled", True):
            self._close_host_connections(hostname)
            address, port = self._connection_endpoints[hostname]
            credentials = self._connection_credentials[hostname]
            try:
                manifest = self.snapshots.capture(
                    job_id=job_id,
                    hostname=hostname,
                    address=address,
                    port=port,
                    username=credentials["username"],
                    password=credentials["password"],
                    enable_password=credentials.get("secret"),
                    phase=after_phase,
                    core_features=comparison.get(
                        "core_features", CORE_FEATURES
                    ),
                    optional_features=comparison.get(
                        "optional_features", OPTIONAL_FEATURES
                    ),
                    required_features=comparison.get(
                        "required_features", ["interface", "platform"]
                    ),
                    connect_timeout=int(
                        self.config.get("connection", {}).get("timeout", 120)
                    ),
                )
                comparison_report = self.snapshots.compare(
                    job_id=job_id,
                    hostname=hostname,
                    after_phase=after_phase,
                    critical_features=comparison.get(
                        "critical_features", DEFAULT_CRITICAL_FEATURES
                    ),
                    required_features=comparison.get(
                        "required_features", ["interface", "platform"]
                    ),
                )
            except SnapshotError as error:
                self._post_check_failure(
                    job_id,
                    hostname,
                    str(error),
                    metadata={"after_snapshot_phase": after_phase},
                    state=DeviceState.POST_CHECK_FAILED,
                )
                return
            if not comparison_report.passed:
                self._post_check_failure(
                    job_id,
                    hostname,
                    "Genie comparison failed for: "
                    + ", ".join(comparison_report.blocking_features),
                    metadata={
                        "comparison_report": comparison_report.report_path,
                        "after_snapshot": manifest,
                        "after_snapshot_phase": after_phase,
                    },
                    state=DeviceState.POST_CHECK_FAILED,
                )
                return

        deadline = self._auto_abort_deadline(device)
        commit_pending_message = (
            "Post-upgrade checks passed; target remains uncommitted"
        )
        if deadline is not None:
            commit_pending_message += (
                f"; commit before {deadline.isoformat()} or the device reverts"
            )
        self._transition(
            job_id,
            hostname,
            DeviceState.COMMIT_PENDING,
            commit_pending_message,
            metadata={
                "facts_post_check": facts.to_dict(),
                "comparison_report": (
                    comparison_report.report_path
                    if comparison_report
                    else None
                ),
                "verified_at": utc_now(),
                "auto_abort_expires_at": (
                    deadline.isoformat() if deadline else None
                ),
            },
        )

    @staticmethod
    def _post_check_phase(metadata: Dict[str, Any]) -> str:
        """Name the snapshot phase for this post-check attempt.

        Retries write to their own phase so the snapshots that produced the
        first failure are still there to compare against afterwards.
        """
        attempts = int(metadata.get("post_check_attempts", 0) or 0)
        return "after" if attempts < 1 else f"after-retry-{attempts}"

    def _auto_abort_deadline(
        self,
        device: Dict[str, Any],
    ) -> Optional[datetime]:
        """Return when the device will revert on its own, if that is known."""
        value = device.get("metadata", {}).get("auto_abort_expires_at")
        if not value:
            return None
        try:
            deadline = datetime.fromisoformat(str(value))
        except ValueError:
            return None
        if deadline.tzinfo is None:
            deadline = deadline.replace(tzinfo=timezone.utc)
        return deadline

    def _auto_abort_window_expired(self, device: Dict[str, Any]) -> bool:
        deadline = self._auto_abort_deadline(device)
        return deadline is not None and datetime.now(timezone.utc) >= deadline

    def _reconcile_expired_auto_abort(
        self,
        job_id: str,
        hostname: str,
        device: Dict[str, Any],
    ) -> bool:
        """Decide what to do when the commit deadline has already passed.

        Returns True when it is still safe to run `install commit`.
        """
        deadline = self._auto_abort_deadline(device)
        target = device["target_version"]
        previous_version = device["metadata"].get("version_before")
        facts = self._collect_facts(
            hostname,
            job_id=job_id,
            label="auto-abort-reconcile",
        )
        target_state = self._state_for_version(facts, target)
        if target_state == "C":
            self._mark_cleanup_pending(job_id, hostname, facts)
            return False
        if target_state == "U":
            # Still activated and uncommitted, so the timer was stopped or our
            # deadline was conservative. Commit, but say so in the audit trail.
            self.store.record_event(
                job_id,
                (
                    f"Commit deadline {deadline.isoformat()} has passed but "
                    f"IOS-XE {target} is still activated and uncommitted; "
                    "committing now"
                ),
                hostname=hostname,
                level="WARNING",
                state=device["state"],
            )
            return True
        reverted = previous_version and target_version_matches(
            facts.current_version, str(previous_version)
        )
        message = (
            "The install auto-abort timer expired before commit "
            f"(deadline {deadline.isoformat()}); device is running IOS-XE "
            f"{facts.current_version} with target state "
            f"{target_state or 'absent'}"
        )
        metadata = {
            "facts_post_check": facts.to_dict(),
            "auto_abort_expired": True,
            "auto_abort_expires_at": (
                deadline.isoformat() if deadline else None
            ),
        }
        if reverted:
            # The device lease is released by the caller's state-machine loop
            # when it observes this final state on its next pass.
            self._transition(
                job_id,
                hostname,
                DeviceState.ROLLED_BACK,
                message + "; the device reverted to its previous image",
                metadata=metadata,
                level="ERROR",
            )
        else:
            self._transition(
                job_id,
                hostname,
                DeviceState.COMMIT_FAILED,
                message,
                metadata=metadata,
                level="ERROR",
            )
        return False

    def _hold_auto_abort_timer(self, job_id: str, hostname: str) -> None:
        """Stop the rollback timer so a failed device waits for an operator."""
        failed, result = self._run_host_task(
            hostname,
            transport.execute_install_auto_abort_timer_stop,
            max_wait=int(
                self.config.get("upgrade", {}).get(
                    "auto_abort_timer_stop_timeout", 300
                )
            ),
        )
        if failed:
            self.store.record_event(
                job_id,
                self._operation_error(
                    result,
                    "Could not stop the install auto-abort timer; the device "
                    "will revert on its own",
                ),
                hostname=hostname,
                level="ERROR",
                state=DeviceState.ROLLBACK_REQUIRED.value,
            )
            return
        self.store.record_event(
            job_id,
            (
                "Install auto-abort timer stopped; the device will hold the "
                "uncommitted image until an operator acts"
            ),
            hostname=hostname,
            level="WARNING",
            state=DeviceState.ROLLBACK_REQUIRED.value,
        )

    def _post_check_failure(
        self,
        job_id: str,
        hostname: str,
        message: str,
        *,
        metadata: Optional[Dict[str, Any]] = None,
        state: DeviceState = DeviceState.ROLLBACK_REQUIRED,
    ) -> None:
        device = self.store.get_device(job_id, hostname)
        deadline = self._auto_abort_deadline(device)
        metadata = dict(metadata or {})
        if deadline is not None:
            metadata["auto_abort_expires_at"] = deadline.isoformat()
            message = (
                f"{message}. The image is activated but uncommitted: without "
                f"operator action the device reverts at {deadline.isoformat()}"
            )
        self._transition(
            job_id,
            hostname,
            state,
            message,
            metadata=metadata,
            level="ERROR",
        )
        if (
            deadline is not None
            and not self.config.get("rollback", {}).get(
                "auto_rollback_on_failure", False
            )
            and self.config.get("upgrade", {}).get(
                "hold_auto_abort_timer_on_post_check_failure", False
            )
        ):
            try:
                self._hold_auto_abort_timer(job_id, hostname)
            except Exception as error:
                self.store.record_event(
                    job_id,
                    f"Could not stop the install auto-abort timer: {error}",
                    hostname=hostname,
                    level="ERROR",
                    state=state.value,
                )
        if self.config.get("rollback", {}).get(
            "auto_rollback_on_failure", False
        ):
            try:
                self._abort_uncommitted(job_id, hostname)
            except Exception as error:
                self.store.record_event(
                    job_id,
                    f"Automatic rollback failed: {error}",
                    hostname=hostname,
                    level="ERROR",
                    state=DeviceState.ROLLBACK_REQUIRED.value,
                )

    def _abort_uncommitted(self, job_id: str, hostname: str) -> None:
        device = self.store.get_device(job_id, hostname)
        previous_version = device["metadata"].get("version_before")
        if not previous_version:
            raise UpgradeOperationError(
                "Previous version is not recorded; refusing blind rollback"
            )
        facts = self._collect_facts(
            hostname,
            job_id=job_id,
            label="rollback-preflight",
        )
        if self._state_for_version(facts, device["target_version"]) != "U":
            raise UpgradeOperationError(
                "Target image is not activated and uncommitted"
            )
        self._transition(
            job_id,
            hostname,
            DeviceState.ROLLING_BACK,
            "Aborting failed uncommitted activation",
            metadata={
                "expected_version": previous_version,
                "rollback_in_progress": True,
                "rollback_requested_at": utc_now(),
            },
        )
        failed, result = self._run_host_task(
            hostname,
            transport.execute_install_abort,
            max_wait=int(
                self.config.get("rollback", {}).get("command_timeout", 600)
            ),
        )
        if failed or not result.get("reload_initiated"):
            raise UpgradeOperationError(
                self._operation_error(result, "install abort failed")
            )
        self._transition(
            job_id,
            hostname,
            DeviceState.REBOOTING,
            "Rollback reload initiated",
            metadata={
                "expected_version": previous_version,
                "rollback_in_progress": True,
            },
        )
        if not self._wait_for_version(
            job_id,
            hostname,
            expected_version=previous_version,
            success_state=DeviceState.ROLLED_BACK,
            success_message=f"Rolled back to {previous_version}",
        ):
            raise UpgradeOperationError("Device did not return on prior image")
        self.store.release_device_lock(
            job_id,
            hostname,
            self.execution_owner,
        )

    def _commit(self, job_id: str, hostname: str) -> None:
        device = self.store.get_device(job_id, hostname)
        target = device["target_version"]
        if device["state"] == DeviceState.COMMITTING.value:
            facts = self._collect_facts(
                hostname,
                job_id=job_id,
                label="commit",
            )
            if self._state_for_version(facts, target) == "C":
                self._mark_cleanup_pending(job_id, hostname, facts)
                return
        if self._auto_abort_window_expired(device):
            if not self._reconcile_expired_auto_abort(job_id, hostname, device):
                return
        self._transition(
            job_id,
            hostname,
            DeviceState.COMMITTING,
            "Committing verified IOS-XE installation",
            metadata={"commit_requested_at": utc_now()},
        )
        failed, result = self._run_host_task(
            hostname,
            transport.execute_install_commit,
            max_wait=int(
                self.config.get("upgrade", {}).get("commit_timeout", 600)
            ),
        )
        if failed:
            self._transition(
                job_id,
                hostname,
                DeviceState.COMMIT_FAILED,
                self._operation_error(result, "install commit failed"),
                level="ERROR",
            )
            return
        facts = self._collect_facts(
            hostname,
            job_id=job_id,
            label="commit-2",
        )
        if self._state_for_version(facts, target) != "C":
            self._transition(
                job_id,
                hostname,
                DeviceState.COMMIT_FAILED,
                "Target image is not committed in show install summary",
                metadata={"commit_output": result},
                level="ERROR",
            )
            return
        self._mark_cleanup_pending(
            job_id,
            hostname,
            facts,
            commit_output=result,
        )

    def _mark_cleanup_pending(
        self,
        job_id: str,
        hostname: str,
        facts: InstallFacts,
        *,
        commit_output: Optional[Dict[str, Any]] = None,
    ) -> None:
        soak_seconds = int(
            self.config.get("cleanup", {}).get("soak_seconds", 3600)
        )
        eligible_at = (
            datetime.now(timezone.utc) + timedelta(seconds=soak_seconds)
        ).isoformat()
        self._transition(
            job_id,
            hostname,
            DeviceState.CLEANUP_PENDING,
            (
                "Upgrade committed; inactive-package cleanup eligible at "
                f"{eligible_at}"
            ),
            metadata={
                "facts_committed": facts.to_dict(),
                "commit_output": commit_output,
                "committed_at": utc_now(),
                "cleanup_eligible_at": eligible_at,
            },
        )

    def _cleanup(
        self,
        job_id: str,
        hostname: str,
        *,
        explicit: bool,
    ) -> None:
        device = self.store.get_device(job_id, hostname)
        eligible_text = device["metadata"].get("cleanup_eligible_at")
        if eligible_text:
            eligible_at = datetime.fromisoformat(eligible_text)
            if datetime.now(timezone.utc) < eligible_at:
                if explicit:
                    self.store.record_event(
                        job_id,
                        f"Cleanup is not eligible until {eligible_text}",
                        hostname=hostname,
                        level="WARNING",
                        state=DeviceState.CLEANUP_PENDING.value,
                    )
                return
        facts = self._collect_facts(
            hostname,
            job_id=job_id,
            label="cleanup-preflight",
        )
        target = device["target_version"]
        committed_facts = device["metadata"].get("facts_committed")
        failures: List[str] = []
        if not isinstance(committed_facts, dict):
            failures.append("Committed install-state baseline is missing")
            expected_inactive: set[str] = set()
        else:
            expected_inactive = {
                str(entry)
                for entry in committed_facts.get("install_states", {}).get(
                    "I", []
                )
            }
        current_inactive = {
            str(entry) for entry in facts.install_states.get("I", [])
        }
        unexpected_inactive = current_inactive - expected_inactive
        if (
            not target_version_matches(facts.current_version, target)
            or self._state_for_version(facts, target) != "C"
            or not facts.install_mode
            or not facts.install_summary_valid
            or facts.install_states.get("U")
            or facts.install_states.get("D")
        ):
            failures.append(
                "Target is no longer the current committed install-mode image"
            )
        baseline_serial = device["metadata"].get("facts_before", {}).get(
            "serial"
        )
        if (
            baseline_serial
            and baseline_serial != "Unknown"
            and facts.serial != baseline_serial
        ):
            failures.append("Device serial changed after the upgrade")
        if unexpected_inactive:
            failures.append(
                "Unexpected inactive packages appeared during soak: "
                + ", ".join(sorted(unexpected_inactive))
            )
        if failures:
            self._transition(
                job_id,
                hostname,
                DeviceState.CLEANUP_FAILED,
                "; ".join(failures),
                metadata={"cleanup_preflight": facts.to_dict()},
                level="ERROR",
            )
            return
        if not current_inactive:
            self._complete_cleanup(
                job_id,
                hostname,
                facts,
                {"already_clean": True},
            )
            return
        self._transition(
            job_id,
            hostname,
            DeviceState.CLEANING,
            "Removing inactive IOS-XE packages",
        )
        failed, result = self._run_host_task(
            hostname,
            transport.execute_install_cleanup,
            max_wait=int(
                self.config.get("cleanup", {}).get("command_timeout", 1800)
            ),
        )
        if failed:
            self._transition(
                job_id,
                hostname,
                DeviceState.CLEANUP_FAILED,
                self._operation_error(
                    result, "install remove inactive failed"
                ),
                level="ERROR",
            )
            return
        facts_after = self._collect_facts(
            hostname,
            job_id=job_id,
            label="cleanup-preflight-2",
        )
        if (
            not target_version_matches(facts_after.current_version, target)
            or self._state_for_version(facts_after, target) != "C"
            or facts_after.install_states.get("I")
            or facts_after.install_states.get("U")
            or facts_after.install_states.get("D")
        ):
            self._transition(
                job_id,
                hostname,
                DeviceState.CLEANUP_FAILED,
                "Inactive packages remain or committed state changed after cleanup",
                metadata={
                    "cleanup_output": result,
                    "cleanup_reconciliation": facts_after.to_dict(),
                },
                level="ERROR",
            )
            return
        self._complete_cleanup(job_id, hostname, facts_after, result)

    def _complete_cleanup(
        self,
        job_id: str,
        hostname: str,
        facts: InstallFacts,
        result: Dict[str, Any],
    ) -> None:
        self._transition(
            job_id,
            hostname,
            DeviceState.COMPLETE,
            "Upgrade verified, committed, and inactive packages removed",
            metadata={
                "cleanup_output": result,
                "facts_cleaned": facts.to_dict(),
                "completed_at": utc_now(),
            },
        )
        self.store.release_device_lock(
            job_id,
            hostname,
            self.execution_owner,
        )

    def _collect_facts(
        self,
        hostname: str,
        *,
        job_id: Optional[str] = None,
        label: Optional[str] = None,
    ) -> InstallFacts:
        failed, result = self._run_host_task(
            hostname,
            transport.collect_upgrade_facts,
        )
        if failed:
            raise UpgradeOperationError(
                self._operation_error(
                    result, f"Could not collect upgrade facts from {hostname}"
                )
            )
        facts = parse_install_facts(result["outputs"])
        if job_id and label:
            self._persist_facts_raw(job_id, hostname, label, facts)
        return facts

    def _facts_raw_directory(self, job_id: str, hostname: str) -> Path:
        return (
            Path(self.snapshots.snapshot_directory)
            / job_id
            / hostname
            / "facts"
        )

    def _persist_facts_raw(
        self,
        job_id: str,
        hostname: str,
        label: str,
        facts: InstallFacts,
    ) -> Optional[str]:
        """Write the raw show-command output for one collection to disk.

        Keeping it out of the state database and next to the Genie snapshots
        means the evidence is still there for a post-mortem without storing the
        same text on every transition.
        """
        directory = self._facts_raw_directory(job_id, hostname)
        path = directory / f"{label}.json"
        temporary_path = path.with_name(path.name + ".tmp")
        try:
            directory.mkdir(parents=True, exist_ok=True)
            with temporary_path.open("w", encoding="utf-8") as handle:
                json.dump(
                    {
                        "job_id": job_id,
                        "hostname": hostname,
                        "label": label,
                        "captured_at": utc_now(),
                        "raw": facts.raw,
                    },
                    handle,
                    indent=2,
                    sort_keys=True,
                    default=str,
                )
                handle.write("\n")
            temporary_path.replace(path)
            return str(path)
        except OSError as error:
            self.store.record_event(
                job_id,
                (
                    f"Could not persist raw device output for {label}: {error}"
                ),
                hostname=hostname,
                level="WARNING",
            )
            return None

    def _set_execution_context(
        self,
        job_id: str,
        hostname: str,
        lease_lost: threading.Event,
    ) -> None:
        self._execution_context.job_id = job_id
        self._execution_context.hostname = hostname
        self._execution_context.lease_lost = lease_lost

    def _clear_execution_context(self) -> None:
        for attribute in ("job_id", "hostname", "lease_lost"):
            if hasattr(self._execution_context, attribute):
                delattr(self._execution_context, attribute)

    def _assert_execution_lease(self, job_id: str, hostname: str) -> None:
        self._assert_job_execution_lease(job_id)
        if (
            getattr(self._execution_context, "job_id", None) != job_id
            or getattr(self._execution_context, "hostname", None) != hostname
        ):
            return
        lease_lost = self._execution_context.lease_lost
        if lease_lost.is_set():
            raise DeviceLockedError(
                f"Execution lease for {hostname} was lost"
            )
        self.store.assert_execution_lock(
            job_id,
            hostname,
            self.execution_owner,
        )

    def _run_host_task(self, hostname: str, task, **kwargs):
        if self.nr is None:
            raise RuntimeError("Nornir runtime has not been prepared")
        context_job = getattr(self._execution_context, "job_id", None)
        if context_job is not None:
            self._assert_execution_lease(context_job, hostname)
        self.nr.data.failed_hosts.discard(hostname)
        host_nr = self.nr.filter(
            filter_func=lambda host: host.name == hostname
        )
        results = host_nr.run(
            task=task,
            on_good=True,
            on_failed=True,
            raise_on_error=False,
            **kwargs,
        )
        result = results[hostname]
        return result.failed, result.result

    def _close_host_connections(self, hostname: str) -> None:
        if self.nr is None:
            return
        host = self.nr.inventory.hosts.get(hostname)
        if host:
            host.close_connections()

    def _tcp_reachable(self, hostname: str) -> bool:
        address, port = self._connection_endpoints[hostname]
        try:
            with socket.create_connection((address, port), timeout=3):
                return True
        except OSError:
            return False

    # `show install summary` can list one version under more than one state,
    # for example after a partial operation or alongside an SMU. Resolve in a
    # fixed precedence rather than whichever key the parser happened to fill
    # first: committed beats activated, which beats deactivated and inactive.
    INSTALL_STATE_PRECEDENCE = ("C", "U", "D", "I")

    def _state_for_version(
        self,
        facts: InstallFacts,
        version: str,
    ) -> Optional[str]:
        matched = [
            state
            for state, installed_versions in facts.install_states.items()
            if any(
                target_version_matches(candidate, version)
                for candidate in installed_versions
            )
        ]
        if not matched:
            return None
        for state in self.INSTALL_STATE_PRECEDENCE:
            if state in matched:
                return state
        return matched[0]

    def _within_maintenance_window(self, hostname: str) -> bool:
        device = self._inventory_devices[hostname]
        start_text = device.get("maintenance_window_start") or device.get(
            "maintenance_window"
        )
        end_text = device.get("maintenance_window_end")
        if not start_text:
            return True
        try:
            start = datetime.fromisoformat(str(start_text))
            if start.tzinfo is None:
                start = start.astimezone()
            if end_text:
                end = datetime.fromisoformat(str(end_text))
                if end.tzinfo is None:
                    end = end.astimezone()
            else:
                duration = int(
                    self.config.get("workflow", {}).get(
                        "maintenance_window_minutes", 240
                    )
                )
                end = start + timedelta(minutes=duration)
        except ValueError as error:
            raise UpgradeOperationError(
                f"Invalid maintenance window for {hostname}: {error}"
            ) from error
        now = datetime.now(timezone.utc)
        return start.astimezone(timezone.utc) <= now <= end.astimezone(
            timezone.utc
        )

    def _reset_failed_state(
        self,
        job_id: str,
        hostname: str,
        state: str,
        *,
        retry_activation: bool,
    ) -> bool:
        """Move a failed device back to a retryable state.

        Returns True only when the device actually moved. ``rollback_required``
        is deliberately absent from the map: it means an operator has to decide
        between accepting the image and rolling it back, and re-running the
        post-checks that produced it would only overwrite the evidence.
        """
        retry_map = {
            DeviceState.INCOMPATIBLE.value: DeviceState.VALIDATING,
            DeviceState.PRECHECK_FAILED.value: DeviceState.VALIDATING,
            DeviceState.BACKUP_FAILED.value: DeviceState.BACKING_UP,
            DeviceState.BASELINE_FAILED.value: DeviceState.BASELINE_CAPTURE,
            DeviceState.STAGING_FAILED.value: DeviceState.STAGING,
            DeviceState.RECONNECT_TIMEOUT.value: DeviceState.RECONNECTING,
            DeviceState.COMMIT_FAILED.value: DeviceState.COMMIT_PENDING,
            DeviceState.CLEANUP_FAILED.value: DeviceState.CLEANUP_PENDING,
            DeviceState.ACTIVATION_FAILED.value: DeviceState.ACTIVATION_PENDING,
            DeviceState.ACTIVATION_UNCERTAIN.value: DeviceState.ACTIVATION_PENDING,
        }
        if state == DeviceState.POST_CHECK_FAILED.value:
            return self._retry_post_checks(job_id, hostname)
        if state == DeviceState.EXECUTION_FAILED.value:
            persisted = self.store.get_device(job_id, hostname)
            retry_state = persisted["metadata"].get(
                "retry_state", DeviceState.VALIDATING.value
            )
            try:
                next_state = DeviceState(retry_state)
            except ValueError:
                next_state = DeviceState.VALIDATING
        else:
            next_state = retry_map.get(state)
        if next_state is None:
            return False
        if state in {
            DeviceState.ACTIVATION_FAILED.value,
            DeviceState.ACTIVATION_UNCERTAIN.value,
        } and not retry_activation:
            return False
        self._transition(
            job_id,
            hostname,
            next_state,
            f"Retrying from {state}",
            level="WARNING",
        )
        return True

    def _retry_post_checks(self, job_id: str, hostname: str) -> bool:
        """Re-run the post-upgrade health gate, at most a bounded number of times.

        Each attempt costs a stabilisation wait and two full Genie captures, so
        it is capped, and every attempt writes to its own snapshot phase so the
        evidence from the original failure survives.
        """
        device = self.store.get_device(job_id, hostname)
        attempts = int(device["metadata"].get("post_check_attempts", 0) or 0)
        maximum = max(
            0,
            int(
                self.config.get("post_checks", {}).get(
                    "max_recheck_attempts", 1
                )
            ),
        )
        if attempts >= maximum:
            self.store.record_event(
                job_id,
                (
                    f"Post-upgrade checks already re-ran {attempts} time(s) "
                    f"(limit {maximum}); leaving {hostname} for an operator"
                ),
                hostname=hostname,
                level="WARNING",
                state=DeviceState.POST_CHECK_FAILED.value,
            )
            return False
        self._transition(
            job_id,
            hostname,
            DeviceState.POST_CHECKING,
            (
                "Re-running post-upgrade checks "
                f"(attempt {attempts + 1} of {maximum})"
            ),
            metadata={"post_check_attempts": attempts + 1},
            level="WARNING",
        )
        return True

    def _transition(
        self,
        job_id: str,
        hostname: str,
        state: DeviceState,
        message: str,
        *,
        metadata: Optional[Dict[str, Any]] = None,
        level: str = "INFO",
        increment_attempt: bool = False,
    ) -> None:
        self._assert_execution_lease(job_id, hostname)
        owns_device_lease = (
            getattr(self._execution_context, "job_id", None) == job_id
            and getattr(self._execution_context, "hostname", None) == hostname
        )
        self.store.transition(
            job_id,
            hostname,
            state,
            message,
            metadata=metadata,
            level=level,
            increment_attempt=increment_attempt,
            owner_token=(
                self.execution_owner if owns_device_lease else None
            ),
            require_job_lock=(
                owns_device_lease and self._job_execution_active
            ),
        )
        if (
            self.inventory_manager
            and self.config.get("state", {}).get("update_excel", True)
        ):
            try:
                self.inventory_manager.update_device_status(
                    hostname,
                    state.value,
                    message if level in {"ERROR", "WARNING"} else None,
                )
            except (OSError, PermissionError, ValueError, KeyError) as error:
                self.store.record_event(
                    job_id,
                    f"Could not mirror status to Excel: {error}",
                    hostname=hostname,
                    level="WARNING",
                    state=state.value,
                )

    def _job_batches(self, job_id: str) -> List[List[str]]:
        grouped: Dict[int, List[str]] = {}
        for device in self.store.get_devices(job_id):
            if not device["batch_number"] or int(device["batch_number"]) < 1:
                raise UpgradeOperationError(
                    f"Job {job_id} has no complete persisted batch plan"
                )
            grouped.setdefault(int(device["batch_number"]), []).append(
                device["hostname"]
            )
        return [grouped[number] for number in sorted(grouped)]

    def _needs_run(
        self,
        job_id: str,
        hostname: str,
        run_mode: str,
        *,
        retry_failed: bool,
    ) -> bool:
        state = self.store.get_device(job_id, hostname)["state"]
        if state in FINAL_STATES:
            return False
        if run_mode == "stage" and state == DeviceState.STAGED.value:
            return False
        if state in FAILURE_STATES and not retry_failed:
            return False
        return True

    def _must_halt_after_batch(
        self,
        job_id: str,
        hostnames: Sequence[str],
        *,
        run_mode: str,
        is_canary: bool,
    ) -> bool:
        states = [
            self.store.get_device(job_id, hostname)["state"]
            for hostname in hostnames
        ]
        success_states = (
            {DeviceState.STAGED.value, DeviceState.COMPLETE.value}
            if run_mode == "stage"
            else {
                DeviceState.CLEANUP_PENDING.value,
                DeviceState.COMPLETE.value,
            }
        )
        unfinished = [
            state
            for state in states
            if state not in success_states and state not in FAILURE_STATES
        ]
        if unfinished:
            return True
        failed = sum(1 for state in states if state in FAILURE_STATES)
        if failed == 0:
            return False
        if is_canary and self.config.get("workflow", {}).get(
            "halt_on_canary_failure", True
        ):
            return True
        workflow = self.config.get("workflow", {})
        max_failures = int(workflow.get("max_batch_failures", 0))
        if failed > max_failures:
            return True
        maximum_percentage = float(
            workflow.get("max_batch_failure_percentage", 0)
        )
        return (failed / len(hostnames) * 100) > maximum_percentage

    def _set_final_job_status(
        self,
        job_id: str,
        run_mode: str,
        summary: Dict[str, Any],
    ) -> None:
        current = self.store.get_job(job_id)["status"]
        if current == "halted":
            return
        if summary["failed"]:
            self._set_job_status(job_id, "failed")
        elif summary["complete"] == summary["total"]:
            self._set_job_status(job_id, "completed")
            self.store.release_job_locks(job_id)
        elif run_mode == "stage" and all(
            device["state"]
            in {DeviceState.STAGED.value, DeviceState.COMPLETE.value}
            for device in self.store.get_devices(job_id)
        ):
            self._set_job_status(job_id, "staged")
        elif summary["cleanup_pending"]:
            self._set_job_status(job_id, "awaiting_cleanup")
        else:
            self._set_job_status(job_id, "paused")

    def _wait_until_cleanup_eligible(
        self,
        job_id: str,
        hostnames: Sequence[str],
    ) -> None:
        eligible_times = []
        for hostname in hostnames:
            value = self.store.get_device(job_id, hostname)["metadata"].get(
                "cleanup_eligible_at"
            )
            if value:
                eligible_times.append(datetime.fromisoformat(value))
        if eligible_times:
            remaining = (
                max(eligible_times) - datetime.now(timezone.utc)
            ).total_seconds()
            if remaining > 0:
                self.sleeper(remaining)

    @staticmethod
    def _operation_error(result: Any, fallback: str) -> str:
        if isinstance(result, dict):
            if result.get("error"):
                return str(result["error"])
            errors = result.get("errors")
            if errors:
                return "; ".join(
                    f"{command}: {message}"
                    for command, message in errors.items()
                )
            output = result.get("output")
            if output:
                return f"{fallback}: {str(output)[-500:]}"
        return fallback
