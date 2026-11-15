"""Canary and topology-aware batch planning."""

from __future__ import annotations

from typing import Any, Dict, List, Sequence


def build_upgrade_batches(
    devices: Sequence[Dict[str, Any]],
    *,
    batch_size: int,
    canary_count: int = 1,
) -> List[List[str]]:
    """Build bounded batches without co-scheduling a redundancy group."""
    if batch_size < 1:
        raise ValueError("batch_size must be at least 1")
    if canary_count < 0:
        raise ValueError("canary_count cannot be negative")
    if not devices:
        return []

    remaining = list(devices)
    batches: List[List[str]] = []
    next_size = min(canary_count, batch_size) if canary_count else batch_size

    while remaining:
        target_size = max(1, min(next_size, len(remaining)))
        batch: List[str] = []
        batch_groups = set()
        selected_indexes = []

        for index, device in enumerate(remaining):
            group = _group_key(device)
            if group in batch_groups:
                continue
            batch.append(str(device["hostname"]))
            batch_groups.add(group)
            selected_indexes.append(index)
            if len(batch) >= target_size:
                break

        if not batch:
            raise ValueError("Could not construct a topology-safe upgrade batch")

        for index in reversed(selected_indexes):
            remaining.pop(index)
        batches.append(batch)
        next_size = batch_size

    return batches


def validate_batches(
    batches: Sequence[Sequence[str]],
    devices: Sequence[Dict[str, Any]],
) -> None:
    """Validate coverage, uniqueness, and redundancy separation."""
    by_hostname = {str(device["hostname"]): device for device in devices}
    planned = [hostname for batch in batches for hostname in batch]
    if len(planned) != len(set(planned)):
        raise ValueError("A device appears in more than one batch")
    if set(planned) != set(by_hostname):
        raise ValueError("Batch plan does not cover the inventory exactly once")

    for batch_number, batch in enumerate(batches, start=1):
        groups = [_group_key(by_hostname[hostname]) for hostname in batch]
        if len(groups) != len(set(groups)):
            raise ValueError(
                f"Batch {batch_number} contains the same redundancy group twice"
            )


def _group_key(device: Dict[str, Any]) -> str:
    group = str(device.get("redundancy_group") or "").strip().lower()
    return f"group:{group}" if group else f"device:{device['hostname']}"
