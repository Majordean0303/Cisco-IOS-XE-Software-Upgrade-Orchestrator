# Advanced Features

## Durable jobs and idempotency

Every job and device transition is stored in SQLite. Before sending a
disruptive command, the engine writes intent such as `activating`, `committing`,
or `rolling_back`. After a restart it reconciles `show version` and
`show install summary` rather than assuming the last command succeeded.

The device lock table prevents two jobs from targeting one canonical management
address, even when different hostname aliases are used. Locks remain while a
job is staged, waiting for cleanup, or requires operator attention. A renewable
execution lease also prevents two processes from advancing the same job; losing
that lease fences subsequent state changes and device commands.

The complete non-secret configuration is bound to the job when it is created.
Resume, cleanup, and rollback require the same configuration so a missing or
changed health gate cannot silently weaken an in-progress job.

## Canary and topology-aware batches

```yaml
workflow:
  max_parallel_devices: 5
  batch_size: 5
  canary_count: 1
  halt_on_canary_failure: true
  max_batch_failures: 0
  max_batch_failure_percentage: 0
  execution_lock_lease_seconds: 120
```

`redundancy_group` is an inventory safety constraint. The planner greedily
fills batches while preventing duplicate groups. It does not infer redundancy
from hostnames.

## Genie comparison policy

Core learns:

```yaml
comparison:
  core_features: [interface, platform, vlan, stp, lag, lldp]
  optional_features: [routing, ospf, bgp, arp]
  required_features: [interface, platform]
  critical_features: [interface, platform, vlan, stp, lag, lldp]
```

Optional protocol features are useful only where those protocols are deployed.
For a routed core, promote `routing`, `ospf`, or `bgp` into
`critical_features`. For a large route table, consider removing `routing` and
relying on protocol adjacency and purpose-built route checks to avoid an
expensive full-table learn.

The comparison removes byte/packet counters, rates, uptime, ages, and protocol
timers. It retains operational state and topology. Raw snapshots and diff
reports are JSON files under `state/snapshots`.

## Credentials

Resolution order:

1. System keyring
2. Environment or `.env`
3. Values substituted into `config/config.yaml`

Store global or per-device credentials interactively:

```bash
python orchestrator.py setup-credentials
```

For per-device credentials, set:

```yaml
credentials:
  per_device_credentials: true
```

Then enter the exact inventory hostname when prompted. The keyring service uses
`cisco-ios-xe-orchestrator-devices:<hostname>`.

Credentials are kept in memory only and secret configuration fields are
redacted before a job is persisted.

## Jump host

```yaml
connection:
  use_jump_host: true
  jump_host:
    hostname: bastion.example.net
    port: 22
    username: ${JUMP_HOST_USERNAME}
    password: ${JUMP_HOST_PASSWORD}
```

The orchestrator creates one local SSH tunnel per target. Nornir/Netmiko and
pyATS/Genie both connect through that endpoint, so baseline and upgrade traffic
follow the same path. One jump host and password authentication are currently
supported.

## Maintenance windows

Use timezone-aware ISO-8601 values:

```text
maintenance_window_start = 2026-08-01T22:00:00+01:00
maintenance_window_end   = 2026-08-02T02:00:00+01:00
```

Staging can run before the window. Activation remains
`activation_pending` outside it and can be resumed during the window.

## Commit, rollback, and cleanup

The command sequence is intentionally split:

```text
install add file flash:<image>
install activate auto-abort-timer <upgrade.auto_abort_timer_minutes>
<reload, reconnect, health comparison>
install commit
<cleanup soak>
install remove inactive
```

Before commit, `install abort` can return an activated/uncommitted device to the
recorded previous version. After commit, the tool requires a separately planned
software change rather than claiming a no-risk rollback.

### The commit deadline

`install activate` starts a rollback timer. If `install commit` has not run when
it expires, IOS-XE reloads the device back onto the previously committed image.
The orchestrator sets that timer explicitly, records the resulting deadline as
`auto_abort_expires_at`, and repeats it in the `activating` and `commit_pending`
messages so `status` shows how long is left.

`install commit` re-reads `show install summary` when the deadline has passed
rather than sending the command blind:

| Observed state | Result |
|---|---|
| Target committed | Treated as committed, moves to `cleanup_pending` |
| Target activated, uncommitted | Commits, with a warning in the audit log |
| Device back on the previous image | `rolled_back`, with the deadline in the message |
| Anything else | `commit_failed`, with the observed facts |

Size `upgrade.auto_abort_timer_minutes` to cover activation, reload,
stabilisation, the Genie comparison, and enough headroom for an operator to read
a failed health gate. Set `upgrade.hold_auto_abort_timer_on_post_check_failure`
to stop the timer instead of letting a failed device self-revert - that trades an
unplanned reload for an indefinite uncommitted state, so it is off by default.

### Retrying a failed health gate

A failed Genie comparison leaves the device in `post_check_failed`.
`--retry-failed` re-runs the gate at most `post_checks.max_recheck_attempts`
times, and each attempt writes to its own snapshot phase (`after-retry-1`,
`after-retry-2`, ...) so the snapshots behind the first failure survive.

`rollback_required` is never retried automatically: it means the device is not
where it should be and an operator has to choose between accepting the image and
rolling it back.

## Per-device status

Use:

```bash
python orchestrator.py status --job-id <job-id> --events
```

The table shows batch, device, durable state, last message, and update time.
Excel mirrors this state when `state.update_excel` is enabled, but an open or
locked workbook cannot stop SQLite checkpointing.
