# Getting Started

## 1. Use Linux or WSL

The upgrade workflow requires pyATS/Genie, which is not supported on native
Windows. Windows users should clone the repository inside the WSL filesystem
and run all device-facing commands there.

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements-genie.txt
```

## 2. Configure the tool

```bash
cp config/config.example.yaml config/config.yaml
cp .env.example .env
```

Edit `config/config.yaml`. Start conservatively:

```yaml
workflow:
  max_parallel_devices: 2
  batch_size: 2
  canary_count: 1
  max_batch_failures: 0

cleanup:
  soak_seconds: 3600
```

Provide credentials with the system keyring:

```bash
python orchestrator.py setup-credentials
```

Or set:

```text
DEVICE_USERNAME=automation
DEVICE_PASSWORD=replace_me
DEVICE_ENABLE_PASSWORD=replace_me
```

## 3. Build inventory

```bash
python orchestrator.py create-template --output inventory/devices.xlsx
```

Populate the required fields. Use a local Linux/WSL image path, such as
`/srv/iosxe/cat9k_iosxe.17.12.04.SPA.bin`, or a previously copied
`flash:cat9k_iosxe.17.12.04.SPA.bin`.

Give switches in a redundant pair the same `redundancy_group`. They will be
placed in different batches.

## 4. Validate and plan

```bash
python orchestrator.py validate-inventory --file inventory/devices.xlsx
python orchestrator.py plan --file inventory/devices.xlsx
```

Record the displayed job ID. A plan can later be staged or upgraded directly:

```bash
python orchestrator.py stage --job-id <job-id>
python orchestrator.py upgrade --job-id <job-id>
```

## 5. Optional pre-staging

Run this before the maintenance window:

```bash
python orchestrator.py stage --file inventory/devices.xlsx
```

This copies the image, checks MD5 on the switch, and runs `install add`. It does
not activate or reload.

## 6. Upgrade

```bash
python orchestrator.py upgrade --file inventory/devices.xlsx
```

The first batch is the canary. Each device has its own live state. Typical
transitions are:

```text
validating -> backing_up -> baseline_capture -> staging -> staged
-> activating -> rebooting -> reconnecting -> stabilizing
-> commit_pending -> committing -> cleanup_pending -> complete
```

If the CLI exits while devices reload, restart it with:

```bash
python orchestrator.py resume --job-id <job-id>
```

The persisted `rebooting` state causes the tool to wait for SSH, verify the
target version, capture the post-upgrade Genie snapshot, compare health, and
continue to commit. It does not send `install activate` again.

## 7. Monitor

```bash
python orchestrator.py status --job-id <job-id>
python orchestrator.py status --job-id <job-id> --events
```

SQLite remains authoritative if Excel is open or cannot be updated.

## 8. Cleanup

After commit, devices remain `cleanup_pending` until the soak period ends:

```bash
python orchestrator.py cleanup --job-id <job-id>
```

To keep the command running until every selected device becomes eligible:

```bash
python orchestrator.py cleanup --job-id <job-id> --wait-for-soak
```

Cleanup runs `install remove inactive`. It is never allowed before successful
post-checks, commit, and soak.

## 9. Failure handling

- Canary or batch failure: inspect `status --events`; later batches remain
  untouched.
- Failed read-only/post checks: use `resume --retry-failed` after correcting the
  cause.
- Failed or uncertain activation: investigate device state first, then use both
  `--retry-failed --retry-activation` only when replay is known to be safe.
- Failed post-upgrade comparison while uncommitted: use
  `rollback --job-id <job-id> --device <hostname>`.
- Reconnect timeout: verify console/out-of-band state, then resume when SSH is
  available.
- `Administratively prohibited` during staging: the switch is refusing the SCP
  channel. Configure `ip scp server enable` and use a privilege-15 account.
- A failed job blocking later jobs on the same devices: run
  `cancel --job-id <job-id>` to release its reservations. It refuses past
  staging unless you pass `--force` after checking the device by hand.

Never delete `state/orchestrator.db` to clear an error. It contains the
checkpoint needed to make the next action safe.

Use the same `config/config.yaml` for the full lifetime of a job. The CLI fails
closed when the file is missing or its persisted safety policy has changed.
