"""
Transport and Execution Module

Nornir tasks for device connection, command execution, and file transfers.
Handles SSH connections via Netmiko and SCP file transfers.
"""

from typing import Dict, Optional, Any
from nornir.core.task import Task, Result
from nornir_netmiko import netmiko_send_command, netmiko_send_config
from netmiko import file_transfer
import re
import time
from pathlib import Path
from rich.console import Console

from src.device.iosxe import parse_flash_space

console = Console()

UPGRADE_FACT_COMMANDS = (
    "show version",
    "show boot",
    "show switch",
    "show install summary",
    "dir flash:",
)

COMMAND_ERROR_PATTERNS = (
    r"%\s*(?:invalid input|error|failed|incomplete command|ambiguous command)",
    r"\bFAILED:\s*install_",
    r"\bError:\s",
    r"\boperation cancelled\b",
)

INSTALL_OPERATION_NAMES = {
    "install add": "install_add",
    "install activate": "install_activate",
    "install commit": "install_commit",
    "install remove inactive": "install_remove",
    "install abort": "install_abort",
    "install rollback to committed": "install_rollback",
    "install auto-abort-timer stop": "install_auto_abort_timer_stop",
}

# `install activate` starts a rollback timer. If `install commit` does not run
# before it expires the device reloads back onto the previously committed
# image. IOS-XE accepts `install activate auto-abort-timer <30-1200>` (minutes);
# the device default is 120 minutes.
AUTO_ABORT_TIMER_MINIMUM_MINUTES = 30
AUTO_ABORT_TIMER_MAXIMUM_MINUTES = 1200
AUTO_ABORT_TIMER_DEFAULT_MINUTES = 120

# When the IOS-XE SSH server refuses to open the exec channel that SCP needs,
# paramiko raises ChannelException(1, "Administratively prohibited"). The usual
# cause is that the SCP server is not enabled on the switch, or the login account
# is not privilege 15. Netmiko's file_transfer does not enable it for us.
SCP_CHANNEL_REFUSED_HINT = (
    "The switch refused the SCP channel (SSH reason 'Administratively "
    "prohibited'). Configure 'ip scp server enable' on the device and make sure "
    "the login account is privilege level 15, then retry staging."
)


def _describe_transfer_error(error: Exception) -> str:
    """Return an actionable message for common SCP transfer failures."""
    text = str(error).strip()
    lowered = text.lower()
    if (
        "administratively prohibited" in lowered
        or type(error).__name__ == "ChannelException"
    ):
        detail = text or type(error).__name__
        return f"{SCP_CHANNEL_REFUSED_HINT} (underlying error: {detail})"
    return text or type(error).__name__


def test_connectivity(task: Task) -> Result:
    """
    Test SSH connectivity to device.
    
    Args:
        task: Nornir task object
        
    Returns:
        Result with connectivity status
    """
    try:
        # Try to get hostname
        result = task.run(
            task=netmiko_send_command,
            command_string="show version | include uptime",
            use_textfsm=False
        )
        
        return Result(
            host=task.host,
            result={
                'reachable': True,
                'output': result.result
            }
        )
    except Exception as e:
        return Result(
            host=task.host,
            failed=True,
            result={
                'reachable': False,
                'error': str(e)
            }
        )


def execute_show_command(task: Task, command: str) -> Result:
    """
    Execute a show command on the device.
    
    Args:
        task: Nornir task object
        command: Show command to execute
        
    Returns:
        Result with command output
    """
    try:
        result = task.run(
            task=netmiko_send_command,
            command_string=command,
            use_textfsm=False
        )
        
        return Result(
            host=task.host,
            result=result.result
        )
    except Exception as e:
        return Result(
            host=task.host,
            failed=True,
            result=f"Command execution failed: {str(e)}"
        )


def execute_show_commands(task: Task, commands: list) -> Result:
    """
    Execute multiple show commands on the device.
    
    Args:
        task: Nornir task object
        commands: List of show commands to execute
        
    Returns:
        Result with dictionary of command outputs
    """
    outputs = {}
    errors = {}
    
    for command in commands:
        try:
            result = task.run(
                task=netmiko_send_command,
                command_string=command,
                use_textfsm=False
            )
            outputs[command] = result.result
        except Exception as e:
            outputs[command] = f"ERROR: {str(e)}"
            errors[command] = str(e)
    
    return Result(
        host=task.host,
        failed=bool(errors),
        result={
            'outputs': outputs,
            'errors': errors,
        }
    )


def collect_upgrade_facts(task: Task) -> Result:
    """Collect the command outputs required for install-mode safety gates."""
    outputs: Dict[str, str] = {}
    errors: Dict[str, str] = {}
    required = {
        "show version",
        "show boot",
        "show install summary",
        "dir flash:",
    }
    try:
        connection = task.host.get_connection("netmiko", task.nornir.config)
    except Exception as error:
        return Result(
            host=task.host,
            failed=True,
            result={"outputs": outputs, "errors": {"connection": str(error)}},
        )

    for command in UPGRADE_FACT_COMMANDS:
        try:
            outputs[command] = connection.send_command(
                command,
                read_timeout=120,
                use_textfsm=False,
            )
        except Exception as error:
            errors[command] = str(error)
            outputs[command] = ""

    return Result(
        host=task.host,
        failed=bool(required.intersection(errors)),
        result={"outputs": outputs, "errors": errors},
    )


def backup_configuration(task: Task, backup_dir: str) -> Result:
    """
    Backup device configuration files.
    
    Args:
        task: Nornir task object
        backup_dir: Directory to save backups
        
    Returns:
        Result with backup file paths
    """
    from datetime import datetime
    import os
    
    backup_files = {}
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    hostname = task.host.name
    
    # Create backup directory if it doesn't exist
    backup_path = Path(backup_dir) / hostname
    backup_path.mkdir(parents=True, exist_ok=True)
    
    # Commands to backup
    backup_commands = {
        'running-config': 'show running-config',
        'startup-config': 'show startup-config',
        'version': 'show version',
        'inventory': 'show inventory',
        'boot': 'show boot',
    }
    
    try:
        for name, command in backup_commands.items():
            result = task.run(
                task=netmiko_send_command,
                command_string=command,
                use_textfsm=False
            )
            
            filename = f"{hostname}_{name}_{timestamp}.txt"
            filepath = backup_path / filename
            
            with open(filepath, 'w') as f:
                f.write(result.result)
            
            backup_files[name] = str(filepath)
        
        return Result(
            host=task.host,
            result={
                'success': True,
                'backup_files': backup_files,
                'backup_directory': str(backup_path)
            }
        )
    
    except Exception as e:
        return Result(
            host=task.host,
            failed=True,
            result={
                'success': False,
                'error': str(e)
            }
        )


def check_flash_space(task: Task, flash_location: str = "flash:") -> Result:
    """
    Check available flash space on device.
    
    Args:
        task: Nornir task object
        flash_location: Flash filesystem location
        
    Returns:
        Result with flash space information
    """
    try:
        result = task.run(
            task=netmiko_send_command,
            command_string=f"dir {flash_location}",
            use_textfsm=False
        )
        
        output = result.result
        total_bytes, free_bytes = parse_flash_space(output)
        space_info = {
            'total_bytes': total_bytes,
            'free_bytes': free_bytes,
            'free_mb': free_bytes / (1024 * 1024),
            'free_gb': free_bytes / (1024 * 1024 * 1024),
        }
        if not free_bytes:
            return Result(
                host=task.host,
                failed=True,
                result={
                    "error": "Could not parse free space from dir output",
                    "output": output,
                },
            )
        
        return Result(
            host=task.host,
            result=space_info
        )
    
    except Exception as e:
        return Result(
            host=task.host,
            failed=True,
            result={'error': str(e)}
        )


def transfer_image_scp(
    task: Task,
    source_file: str,
    dest_file: str = None,
    file_system: str = "flash:",
) -> Result:
    """
    Transfer IOS-XE image to device using SCP.
    
    Args:
        task: Nornir task object
        source_file: Local path to image file
        dest_file: Destination filename (optional)
        file_system: Destination file system
        
    Returns:
        Result with transfer status
    """
    try:
        # Get NetMiko connection from Nornir
        net_connect = task.host.get_connection("netmiko", task.nornir.config)
        
        if not dest_file:
            dest_file = Path(source_file).name
        
        # Prepare transfer dict
        transfer_dict = file_transfer(
            net_connect,
            source_file=source_file,
            dest_file=dest_file,
            file_system=file_system,
            direction='put',
            overwrite_file=False,
        )
        
        return Result(
            host=task.host,
            result={
                'success': True,
                'file_transferred': transfer_dict.get('file_transferred', False),
                'file_exists': transfer_dict.get('file_exists', False),
                'dest_file': dest_file,
                'file_system': file_system
            }
        )
    
    except Exception as e:
        return Result(
            host=task.host,
            failed=True,
            result={
                'success': False,
                'error': _describe_transfer_error(e)
            }
        )


def verify_image_hash(
    task: Task,
    image_name: str,
    expected_md5: str,
    file_system: str = "flash:",
    read_timeout: int = 1800,
) -> Result:
    """
    Verify MD5 hash of transferred image.

    Args:
        task: Nornir task object
        image_name: Image filename on device
        expected_md5: Expected MD5 hash
        file_system: File system location
        read_timeout: Seconds to wait for the device prompt to return after
            issuing ``verify /md5``. Catalyst switches can take many minutes to
            hash a multi-hundred-megabyte image, so the default (1800s) is much
            larger than Netmiko's built-in 100s. ``delay_factor`` no longer
            extends ``read_timeout`` in modern Netmiko, which caused staging to
            fail with ``ReadTimeout: Pattern not detected`` immediately after a
            successful SCP transfer.

    Returns:
        Result with hash verification status
    """
    try:
        command = f"verify /md5 {file_system}{image_name}"

        # Use the underlying Netmiko connection directly so we can pass an
        # explicit ``read_timeout``. ``nornir_netmiko.netmiko_send_command``
        # forwards kwargs to ``send_command``, but going through the connection
        # matches the pattern used by ``collect_upgrade_facts`` and keeps the
        # timeout behaviour obvious.
        connection = task.host.get_connection("netmiko", task.nornir.config)
        output = connection.send_command(
            command,
            read_timeout=read_timeout,
            use_textfsm=False,
        )
        
        # IOS-XE commonly prints either "... = <hash>" or "verify ... (<hash>)".
        hash_match = re.search(r"\b([a-fA-F0-9]{32})\b", output)
        device_md5 = hash_match.group(1).lower() if hash_match else None
        
        expected_md5_lower = expected_md5.lower().strip()
        match = device_md5 == expected_md5_lower if device_md5 else False
        
        return Result(
            host=task.host,
            failed=not match,
            result={
                'hash_match': match,
                'expected_md5': expected_md5_lower,
                'device_md5': device_md5,
                'output': output
            }
        )
    
    except Exception as e:
        return Result(
            host=task.host,
            failed=True,
            result={
                'hash_match': False,
                'error': str(e)
            }
        )


def save_running_configuration(task: Task) -> Result:
    """Persist the running configuration before a disruptive operation."""
    try:
        connection = task.host.get_connection("netmiko", task.nornir.config)
        output = connection.save_config()
        return Result(host=task.host, result={"success": True, "output": output})
    except Exception as error:
        return Result(
            host=task.host,
            failed=True,
            result={"success": False, "error": str(error)},
        )


def execute_install_add(
    task: Task,
    image_path: str,
    max_wait: int = 1800,
) -> Result:
    """Add and expand an IOS-XE image without activating it."""
    return _run_install_command(
        task,
        command=f"install add file {image_path}",
        max_wait=max_wait,
        expect_reload=False,
    )


def normalize_auto_abort_timer(minutes: Any) -> Optional[int]:
    """Clamp a configured auto-abort timer to the range IOS-XE accepts.

    Returns ``None`` when no timer was requested, which leaves the device on
    its own default of 120 minutes.
    """
    if minutes is None:
        return None
    try:
        value = int(minutes)
    except (TypeError, ValueError):
        return None
    if value <= 0:
        return None
    return max(
        AUTO_ABORT_TIMER_MINIMUM_MINUTES,
        min(AUTO_ABORT_TIMER_MAXIMUM_MINUTES, value),
    )


def execute_install_activate(
    task: Task,
    max_wait: int = 1800,
    auto_abort_timer_minutes: Optional[int] = None,
) -> Result:
    """Activate the added package and confirm the expected reload.

    When ``auto_abort_timer_minutes`` is supplied the rollback window is set
    explicitly, so the orchestrator knows exactly how long it has to reach
    `install commit` before the device reverts on its own.
    """
    command = "install activate"
    timer = normalize_auto_abort_timer(auto_abort_timer_minutes)
    if timer is not None:
        command = f"{command} auto-abort-timer {timer}"
    result = _run_install_command(
        task,
        command=command,
        max_wait=max_wait,
        expect_reload=True,
    )
    if isinstance(result.result, dict):
        result.result["auto_abort_timer_minutes"] = timer
    return result


def execute_install_auto_abort_timer_stop(
    task: Task,
    max_wait: int = 300,
) -> Result:
    """Stop the rollback timer, holding the device on the uncommitted image.

    IOS-XE does not print a documented ``SUCCESS: <operation>`` banner for this
    command, so it is accepted once the device returns to the prompt without
    reporting an error.
    """
    return _run_install_command(
        task,
        command="install auto-abort-timer stop",
        max_wait=max_wait,
        expect_reload=False,
        success_on_prompt_return=True,
    )


def execute_install_commit(task: Task, max_wait: int = 600) -> Result:
    """Commit the currently activated package without reloading."""
    return _run_install_command(
        task,
        command="install commit",
        max_wait=max_wait,
        expect_reload=False,
    )


def execute_install_cleanup(task: Task, max_wait: int = 1800) -> Result:
    """Remove inactive packages after the configured soak period."""
    return _run_install_command(
        task,
        command="install remove inactive",
        max_wait=max_wait,
        expect_reload=False,
    )


def execute_install_abort(task: Task, max_wait: int = 600) -> Result:
    """Abort an activated, uncommitted install and reload the prior image."""
    return _run_install_command(
        task,
        command="install abort",
        max_wait=max_wait,
        expect_reload=True,
    )


def execute_install_rollback(task: Task, max_wait: int = 600) -> Result:
    """Rollback to the last committed package and reload."""
    return _run_install_command(
        task,
        command="install rollback to committed",
        max_wait=max_wait,
        expect_reload=True,
    )


def _run_install_command(
    task: Task,
    *,
    command: str,
    max_wait: int,
    expect_reload: bool,
    success_on_prompt_return: bool = False,
) -> Result:
    """Execute an install command and answer only known IOS-XE prompts."""
    output = ""
    reload_confirmation_sent = False
    operation = _install_operation_name(command)
    deadline = time.monotonic() + max_wait
    # Only text that has not been examined yet is scanned for errors. Rescanning
    # the whole transcript on every iteration means one stray line keeps
    # re-triggering, and once the reload has been confirmed the device may print
    # anything at all on its way down.
    scanned_to = 0
    try:
        connection = task.host.get_connection("netmiko", task.nornir.config)
        _ensure_enable_mode(connection)
        output = connection.send_command_timing(
            command,
            strip_prompt=False,
            strip_command=False,
            read_timeout=max_wait,
            last_read=10,
        )

        prompt_responses = 0
        while True:
            if _contains_command_error(_unscanned(output, scanned_to)):
                if reload_confirmation_sent:
                    # The reload was already accepted, so the command cannot be
                    # failed outright. Hand it to the caller for reconciliation
                    # against the version the device actually comes back on.
                    return _install_result(
                        task,
                        command,
                        output,
                        failed=True,
                        reload_initiated=True,
                        error=(
                            "IOS-XE reported an error after the reload was "
                            "confirmed; device state must be reconciled"
                        ),
                    )
                return _install_result(
                    task,
                    command,
                    output,
                    failed=True,
                    reload_initiated=False,
                    error="IOS-XE reported an install command failure",
                )
            scanned_to = len(output)
            if _contains_install_success(output, operation):
                return _install_result(
                    task,
                    command,
                    output,
                    failed=False,
                    reload_initiated=(
                        expect_reload and reload_confirmation_sent
                    ),
                )
            prompt = _prompt_response(output)
            if prompt is not None:
                if time.monotonic() >= deadline:
                    break
                prompt_responses += 1
                if prompt_responses > 20:
                    return _install_result(
                        task,
                        command,
                        output,
                        failed=True,
                        reload_initiated=False,
                        error="Install command exceeded the prompt-response limit",
                    )
                response, confirms_reload = prompt
                reload_confirmation_sent = (
                    reload_confirmation_sent or confirms_reload
                )
                try:
                    output += connection.send_command_timing(
                        response,
                        strip_prompt=False,
                        strip_command=False,
                        read_timeout=max(
                            1,
                            int(deadline - time.monotonic()),
                        ),
                        last_read=10,
                    )
                except Exception as error:
                    if expect_reload and reload_confirmation_sent:
                        return _install_result(
                            task,
                            command,
                            output,
                            failed=False,
                            reload_initiated=True,
                            disconnect=str(error),
                        )
                    raise
                continue

            if time.monotonic() >= deadline:
                break
            if (
                expect_reload
                and reload_confirmation_sent
                and hasattr(connection, "is_alive")
                and not connection.is_alive()
            ):
                return _install_result(
                    task,
                    command,
                    output,
                    failed=False,
                    reload_initiated=True,
                )
            if not hasattr(connection, "read_channel"):
                break
            try:
                chunk = connection.read_channel()
            except Exception as error:
                if expect_reload and reload_confirmation_sent:
                    return _install_result(
                        task,
                        command,
                        output,
                        failed=False,
                        reload_initiated=True,
                        disconnect=str(error),
                    )
                raise
            if chunk:
                output += chunk
                continue
            if _at_device_prompt(output):
                if success_on_prompt_return:
                    return _install_result(
                        task,
                        command,
                        output,
                        failed=False,
                        reload_initiated=False,
                    )
                break
            time.sleep(1)

        return _install_result(
            task,
            command,
            output,
            failed=True,
            reload_initiated=False,
            error=(
                f"Timed out or returned to the prompt without an explicit "
                f"SUCCESS: {operation} result"
            )
        )
    except Exception as error:
        if expect_reload and reload_confirmation_sent:
            return _install_result(
                task,
                command,
                output,
                failed=False,
                reload_initiated=True,
                disconnect=str(error),
            )
        return _install_result(
            task,
            command,
            output,
            failed=True,
            reload_initiated=False,
            error=str(error),
        )


def _prompt_response(output: str):
    tail = output[-1000:]
    if re.search(
        r"(?:requires? a reload|reload of the system).*?\[y/n\]\s*$",
        tail,
        flags=re.IGNORECASE | re.DOTALL,
    ):
        return "y", True
    if re.search(
        r"(?:\[(?:y/n/q|y/n)\]|do you want to remove the above files\?.*?[?:])\s*$",
        tail,
        flags=re.IGNORECASE | re.DOTALL,
    ):
        return "y", False
    return None


def _unscanned(output: str, scanned_to: int) -> str:
    """Return the text not yet scanned, with a small overlap.

    The overlap keeps a pattern that straddles two reads from being missed
    without re-reporting text that has already been judged.
    """
    start = max(0, min(scanned_to, len(output)) - 200)
    return output[start:]


def _contains_command_error(output: str) -> bool:
    return any(
        re.search(pattern, output, flags=re.IGNORECASE)
        for pattern in COMMAND_ERROR_PATTERNS
    )


def _install_operation_name(command: str) -> str:
    normalized = " ".join(command.lower().split())
    for prefix, operation in sorted(
        INSTALL_OPERATION_NAMES.items(),
        key=lambda item: len(item[0]),
        reverse=True,
    ):
        if normalized.startswith(prefix):
            return operation
    raise ValueError(f"Unsupported install operation: {command}")


def _contains_install_success(output: str, operation: str) -> bool:
    return bool(
        re.search(
            rf"\bSUCCESS:\s*{re.escape(operation)}\b",
            output,
            flags=re.IGNORECASE,
        )
    )


def _at_device_prompt(output: str) -> bool:
    return bool(
        re.search(
            r"(?:^|\n)[^\r\n]+[>#]\s*$",
            output[-500:],
        )
    )


def _ensure_enable_mode(connection) -> None:
    if not hasattr(connection, "check_enable_mode"):
        return
    if connection.check_enable_mode():
        return
    connection.enable()
    if not connection.check_enable_mode():
        raise RuntimeError("Could not enter privileged EXEC mode")


def _install_result(
    task: Task,
    command: str,
    output: str,
    *,
    failed: bool,
    reload_initiated: bool,
    error: Optional[str] = None,
    disconnect: Optional[str] = None,
) -> Result:
    result = {
        "success": not failed,
        "command": command,
        "reload_initiated": reload_initiated,
        "output": output,
    }
    if error:
        result["error"] = error
    if disconnect:
        result["disconnect"] = disconnect
    return Result(host=task.host, failed=failed, result=result)


def execute_install_command(
    task: Task,
    command: str,
    delay_factor: int = 2,
    max_wait: int = 600
) -> Result:
    """
    Execute install mode command.
    
    Args:
        task: Nornir task object
        command: Install command to execute
        delay_factor: Delay factor for command execution
        max_wait: Maximum time to wait for command completion
        
    Returns:
        Result with command output
    """
    del delay_factor
    normalized = " ".join(command.strip().lower().split())
    return _run_install_command(
        task,
        command=command,
        max_wait=max_wait,
        expect_reload=(
            normalized.startswith("install activate")
            or normalized in {"install abort", "install rollback to committed"}
        ),
        success_on_prompt_return=normalized.startswith(
            "install auto-abort-timer"
        ),
    )


def get_install_summary(task: Task) -> Result:
    """
    Get install mode summary.
    
    Args:
        task: Nornir task object
        
    Returns:
        Result with install summary
    """
    try:
        result = task.run(
            task=netmiko_send_command,
            command_string="show install summary",
            use_textfsm=False
        )
        
        return Result(
            host=task.host,
            result=result.result
        )
    
    except Exception as e:
        return Result(
            host=task.host,
            failed=True,
            result=f"Failed to get install summary: {str(e)}"
        )


def reload_device(
    task: Task,
    save_config: bool = True,
    confirm: bool = True
) -> Result:
    """
    Reload the device.
    
    Args:
        task: Nornir task object
        save_config: Whether to saveconfig before reload
        confirm: Confirm reload
        
    Returns:
        Result with reload status
    """
    try:
        net_connect = task.host.get_connection("netmiko", task.nornir.config)
        
        if save_config:
            net_connect.save_config()
        
        if confirm:
            # Send reload command
            output = net_connect.send_command_timing(
                'reload',
                strip_prompt=False,
                strip_command=False
            )
            
            # Confirm reload
            if 'confirm' in output.lower():
                output += net_connect.send_command_timing(
                    '\n',
                    strip_prompt=False,
                    strip_command=False
                )
            
            return Result(
                host=task.host,
                result={
                    'success': True,
                    'output': output
                }
            )
    
    except Exception as e:
        return Result(
            host=task.host,
            failed=True,
            result={
                'success': False,
                'error': str(e)
            }
        )
