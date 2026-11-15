"""
Utility Functions and Helpers

Common utility functions used across the orchestrator.
"""

import os
import re
import hashlib
from pathlib import Path
from typing import Optional, Dict, Any
from datetime import datetime
import yaml
from dotenv import load_dotenv


DEFAULT_CONFIG: Dict[str, Any] = {
    "logging": {
        "level": "INFO",
        "console_output": True,
        "file_output": True,
        "log_directory": "logs",
    },
    "nornir": {
        "runner": {
            "plugin": "threaded",
            "options": {"num_workers": 5},
        }
    },
    "connection": {
        "timeout": 120,
        "auth_timeout": 30,
        "banner_timeout": 30,
        "blocking_timeout": 30,
        "session_timeout": 300,
        "keepalive": 30,
        "use_jump_host": False,
    },
    "credentials": {
        "use_credential_manager": True,
        "per_device_credentials": False,
        "credman_service_devices": "cisco-ios-xe-orchestrator-devices",
        "credman_service_jumphost": "cisco-ios-xe-orchestrator-jumphost",
    },
    "state": {
        "database": "state/orchestrator.db",
        "snapshot_directory": "state/snapshots",
        "update_excel": True,
    },
    "workflow": {
        "max_parallel_devices": 5,
        "batch_size": 5,
        "canary_count": 1,
        "halt_on_canary_failure": True,
        "max_batch_failures": 0,
        "max_batch_failure_percentage": 0,
        "pause_between_batches": 0,
        "maintenance_window_minutes": 240,
        "execution_lock_lease_seconds": 120,
        "require_approval": True,
    },
    "safety": {
        "allow_stacks": False,
        "allow_downgrade": False,
        "supported_major_versions": [16, 17],
        "require_console_confirmation": False,
    },
    "pre_checks": {
        "minimum_free_space_mb": 2048,
        "image_space_buffer_multiplier": 1.5,
    },
    "backup": {
        "enabled": True,
        "directory": "backups",
    },
    "image_transfer": {
        "timeout": 3600,
        "install_add_timeout": 1800,
    },
    "upgrade": {
        "mode": "install",
        "save_configuration_before_reload": True,
        "activation_timeout": 1800,
        "commit_timeout": 600,
        "auto_abort_timer_minutes": 120,
        "hold_auto_abort_timer_on_post_check_failure": False,
        "auto_abort_timer_stop_timeout": 300,
    },
    "post_checks": {
        "stabilization_seconds": 120,
        "max_reload_wait": 1800,
        "reconnect_interval": 20,
        "max_recheck_attempts": 1,
    },
    "comparison": {
        "enabled": True,
        "core_features": [
            "interface",
            "platform",
            "vlan",
            "stp",
            "lag",
            "lldp",
        ],
        "optional_features": ["routing", "ospf", "bgp", "arp"],
        "required_features": ["interface", "platform"],
        "critical_features": [
            "interface",
            "platform",
            "vlan",
            "stp",
            "lag",
            "lldp",
        ],
    },
    "rollback": {
        "auto_rollback_on_failure": False,
        "command_timeout": 600,
    },
    "cleanup": {
        "auto_cleanup": True,
        "soak_seconds": 3600,
        "command_timeout": 1800,
    },
}


def load_config(
    config_file: str = "config/config.yaml",
    *,
    allow_missing: bool = False,
) -> Dict[str, Any]:
    """
    Load configuration from YAML file.
    
    Args:
        config_file: Path to config file
        
    Returns:
        Configuration dictionary
    """
    config_path = Path(config_file)
    
    if not config_path.exists() and not allow_missing:
        raise FileNotFoundError(f"Configuration file not found: {config_file}")

    # Load environment variables
    load_dotenv()

    file_config: Dict[str, Any] = {}
    if config_path.exists():
        with open(config_path, 'r', encoding='utf-8') as f:
            loaded = yaml.safe_load(f)
        if loaded is not None and not isinstance(loaded, dict):
            raise ValueError("Configuration root must be a mapping")
        file_config = loaded or {}

    config = _deep_merge(DEFAULT_CONFIG, file_config)
    # Substitute environment variables
    config = _substitute_env_vars(config)

    if config.get("upgrade", {}).get("mode") != "install":
        raise ValueError(
            "Only IOS-XE install mode is supported; upgrade.mode must be 'install'"
        )
    return config


def _deep_merge(
    defaults: Dict[str, Any],
    overrides: Dict[str, Any],
) -> Dict[str, Any]:
    merged: Dict[str, Any] = {}
    for key, value in defaults.items():
        if isinstance(value, dict):
            override = overrides.get(key, {})
            merged[key] = _deep_merge(
                value,
                override if isinstance(override, dict) else {},
            )
        else:
            merged[key] = value
    for key, value in overrides.items():
        if (
            key in merged
            and isinstance(merged[key], dict)
            and isinstance(value, dict)
        ):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def _substitute_env_vars(config: Any) -> Any:
    """
    Recursively substitute environment variables in config.
    
    Args:
        config: Configuration object
        
    Returns:
        Configuration with substituted values
    """
    if isinstance(config, dict):
        return {k: _substitute_env_vars(v) for k, v in config.items()}
    elif isinstance(config, list):
        return [_substitute_env_vars(item) for item in config]
    elif isinstance(config, str):
        # Replace ${VAR_NAME} with environment variable value
        pattern = r'\$\{([^}]+)\}'
        matches = re.findall(pattern, config)
        for var in matches:
            value = os.getenv(var, '')
            config = config.replace(f'${{{var}}}', value)
        return config
    else:
        return config


def calculate_file_hash(file_path: str, algorithm: str = 'md5') -> str:
    """
    Calculate hash of a file.
    
    Args:
        file_path: Path to file
        algorithm: Hash algorithm ('md5' or 'sha256')
        
    Returns:
        Hex digest of file hash
    """
    if algorithm == 'md5':
        hasher = hashlib.md5()
    elif algorithm == 'sha256':
        hasher = hashlib.sha256()
    else:
        raise ValueError(f"Unsupported hash algorithm: {algorithm}")
    
    with open(file_path, 'rb') as f:
        for chunk in iter(lambda: f.read(4096), b''):
            hasher.update(chunk)
    
    return hasher.hexdigest()


def get_file_size_mb(file_path: str) -> float:
    """
    Get file size in megabytes.
    
    Args:
        file_path: Path to file
        
    Returns:
        File size in MB
    """
    size_bytes = os.path.getsize(file_path)
    return size_bytes / (1024 * 1024)


def format_bytes(bytes_value: int) -> str:
    """
    Format bytes into human-readable string.
    
    Args:
        bytes_value: Size in bytes
        
    Returns:
        Formatted string (e.g., "2.5 GB")
    """
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_value < 1024.0:
            return f"{bytes_value:.2f} {unit}"
        bytes_value /= 1024.0
    return f"{bytes_value:.2f} PB"


def sanitize_hostname(hostname: str) -> str:
    """
    Sanitize hostname for use in filenames.
    
    Args:
        hostname: Device hostname
        
    Returns:
        Sanitized hostname
    """
    # Remove invalid filename characters
    return re.sub(r'[<>:"/\\|?*]', '_', hostname)


def create_timestamp() -> str:
    """
    Create formatted timestamp string.
    
    Returns:
        Timestamp in format YYYYMMDD_HHMMSS
    """
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def parse_version_string(version_str: str) -> Dict[str, Any]:
    """
    Parse IOS-XE version string.
    
    Args:
        version_str: Version string (e.g., "17.9.4a")
        
    Returns:
        Dictionary with parsed version components
    """
    # Pattern: major.minor.maintenance[letter]
    pattern = r'(\d+)\.(\d+)\.(\d+)([a-zA-Z]*)'
    match = re.search(pattern, version_str)
    
    if match:
        return {
            'major': int(match.group(1)),
            'minor': int(match.group(2)),
            'maintenance': int(match.group(3)),
            'rebuild': match.group(4) if match.group(4) else '',
            'full_version': version_str
        }
    else:
        return {
            'major': 0,
            'minor': 0,
            'maintenance': 0,
            'rebuild': '',
            'full_version': version_str
        }


def compare_versions(version1: str, version2: str) -> int:
    """
    Compare two version strings.
    
    Args:
        version1: First version string
        version2: Second version string
        
    Returns:
        -1 if version1 < version2, 0 if equal, 1 if version1 > version2
    """
    v1 = parse_version_string(version1)
    v2 = parse_version_string(version2)
    
    # Compare major.minor.maintenance
    for key in ['major', 'minor', 'maintenance']:
        if v1[key] < v2[key]:
            return -1
        elif v1[key] > v2[key]:
            return 1
    
    # Compare rebuild letter
    if v1['rebuild'] < v2['rebuild']:
        return -1
    elif v1['rebuild'] > v2['rebuild']:
        return 1
    
    return 0


def validate_ip_address(ip: str) -> bool:
    """
    Validate IP address format.
    
    Args:
        ip: IP address string
        
    Returns:
        True if valid IPv4 address
    """
    pattern = r'^(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})$'
    match = re.match(pattern, ip)
    
    if not match:
        return False
    
    # Check each octet is 0-255
    try:
        return all(0 <= int(octet) <= 255 for octet in match.groups())
    except ValueError:
        return False


def ensure_directory(directory: str) -> Path:
    """
    Ensure directory exists, create if it doesn't.
    
    Args:
        directory: Directory path
        
    Returns:
        Path object
    """
    dir_path = Path(directory)
    dir_path.mkdir(parents=True, exist_ok=True)
    return dir_path


def get_credentials_from_env() -> Dict[str, str]:
    """
    Get device credentials from environment variables.
    
    Returns:
        Dictionary with username and password
    """
    load_dotenv()
    
    return {
        'username': os.getenv('DEVICE_USERNAME', ''),
        'password': os.getenv('DEVICE_PASSWORD', ''),
        'secret': os.getenv('DEVICE_ENABLE_PASSWORD', '')
    }


def create_env_template(output_file: str = ".env.example") -> None:
    """
    Create .env template file.
    
    Args:
        output_file: Output filename
    """
    template = """# Device Credentials
# Copy this file to .env and fill in your credentials

DEVICE_USERNAME=admin
DEVICE_PASSWORD=your_password_here
DEVICE_ENABLE_PASSWORD=your_enable_password_here
"""
    
    with open(output_file, 'w') as f:
        f.write(template)
