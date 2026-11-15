"""
Device Abstraction Layer

This module provides platform-specific logic and abstraction for different Cisco IOS-XE devices.
Handles device capability detection, command mapping, and platform-specific workflows.
"""

from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from enum import Enum


class Platform(Enum):
    """Supported device platforms."""
    CATALYST_9200 = "catalyst9200"
    CATALYST_9300 = "catalyst9300"
    UNKNOWN = "unknown"


class UpgradeMode(Enum):
    """Upgrade execution modes."""
    INSTALL = "install"


@dataclass
class PlatformCapabilities:
    """Platform-specific capabilities and requirements."""
    name: str
    supports_install_mode: bool
    supports_issu: bool
    supports_ssu: bool
    default_flash: str
    min_flash_space_mb: int
    default_upgrade_mode: UpgradeMode
    supported_versions: List[str]


class DeviceAbstraction:
    """
    Provides platform-specific abstractions for IOS-XE devices.
    Encapsulates command generation and capability detection.
    """
    
    # Platform capabilities database
    PLATFORM_CAPABILITIES = {
        Platform.CATALYST_9200: PlatformCapabilities(
            name="Catalyst 9200",
            supports_install_mode=True,
            supports_issu=False,  # Single SUP only
            supports_ssu=False,
            default_flash="flash:",
            min_flash_space_mb=2048,
            default_upgrade_mode=UpgradeMode.INSTALL,
            supported_versions=["17.x", "16.x"]
        ),
        Platform.CATALYST_9300: PlatformCapabilities(
            name="Catalyst 9300",
            supports_install_mode=True,
            supports_issu=False,  # For now, single switches only
            supports_ssu=False,
            default_flash="flash:",
            min_flash_space_mb=2048,
            default_upgrade_mode=UpgradeMode.INSTALL,
            supported_versions=["17.x", "16.x"]
        ),
    }
    
    def __init__(self, platform: str):
        """
        Initialize device abstraction for a specific platform.
        
        Args:
            platform: Platform identifier (e.g., 'catalyst9300')
        """
        self.platform = self._normalize_platform(platform)
        self.capabilities = self._get_capabilities()
    
    def _normalize_platform(self, platform: str) -> Platform:
        """Normalize platform string to Platform enum."""
        platform_lower = platform.lower().replace('-', '').replace('_', '')
        
        platform_map = {
            'catalyst9200': Platform.CATALYST_9200,
            'cat9200': Platform.CATALYST_9200,
            'c9200': Platform.CATALYST_9200,
            'catalyst9300': Platform.CATALYST_9300,
            'cat9300': Platform.CATALYST_9300,
            'c9300': Platform.CATALYST_9300,
        }
        
        return platform_map.get(platform_lower, Platform.UNKNOWN)
    
    def _get_capabilities(self) -> PlatformCapabilities:
        """Get platform capabilities."""
        return self.PLATFORM_CAPABILITIES.get(
            self.platform,
            PlatformCapabilities(
                name="Unknown",
                supports_install_mode=False,
                supports_issu=False,
                supports_ssu=False,
                default_flash="flash:",
                min_flash_space_mb=2048,
                default_upgrade_mode=UpgradeMode.INSTALL,
                supported_versions=[]
            )
        )
    
    def get_pre_check_commands(self) -> List[str]:
        """
        Get platform-specific pre-check commands.
        
        Returns:
            List of show commands for pre-upgrade checks
        """
        return [
            "show version",
            "show inventory",
            "show boot",
            "show file systems",
            f"dir {self.capabilities.default_flash}",
            "show running-config | include boot",
            "show module",
            "show environment all",
            "show processes cpu sorted",
            "show memory statistics",
            "show interfaces status",
            "show ip interface brief",
            "show logging",
            "show redundancy",
            "show install summary",
            "show license summary",
        ]
    
    def get_post_check_commands(self) -> List[str]:
        """
        Get platform-specific post-upgrade verification commands.
        
        Returns:
            List of show commands for post-upgrade verification
        """
        return [
            "show version",
            "show install summary",
            "show boot",
            "show module",
            "show environment all",
            "show interfaces status",
            "show ip interface brief",
            "show logging | include %",
            "show redundancy",
            "show license summary",
        ]
    
    def get_install_add_command(self, image_path: str) -> str:
        """
        Generate install add command for the platform.
        
        Args:
            image_path: Path to the image file
            
        Returns:
            Install add command string
        """
        if not image_path.startswith(self.capabilities.default_flash):
            image_path = f"{self.capabilities.default_flash}{image_path}"
        
        return f"install add file {image_path}"
    
    def get_install_activate_command(self) -> str:
        """
        Generate install activate command.
        
        Returns:
            Install activate command string
        """
        return "install activate"
    
    def get_install_commit_command(self) -> str:
        """
        Generate install commit command.
        
        Returns:
            Install commit command string
        """
        return "install commit"
    
    def get_install_rollback_command(self) -> str:
        """
        Generate install rollback command.
        
        Returns:
            Install rollback command string
        """
        return "install rollback to committed"
    
    def get_backup_commands(self) -> Dict[str, str]:
        """
        Get commands to backup device configuration.
        
        Returns:
            Dictionary of backup commands
        """
        return {
            'running_config': 'show running-config',
            'startup_config': 'show startup-config',
            'version': 'show version',
            'inventory': 'show inventory',
            'boot': 'show boot',
        }
    
    def parse_flash_space(self, dir_output: str) -> Dict[str, int]:
        """
        Parse 'dir flash:' output to get available space.
        
        Args:
            dir_output: Output from 'dir flash:' command
            
        Returns:
            Dictionary with total and free space in bytes
        """
        space_info = {
            'total_bytes': 0,
            'free_bytes': 0,
            'used_bytes': 0
        }
        
        try:
            # Look for line like: "11353194496 bytes total (5648769024 bytes free)"
            for line in dir_output.split('\n'):
                if 'bytes total' in line.lower() and 'bytes free' in line.lower():
                    # Extract numbers
                    parts = line.split()
                    for i, part in enumerate(parts):
                        if part.isdigit():
                            if 'total' in parts[i+1].lower():
                                space_info['total_bytes'] = int(part)
                            elif 'free' in parts[i+1].lower():
                                space_info['free_bytes'] = int(part)
                    
                    space_info['used_bytes'] = (
                        space_info['total_bytes'] - space_info['free_bytes']
                    )
                    break
        except Exception:
            pass
        
        return space_info
    
    def parse_version_info(self, version_output: str) -> Dict[str, str]:
        """
        Parse 'show version' output to extract key information.
        
        Args:
            version_output: Output from 'show version' command
            
        Returns:
            Dictionary with parsed version information
        """
        version_info = {
            'version': 'Unknown',
            'image': 'Unknown',
            'model': 'Unknown',
            'serial': 'Unknown',
            'uptime': 'Unknown',
            'hostname': 'Unknown'
        }
        
        try:
            for line in version_output.split('\n'):
                line = line.strip()
                
                # Cisco IOS XE Software, Version 17.9.4a
                if 'Cisco IOS XE Software, Version' in line:
                    version_info['version'] = line.split('Version')[-1].strip()
                
                # System image file is "flash:packages.conf"
                elif 'System image file is' in line:
                    version_info['image'] = line.split('is')[-1].strip().strip('"')
                
                # Model Number: C9300-48P
                elif 'Model Number' in line or 'Model number' in line:
                    version_info['model'] = line.split(':')[-1].strip()
                
                # Processor board ID FCW2343L0MA
                elif 'Processor board ID' in line:
                    version_info['serial'] = line.split('ID')[-1].strip()
                
                # Switch uptime is 2 weeks, 3 days, 4 hours, 15 minutes
                elif 'uptime is' in line:
                    version_info['uptime'] = line.split('is')[-1].strip()
                
                # Hostname might be in the version output
                elif line and not line.startswith(' '):
                    # First non-indented line is often hostname#
                    if '#' in line:
                        version_info['hostname'] = line.split('#')[0]
        
        except Exception:
            pass
        
        return version_info
    
    def calculate_required_space(self, image_size_mb: int) -> int:
        """
        Calculate total required flash space for upgrade.
        
        Args:
            image_size_mb: Size of the image file in MB
            
        Returns:
            Required free space in MB (includes buffer)
        """
        # Require image size + 50% buffer for install mode operations
        buffer_multiplier = 1.5
        return int(image_size_mb * buffer_multiplier)
    
    def is_install_mode_supported(self) -> bool:
        """Check if install mode is supported on this platform."""
        return self.capabilities.supports_install_mode
    
    def get_recommended_upgrade_mode(self) -> UpgradeMode:
        """Get recommended upgrade mode for this platform."""
        return self.capabilities.default_upgrade_mode
    
    def validate_target_version(self, target_version: str) -> bool:
        """
        Validate that target version is supported.
        
        Args:
            target_version: Target IOS-XE version
            
        Returns:
            True if version is supported
        """
        # Basic validation - check if version matches supported patterns
        if not self.capabilities.supported_versions:
            return True  # No restrictions
        
        for pattern in self.capabilities.supported_versions:
            if pattern.replace('x', '') in target_version:
                return True
        
        return False


class DeviceFactory:
    """Factory for creating platform-specific device abstractions."""
    
    @staticmethod
    def create(platform: str) -> DeviceAbstraction:
        """
        Create device abstraction for specified platform.
        
        Args:
            platform: Platform identifier
            
        Returns:
            DeviceAbstraction instance
        """
        return DeviceAbstraction(platform)
    
    @staticmethod
    def get_supported_platforms() -> List[str]:
        """Get list of supported platform identifiers."""
        return [
            'catalyst9200', 'cat9200', 'c9200',
            'catalyst9300', 'cat9300', 'c9300'
        ]
