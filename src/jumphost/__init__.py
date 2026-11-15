"""
Jump Host / Bastion Connection Module

Provides SSH tunneling support for connecting to devices through jump hosts.
"""

from typing import Dict, Optional, Any
from sshtunnel import SSHTunnelForwarder
from rich.console import Console
import time

console = Console()


class JumpHostManager:
    """
    Manages SSH tunnels through jump hosts/bastions.
    """
    
    def __init__(self, config: Dict[str, Any], jump_creds: Dict[str, str]):
        """
        Initialize jump host manager.
        
        Args:
            config: Jump host configuration
            jump_creds: Jump host credentials (username, password)
        """
        self.config = config
        self.jump_creds = jump_creds
        self.tunnels: Dict[str, SSHTunnelForwarder] = {}
        
    def create_tunnel(
        self, 
        target_host: str, 
        target_port: int = 22
    ) -> Optional[tuple]:
        """
        Create SSH tunnel to target device through jump host.
        
        Args:
            target_host: Target device IP/hostname
            target_port: Target device SSH port (default 22)
            
        Returns:
            Tuple of (local_host, local_port) or None if failed
        """
        if not self.config.get('hostname'):
            console.print("[yellow]No jump host configured[/yellow]")
            return None
        
        tunnel_key = f"{target_host}:{target_port}"
        
        # Check if tunnel already exists
        if tunnel_key in self.tunnels and self.tunnels[tunnel_key].is_active:
            server = self.tunnels[tunnel_key]
            return ('127.0.0.1', server.local_bind_port)
        
        try:
            # Create SSH tunnel
            server = SSHTunnelForwarder(
                ssh_address_or_host=(
                    self.config['hostname'],
                    self.config.get('port', 22)
                ),
                ssh_username=self.jump_creds['username'],
                ssh_password=self.jump_creds['password'],
                remote_bind_address=(target_host, target_port),
                local_bind_address=('127.0.0.1', 0)  # 0 = auto-assign port
            )
            
            # Start the tunnel
            server.start()
            
            # Wait a moment for tunnel to establish
            time.sleep(0.5)
            
            if server.is_active:
                self.tunnels[tunnel_key] = server
                local_port = server.local_bind_port
                
                console.print(
                    f"[dim]SSH tunnel established: "
                    f"localhost:{local_port} -> {self.config['hostname']} -> {target_host}:{target_port}[/dim]"
                )
                
                return ('127.0.0.1', local_port)
            else:
                console.print(f"[red]Failed to establish tunnel to {target_host}[/red]")
                return None
        
        except Exception as e:
            console.print(f"[red]Jump host tunnel error: {str(e)}[/red]")
            return None
    
    def close_tunnel(self, target_host: str, target_port: int = 22) -> None:
        """
        Close SSH tunnel to a specific target.
        
        Args:
            target_host: Target device IP/hostname
            target_port: Target device SSH port
        """
        tunnel_key = f"{target_host}:{target_port}"
        
        if tunnel_key in self.tunnels:
            try:
                self.tunnels[tunnel_key].stop()
                del self.tunnels[tunnel_key]
                console.print(f"[dim]Closed tunnel to {target_host}:{target_port}[/dim]")
            except Exception as e:
                console.print(f"[yellow]Error closing tunnel: {str(e)}[/yellow]")
    
    def close_all_tunnels(self) -> None:
        """Close all active SSH tunnels."""
        for tunnel_key in list(self.tunnels.keys()):
            try:
                self.tunnels[tunnel_key].stop()
            except:
                pass
        
        self.tunnels.clear()
        console.print("[dim]All SSH tunnels closed[/dim]")
    
    def is_tunnel_active(self, target_host: str, target_port: int = 22) -> bool:
        """
        Check if tunnel to target is active.
        
        Args:
            target_host: Target device IP/hostname
            target_port: Target device SSH port
            
        Returns:
            True if tunnel is active
        """
        tunnel_key = f"{target_host}:{target_port}"
        return tunnel_key in self.tunnels and self.tunnels[tunnel_key].is_active
    
    def get_tunnel_info(self, target_host: str, target_port: int = 22) -> Optional[Dict[str, Any]]:
        """
        Get tunnel information for a target.
        
        Args:
            target_host: Target device IP/hostname
            target_port: Target device SSH port
            
        Returns:
            Dictionary with tunnel details or None
        """
        tunnel_key = f"{target_host}:{target_port}"
        
        if tunnel_key in self.tunnels:
            server = self.tunnels[tunnel_key]
            return {
                'active': server.is_active,
                'local_host': '127.0.0.1',
                'local_port': server.local_bind_port,
                'remote_host': target_host,
                'remote_port': target_port,
                'jump_host': self.config['hostname']
            }
        
        return None
