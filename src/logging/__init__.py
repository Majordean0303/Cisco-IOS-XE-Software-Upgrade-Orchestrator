"""
Logging and Observability Module

Provides structured logging, audit trails, and telemetry for the orchestrator.
"""

import logging
import sys
from pathlib import Path
from datetime import datetime
from typing import Optional
from rich.logging import RichHandler
from rich.console import Console

console = Console()


class OrchestrationLogger:
    """
    Centralized logging for the upgrade orchestrator.
    Provides console and file logging with rich formatting.
    """
    
    def __init__(
        self,
        name: str = "IOS-XE-Orchestrator",
        log_dir: str = "logs",
        log_level: str = "INFO",
        console_output: bool = True,
        file_output: bool = True
    ):
        """
        Initialize the orchestration logger.
        
        Args:
            name: Logger name
            log_dir: Directory for log files
            log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
            console_output: Enable console logging
            file_output: Enable file logging
        """
        self.name = name
        self.log_dir = Path(log_dir)
        self.log_level = getattr(logging, log_level.upper(), logging.INFO)
        
        # Create log directory
        self.log_dir.mkdir(parents=True, exist_ok=True)
        
        # Create logger
        self.logger = logging.getLogger(name)
        self.logger.setLevel(self.log_level)
        
        # Remove existing handlers
        self.logger.handlers = []
        
        # Add console handler with Rich formatting
        if console_output:
            console_handler = RichHandler(
                rich_tracebacks=True,
                show_path=False,
                console=console
            )
            console_handler.setLevel(self.log_level)
            console_formatter = logging.Formatter(
                "%(message)s",
                datefmt="[%X]"
            )
            console_handler.setFormatter(console_formatter)
            self.logger.addHandler(console_handler)
        
        # Add file handler
        if file_output:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            log_file = self.log_dir / f"orchestrator_{timestamp}.log"
            
            file_handler = logging.FileHandler(log_file, encoding='utf-8')
            file_handler.setLevel(self.log_level)
            file_formatter = logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S"
            )
            file_handler.setFormatter(file_formatter)
            self.logger.addHandler(file_handler)
            
            self.log_file = log_file
            console.print(f"[dim]Logging to: {log_file}[/dim]")
    
    def debug(self, message: str) -> None:
        """Log debug message."""
        self.logger.debug(message)
    
    def info(self, message: str) -> None:
        """Log info message."""
        self.logger.info(message)
    
    def warning(self, message: str) -> None:
        """Log warning message."""
        self.logger.warning(message)
    
    def error(self, message: str) -> None:
        """Log error message."""
        self.logger.error(message)
    
    def critical(self, message: str) -> None:
        """Log critical message."""
        self.logger.critical(message)
    
    def log_stage_start(self, stage: str, device: Optional[str] = None) -> None:
        """
        Log the start of a workflow stage.
        
        Args:
            stage: Stage name
            device: Optional device hostname
        """
        device_info = f" for {device}" if device else ""
        self.info(f"Starting stage: {stage}{device_info}")
    
    def log_stage_complete(self, stage: str, device: Optional[str] = None, success: bool = True) -> None:
        """
        Log the completion of a workflow stage.
        
        Args:
            stage: Stage name
            device: Optional device hostname
            success: Whether stage was successful
        """
        device_info = f" for {device}" if device else ""
        status = "successfully" if success else "with failures"
        
        if success:
            self.info(f"Completed stage: {stage}{device_info} {status}")
        else:
            self.error(f"Completed stage: {stage}{device_info} {status}")
    
    def log_device_action(self, device: str, action: str, result: str) -> None:
        """
        Log a device-specific action.
        
        Args:
            device: Device hostname
            action: Action performed
            result: Result of action
        """
        self.info(f"[{device}] {action}: {result}")
    
    def log_error_with_traceback(self, message: str, exc: Exception) -> None:
        """
        Log error with full traceback.
        
        Args:
            message: Error message
            exc: Exception object
        """
        self.logger.exception(f"{message}: {str(exc)}")
    
    def create_audit_entry(
        self,
        event_type: str,
        device: str,
        action: str,
        result: str,
        user: Optional[str] = None
    ) -> None:
        """
        Create an audit trail entry.
        
        Args:
            event_type: Type of event (upgrade, rollback, etc.)
            device: Device hostname
            action: Action performed
            result: Result of action
            user: Optional user/operator name
        """
        user_info = f" by {user}" if user else ""
        audit_message = f"AUDIT: {event_type} - [{device}] {action} -> {result}{user_info}"
        self.info(audit_message)


class MetricsCollector:
    """
    Collect and track orchestration metrics.
    """
    
    def __init__(self):
        """Initialize metrics collector."""
        self.metrics = {
            'devices_total': 0,
            'devices_successful': 0,
            'devices_failed': 0,
            'stages_executed': 0,
            'stages_passed': 0,
            'stages_failed': 0,
            'transfer_times': [],
            'upgrade_times': [],
            'total_duration': 0
        }
        self.start_time = None
        self.end_time = None
    
    def start_tracking(self) -> None:
        """Start metrics tracking."""
        self.start_time = datetime.now()
    
    def stop_tracking(self) -> None:
        """Stop metrics tracking."""
        self.end_time = datetime.now()
        if self.start_time:
            self.metrics['total_duration'] = (self.end_time - self.start_time).total_seconds()
    
    def record_device_result(self, success: bool) -> None:
        """
        Record device upgrade result.
        
        Args:
            success: Whether device upgrade was successful
        """
        self.metrics['devices_total'] += 1
        if success:
            self.metrics['devices_successful'] += 1
        else:
            self.metrics['devices_failed'] += 1
    
    def record_stage_result(self, success: bool) -> None:
        """
        Record stage execution result.
        
        Args:
            success: Whether stage was successful
        """
        self.metrics['stages_executed'] += 1
        if success:
            self.metrics['stages_passed'] += 1
        else:
            self.metrics['stages_failed'] += 1
    
    def record_transfer_time(self, duration_seconds: float) -> None:
        """
        Record image transfer time.
        
        Args:
            duration_seconds: Transfer duration in seconds
        """
        self.metrics['transfer_times'].append(duration_seconds)
    
    def record_upgrade_time(self, duration_seconds: float) -> None:
        """
        Record upgrade execution time.
        
        Args:
            duration_seconds: Upgrade duration in seconds
        """
        self.metrics['upgrade_times'].append(duration_seconds)
    
    def get_summary(self) -> dict:
        """
        Get metrics summary.
        
        Returns:
            Dictionary with summary statistics
        """
        summary = self.metrics.copy()
        
        # Calculate averages
        if summary['transfer_times']:
            summary['avg_transfer_time'] = sum(summary['transfer_times']) / len(summary['transfer_times'])
        else:
            summary['avg_transfer_time'] = 0
        
        if summary['upgrade_times']:
            summary['avg_upgrade_time'] = sum(summary['upgrade_times']) / len(summary['upgrade_times'])
        else:
            summary['avg_upgrade_time'] = 0
        
        # Calculate success rates
        if summary['devices_total'] > 0:
            summary['device_success_rate'] = (summary['devices_successful'] / summary['devices_total']) * 100
        else:
            summary['device_success_rate'] = 0
        
        if summary['stages_executed'] > 0:
            summary['stage_success_rate'] = (summary['stages_passed'] / summary['stages_executed']) * 100
        else:
            summary['stage_success_rate'] = 0
        
        return summary
    
    def display_metrics(self) -> None:
        """Display metrics summary."""
        summary = self.get_summary()
        
        console.print("\n[bold cyan]Performance Metrics[/bold cyan]\n")
        
        console.print("[bold]Devices:[/bold]")
        console.print(f"  Total: {summary['devices_total']}")
        console.print(f"  [green]Successful: {summary['devices_successful']}[/green]")
        console.print(f"  [red]Failed: {summary['devices_failed']}[/red]")
        console.print(f"  Success Rate: {summary['device_success_rate']:.1f}%\n")
        
        console.print("[bold]Stages:[/bold]")
        console.print(f"  Executed: {summary['stages_executed']}")
        console.print(f"  [green]Passed: {summary['stages_passed']}[/green]")
        console.print(f"  [red]Failed: {summary['stages_failed']}[/red]")
        console.print(f"  Success Rate: {summary['stage_success_rate']:.1f}%\n")
        
        if summary['avg_transfer_time'] > 0:
            console.print(f"[bold]Average Transfer Time:[/bold] {summary['avg_transfer_time']:.1f}s")
        
        if summary['avg_upgrade_time'] > 0:
            console.print(f"[bold]Average Upgrade Time:[/bold] {summary['avg_upgrade_time']:.1f}s")
        
        if summary['total_duration'] > 0:
            console.print(f"[bold]Total Duration:[/bold] {summary['total_duration']:.1f}s")
        
        console.print()
