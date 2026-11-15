"""
Inventory and State Management Module

This module handles Excel-based inventory management, device state tracking,
and bidirectional updates between the orchestrator and the Excel source-of-truth.
"""

import os
import ipaddress
import re
import threading
from collections import Counter
from datetime import datetime
from typing import Dict, List, Optional, Any
from pathlib import Path
import openpyxl
from openpyxl import Workbook, load_workbook
from openpyxl.styles import Font, PatternFill, Alignment
from rich.console import Console

console = Console()
_EXCEL_WRITE_LOCK = threading.RLock()


class InventoryManager:
    """
    Manages device inventory and state using Excel as the source of truth.
    Provides methods to read, validate, and update device information and upgrade status.
    """
    
    # Excel column definitions
    REQUIRED_COLUMNS = [
        'hostname', 'ip_address', 'platform', 'target_version', 
        'image_path', 'image_md5'
    ]
    
    OPTIONAL_COLUMNS = [
        'current_version', 'maintenance_window', 'status', 
        'last_updated', 'notes', 'backup_path', 'rollback_available',
        'redundancy_group', 'console_verified', 'enabled',
        'maintenance_window_start', 'maintenance_window_end',
        'ssh_port'
    ]
    
    STATUS_VALUES = [
        'pending', 'pre-check-passed', 'pre-check-failed',
        'transferring', 'transfer-complete', 'transfer-failed',
        'upgrading', 'upgrade-complete', 'upgrade-failed',
        'verifying', 'verified', 'verification-failed',
        'complete', 'rollback-required', 'rolled-back', 'error',
        'validating', 'ready', 'backing_up', 'baseline_capture',
        'staging', 'hash_verified', 'staged', 'activation_pending',
        'activating', 'rebooting', 'reconnecting', 'post_checking',
        'stabilizing', 'commit_pending', 'committing',
        'cleanup_pending', 'cleaning', 'incompatible',
        'precheck_failed', 'backup_failed', 'baseline_failed',
        'staging_failed', 'activation_uncertain', 'activation_failed',
        'reconnect_timeout', 'post_check_failed', 'commit_failed',
        'cleanup_failed', 'execution_failed', 'rollback_required', 'rolling_back',
        'rolled_back', 'failed'
    ]
    
    def __init__(self, excel_file: str):
        """
        Initialize the inventory manager.
        
        Args:
            excel_file: Path to the Excel inventory file
        """
        self.excel_file = Path(excel_file)
        self.devices: List[Dict[str, Any]] = []
        self._validate_file_exists()
        
    def _validate_file_exists(self) -> None:
        """Validate that the Excel file exists."""
        if not self.excel_file.exists():
            raise FileNotFoundError(
                f"Inventory file not found: {self.excel_file}"
            )
    
    def load_inventory(self) -> List[Dict[str, Any]]:
        """
        Load device inventory from Excel file.
        
        Returns:
            List of device dictionaries
            
        Raises:
            ValueError: If required columns are missing
        """
        console.print(f"[cyan]Loading inventory from: {self.excel_file}[/cyan]")
        
        try:
            workbook = load_workbook(self.excel_file, read_only=True, data_only=True)
            if 'Devices' not in workbook.sheetnames:
                raise ValueError("Inventory workbook must contain a 'Devices' sheet")
            worksheet = workbook['Devices']
            rows = worksheet.iter_rows(values_only=True)
            try:
                raw_headers = next(rows)
            except StopIteration as error:
                raise ValueError("Inventory sheet is empty") from error
            headers = [
                str(value).strip() if value is not None else ''
                for value in raw_headers
            ]
            if len(headers) != len(set(headers)):
                raise ValueError("Inventory contains duplicate column names")

            missing_cols = set(self.REQUIRED_COLUMNS) - set(headers)
            if missing_cols:
                raise ValueError(
                    f"Missing required columns: {', '.join(missing_cols)}"
                )

            all_headers = headers + [
                column
                for column in self.OPTIONAL_COLUMNS
                if column not in headers
            ]
            self.devices = []
            for values in rows:
                if not any(value is not None and str(value).strip() for value in values):
                    continue
                row = {
                    header: self._clean_cell(values[index])
                    if index < len(values)
                    else None
                    for index, header in enumerate(headers)
                    if header
                }
                for column in all_headers:
                    row.setdefault(column, None)
                if not self._as_bool(row.get('enabled'), default=True):
                    continue
                row['status'] = row.get('status') or 'pending'
                self.devices.append(row)
            workbook.close()
            
            console.print(
                f"[green]Loaded {len(self.devices)} devices from inventory[/green]"
            )
            
            return self.devices
            
        except Exception as e:
            console.print(f"[red]Error loading inventory: {str(e)}[/red]")
            raise
    
    def validate_inventory(self) -> Dict[str, Any]:
        """
        Validate inventory data and return validation results.
        
        Returns:
            Dictionary containing validation results and issues
        """
        console.print("[cyan]Validating inventory data...[/cyan]")
        
        validation_results = {
            'valid': True,
            'total_devices': len(self.devices),
            'errors': [],
            'warnings': []
        }
        
        for idx, device in enumerate(self.devices, start=1):
            hostname = device.get('hostname', f'Row {idx}')
            
            # Validate required fields
            for field in self.REQUIRED_COLUMNS:
                if not device.get(field):
                    validation_results['errors'].append(
                        f"{hostname}: Missing required field '{field}'"
                    )
                    validation_results['valid'] = False
            
            # Validate IP address format
            ip = device.get('ip_address', '')
            if ip and not self._is_valid_ip(str(ip)):
                validation_results['errors'].append(
                    f"{hostname}: IP address '{ip}' is invalid"
                )
                validation_results['valid'] = False
            
            # Validate platform
            platform = str(device.get('platform') or '').lower()
            if platform not in [
                'catalyst9200', 'catalyst9300', 'cat9200', 'cat9300',
                'c9200', 'c9300'
            ]:
                validation_results['errors'].append(
                    f"{hostname}: Platform '{platform}' is not supported"
                )
                validation_results['valid'] = False

            image_md5 = str(device.get('image_md5') or '').strip()
            if image_md5 and not re.fullmatch(r'[a-fA-F0-9]{32}', image_md5):
                validation_results['errors'].append(
                    f"{hostname}: image_md5 must contain exactly 32 hexadecimal characters"
                )
                validation_results['valid'] = False

            ssh_port = device.get('ssh_port')
            if ssh_port not in (None, ''):
                try:
                    port_number = int(ssh_port)
                except (TypeError, ValueError):
                    port_number = -1
                if not 1 <= port_number <= 65535:
                    validation_results['errors'].append(
                        f"{hostname}: ssh_port '{ssh_port}' must be a TCP port between 1 and 65535"
                    )
                    validation_results['valid'] = False
            
            # Validate status
            status = device.get('status')
            if status and status not in self.STATUS_VALUES:
                validation_results['warnings'].append(
                    f"{hostname}: Unknown status '{status}'"
                )

        hostnames = [
            str(device.get('hostname')).lower()
            for device in self.devices
            if device.get('hostname')
        ]
        duplicate_hostnames = sorted(
            hostname
            for hostname, count in Counter(hostnames).items()
            if count > 1
        )
        for hostname in duplicate_hostnames:
            validation_results['errors'].append(
                f"Duplicate hostname: {hostname}"
            )
            validation_results['valid'] = False

        addresses = []
        for device in self.devices:
            value = device.get('ip_address')
            if value and self._is_valid_ip(str(value)):
                addresses.append(str(ipaddress.ip_address(str(value))))
        duplicate_addresses = sorted(
            address
            for address, count in Counter(addresses).items()
            if count > 1
        )
        for address in duplicate_addresses:
            validation_results['errors'].append(
                f"Duplicate management address: {address}"
            )
            validation_results['valid'] = False
        
        # Summary
        if validation_results['valid']:
            console.print(
                f"[green]Inventory validation passed "
                f"({len(self.devices)} devices)[/green]"
            )
        else:
            console.print(
                f"[red]Inventory validation failed "
                f"({len(validation_results['errors'])} errors)[/red]"
            )
        
        if validation_results['warnings']:
            console.print(
                f"[yellow]{len(validation_results['warnings'])} warnings found[/yellow]"
            )
        
        return validation_results
    
    def _is_valid_ip(self, ip: str) -> bool:
        """Validate an IPv4 or IPv6 management address."""
        try:
            ipaddress.ip_address(ip)
            return True
        except ValueError:
            return False

    @staticmethod
    def _clean_cell(value: Any) -> Any:
        if isinstance(value, str):
            stripped = value.strip()
            return stripped or None
        return value

    @staticmethod
    def _as_bool(value: Any, *, default: bool = False) -> bool:
        if value is None:
            return default
        if isinstance(value, bool):
            return value
        return str(value).strip().lower() in {'1', 'true', 'yes', 'y'}
    
    def update_device_status(
        self, 
        hostname: str, 
        status: str, 
        notes: Optional[str] = None
    ) -> None:
        """
        Update device status in memory and Excel.
        
        Args:
            hostname: Device hostname
            status: New status value
            notes: Optional notes to add
        """
        # Update in-memory data
        for device in self.devices:
            if device['hostname'] == hostname:
                device['status'] = status
                device['last_updated'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                if notes:
                    current_notes = device.get('notes', '')
                    device['notes'] = f"{current_notes}\n{notes}" if current_notes else notes
                break
        
        # Update Excel file
        self._update_excel_status(hostname, status, notes)
    
    def _update_excel_status(
        self, 
        hostname: str, 
        status: str, 
        notes: Optional[str] = None
    ) -> None:
        """
        Update status directly in Excel file.
        
        Args:
            hostname: Device hostname
            status: New status value
            notes: Optional notes to add
        """
        with _EXCEL_WRITE_LOCK:
            wb = load_workbook(self.excel_file)
            ws = wb['Devices']
            
            # Find header row
            headers = [cell.value for cell in ws[1]]
            hostname_col = headers.index('hostname') + 1
            status_col = headers.index('status') + 1 if 'status' in headers else None
            updated_col = headers.index('last_updated') + 1 if 'last_updated' in headers else None
            notes_col = headers.index('notes') + 1 if 'notes' in headers else None
            
            # Find device row
            for row_idx, row in enumerate(ws.iter_rows(min_row=2, values_only=False), start=2):
                if row[hostname_col - 1].value == hostname:
                    if status_col:
                        ws.cell(row_idx, status_col, status)
                        # Apply color coding based on status
                        self._apply_status_color(ws.cell(row_idx, status_col), status)
                    
                    if updated_col:
                        ws.cell(row_idx, updated_col, datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
                    
                    if notes and notes_col:
                        current = ws.cell(row_idx, notes_col).value or ''
                        ws.cell(row_idx, notes_col, f"{current}\n{notes}" if current else notes)
                    
                    break
            
            wb.save(self.excel_file)
            wb.close()
    
    def _apply_status_color(self, cell, status: str) -> None:
        """Apply color coding to status cells."""
        color_map = {
            'complete': '90EE90',  # Light green
            'verified': '90EE90',
            'pre-check-passed': 'ADD8E6',  # Light blue
            'transfer-complete': 'ADD8E6',
            'upgrade-complete': 'ADD8E6',
            'error': 'FFB6C1',  # Light red
            'upgrade-failed': 'FFB6C1',
            'verification-failed': 'FFB6C1',
            'rollback-required': 'FFA500',  # Orange
            'upgrading': 'FFFF99',  # Light yellow
            'transferring': 'FFFF99',
        }
        
        if status in color_map:
            cell.fill = PatternFill(start_color=color_map[status], 
                                   end_color=color_map[status], 
                                   fill_type='solid')
    
    def get_devices_by_status(self, status: str) -> List[Dict[str, Any]]:
        """
        Get all devices with a specific status.
        
        Args:
            status: Status to filter by
            
        Returns:
            List of devices matching the status
        """
        return [d for d in self.devices if d.get('status') == status]
    
    def get_device_by_hostname(self, hostname: str) -> Optional[Dict[str, Any]]:
        """
        Get device by hostname.
        
        Args:
            hostname: Device hostname
            
        Returns:
            Device dictionary or None if not found
        """
        for device in self.devices:
            if device.get('hostname') == hostname:
                return device
        return None
    
    def export_summary_report(self, output_file: Optional[str] = None) -> None:
        """
        Export a summary report of the upgrade status.
        
        Args:
            output_file: Optional output file path
        """
        if not output_file:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_file = f"upgrade_summary_{timestamp}.xlsx"
        
        # Create summary statistics
        status_counts = {}
        for device in self.devices:
            status = device.get('status', 'unknown')
            status_counts[status] = status_counts.get(status, 0) + 1
        
        # Create workbook
        wb = Workbook()
        ws = wb.active
        ws.title = "Summary"
        
        # Write summary
        ws['A1'] = "Upgrade Summary Report"
        ws['A1'].font = Font(bold=True, size=14)
        ws['A3'] = "Total Devices:"
        ws['B3'] = len(self.devices)
        
        row = 5
        ws[f'A{row}'] = "Status"
        ws[f'B{row}'] = "Count"
        ws[f'A{row}'].font = Font(bold=True)
        ws[f'B{row}'].font = Font(bold=True)
        
        for status, count in sorted(status_counts.items()):
            row += 1
            ws[f'A{row}'] = status
            ws[f'B{row}'] = count
        
        wb.save(output_file)
        console.print(f"[green]Summary report saved to: {output_file}[/green]")


def create_inventory_template(output_file: str = "inventory_template.xlsx") -> None:
    """
    Create a template Excel file for device inventory.
    
    Args:
        output_file: Output file path
    """
    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    wb = Workbook()
    ws = wb.active
    ws.title = "Devices"
    
    # Define headers
    headers = [
        'hostname', 'ip_address', 'platform', 'current_version',
        'target_version', 'image_path', 'image_md5', 'maintenance_window',
        'maintenance_window_start', 'maintenance_window_end',
        'redundancy_group', 'console_verified', 'enabled', 'ssh_port',
        'status', 'last_updated', 'notes', 'backup_path', 'rollback_available'
    ]
    
    # Write headers with formatting
    for col_idx, header in enumerate(headers, start=1):
        cell = ws.cell(1, col_idx, header)
        cell.font = Font(bold=True, color="FFFFFF")
        cell.fill = PatternFill(start_color="4472C4", end_color="4472C4", fill_type="solid")
        cell.alignment = Alignment(horizontal="center")
    
    # Add example data
    example_data = [
        {
            'hostname': 'SW-ACCESS-01',
            'ip_address': '192.168.1.101',
            'platform': 'catalyst9300',
            'current_version': '17.9.4a',
            'target_version': '17.12.1',
            'image_path': '/images/cat9k_iosxe.17.12.01.SPA.bin',
            'image_md5': '00000000000000000000000000000000',
            'maintenance_window': '2026-02-15 22:00',
            'maintenance_window_start': '',
            'maintenance_window_end': '',
            'redundancy_group': 'distribution-pair-1',
            'console_verified': 'Yes',
            'enabled': 'Yes',
            'ssh_port': '',
            'status': 'pending',
            'last_updated': '',
            'notes': '',
            'backup_path': '',
            'rollback_available': 'No'
        }
    ]
    
    for row_idx, device in enumerate(example_data, start=2):
        for col_idx, header in enumerate(headers, start=1):
            ws.cell(row_idx, col_idx, device.get(header, ''))
    
    # Adjust column widths
    for col in ws.columns:
        max_length = 0
        column = col[0].column_letter
        for cell in col:
            try:
                if len(str(cell.value)) > max_length:
                    max_length = len(cell.value)
            except:
                pass
        adjusted_width = min(max_length + 2, 50)
        ws.column_dimensions[column].width = adjusted_width
    
    wb.save(output_path)
    console.print(f"[green]Inventory template created: {output_path}[/green]")
