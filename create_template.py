"""
Script to create Excel inventory template
"""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent))

from src.inventory import create_inventory_template

if __name__ == '__main__':
    output_file = 'inventory/inventory_template.xlsx'
    create_inventory_template(output_file)
    print(f"Template created: {output_file}")
