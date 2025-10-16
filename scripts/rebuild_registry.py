#!/usr/bin/env python3
"""
Rebuild Registry Utility

CLI utility to rebuild the build registry by scanning existing build folders.
This is useful when the registry gets corrupted or needs to be reconstructed
from existing data.

Usage:
    python scripts/rebuild_registry.py
"""

import sys
import os
from pathlib import Path
from datetime import datetime, timezone

# Add project root to path for imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.registry.build_registry import list_all_builds, refresh_registry


def main():
    """Main function to rebuild the registry."""
    print("Rebuilding build registry from existing folders...")
    
    try:
        refresh_registry()
        
        # Load and display the rebuilt registry
        registry = list_all_builds()
        
        print(f"\nRegistry rebuilt successfully!")
        print(f"Found {len(registry)} slices:")
        
        for slice_key, info in sorted(registry.items()):
            print(f"  {slice_key}: {info['latest_build']} (updated: {info['last_updated']})")
            
    except Exception as e:
        print(f"Error rebuilding registry: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
