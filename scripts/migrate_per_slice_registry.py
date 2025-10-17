#!/usr/bin/env python3
"""
Migrate existing build directories to per-slice registry entries.

This ensures all historical builds are properly tracked in the registry.
"""

import sys
from pathlib import Path
from datetime import datetime

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.registry.registry import get_registry

def migrate_builds():
    registry = get_registry()
    build_root = Path("data/games")
    
    if not build_root.exists():
        print(f"Build directory {build_root} does not exist")
        return
    
    migrated_count = 0
    
    # Scan all build directories
    for build_dir in sorted(build_root.glob("build_*")):
        if not build_dir.is_dir():
            continue
        
        print(f"Scanning {build_dir.name}...")
        
        # Find all games CSV files in this build
        for csv_file in build_dir.glob("games_gotsport_*.csv"):
            # Extract slice key from filename
            # Example: games_gotsport_AZ_M_U10.csv → AZ_M_U10
            filename = csv_file.stem
            parts = filename.split('_')
            
            if len(parts) >= 5:  # games, gotsport, STATE, GENDER, AGE
                slice_key = '_'.join(parts[2:])  # STATE_GENDER_AGE
                
                # Update registry
                registry.update_build_registry(slice_key, build_dir.name)
                migrated_count += 1
                print(f"  [OK] Registered {slice_key} -> {build_dir.name}")
    
    print(f"\n[SUCCESS] Migration complete! Registered {migrated_count} slice entries.")
    
    # Show summary
    stats = registry.get_registry_stats()
    print(f"Total slices in registry: {stats.get('total_slices', 0)}")
    print(f"Registry health: {stats.get('registry_health', 0)}%")

if __name__ == "__main__":
    migrate_builds()
