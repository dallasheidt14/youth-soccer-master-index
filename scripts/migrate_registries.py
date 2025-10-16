#!/usr/bin/env python3
"""
Registry Migration Script

One-time script to migrate legacy registries to the unified system.
This script moves data from old registry locations to the new unified location.
"""

import logging
import sys
import os
from pathlib import Path

# Add project root to path for imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.registry.registry import UnifiedRegistry


def main():
    """Run the registry migration."""
    logging.basicConfig(
        level=logging.INFO, 
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    print("Starting registry migration...")
    print("=" * 50)
    
    try:
        registry = UnifiedRegistry()
        
        # Check if migration is already completed
        if registry._migration_completed:
            print("Migration already completed, skipping...")
            print("Registry is already unified at:", registry.base_path)
            return
        
        print("Pre-migration status:")
        print(f"  Build registry: {registry.build_registry_path}")
        print(f"  Metadata registry: {registry.metadata_registry_path}")
        print(f"  History registry: {registry.history_registry_path}")
        
        # Check for existing legacy files
        legacy_metadata = Path("data/master/metadata_registry.json")
        legacy_history = Path("data/master/history/history_registry.json")
        
        print("\nChecking for legacy registries:")
        print(f"  Legacy metadata: {'Found' if legacy_metadata.exists() else 'Not found'}")
        print(f"  Legacy history: {'Found' if legacy_history.exists() else 'Not found'}")
        
        if not legacy_metadata.exists() and not legacy_history.exists():
            print("\nNo legacy registries found to migrate.")
            print("Creating empty unified registries...")
            registry._mark_migration_completed()
            print("Migration completed (no data to migrate)")
            return
        
        # Run the migration
        print("\nStarting migration...")
        registry.migrate_legacy_registries()
        
        # Show summary after migration
        print("\nPost-migration summary:")
        summary = registry.get_comprehensive_summary()
        
        print(f"  Build Registry: {summary['build_registry']['total_slices']} slices")
        print(f"  Metadata Registry: {summary['metadata_registry']['total_builds']} builds")
        print(f"  History Registry: {summary['history_registry']['total_builds']} builds")
        print(f"  Migration Status: {'Completed' if summary['system_status']['migration_completed'] else 'Failed'}")
        
        print(f"\nUnified registry location: {summary['system_status']['registry_path']}")
        
        print("\nMigration completed successfully!")
        print("\nNext steps:")
        print("  1. Test the unified registry: python -m src.registry.registry --summary")
        print("  2. Verify all scripts work with the new system")
        print("  3. Remove old registry modules after confirming everything works")
        
    except Exception as e:
        print(f"\nMigration failed: {e}")
        logging.exception("Migration failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
