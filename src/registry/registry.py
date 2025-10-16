#!/usr/bin/env python3
"""
Unified Registry System - Single source of truth for all registry operations.

This module provides a unified interface for accessing and managing all registry
types (build, metadata, history) through a single API, eliminating the need for
modules to know about different registry paths and schemas.

Author: Youth Soccer Master Index System
"""

import json
import logging
import os
import time
import uuid
from pathlib import Path
from typing import Dict, List, Optional, Any, Union
from datetime import datetime, timezone

from src.utils.json_safety import serialize_paths


class UnifiedRegistry:
    """
    Unified registry system that provides a single interface for all registry operations.
    
    This class consolidates the functionality of build_registry, metadata_registry,
    and history_registry into a single, consistent API.
    """
    
    def __init__(self, base_path: str = "data/registry"):
        """
        Initialize the unified registry system.
        
        Args:
            base_path: Base directory for all registry files
        """
        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)
        
        # Registry file paths
        self.build_registry_path = self.base_path / "build_registry.json"
        self.metadata_registry_path = self.base_path / "metadata_registry.json"
        self.history_registry_path = self.base_path / "history_registry.json"
        
        self.logger = logging.getLogger(__name__)
        
        # Migration flag
        self._migration_completed = self._check_migration_status()
    
    def _check_migration_status(self) -> bool:
        """Check if migration from old registry locations has been completed."""
        migration_flag = self.base_path / ".migration_completed"
        return migration_flag.exists()
    
    def _mark_migration_completed(self) -> None:
        """Mark migration as completed."""
        migration_flag = self.base_path / ".migration_completed"
        migration_flag.touch()
    
    # ============================================================================
    # BUILD REGISTRY METHODS
    # ============================================================================
    
    def get_latest_build(self, slice_key: str) -> Optional[str]:
        """
        Get the latest build directory for a slice.
        
        Args:
            slice_key: Slice identifier (e.g., 'AZ_M_U10')
            
        Returns:
            Build directory name if found, None otherwise
        """
        registry = self._load_build_registry()
        
        if slice_key in registry:
            return registry[slice_key]["latest_build"]
        
        # Auto-detect by scanning build folders
        self.logger.info(f"Slice {slice_key} not in registry, auto-detecting...")
        return self._auto_detect_latest_build(slice_key)
    
    def update_build_registry(self, slice_key: str, build_dir: str) -> None:
        """
        Update the build registry with a new build directory.
        
        Args:
            slice_key: Slice identifier (e.g., 'AZ_M_U10')
            build_dir: Build directory name (e.g., 'build_20251015_1348')
        """
        registry = self._load_build_registry()
        
        registry[slice_key] = {
            "latest_build": build_dir,
            "last_updated": datetime.now(timezone.utc).isoformat()
        }
        
        self._save_build_registry(registry)
        self.logger.info(f"Updated build registry for {slice_key}: {build_dir}")
    
    def list_all_builds(self) -> Dict[str, Any]:
        """
        Get all build registry entries.
        
        Returns:
            Dictionary with all build registry entries
        """
        return self._load_build_registry()
    
    def refresh_build_registry(self) -> None:
        """Force rebuild of build registry by scanning existing build folders."""
        self.logger.info("Refreshing build registry by scanning existing folders...")
        
        registry = {}
        games_dir = Path("data/games")
        
        if not games_dir.exists():
            self.logger.warning("Games directory does not exist")
            self._save_build_registry(registry)
            return
        
        # Scan all build directories
        for build_dir in sorted(games_dir.glob("build_*")):
            if not build_dir.is_dir():
                continue
                
            # Look for all CSV files in this build directory
            for csv_file in build_dir.glob("games_gotsport_*.csv"):
                # Extract slice_key: AZ_M_U10 from games_gotsport_AZ_M_U10.csv
                parts = csv_file.stem.split("_")
                if len(parts) >= 3:
                    slice_key = "_".join(parts[2:])  # Skip 'games' and 'gotsport'
                    
                    # Extract timestamp from build directory name
                    try:
                        timestamp_str = build_dir.name.replace("build_", "")
                        timestamp = datetime.strptime(timestamp_str, "%Y%m%d_%H%M")
                        
                        # Only keep the latest build for each slice
                        if slice_key not in registry:
                            registry[slice_key] = {
                                "latest_build": build_dir.name,
                                "last_updated": datetime.now(timezone.utc).isoformat()
                            }
                        else:
                            # Safely parse the stored latest build name
                            latest_name = registry[slice_key].get("latest_build")
                            if latest_name:
                                try:
                                    latest_timestamp_str = latest_name.replace("build_", "")
                                    latest_timestamp = datetime.strptime(latest_timestamp_str, "%Y%m%d_%H%M")
                                    if timestamp > latest_timestamp:
                                        registry[slice_key] = {
                                            "latest_build": build_dir.name,
                                            "last_updated": datetime.now(timezone.utc).isoformat()
                                        }
                                except ValueError:
                                    self.logger.warning(f"Could not parse timestamp from stored build name '{latest_name}', replacing with current build '{build_dir.name}'")
                                    registry[slice_key] = {
                                        "latest_build": build_dir.name,
                                        "last_updated": datetime.now(timezone.utc).isoformat()
                                    }
                            else:
                                # No stored build name, use current one
                                registry[slice_key] = {
                                    "latest_build": build_dir.name,
                                    "last_updated": datetime.now(timezone.utc).isoformat()
                                }
                    except ValueError:
                        self.logger.warning(f"Could not parse timestamp from {build_dir.name}")
                        continue
        
        self._save_build_registry(registry)
        self.logger.info(f"Registry refreshed with {len(registry)} slices")
    
    def _auto_detect_latest_build(self, slice_key: str) -> Optional[str]:
        """Auto-detect latest build for a slice by scanning folders."""
        games_dir = Path("data/games")
        if not games_dir.exists():
            return None
        
        # Find all build directories
        build_dirs = []
        for build_dir in games_dir.glob("build_*"):
            if not build_dir.is_dir():
                continue
                
            # Look for matching CSV file
            csv_pattern = f"games_gotsport_{slice_key}.csv"
            csv_file = build_dir / csv_pattern
            
            if csv_file.exists():
                # Extract timestamp from build directory name
                try:
                    # Parse build_YYYYMMDD_HHMM format
                    timestamp_str = build_dir.name.replace("build_", "")
                    timestamp = datetime.strptime(timestamp_str, "%Y%m%d_%H%M")
                    build_dirs.append((timestamp, build_dir.name))
                except ValueError:
                    self.logger.warning(f"Could not parse timestamp from {build_dir.name}")
                    continue
        
        if not build_dirs:
            self.logger.info(f"No existing builds found for {slice_key}")
            return None
        
        # Sort by timestamp (most recent first)
        build_dirs.sort(key=lambda x: x[0], reverse=True)
        latest_build = build_dirs[0][1]
        
        # Auto-add to registry
        self.logger.info(f"Auto-detected latest build for {slice_key}: {latest_build}")
        self.update_build_registry(slice_key, latest_build)
        
        return latest_build
    
    # ============================================================================
    # METADATA REGISTRY METHODS
    # ============================================================================
    
    def add_metadata_entry(self, entry: Dict[str, Any]) -> None:
        """
        Add a new metadata entry to the registry.
        
        Args:
            entry: Metadata dictionary to add
        """
        registry = self._load_metadata_registry()
        
        # Add timestamp if not provided
        if 'timestamp' not in entry:
            entry['timestamp'] = datetime.now().strftime("%Y-%m-%d_%H%M")
        
        # Add entry to registry
        registry.append(entry)
        
        self._save_metadata_registry(registry)
        self.logger.info(f"Added metadata entry: {entry.get('timestamp', 'unknown')}")
    
    def get_latest_metadata(self) -> Optional[Dict[str, Any]]:
        """
        Get the latest metadata entry.
        
        Returns:
            Latest metadata entry, or None if registry is empty
        """
        registry = self._load_metadata_registry()
        return registry[-1] if registry else None
    
    def get_metadata_by_timestamp(self, timestamp: str) -> Optional[Dict[str, Any]]:
        """
        Get a specific metadata entry by timestamp.
        
        Args:
            timestamp: Timestamp string to search for
            
        Returns:
            Matching metadata entry, or None if not found
        """
        registry = self._load_metadata_registry()
        
        for entry in registry:
            if entry.get('timestamp') == timestamp:
                return entry
        
        return None
    
    def get_metadata_summary(self) -> Dict[str, Any]:
        """
        Get a summary of all metadata entries.
        
        Returns:
            Summary dictionary with build counts, trends, etc.
        """
        registry = self._load_metadata_registry()
        
        if not registry:
            return {
                'total_builds': 0,
                'latest_timestamp': None,
                'latest_teams': 0,
                'latest_states': 0,
                'latest_quality': 0
            }
        
        latest = registry[-1]
        
        # Calculate trends
        team_counts = [entry.get('teams_total', 0) for entry in registry]
        state_counts = [entry.get('states_total', 0) for entry in registry]
        quality_scores = [entry.get('data_quality', 0) for entry in registry]
        
        return {
            'total_builds': len(registry),
            'latest_timestamp': latest.get('timestamp'),
            'latest_teams': latest.get('teams_total', 0),
            'latest_states': latest.get('states_total', 0),
            'latest_quality': latest.get('data_quality', 0),
            'team_trend': {
                'min': min(team_counts) if team_counts else 0,
                'max': max(team_counts) if team_counts else 0,
                'avg': sum(team_counts) / len(team_counts) if team_counts else 0,
                'growth': team_counts[-1] - team_counts[0] if len(team_counts) > 1 else 0
            },
            'state_trend': {
                'min': min(state_counts) if state_counts else 0,
                'max': max(state_counts) if state_counts else 0,
                'avg': sum(state_counts) / len(state_counts) if state_counts else 0
            },
            'quality_trend': {
                'min': min(quality_scores) if quality_scores else 0,
                'max': max(quality_scores) if quality_scores else 0,
                'avg': sum(quality_scores) / len(quality_scores) if quality_scores else 0
            }
        }
    
    # ============================================================================
    # HISTORY REGISTRY METHODS
    # ============================================================================
    
    def add_history_entry(self, build_info: Dict[str, Any], deltas: Dict[str, int]) -> None:
        """
        Add a new history entry to the registry.
        
        Args:
            build_info: Dictionary with build metadata
            deltas: Dictionary with delta counts (added, removed, renamed)
        """
        registry = self._load_history_registry()
        
        # Create new entry
        new_entry = {
            "timestamp": build_info.get("timestamp", datetime.now().strftime("%Y-%m-%d_%H%M")),
            "build_file": build_info.get("build_file", ""),
            "teams_total": build_info.get("teams_total", 0),
            "added": deltas.get("added", 0),
            "removed": deltas.get("removed", 0),
            "renamed": deltas.get("renamed", 0),
            "notes": build_info.get("notes", ""),
            "build_type": build_info.get("build_type", "incremental"),
            "duration_seconds": build_info.get("duration_seconds", 0),
            "providers": build_info.get("providers", ["GotSport"]),
            "states_covered": build_info.get("states_covered", 0)
        }
        
        # Add to registry
        registry.append(new_entry)
        
        # Keep only the last 20 builds
        max_entries = 20
        if len(registry) > max_entries:
            # Archive old entries before trimming
            old_entries = registry[:-max_entries]
            if old_entries:
                self._archive_old_entries(old_entries)
            
            registry = registry[-max_entries:]
            self.logger.info(f"Trimmed history registry to last {max_entries} builds")
        
        self._save_history_registry(registry)
        self.logger.info(f"Added history entry: {new_entry.get('timestamp', 'unknown')}")
    
    def add_games_build_entry(self, build_id: str, providers: List[str], slices: List[Dict[str, str]], 
                             results: List[Dict[str, Any]]) -> None:
        """
        Add a games build entry to the history registry.
        
        Args:
            build_id: Build identifier
            providers: List of providers used
            slices: List of slice combinations processed
            results: List of slice processing results
        """
        registry = self._load_history_registry()
        
        # Calculate build statistics
        total_teams = sum(r.get('teams_processed', 0) for r in results)
        total_games = sum(r.get('games_scraped', 0) for r in results)
        successful_slices = sum(1 for r in results if r.get('success', False))
        
        # Create build entry
        build_entry = {
            "build_id": build_id,
            "build_type": "games_build",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "providers": providers,
            "slices": slices,
            "total_slices": len(slices),
            "successful_slices": successful_slices,
            "total_teams_processed": total_teams,
            "total_games_scraped": total_games,
            "per_slice_results": results
        }
        
        # Add to registry
        registry.insert(0, build_entry)  # Add to beginning
        
        # Keep only the most recent entries
        max_entries = 20
        if len(registry) > max_entries:
            registry = registry[:max_entries]
        
        self._save_history_registry(registry)
        self.logger.info(f"Added games build to history registry: {build_id}")
    
    def get_build_history(self, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Get recent build history entries.
        
        Args:
            limit: Number of recent entries to return
            
        Returns:
            List of recent build history entries
        """
        registry = self._load_history_registry()
        return registry[-limit:] if registry else []
    
    def get_history_summary(self) -> Dict[str, Any]:
        """
        Get a summary of all history entries.
        
        Returns:
            Dictionary with build summary statistics
        """
        registry = self._load_history_registry()
        
        if not registry:
            return {
                "total_builds": 0,
                "latest_teams": 0,
                "latest_states": 0,
                "total_added": 0,
                "total_removed": 0,
                "total_renamed": 0
            }
        
        latest = registry[-1]
        
        summary = {
            "total_builds": len(registry),
            "latest_teams": latest.get("teams_total", 0),
            "latest_states": latest.get("states_covered", 0),
            "total_added": sum(entry.get("added", 0) for entry in registry),
            "total_removed": sum(entry.get("removed", 0) for entry in registry),
            "total_renamed": sum(entry.get("renamed", 0) for entry in registry),
            "latest_timestamp": latest.get("timestamp", ""),
            "latest_build_file": latest.get("build_file", ""),
            "avg_duration": sum(entry.get("duration_seconds", 0) for entry in registry) / len(registry),
            "build_types": list(set(entry.get("build_type", "incremental") for entry in registry))
        }
        
        return summary
    
    # ============================================================================
    # UNIFIED METHODS
    # ============================================================================
    
    def get_comprehensive_summary(self) -> Dict[str, Any]:
        """
        Get a comprehensive summary combining all registry types.
        
        Returns:
            Dictionary with comprehensive build summary
        """
        build_summary = self.list_all_builds()
        metadata_summary = self.get_metadata_summary()
        history_summary = self.get_history_summary()
        
        return {
            "build_registry": {
                "total_slices": len(build_summary),
                "latest_builds": build_summary
            },
            "metadata_registry": metadata_summary,
            "history_registry": history_summary,
            "system_status": {
                "migration_completed": self._migration_completed,
                "registry_path": str(self.base_path),
                "last_updated": datetime.now(timezone.utc).isoformat()
            }
        }
    
    def migrate_legacy_registries(self) -> None:
        """
        Migrate data from old registry locations to unified location.
        
        This is a one-time migration that moves data from:
        - data/master/metadata_registry.json -> data/registry/metadata_registry.json
        - data/master/history/history_registry.json -> data/registry/history_registry.json
        """
        if self._migration_completed:
            self.logger.info("Migration already completed, skipping...")
            return
        
        self.logger.info("Starting migration of legacy registries...")
        
        # Migrate metadata registry
        old_metadata_path = Path("data/master/metadata_registry.json")
        if old_metadata_path.exists():
            try:
                with open(old_metadata_path, 'r', encoding='utf-8') as f:
                    old_data = json.load(f)
                self._save_metadata_registry(old_data)
                self.logger.info(f"Migrated metadata registry from {old_metadata_path}")
            except Exception as e:
                self.logger.error(f"Failed to migrate metadata registry: {e}")
        
        # Migrate history registry
        old_history_path = Path("data/master/history/history_registry.json")
        if old_history_path.exists():
            try:
                with open(old_history_path, 'r', encoding='utf-8') as f:
                    old_data = json.load(f)
                self._save_history_registry(old_data)
                self.logger.info(f"Migrated history registry from {old_history_path}")
            except Exception as e:
                self.logger.error(f"Failed to migrate history registry: {e}")
        
        # Mark migration as completed
        self._mark_migration_completed()
        self.logger.info("Migration completed successfully!")
    
    # ============================================================================
    # PRIVATE HELPER METHODS
    # ============================================================================
    
    def _load_build_registry(self) -> Dict[str, Any]:
        """Load build registry from JSON file."""
        if not self.build_registry_path.exists():
            return {}
        
        try:
            with open(self.build_registry_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError) as e:
            self.logger.warning(f"Could not load build registry: {e}")
            return {}
    
    def _save_build_registry(self, registry: Dict[str, Any]) -> None:
        """Save build registry to JSON file with atomic write."""
        self._atomic_save(self.build_registry_path, registry)
    
    def _load_metadata_registry(self) -> List[Dict[str, Any]]:
        """Load metadata registry from JSON file."""
        if not self.metadata_registry_path.exists():
            return []
        
        try:
            with open(self.metadata_registry_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError) as e:
            self.logger.warning(f"Could not load metadata registry: {e}")
            return []
    
    def _save_metadata_registry(self, registry: List[Dict[str, Any]]) -> None:
        """Save metadata registry to JSON file."""
        self._atomic_save(self.metadata_registry_path, registry)
    
    def _load_history_registry(self) -> List[Dict[str, Any]]:
        """Load history registry from JSON file."""
        if not self.history_registry_path.exists():
            return []
        
        try:
            with open(self.history_registry_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError) as e:
            self.logger.warning(f"Could not load history registry: {e}")
            return []
    
    def _save_history_registry(self, registry: List[Dict[str, Any]]) -> None:
        """Save history registry to JSON file."""
        self._atomic_save(self.history_registry_path, registry)
    
    def _atomic_save(self, file_path: Path, data: Any) -> None:
        """Save data to file using atomic write pattern."""
        # Create unique temporary file
        temp_suffix = f".tmp.{os.getpid()}.{int(time.time() * 1000000)}.{uuid.uuid4().hex[:8]}"
        temp_path = file_path.with_suffix(temp_suffix)
        
        # Ensure temp file doesn't already exist
        MAX_TEMP_RETRIES = 10
        retry_count = 0
        while temp_path.exists():
            retry_count += 1
            if retry_count > MAX_TEMP_RETRIES:
                raise RuntimeError(f"Temp file collision after {MAX_TEMP_RETRIES} attempts")
            temp_suffix = f".tmp.{os.getpid()}.{int(time.time() * 1000000)}.{uuid.uuid4().hex[:8]}"
            temp_path = file_path.with_suffix(temp_suffix)
        
        try:
            # Write to temporary file
            cleaned_data = serialize_paths(data)
            
            with open(temp_path, 'w', encoding='utf-8') as f:
                json.dump(cleaned_data, f, indent=2, ensure_ascii=False)
            
            # Atomically rename to final destination
            MAX_RENAME_RETRIES = 3
            rename_retry = 0
            while rename_retry < MAX_RENAME_RETRIES:
                try:
                    temp_path.replace(file_path)
                    break
                except OSError as e:
                    rename_retry += 1
                    if rename_retry >= MAX_RENAME_RETRIES:
                        self.logger.error(f"Failed to rename {temp_path} to {file_path} after {MAX_RENAME_RETRIES} attempts: {e}")
                        raise
                    self.logger.warning(f"Rename attempt {rename_retry} failed: {e}, retrying...")
                    time.sleep(0.1 * rename_retry)  # Exponential backoff
            
            self.logger.debug(f"Successfully wrote registry: {file_path}")
            
        except Exception as e:
            # Clean up temporary file on error
            if temp_path.exists():
                temp_path.unlink()
            self.logger.error(f"Failed to write registry to {file_path}: {e}")
            raise
    
    def _archive_old_entries(self, old_entries: List[Dict[str, Any]]) -> None:
        """Archive old registry entries to compressed files."""
        import zipfile
        
        try:
            # Create archive directory
            archive_dir = Path("data/archive")
            archive_dir.mkdir(parents=True, exist_ok=True)
            
            # Create archive filename with timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M")
            archive_filename = f"archive_{timestamp}.zip"
            archive_path = archive_dir / archive_filename
            
            # Create ZIP archive
            with zipfile.ZipFile(archive_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                # Add registry entries as JSON
                entries_json = json.dumps(old_entries, indent=2)
                zipf.writestr("history_entries.json", entries_json)
                
                # Add metadata
                metadata = {
                    "archived_at": datetime.now().isoformat(),
                    "entries_count": len(old_entries),
                    "oldest_entry": old_entries[0].get("timestamp", "unknown") if old_entries else "none",
                    "newest_entry": old_entries[-1].get("timestamp", "unknown") if old_entries else "none"
                }
                metadata_json = json.dumps(metadata, indent=2)
                zipf.writestr("archive_metadata.json", metadata_json)
            
            self.logger.info(f"Archived {len(old_entries)} old entries to: {archive_path}")
            
        except Exception as e:
            self.logger.error(f"Failed to archive old entries: {e}")
            # Don't raise - archival failure shouldn't stop the main process


# ============================================================================
# CONVENIENCE FUNCTIONS (Backward Compatibility)
# ============================================================================

# Global registry instance
_registry_instance = None

def get_registry() -> UnifiedRegistry:
    """Get the global registry instance."""
    global _registry_instance
    if _registry_instance is None:
        _registry_instance = UnifiedRegistry()
    return _registry_instance

# Build registry convenience functions
def get_latest_build(slice_key: str) -> Optional[str]:
    """Get the latest build directory for a slice."""
    return get_registry().get_latest_build(slice_key)

def update_registry(slice_key: str, build_dir: str) -> None:
    """Update the build registry with a new build directory."""
    get_registry().update_build_registry(slice_key, build_dir)

def list_all_builds() -> Dict[str, Any]:
    """Get all build registry entries."""
    return get_registry().list_all_builds()

def refresh_registry() -> None:
    """Force rebuild of build registry by scanning existing build folders."""
    get_registry().refresh_build_registry()

# Metadata registry convenience functions
def create_build_entry(teams_total: int, states_total: int, data_quality: float,
                      source_file: str, master_file: str, timestamp: Optional[str] = None,
                      **kwargs) -> Dict[str, Any]:
    """Create a standardized metadata entry for a build."""
    entry = {
        'timestamp': timestamp or datetime.now().strftime("%Y-%m-%d_%H%M"),
        'teams_total': teams_total,
        'states_total': states_total,
        'data_quality': data_quality,
        'source_file': source_file,
        'master_file': master_file,
        'build_duration_seconds': kwargs.get('build_duration_seconds'),
        'providers': kwargs.get('providers', []),
        'age_groups': kwargs.get('age_groups', []),
        'genders': kwargs.get('genders', []),
        'notes': kwargs.get('notes', '')
    }
    return entry

def add_metadata_entry(entry: Dict[str, Any]) -> None:
    """Add a new metadata entry to the registry."""
    get_registry().add_metadata_entry(entry)

def get_latest_metadata() -> Optional[Dict[str, Any]]:
    """Get the latest metadata entry."""
    return get_registry().get_latest_metadata()

# History registry convenience functions
def update_history_registry(build_info: Dict[str, Any], deltas: Dict[str, int], 
                           logger: Optional[logging.Logger] = None) -> None:
    """Update the history registry with a new build entry."""
    get_registry().add_history_entry(build_info, deltas)

def add_games_build_to_registry(build_id: str, providers: List[str], slices: List[Dict[str, str]], 
                               results: List[Dict[str, Any]]) -> None:
    """Add a games build to the history registry."""
    get_registry().add_games_build_entry(build_id, providers, slices, results)

def get_build_summary() -> Dict[str, Any]:
    """Get build summary from history registry."""
    return get_registry().get_history_summary()


if __name__ == "__main__":
    # Test the unified registry
    import argparse
    
    parser = argparse.ArgumentParser(description="Unified Registry Test")
    parser.add_argument("--migrate", action="store_true", help="Migrate legacy registries")
    parser.add_argument("--summary", action="store_true", help="Show comprehensive summary")
    parser.add_argument("--refresh-builds", action="store_true", help="Refresh build registry")
    parser.add_argument("--test-slice", type=str, help="Test get_latest_build for a slice")
    
    args = parser.parse_args()
    
    # Set up logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    registry = UnifiedRegistry()
    
    if args.migrate:
        registry.migrate_legacy_registries()
    elif args.summary:
        summary = registry.get_comprehensive_summary()
        print(f"\nComprehensive Registry Summary:")
        print(f"Build Registry: {summary['build_registry']['total_slices']} slices")
        print(f"Metadata Registry: {summary['metadata_registry']['total_builds']} builds")
        print(f"History Registry: {summary['history_registry']['total_builds']} builds")
        print(f"Migration Status: {'Completed' if summary['system_status']['migration_completed'] else 'Pending'}")
    elif args.refresh_builds:
        registry.refresh_build_registry()
    elif args.test_slice:
        latest = registry.get_latest_build(args.test_slice)
        print(f"Latest build for {args.test_slice}: {latest}")
    else:
        print("Use --help for available options")
