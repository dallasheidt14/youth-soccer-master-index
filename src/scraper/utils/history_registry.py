#!/usr/bin/env python3
"""
History Registry - Maintains a structured registry of all master index builds.

This module provides functionality to track and manage the history of all master index builds,
including metadata about changes (added, removed, renamed teams) and build statistics.

Author: Youth Soccer Master Index System
"""

import json
import logging
from pathlib import Path
from typing import Dict, List, Optional, Any
from datetime import datetime
import sys
import os

# Add project root to path for imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))


class HistoryRegistry:
    """Manages the history registry for master index builds."""
    
    def __init__(self, registry_path: str = "data/master/history/history_registry.json"):
        """
        Initialize the history registry.
        
        Args:
            registry_path: Path to the JSON registry file
        """
        self.registry_path = Path(registry_path)
        self.max_entries = 20  # Keep last 20 builds
        
    def _ensure_history_dir(self) -> None:
        """Ensure the history directory exists."""
        self.registry_path.parent.mkdir(parents=True, exist_ok=True)
    
    def load_registry(self) -> List[Dict[str, Any]]:
        """
        Load the history registry from JSON file.
        
        Returns:
            List of build entries, or empty list if file doesn't exist
        """
        self._ensure_history_dir()
        
        if not self.registry_path.exists():
            return []
        
        try:
            with open(self.registry_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError) as e:
            print(f"Warning: Could not load history registry: {e}")
            return []
    
    def save_registry(self, registry_data: List[Dict[str, Any]]) -> None:
        """
        Save the history registry to JSON file.
        
        Args:
            registry_data: List of build entries to save
        """
        self._ensure_history_dir()
        
        try:
            with open(self.registry_path, 'w', encoding='utf-8') as f:
                json.dump(registry_data, f, indent=2, ensure_ascii=False)
        except IOError as e:
            print(f"Error: Could not save history registry: {e}")
            raise
    
    def update_history_registry(self, build_info: Dict[str, Any], deltas: Dict[str, int], logger: Optional[logging.Logger] = None) -> None:
        """
        Update the history registry with a new build entry.
        
        Args:
            build_info: Dictionary with build metadata (timestamp, build_file, teams_total, etc.)
            deltas: Dictionary with delta counts (added, removed, renamed)
            logger: Optional logger instance for output
        """
        # Load existing registry
        registry = self.load_registry()
        
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
        
        # Keep only the last max_entries builds
        if len(registry) > self.max_entries:
            registry = registry[-self.max_entries:]
            if logger:
                logger.info(f"📚 Trimmed history registry to last {self.max_entries} builds")
        
        # Save updated registry
        self.save_registry(registry)
        
        if logger:
            logger.info(f"✅ History registry updated (total builds: {len(registry)})")
            logger.info(f"📁 Registry saved to: {self.registry_path}")
    
    def get_last_build_info(self) -> Optional[Dict[str, Any]]:
        """
        Get information about the last build.
        
        Returns:
            Dictionary with last build info, or None if no builds exist
        """
        registry = self.load_registry()
        return registry[-1] if registry else None
    
    def get_build_summary(self) -> Dict[str, Any]:
        """
        Get a summary of all builds in the registry.
        
        Returns:
            Dictionary with build summary statistics
        """
        registry = self.load_registry()
        
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
    
    def get_recent_builds(self, count: int = 5) -> List[Dict[str, Any]]:
        """
        Get the most recent builds.
        
        Args:
            count: Number of recent builds to return
            
        Returns:
            List of recent build entries
        """
        registry = self.load_registry()
        return registry[-count:] if registry else []
    
    def search_builds(self, **filters) -> List[Dict[str, Any]]:
        """
        Search builds by criteria.
        
        Args:
            **filters: Key-value pairs to filter by (e.g., build_type="incremental")
            
        Returns:
            List of matching build entries
        """
        registry = self.load_registry()
        
        if not filters:
            return registry
        
        matching_builds = []
        for entry in registry:
            match = True
            for key, value in filters.items():
                if entry.get(key) != value:
                    match = False
                    break
            if match:
                matching_builds.append(entry)
        
        return matching_builds
    
    def export_to_csv(self, output_path: Optional[str] = None) -> Path:
        """
        Export the history registry to CSV format.
        
        Args:
            output_path: Optional custom output path
            
        Returns:
            Path to the exported CSV file
        """
        import pandas as pd
        
        registry = self.load_registry()
        
        if not registry:
            raise ValueError("No build history to export")
        
        df = pd.DataFrame(registry)
        
        if output_path is None:
            output_path = self.registry_path.parent / "history_registry.csv"
        else:
            output_path = Path(output_path)
        
        df.to_csv(output_path, index=False)
        return output_path


def update_history_registry(build_info: Dict[str, Any], deltas: Dict[str, int], logger: Optional[logging.Logger] = None) -> None:
    """
    Convenience function to update the history registry.
    
    Args:
        build_info: Dictionary with build metadata
        deltas: Dictionary with delta counts
        logger: Optional logger instance
    """
    registry = HistoryRegistry()
    registry.update_history_registry(build_info, deltas, logger)


def get_last_build_info() -> Optional[Dict[str, Any]]:
    """
    Convenience function to get the last build info.
    
    Returns:
        Dictionary with last build info, or None
    """
    registry = HistoryRegistry()
    return registry.get_last_build_info()


def get_build_summary() -> Dict[str, Any]:
    """
    Convenience function to get build summary.
    
    Returns:
        Dictionary with build summary statistics
    """
    registry = HistoryRegistry()
    return registry.get_build_summary()


if __name__ == "__main__":
    # Test the history registry
    print("Testing History Registry...")
    
    # Create test registry
    registry = HistoryRegistry("test_history_registry.json")
    
    # Test adding entries
    test_build_info = {
        "timestamp": "2025-10-14_1200",
        "build_file": "test_master.csv",
        "teams_total": 1000,
        "notes": "Test build",
        "build_type": "incremental",
        "duration_seconds": 30,
        "states_covered": 5
    }
    
    test_deltas = {
        "added": 50,
        "removed": 10,
        "renamed": 5
    }
    
    # Update registry
    registry.update_history_registry(test_build_info, test_deltas)
    
    # Test retrieval
    summary = registry.get_build_summary()
    print(f"Build summary: {summary}")
    
    # Test export
    csv_path = registry.export_to_csv()
    print(f"Exported to: {csv_path}")
    
    # Cleanup
    registry.registry_path.unlink(missing_ok=True)
    csv_path.unlink(missing_ok=True)
    
    print("History registry test completed successfully!")
