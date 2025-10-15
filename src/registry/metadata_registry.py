#!/usr/bin/env python3
"""
Metadata Registry for Master Team Index Builds

Maintains a JSON registry of every master build with key metrics for tracking
growth trends and data quality across runs.
"""

import json
import os
import logging
from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import datetime


class MetadataRegistry:
    """Manages metadata registry for master team index builds."""
    
    def __init__(self, registry_path: str = "data/master/metadata_registry.json"):
        """
        Initialize the metadata registry.
        
        Args:
            registry_path: Path to the JSON registry file
        """
        self.registry_path = Path(registry_path)
        self.logger = logging.getLogger("youth-soccer-index.metadata_registry")
        self._ensure_registry_dir()
    
    def _ensure_registry_dir(self) -> None:
        """Ensure the registry directory exists."""
        self.registry_path.parent.mkdir(parents=True, exist_ok=True)
    
    def load_registry(self) -> List[Dict[str, Any]]:
        """
        Load the metadata registry from JSON file.
        
        Returns:
            List of metadata entries, or empty list if file doesn't exist
        """
        if not self.registry_path.exists():
            return []
        
        try:
            with open(self.registry_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError) as e:
            self.logger.warning(f"Could not load registry from {self.registry_path}: {e}")
            return []
    
    def append_entry(self, entry: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Append a new metadata entry to the registry and save to JSON.
        
        Args:
            entry: Metadata dictionary to append
            
        Returns:
            Updated registry list
        """
        registry = self.load_registry()
        
        # Add timestamp if not provided
        if 'timestamp' not in entry:
            entry['timestamp'] = datetime.now().strftime("%Y-%m-%d_%H%M")
        
        # Add entry to registry
        registry.append(entry)
        
        # Save updated registry
        try:
            with open(self.registry_path, 'w', encoding='utf-8') as f:
                json.dump(registry, f, indent=2, ensure_ascii=False)
        except IOError as e:
            self.logger.error(f"Could not save registry to {self.registry_path}: {e}")
        else:
            self.logger.info(f"Registry updated -> {self.registry_path}")
        
        return registry
    
    def get_latest_entry(self) -> Optional[Dict[str, Any]]:
        """
        Get the latest metadata entry from the registry.
        
        Returns:
            Latest metadata entry, or None if registry is empty
        """
        registry = self.load_registry()
        if not registry:
            return None
        
        return registry[-1]
    
    def get_entry_by_timestamp(self, timestamp: str) -> Optional[Dict[str, Any]]:
        """
        Get a specific metadata entry by timestamp.
        
        Args:
            timestamp: Timestamp string to search for
            
        Returns:
            Matching metadata entry, or None if not found
        """
        registry = self.load_registry()
        
        for entry in registry:
            if entry.get('timestamp') == timestamp:
                return entry
        
        return None
    
    def get_build_summary(self) -> Dict[str, Any]:
        """
        Get a summary of all builds in the registry.
        
        Returns:
            Summary dictionary with build counts, trends, etc.
        """
        registry = self.load_registry()
        
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


def create_build_entry(
    teams_total: int,
    states_total: int,
    data_quality: float,
    source_file: str,
    master_file: str,
    timestamp: Optional[str] = None,
    **kwargs
) -> Dict[str, Any]:
    """
    Create a standardized metadata entry for a build.
    
    Args:
        teams_total: Total number of teams collected
        states_total: Total number of states/regions covered
        data_quality: Data quality score (0-100)
        source_file: Path to source data file
        master_file: Path to master index file
        timestamp: Optional timestamp override
        **kwargs: Additional metadata fields
        
    Returns:
        Metadata entry dictionary
    """
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


if __name__ == "__main__":
    # Test the metadata registry
    registry = MetadataRegistry()
    
    # Test entry
    test_entry = create_build_entry(
        teams_total=90355,
        states_total=71,
        data_quality=96.7,
        source_file="data/master/sources/gotsport_rankings_20251013_1840.csv",
        master_file="data/master/master_team_index_20251013_1840.csv",
        providers=["GotSport"],
        age_groups=["U10", "U11", "U12", "U13", "U14", "U15", "U16", "U17", "U18"],
        genders=["Male", "Female"],
        notes="First comprehensive nationwide build with pagination"
    )
    
    # Append test entry
    updated_registry = registry.append_entry(test_entry)
    
    # Get summary
    summary = registry.get_build_summary()
    print("\nBuild Summary:")
    print(f"Total builds: {summary['total_builds']}")
    print(f"Latest teams: {summary['latest_teams']:,}")
    print(f"Latest states: {summary['latest_states']}")
    print(f"Latest quality: {summary['latest_quality']}")
    
    if summary['total_builds'] > 1:
        print(f"Team growth: {summary['team_trend']['growth']:,}")
        print(f"Quality range: {summary['quality_trend']['min']:.1f} - {summary['quality_trend']['max']:.1f}")
