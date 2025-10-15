#!/usr/bin/env python3
"""
Metrics Snapshot System

Captures and stores build metrics in JSON format for tracking data quality,
build performance, and historical trends across different builds.
"""

import json
from pathlib import Path
from typing import Dict, Any, Optional
from datetime import datetime
import logging

from src.scraper.utils.logger import get_logger
from src.io.safe_write import safe_write_json


def write_metrics_snapshot(build_id: str, metrics_dict: Dict[str, Any], 
                          logger: Optional[logging.Logger] = None) -> Path:
    """
    Write build metrics to a JSON snapshot file.
    
    Args:
        build_id: Unique identifier for the build (e.g., "20251014_1200")
        metrics_dict: Dictionary containing build metrics
        logger: Optional logger instance for output
        
    Returns:
        Path to the saved metrics file
        
    Example metrics_dict:
        {
            "build_id": "20251014_1200",
            "team_count": 91123,
            "new_teams": 187,
            "removed_teams": 14,
            "renamed_teams": 22,
            "states_covered": 49,
            "providers": ["gotsport"],
            "build_duration_seconds": 1250,
            "data_quality_score": 96.7
        }
    """
    if logger is None:
        logger = get_logger(__name__)
    
    # Ensure metrics directory exists
    metrics_dir = Path("data/metrics")
    metrics_dir.mkdir(parents=True, exist_ok=True)
    
    # Create metrics file path
    metrics_file = metrics_dir / f"build_{build_id}.json"
    
    # Add timestamp to metrics
    metrics_dict["timestamp"] = datetime.utcnow().isoformat()
    metrics_dict["build_id"] = build_id
    
    try:
        # Write metrics using safe write
        write_result = safe_write_json(metrics_dict, metrics_file, logger)
        
        logger.info(f"✅ Metrics snapshot written: {metrics_file}")
        logger.info(f"📊 Metrics summary:")
        logger.info(f"   Build ID: {build_id}")
        logger.info(f"   Team count: {metrics_dict.get('team_count', 'N/A'):,}")
        logger.info(f"   States covered: {metrics_dict.get('states_covered', 'N/A')}")
        logger.info(f"   New teams: {metrics_dict.get('new_teams', 'N/A')}")
        logger.info(f"   Removed teams: {metrics_dict.get('removed_teams', 'N/A')}")
        logger.info(f"   Renamed teams: {metrics_dict.get('renamed_teams', 'N/A')}")
        
        return metrics_file
        
    except Exception as e:
        logger.error(f"❌ Failed to write metrics snapshot: {e}")
        raise


def load_metrics_snapshot(build_id: str) -> Optional[Dict[str, Any]]:
    """
    Load metrics snapshot for a specific build.
    
    Args:
        build_id: Build identifier to load
        
    Returns:
        Dictionary containing metrics, or None if not found
    """
    metrics_file = Path("data/metrics") / f"build_{build_id}.json"
    
    if not metrics_file.exists():
        return None
    
    try:
        with open(metrics_file, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        logger = get_logger(__name__)
        logger.error(f"Failed to load metrics snapshot {build_id}: {e}")
        return None


def list_metrics_snapshots() -> list[Path]:
    """
    List all available metrics snapshot files.
    
    Returns:
        List of Path objects for metrics files, sorted by modification time (newest first)
    """
    metrics_dir = Path("data/metrics")
    
    if not metrics_dir.exists():
        return []
    
    metrics_files = list(metrics_dir.glob("build_*.json"))
    metrics_files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    
    return metrics_files


def get_latest_metrics() -> Optional[Dict[str, Any]]:
    """
    Get the most recent metrics snapshot.
    
    Returns:
        Dictionary containing latest metrics, or None if no snapshots exist
    """
    metrics_files = list_metrics_snapshots()
    
    if not metrics_files:
        return None
    
    latest_file = metrics_files[0]
    build_id = latest_file.stem.replace("build_", "")
    return load_metrics_snapshot(build_id)


def compare_metrics_snapshots(build_id1: str, build_id2: str) -> Dict[str, Any]:
    """
    Compare metrics between two builds.
    
    Args:
        build_id1: First build ID
        build_id2: Second build ID
        
    Returns:
        Dictionary containing comparison metrics
    """
    metrics1 = load_metrics_snapshot(build_id1)
    metrics2 = load_metrics_snapshot(build_id2)
    
    if not metrics1 or not metrics2:
        raise ValueError("One or both metrics snapshots not found")
    
    comparison = {
        "build1": build_id1,
        "build2": build_id2,
        "team_count_change": metrics2.get("team_count", 0) - metrics1.get("team_count", 0),
        "states_change": metrics2.get("states_covered", 0) - metrics1.get("states_covered", 0),
        "new_teams": metrics2.get("new_teams", 0),
        "removed_teams": metrics2.get("removed_teams", 0),
        "renamed_teams": metrics2.get("renamed_teams", 0),
        "build_duration_change": metrics2.get("build_duration_seconds", 0) - metrics1.get("build_duration_seconds", 0),
        "data_quality_change": metrics2.get("data_quality_score", 0) - metrics1.get("data_quality_score", 0)
    }
    
    return comparison


def generate_metrics_summary() -> Dict[str, Any]:
    """
    Generate a summary of all metrics snapshots.
    
    Returns:
        Dictionary containing summary statistics
    """
    metrics_files = list_metrics_snapshots()
    
    if not metrics_files:
        return {"total_builds": 0, "message": "No metrics snapshots found"}
    
    all_metrics = []
    for file_path in metrics_files:
        build_id = file_path.stem.replace("build_", "")
        metrics = load_metrics_snapshot(build_id)
        if metrics:
            all_metrics.append(metrics)
    
    if not all_metrics:
        return {"total_builds": 0, "message": "No valid metrics snapshots found"}
    
    # Calculate summary statistics
    team_counts = [m.get("team_count", 0) for m in all_metrics]
    states_counts = [m.get("states_covered", 0) for m in all_metrics]
    durations = [m.get("build_duration_seconds", 0) for m in all_metrics]
    
    summary = {
        "total_builds": len(all_metrics),
        "latest_build": all_metrics[0]["build_id"],
        "earliest_build": all_metrics[-1]["build_id"],
        "team_count_stats": {
            "min": min(team_counts),
            "max": max(team_counts),
            "avg": sum(team_counts) / len(team_counts),
            "latest": team_counts[0]
        },
        "states_stats": {
            "min": min(states_counts),
            "max": max(states_counts),
            "avg": sum(states_counts) / len(states_counts),
            "latest": states_counts[0]
        },
        "build_duration_stats": {
            "min": min(durations),
            "max": max(durations),
            "avg": sum(durations) / len(durations),
            "latest": durations[0]
        }
    }
    
    return summary


if __name__ == "__main__":
    """Test the metrics snapshot system."""
    logger = get_logger(__name__)
    
    print("Testing Metrics Snapshot System")
    print("=" * 50)
    
    # Test metrics snapshot creation
    test_metrics = {
        "team_count": 88707,
        "new_teams": 0,
        "removed_teams": 0,
        "renamed_teams": 0,
        "states_covered": 50,
        "providers": ["gotsport"],
        "build_duration_seconds": 1200,
        "data_quality_score": 98.5
    }
    
    test_build_id = "20251014_test"
    
    try:
        # Write test metrics
        metrics_file = write_metrics_snapshot(test_build_id, test_metrics, logger)
        print(f"✅ Test metrics written: {metrics_file}")
        
        # Load test metrics
        loaded_metrics = load_metrics_snapshot(test_build_id)
        print(f"✅ Test metrics loaded: {loaded_metrics['team_count']} teams")
        
        # List metrics files
        metrics_files = list_metrics_snapshots()
        print(f"✅ Found {len(metrics_files)} metrics files")
        
        # Get latest metrics
        latest = get_latest_metrics()
        if latest:
            print(f"✅ Latest metrics: {latest['build_id']}")
        
        # Generate summary
        summary = generate_metrics_summary()
        print(f"✅ Summary generated: {summary['total_builds']} builds")
        
        print("\nAll tests completed successfully!")
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        raise

