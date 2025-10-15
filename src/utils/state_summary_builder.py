#!/usr/bin/env python3
"""
State-Level Coverage Tracker

Tracks team coverage, provider information, and build metadata for each US state.
Generates comprehensive state summaries for monitoring data completeness and trends.
"""

import pandas as pd
import json
from pathlib import Path
from typing import Dict, Any, Optional
from datetime import datetime
import logging

from src.scraper.utils.logger import get_logger
from src.io.safe_write import safe_write_json


def build_state_summaries(df: pd.DataFrame, build_id: str, 
                         logger: Optional[logging.Logger] = None) -> Path:
    """
    Build state-level summaries from team data.
    
    Args:
        df: DataFrame with team data (must include 'state', 'provider' columns)
        build_id: Build identifier for this summary
        logger: Optional logger instance for output
        
    Returns:
        Path to the saved state summaries JSON file
        
    Example output:
        {
            "build_id": "20251014_1200",
            "build_timestamp": "2025-10-14T12:00:00Z",
            "total_teams": 88707,
            "states": {
                "AZ": {
                    "teams": 1234,
                    "providers": ["gotsport"],
                    "last_build": "20251014_1200",
                    "age_groups": ["U10", "U11", "U12", "U13", "U14", "U15", "U16", "U17", "U18"],
                    "genders": ["M", "F"],
                    "top_teams": 10
                },
                "CA": {
                    "teams": 25951,
                    "providers": ["gotsport"],
                    "last_build": "20251014_1200",
                    "age_groups": ["U10", "U11", "U12", "U13", "U14", "U15", "U16", "U17", "U18"],
                    "genders": ["M", "F"],
                    "top_teams": 10
                }
            }
        }
    """
    if logger is None:
        logger = get_logger(__name__)
    
    logger.info("🏗️ Building state-level summaries...")
    
    # Validate required columns
    required_cols = ['state', 'provider']
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")
    
    # Initialize summary structure
    summary = {
        "build_id": build_id,
        "build_timestamp": datetime.utcnow().isoformat(),
        "total_teams": len(df),
        "states": {}
    }
    
    # Group by state and calculate statistics
    state_groups = df.groupby('state')
    
    for state, state_df in state_groups:
        logger.debug(f"Processing state: {state} ({len(state_df)} teams)")
        
        # Calculate state statistics
        state_summary = {
            "teams": len(state_df),
            "providers": sorted(state_df['provider'].unique().tolist()),
            "last_build": build_id,
            "age_groups": sorted(state_df['age_group'].unique().tolist()) if 'age_group' in state_df.columns else [],
            "genders": sorted(state_df['gender'].unique().tolist()) if 'gender' in state_df.columns else [],
            "top_teams": min(10, len(state_df))  # Number of teams (for display purposes)
        }
        
        # Add age group breakdown if available
        if 'age_group' in state_df.columns:
            age_breakdown = state_df['age_group'].value_counts().to_dict()
            state_summary["age_breakdown"] = {str(k): int(v) for k, v in age_breakdown.items()}
        
        # Add gender breakdown if available
        if 'gender' in state_df.columns:
            gender_breakdown = state_df['gender'].value_counts().to_dict()
            state_summary["gender_breakdown"] = {str(k): int(v) for k, v in gender_breakdown.items()}
        
        # Add provider breakdown if available
        provider_breakdown = state_df['provider'].value_counts().to_dict()
        state_summary["provider_breakdown"] = {str(k): int(v) for k, v in provider_breakdown.items()}
        
        summary["states"][state] = state_summary
    
    # Add overall statistics
    if summary["states"]:
        summary["statistics"] = {
            "total_states": len(summary["states"]),
            "avg_teams_per_state": sum(s["teams"] for s in summary["states"].values()) / len(summary["states"]),
            "max_teams_state": max(summary["states"].items(), key=lambda x: x[1]["teams"]),
            "min_teams_state": min(summary["states"].items(), key=lambda x: x[1]["teams"]),
            "total_providers": len(set().union(*[s["providers"] for s in summary["states"].values()]))
        }
    else:
        summary["statistics"] = {
            "total_states": 0,
            "avg_teams_per_state": 0,
            "max_teams_state": None,
            "min_teams_state": None,
            "total_providers": 0
        }
    
    # Save state summaries
    summaries_file = Path("data/master/state_summaries.json")
    safe_write_json(summary, summaries_file, logger)
    
    logger.info(f"✅ State summaries saved: {summaries_file}")
    logger.info(f"📊 Summary statistics:")
    logger.info(f"   Total states: {summary['statistics']['total_states']}")
    logger.info(f"   Total teams: {summary['total_teams']:,}")
    logger.info(f"   Avg teams per state: {summary['statistics']['avg_teams_per_state']:.0f}")
    
    # Safely log max/min teams state info
    max_state = summary['statistics']['max_teams_state']
    if max_state is not None:
        logger.info(f"   Max teams state: {max_state[0]} ({max_state[1]['teams']:,})")
    else:
        logger.info(f"   Max teams state: N/A")
    
    min_state = summary['statistics']['min_teams_state']
    if min_state is not None:
        logger.info(f"   Min teams state: {min_state[0]} ({min_state[1]['teams']:,})")
    else:
        logger.info(f"   Min teams state: N/A")
    
    return summaries_file


def load_state_summaries() -> Optional[Dict[str, Any]]:
    """
    Load the current state summaries.
    
    Returns:
        Dictionary containing state summaries, or None if not found
    """
    summaries_file = Path("data/master/state_summaries.json")
    
    if not summaries_file.exists():
        return None
    
    try:
        with open(summaries_file, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        logger = get_logger(__name__)
        logger.error(f"Failed to load state summaries: {e}")
        return None


def get_state_coverage(state_code: str) -> Optional[Dict[str, Any]]:
    """
    Get coverage information for a specific state.
    
    Args:
        state_code: 2-letter state code (e.g., "CA", "TX")
        
    Returns:
        Dictionary containing state coverage info, or None if not found
    """
    summaries = load_state_summaries()
    
    if not summaries or "states" not in summaries:
        return None
    
    return summaries["states"].get(state_code.upper())


def compare_state_coverage(build_id1: str, build_id2: str) -> Dict[str, Any]:
    # TODO(FIXME): Historical comparison is not implemented. This function is a
    # stub until we persist versioned state summaries per build (e.g.,
    # `data/master/state_summaries_YYYYMMDD_HHMM.json`) or add a registry to
    # retrieve prior snapshots by `build_id`. Next steps:
    # 1) Persist per-build state summary snapshots and index them by build_id.
    # 2) Load snapshots for `build_id1` and `build_id2`, then compute deltas.
    # 3) Consider raising NotImplementedError until persistence is available.
    # Optionally track via issue: GH-STATE-SUMMARIES-HISTORY (or PR link).
    """
    Compare state coverage between two builds. STUB: This is intentionally a
    placeholder until historical state summaries are persisted and retrievable
    by `build_id`.

    Args:
        build_id1: First build ID
        build_id2: Second build ID
        
    Returns:
        Dictionary containing comparison results (placeholder structure for now).

    Note:
        This function currently returns a placeholder. Once per-build state
        summary snapshots are stored, implement loading snapshots for both
        build IDs and computing differences (e.g., team counts, providers,
        coverage deltas). Consider raising NotImplementedError until the
        persistence layer lands.
    """
    # This would require storing historical state summaries
    # For now, return a placeholder structure
    return {
        "build1": build_id1,
        "build2": build_id2,
        "message": "Historical state comparison not yet implemented",
        "note": "State summaries are currently overwritten with each build"
    }


def get_coverage_report() -> Dict[str, Any]:
    """
    Generate a comprehensive coverage report.
    
    Returns:
        Dictionary containing coverage analysis
    """
    summaries = load_state_summaries()
    
    if not summaries:
        return {"error": "No state summaries found"}
    
    states = summaries.get("states", {})
    
    # Calculate coverage metrics
    total_teams = sum(state["teams"] for state in states.values())
    state_counts = [state["teams"] for state in states.values()]
    
    # Identify states with low coverage
    avg_teams = total_teams / len(states) if states else 0
    low_coverage_threshold = avg_teams * 0.5  # 50% below average
    
    low_coverage_states = [
        {"state": state, "teams": state_data["teams"]}
        for state, state_data in states.items()
        if state_data["teams"] < low_coverage_threshold
    ]
    
    # Identify states with high coverage
    high_coverage_threshold = avg_teams * 1.5  # 50% above average
    
    high_coverage_states = [
        {"state": state, "teams": state_data["teams"]}
        for state, state_data in states.items()
        if state_data["teams"] > high_coverage_threshold
    ]
    
    report = {
        "build_id": summaries.get("build_id", "unknown"),
        "build_timestamp": summaries.get("build_timestamp", "unknown"),
        "total_states": len(states),
        "total_teams": total_teams,
        "avg_teams_per_state": avg_teams,
        "coverage_distribution": {
            "min": min(state_counts) if state_counts else 0,
            "max": max(state_counts) if state_counts else 0,
            "median": sorted(state_counts)[len(state_counts)//2] if state_counts else 0
        },
        "low_coverage_states": sorted(low_coverage_states, key=lambda x: x["teams"]),
        "high_coverage_states": sorted(high_coverage_states, key=lambda x: x["teams"], reverse=True),
        "provider_coverage": {
            provider: len([s for s in states.values() if provider in s["providers"]])
            for provider in set().union(*[s["providers"] for s in states.values()])
        }
    }
    
    return report


if __name__ == "__main__":
    """Test the state coverage tracker."""
    logger = get_logger(__name__)
    
    print("Testing State Coverage Tracker")
    print("=" * 50)
    
    # Create test data
    test_data = {
        'state': ['CA', 'CA', 'TX', 'TX', 'TX', 'NY', 'NY', 'FL', 'FL', 'FL'],
        'provider': ['gotsport', 'gotsport', 'gotsport', 'gotsport', 'gotsport', 
                    'gotsport', 'gotsport', 'gotsport', 'gotsport', 'gotsport'],
        'age_group': ['U10', 'U11', 'U10', 'U11', 'U12', 'U10', 'U11', 'U10', 'U11', 'U12'],
        'gender': ['M', 'F', 'M', 'F', 'M', 'M', 'F', 'M', 'F', 'M'],
        'team_name': [f'Team {i}' for i in range(10)]
    }
    
    df = pd.DataFrame(test_data)
    
    try:
        # Build state summaries
        summaries_file = build_state_summaries(df, "20251014_test", logger)
        print(f"✅ State summaries built: {summaries_file}")
        
        # Load summaries
        summaries = load_state_summaries()
        print(f"✅ State summaries loaded: {len(summaries['states'])} states")
        
        # Get specific state coverage
        ca_coverage = get_state_coverage("CA")
        print(f"✅ CA coverage: {ca_coverage['teams']} teams")
        
        # Generate coverage report
        report = get_coverage_report()
        print(f"✅ Coverage report generated: {report['total_states']} states")
        
        print("\nAll tests completed successfully!")
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        raise

