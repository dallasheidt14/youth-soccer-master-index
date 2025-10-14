#!/usr/bin/env python3
"""
Delta Tracker - Detects changes between master index builds.

This module provides functionality to compare two master index builds and detect:
- Added teams (exist in new but not in old)
- Removed teams (exist in old but not in new) 
- Renamed teams (same metadata but different team names)

Author: Youth Soccer Master Index System
"""

import pandas as pd
import logging
from pathlib import Path
from typing import Dict, Optional, List, Tuple
import sys
import os

# Add project root to path for imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))

try:
    from rapidfuzz import fuzz, process
except ImportError:
    print("Warning: rapidfuzz not installed. Fuzzy matching will be disabled.")
    fuzz = None
    process = None


def compare_builds(new_df: pd.DataFrame, old_df: pd.DataFrame, logger: Optional[logging.Logger] = None) -> Dict[str, pd.DataFrame]:
    """
    Compare two master index builds and detect changes.
    
    Args:
        new_df: DataFrame with the new build data
        old_df: DataFrame with the old build data
        logger: Optional logger instance for output
        
    Returns:
        Dictionary containing:
        {
            "added": DataFrame with teams that exist only in new_df,
            "removed": DataFrame with teams that exist only in old_df,
            "renamed": DataFrame with teams that appear to be renamed
        }
    """
    if logger:
        logger.info("🔍 Starting delta comparison between builds")
        logger.info(f"📊 New build: {len(new_df):,} teams")
        logger.info(f"📊 Old build: {len(old_df):,} teams")
    
    # Define comparison columns (excluding team_name for renamed detection)
    comparison_cols = ["age_group", "gender", "state"]
    
    # Check if all comparison columns exist in both DataFrames
    missing_cols_new = [col for col in comparison_cols if col not in new_df.columns]
    missing_cols_old = [col for col in comparison_cols if col not in old_df.columns]
    
    if missing_cols_new:
        raise ValueError(f"Missing columns in new DataFrame: {missing_cols_new}")
    
    if missing_cols_old:
        raise ValueError(f"Missing columns in old DataFrame: {missing_cols_old}")
    
    # Create comparison keys for both DataFrames (excluding team_name)
    new_keys = new_df[comparison_cols].apply(lambda x: '|'.join(x.astype(str)), axis=1)
    old_keys = old_df[comparison_cols].apply(lambda x: '|'.join(x.astype(str)), axis=1)
    
    # Add keys as temporary columns
    new_df_temp = new_df.copy()
    old_df_temp = old_df.copy()
    new_df_temp['_comparison_key'] = new_keys
    old_df_temp['_comparison_key'] = old_keys
    
    # Find added teams (exist in new but not in old)
    added_mask = ~new_keys.isin(old_keys)
    added_df = new_df_temp[added_mask].drop('_comparison_key', axis=1).copy()
    
    # Find removed teams (exist in old but not in new)
    removed_mask = ~old_keys.isin(new_keys)
    removed_df = old_df_temp[removed_mask].drop('_comparison_key', axis=1).copy()
    
    # Find potentially renamed teams (same metadata but different team names)
    renamed_df = _detect_renamed_teams(new_df_temp, old_df_temp, logger)
    
    # Reset indices
    added_df = added_df.reset_index(drop=True)
    removed_df = removed_df.reset_index(drop=True)
    renamed_df = renamed_df.reset_index(drop=True)
    
    # Log summary
    if logger:
        logger.info("📈 Delta Summary:")
        logger.info(f"🆕 Added: {len(added_df):,} teams")
        logger.info(f"❌ Removed: {len(removed_df):,} teams")
        logger.info(f"✏️ Renamed: {len(renamed_df):,} teams")
        
        # Log breakdown by state for added teams
        if len(added_df) > 0:
            added_by_state = added_df['state'].value_counts()
            logger.info(f"🆕 Added by state: {dict(added_by_state.head(5))}")
        
        # Log breakdown by state for removed teams
        if len(removed_df) > 0:
            removed_by_state = removed_df['state'].value_counts()
            logger.info(f"❌ Removed by state: {dict(removed_by_state.head(5))}")
    
    return {
        "added": added_df,
        "removed": removed_df,
        "renamed": renamed_df
    }


def _detect_renamed_teams(new_df: pd.DataFrame, old_df: pd.DataFrame, logger: Optional[logging.Logger] = None) -> pd.DataFrame:
    """
    Detect potentially renamed teams using fuzzy matching.
    
    This function looks for teams that have the same metadata (age_group, gender, state)
    but different team names, suggesting they might be the same team with a name change.
    
    Args:
        new_df: DataFrame with new build data (includes _comparison_key)
        old_df: DataFrame with old build data (includes _comparison_key)
        logger: Optional logger instance
        
    Returns:
        DataFrame with potentially renamed teams
    """
    if logger:
        logger.info("🔍 Detecting potentially renamed teams...")
    
    renamed_teams = []
    
    # Group by comparison key to find teams with same metadata
    new_grouped = new_df.groupby('_comparison_key')
    old_grouped = old_df.groupby('_comparison_key')
    
    # Find keys that exist in both builds
    common_keys = set(new_df['_comparison_key'].unique()) & set(old_df['_comparison_key'].unique())
    
    for key in common_keys:
        new_teams = new_grouped.get_group(key)
        old_teams = old_grouped.get_group(key)
        
        # If different number of teams with same metadata, might be renamed
        if len(new_teams) != len(old_teams):
            # Use fuzzy matching to find potential renames
            if fuzz is not None and process is not None:
                for _, new_team in new_teams.iterrows():
                    old_names = old_teams['team_name'].tolist()
                    if old_names:
                        # Find best match using fuzzy matching
                        best_match = process.extractOne(new_team['team_name'], old_names)
                        if best_match and best_match[1] > 70:  # 70% similarity threshold
                            # Found a potential rename
                            old_team = old_teams[old_teams['team_name'] == best_match[0]].iloc[0]
                            renamed_teams.append({
                                'old_team_name': old_team['team_name'],
                                'new_team_name': new_team['team_name'],
                                'age_group': new_team['age_group'],
                                'gender': new_team['gender'],
                                'state': new_team['state'],
                                'similarity_score': best_match[1],
                                'old_rank': old_team.get('rank', 'N/A'),
                                'new_rank': new_team.get('rank', 'N/A'),
                                'old_points': old_team.get('points', 'N/A'),
                                'new_points': new_team.get('points', 'N/A')
                            })
    
    if logger and renamed_teams:
        logger.info(f"✏️ Found {len(renamed_teams)} potentially renamed teams")
        for rename in renamed_teams[:3]:  # Show first 3 examples
            logger.info(f"   '{rename['old_team_name']}' → '{rename['new_team_name']}' ({rename['similarity_score']:.1f}% match)")
    
    return pd.DataFrame(renamed_teams)


def save_deltas_to_csv(deltas: Dict[str, pd.DataFrame], timestamp: str, output_dir: str = "data/master/history") -> Dict[str, Path]:
    """
    Save delta DataFrames to CSV files.
    
    Args:
        deltas: Dictionary with 'added', 'removed', 'renamed' DataFrames
        timestamp: Timestamp string for file naming
        output_dir: Directory to save CSV files
        
    Returns:
        Dictionary mapping delta type to saved file path
    """
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    saved_files = {}
    
    for delta_type, df in deltas.items():
        if not df.empty:
            filename = f"{delta_type}_{timestamp}.csv"
            filepath = output_path / filename
            df.to_csv(filepath, index=False)
            saved_files[delta_type] = filepath
    
    return saved_files


def summarize_delta_trends(history_data: List[Dict], logger: Optional[logging.Logger] = None) -> Dict:
    """
    Analyze trends in delta data over time.
    
    Args:
        history_data: List of history registry entries
        logger: Optional logger instance
        
    Returns:
        Dictionary with trend analysis
    """
    if not history_data:
        return {}
    
    # Calculate cumulative changes
    total_added = sum(entry.get('added', 0) for entry in history_data)
    total_removed = sum(entry.get('removed', 0) for entry in history_data)
    total_renamed = sum(entry.get('renamed', 0) for entry in history_data)
    
    # Calculate growth rate
    if len(history_data) > 1:
        first_teams = history_data[0].get('teams_total', 0)
        last_teams = history_data[-1].get('teams_total', 0)
        growth_rate = ((last_teams - first_teams) / first_teams * 100) if first_teams > 0 else 0
    else:
        growth_rate = 0
    
    trends = {
        'total_builds': len(history_data),
        'total_added': total_added,
        'total_removed': total_removed,
        'total_renamed': total_renamed,
        'net_growth': total_added - total_removed,
        'growth_rate_percent': growth_rate,
        'avg_added_per_build': total_added / len(history_data) if history_data else 0,
        'avg_removed_per_build': total_removed / len(history_data) if history_data else 0
    }
    
    if logger:
        logger.info("📊 Delta Trends Summary:")
        logger.info(f"📈 Total builds: {trends['total_builds']}")
        logger.info(f"🆕 Total added: {trends['total_added']:,}")
        logger.info(f"❌ Total removed: {trends['total_removed']:,}")
        logger.info(f"✏️ Total renamed: {trends['total_renamed']:,}")
        logger.info(f"📊 Net growth: {trends['net_growth']:,}")
        logger.info(f"📈 Growth rate: {trends['growth_rate_percent']:.1f}%")
    
    return trends


if __name__ == "__main__":
    # Test the delta tracker with sample data
    print("Testing Delta Tracker...")
    
    # Create sample data
    old_data = {
        'team_name': ['Team Alpha', 'Team Beta', 'Team Gamma'],
        'age_group': ['U10', 'U11', 'U12'],
        'gender': ['Male', 'Female', 'Male'],
        'state': ['CA', 'TX', 'NY'],
        'rank': [1, 2, 3],
        'points': [100, 90, 80]
    }
    
    new_data = {
        'team_name': ['Team Alpha', 'Team Beta New', 'Team Delta'],
        'age_group': ['U10', 'U11', 'U13'],
        'gender': ['Male', 'Female', 'Male'],
        'state': ['CA', 'TX', 'FL'],
        'rank': [1, 2, 1],
        'points': [100, 95, 110]
    }
    
    old_df = pd.DataFrame(old_data)
    new_df = pd.DataFrame(new_data)
    
    # Test comparison
    deltas = compare_builds(new_df, old_df)
    
    print(f"Added: {len(deltas['added'])} teams")
    print(f"Removed: {len(deltas['removed'])} teams")
    print(f"Renamed: {len(deltas['renamed'])} teams")
    
    if not deltas['added'].empty:
        print("Added teams:")
        print(deltas['added'][['team_name', 'age_group', 'gender', 'state']])
    
    if not deltas['removed'].empty:
        print("Removed teams:")
        print(deltas['removed'][['team_name', 'age_group', 'gender', 'state']])
