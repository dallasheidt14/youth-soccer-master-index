#!/usr/bin/env python3
"""
Multi-Provider Merge Policy

Handles merging of data from multiple providers (GotSport, Modular11, AthleteOne)
with conflict resolution, data completeness scoring, and provider tracking.
"""

import pandas as pd
import logging
from typing import Any, Dict, List, Optional, Tuple
from pathlib import Path
import sys
import os

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))

from src.scraper.utils.logger import get_logger
from src.utils.team_id_generator import make_team_id


def calculate_data_completeness_score(row: pd.Series) -> float:
    """
    Calculate a data completeness score for a row.
    
    Args:
        row: pandas Series representing a team record
        
    Returns:
        Float score between 0.0 and 1.0 (higher = more complete)
    """
    # Define fields and their weights
    field_weights = {
        'team_name': 0.25,
        'team_id': 0.20,
        'provider_team_id': 0.15,
        'age_group': 0.10,
        'age_u': 0.10,
        'gender': 0.10,
        'state': 0.10,
        'club_name': 0.05,
        'source_url': 0.05
    }
    
    total_score = 0.0
    total_weight = 0.0
    
    for field, weight in field_weights.items():
        if field in row.index:
            # Check if field has meaningful data (not null, not empty string)
            if pd.notna(row[field]) and str(row[field]).strip() != '':
                total_score += weight
            total_weight += weight
    
    return total_score / total_weight if total_weight > 0 else 0.0


def merge_provider_dataframes(provider_dfs: Dict[str, pd.DataFrame], 
                             logger: Optional[logging.Logger] = None) -> pd.DataFrame:
    """
    Merge data from multiple providers with conflict resolution.
    
    Args:
        provider_dfs: Dictionary mapping provider names to DataFrames
        logger: Optional logger instance for output
        
    Returns:
        Merged DataFrame with conflict resolution applied
    """
    if logger is None:
        logger = get_logger(__name__)
    
    logger.info("🔄 Starting multi-provider merge...")
    
    if not provider_dfs:
        logger.warning("⚠️ No provider data to merge")
        return pd.DataFrame()
    
    # Log provider information
    for provider, df in provider_dfs.items():
        logger.info(f"📊 {provider}: {len(df):,} teams")
    
    # Step 1: Ensure all DataFrames have team_id column
    logger.info("🆔 Ensuring all providers have team_id...")
    for provider, df in provider_dfs.items():
        if 'team_id' not in df.columns:
            logger.info(f"   Generating team_id for {provider}...")
            team_ids = []
            for _, row in df.iterrows():
                try:
                    team_id = make_team_id(
                        row['team_name'],
                        row['state'],
                        row['age_group'],
                        row['gender']
                    )
                    team_ids.append(team_id)
                except (KeyError, ValueError, AttributeError) as e:
                    logger.warning(f"   Failed to generate team_id for {row.get('team_name', '<unknown team>')}: {e}")
                    team_ids.append(None)
                except Exception as e:
                    logger.error(
                        f"   Unexpected error generating team_id for {row.get('team_name', '<unknown team>')}: {e}",
                        exc_info=True,
                    )
                    raise
            
            provider_dfs[provider] = df.assign(team_id=team_ids)
    
    # Step 2: Concatenate all DataFrames
    logger.info("📋 Concatenating provider DataFrames...")
    all_dfs = list(provider_dfs.values())
    merged_df = pd.concat(all_dfs, ignore_index=True)
    
    logger.info(f"📊 Total rows before deduplication: {len(merged_df):,}")
    
    # Step 3: Handle duplicates and conflicts
    logger.info("🔍 Resolving duplicates and conflicts...")
    resolved_df = resolve_merge_conflicts(merged_df, logger)
    
    logger.info(f"✅ Merge complete: {len(resolved_df):,} teams")
    return resolved_df


def resolve_merge_conflicts(df: pd.DataFrame, logger: Optional[logging.Logger] = None) -> pd.DataFrame:
    """
    Resolve conflicts when merging data from multiple providers.
    
    Args:
        df: DataFrame with potential conflicts
        logger: Optional logger instance for output
        
    Returns:
        DataFrame with conflicts resolved
    """
    if logger is None:
        logger = get_logger(__name__)
    
    # Group by team_id to identify conflicts
    grouped = df.groupby('team_id')
    
    resolved_rows = []
    conflict_count = 0
    
    for team_id, group in grouped:
        if len(group) == 1:
            # No conflict, keep the row
            resolved_rows.append(group.iloc[0])
        else:
            # Conflict detected - resolve using data completeness
            conflict_count += 1
            logger.debug(f"   Conflict for team_id {team_id}: {len(group)} records")
            
            # Calculate completeness scores
            group_with_scores = group.copy()
            group_with_scores['completeness_score'] = group.apply(calculate_data_completeness_score, axis=1)
            
            # Sort by completeness score (descending) and provider preference
            provider_preference = {'GotSport': 3, 'Modular11': 2, 'AthleteOne': 1}
            group_with_scores['provider_priority'] = group_with_scores['provider'].map(provider_preference).fillna(0)
            
            # Sort by completeness score first, then provider priority
            group_sorted = group_with_scores.sort_values(
                ['completeness_score', 'provider_priority'], 
                ascending=[False, False]
            )
            
            # Take the best row
            best_row = group_sorted.iloc[0].drop(['completeness_score', 'provider_priority'])
            
            # Create providers list for tracking
            providers = sorted(group['provider'].unique().tolist())
            best_row['providers'] = providers
            
            resolved_rows.append(best_row)
    
    if conflict_count > 0:
        logger.info(f"🔧 Resolved {conflict_count:,} conflicts using data completeness scoring")
    
    # Convert back to DataFrame
    resolved_df = pd.DataFrame(resolved_rows)
    
    # Ensure providers column exists for all rows
    if 'providers' not in resolved_df.columns:
        resolved_df['providers'] = resolved_df['provider'].apply(lambda x: [x])
    
    return resolved_df


def merge_incremental_with_baseline(new_teams_df: pd.DataFrame, 
                                  baseline_df: pd.DataFrame,
                                  logger: Optional[logging.Logger] = None) -> pd.DataFrame:
    """
    Merge new teams with baseline master index.
    
    Args:
        new_teams_df: DataFrame with newly detected teams
        baseline_df: DataFrame with existing baseline teams
        logger: Optional logger instance for output
        
    Returns:
        Combined DataFrame with new teams merged
    """
    if logger is None:
        logger = get_logger(__name__)
    
    logger.info("🔄 Merging new teams with baseline...")
    logger.info(f"📊 Baseline: {len(baseline_df):,} teams")
    logger.info(f"🆕 New teams: {len(new_teams_df):,} teams")
    
    if new_teams_df.empty:
        logger.info("✅ No new teams to merge")
        return baseline_df
    
    # Ensure both DataFrames have team_id
    for df_name, df in [("baseline", baseline_df), ("new_teams", new_teams_df)]:
        if 'team_id' not in df.columns:
            logger.warning(f"⚠️ {df_name} missing team_id column")
            return baseline_df
    
    # Check for duplicates between new teams and baseline
    baseline_team_ids = set(baseline_df['team_id'].dropna())
    new_team_ids = set(new_teams_df['team_id'].dropna())
    
    duplicate_ids = baseline_team_ids.intersection(new_team_ids)
    if duplicate_ids:
        logger.warning(f"⚠️ Found {len(duplicate_ids)} duplicate team_ids between new and baseline")
        # Remove duplicates from new teams
        new_teams_df = new_teams_df[~new_teams_df['team_id'].isin(duplicate_ids)]
        logger.info(f"📊 After deduplication: {len(new_teams_df):,} new teams")
    
    # Combine DataFrames
    combined_df = pd.concat([baseline_df, new_teams_df], ignore_index=True)
    
    logger.info(f"✅ Merge complete: {len(combined_df):,} total teams")
    return combined_df


def get_merge_summary(merged_df: pd.DataFrame, logger: Optional[logging.Logger] = None) -> Dict[str, Any]:
    """
    Get summary statistics for merged data.
    
    Args:
        merged_df: Merged DataFrame
        logger: Optional logger instance for output
        
    Returns:
        Dictionary with merge summary statistics
    """
    if logger is None:
        logger = get_logger(__name__)
    
    summary = {
        "total_teams": len(merged_df),
        "unique_team_ids": merged_df['team_id'].nunique() if 'team_id' in merged_df.columns else 0,
        "providers": merged_df['provider'].unique().tolist() if 'provider' in merged_df.columns else [],
        "states_covered": merged_df['state'].nunique() if 'state' in merged_df.columns else 0,
        "age_groups": sorted(merged_df['age_group'].unique().tolist()) if 'age_group' in merged_df.columns else [],
        "genders": sorted(merged_df['gender'].unique().tolist()) if 'gender' in merged_df.columns else []
    }
    
    # Provider distribution
    if 'provider' in merged_df.columns:
        provider_counts = merged_df['provider'].value_counts().to_dict()
        summary["provider_distribution"] = provider_counts
    
    # Multi-provider teams
    if 'providers' in merged_df.columns:
        multi_provider_teams = merged_df[merged_df['providers'].apply(lambda x: len(x) > 1)]
        summary["multi_provider_teams"] = len(multi_provider_teams)
        summary["multi_provider_percentage"] = len(multi_provider_teams) / len(merged_df) * 100 if len(merged_df) > 0 else 0
    
    # Data completeness
    if not merged_df.empty:
        completeness_scores = merged_df.apply(calculate_data_completeness_score, axis=1)
        summary["avg_completeness_score"] = completeness_scores.mean()
        summary["min_completeness_score"] = completeness_scores.min()
        summary["max_completeness_score"] = completeness_scores.max()
    
    if logger:
        logger.info("📊 Merge Summary:")
        logger.info(f"   Total teams: {summary['total_teams']:,}")
        logger.info(f"   Unique team IDs: {summary['unique_team_ids']:,}")
        logger.info(f"   Providers: {summary['providers']}")
        logger.info(f"   States covered: {summary['states_covered']}")
        if 'provider_distribution' in summary:
            logger.info(f"   Provider distribution: {summary['provider_distribution']}")
        if 'multi_provider_teams' in summary:
            logger.info(f"   Multi-provider teams: {summary['multi_provider_teams']:,} ({summary['multi_provider_percentage']:.1f}%)")
        if 'avg_completeness_score' in summary:
            logger.info(f"   Avg completeness: {summary['avg_completeness_score']:.2f}")
    
    return summary


if __name__ == "__main__":
    """Test the multi-provider merge policy."""
    logger = get_logger(__name__)
    
    print("Testing Multi-Provider Merge Policy")
    print("=" * 50)
    
    # Create test data
    gotsport_data = {
        'team_name': ['Team A', 'Team B', 'Team C'],
        'team_id': ['abc123', 'def456', 'ghi789'],
        'age_group': ['U12', 'U13', 'U14'],
        'gender': ['M', 'F', 'M'],
        'state': ['CA', 'TX', 'FL'],
        'provider': ['GotSport', 'GotSport', 'GotSport'],
        'provider_team_id': ['123', '456', '789'],
        'club_name': ['Club A', 'Club B', 'Club C']
    }
    
    modular11_data = {
        'team_name': ['Team B', 'Team D', 'Team E'],  # Team B is duplicate
        'team_id': ['def456', 'jkl012', 'mno345'],    # def456 is duplicate
        'age_group': ['U13', 'U15', 'U16'],
        'gender': ['F', 'M', 'F'],
        'state': ['TX', 'NY', 'WA'],
        'provider': ['Modular11', 'Modular11', 'Modular11'],
        'provider_team_id': ['456', '012', '345'],
        'club_name': ['Club B', 'Club D', 'Club E']
    }
    
    df_gotsport = pd.DataFrame(gotsport_data)
    df_modular11 = pd.DataFrame(modular11_data)
    
    provider_dfs = {
        'GotSport': df_gotsport,
        'Modular11': df_modular11
    }
    
    try:
        # Test merge
        merged_df = merge_provider_dataframes(provider_dfs, logger)
        print(f"✅ Merged DataFrame: {len(merged_df)} rows")
        
        # Test summary
        summary = get_merge_summary(merged_df, logger)
        print(f"✅ Summary generated: {summary['total_teams']} teams")
        
        print("\nAll tests completed successfully!")
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        raise

