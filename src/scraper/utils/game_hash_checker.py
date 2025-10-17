#!/usr/bin/env python3
"""
Game Hash Checker Module

Detects when providers silently edit or delete historical game data by comparing
SHA256 hashes of game records over time.
"""

import json
import logging
import pandas as pd
import hashlib
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Set
import argparse
import sys

logger = logging.getLogger(__name__)

HASH_STORAGE_DIR = Path("data/game_history/hashes")
HASH_STORAGE_DIR.mkdir(parents=True, exist_ok=True)


def generate_game_hash(game_row: pd.Series) -> str:
    """
    Generate SHA256 hash for a game row using minimal columns.
    
    Args:
        game_row: Pandas Series containing game data
        
    Returns:
        SHA256 hash string
    """
    # Use minimal columns to detect meaningful changes
    hash_columns = [
        'team_id_source',
        'opponent_id', 
        'game_date',
        'goals_for',
        'goals_against',
        'home_away'
    ]
    
    # Build hash string from relevant columns
    hash_parts = []
    for col in hash_columns:
        if col in game_row.index:
            value = game_row[col]
            # Handle NaN values consistently
            if pd.isna(value):
                value = "NULL"
            hash_parts.append(f"{col}:{value}")
    
    # Create hash string
    hash_string = "|".join(hash_parts)
    
    # Generate SHA256 hash
    return hashlib.sha256(hash_string.encode('utf-8')).hexdigest()


def store_game_hashes(games_df: pd.DataFrame, slice_key: str, build_id: str) -> Optional[Path]:
    """
    Store game hashes to hash storage file.
    
    Args:
        games_df: DataFrame containing game data
        slice_key: Slice identifier (e.g., 'AZ_M_U10')
        build_id: Build identifier
        
    Returns:
        Path to stored hash file, or None if games_df is empty
    """
    logger.info(f"Storing game hashes for {slice_key} (build: {build_id})")
    
    if games_df.empty:
        logger.warning(f"No games data to hash for {slice_key}")
        return None
    
    # Generate hashes for all games
    game_hashes = {}
    for idx, game_row in games_df.iterrows():
        # Create unique game ID
        game_id = f"game_{game_row.get('team_id_source', 'unknown')}_{game_row.get('game_date', 'unknown')}"
        game_hash = generate_game_hash(game_row)
        game_hashes[game_id] = game_hash
    
    # Create hash storage data
    hash_data = {
        "slice_key": slice_key,
        "build_id": build_id,
        "last_updated": datetime.now(timezone.utc).isoformat(),
        "total_games": len(game_hashes),
        "games": game_hashes
    }
    
    # Save to file
    hash_file = HASH_STORAGE_DIR / f"{slice_key}.json"
    
    try:
        with open(hash_file, 'w', encoding='utf-8') as f:
            json.dump(hash_data, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Stored {len(game_hashes)} game hashes to {hash_file}")
        return hash_file
        
    except Exception as e:
        logger.error(f"Failed to store game hashes: {e}")
        raise


def load_game_hashes(slice_key: str) -> Optional[Dict[str, Any]]:
    """
    Load stored game hashes for a slice.
    
    Args:
        slice_key: Slice identifier
        
    Returns:
        Hash data dictionary or None if not found
    """
    hash_file = HASH_STORAGE_DIR / f"{slice_key}.json"
    
    if not hash_file.exists():
        logger.debug(f"No hash file found for {slice_key}")
        return None
    
    try:
        with open(hash_file, 'r', encoding='utf-8') as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError) as e:
        logger.error(f"Failed to load hash file for {slice_key}: {e}")
        return None


def check_game_integrity(slice_key: str, current_games_df: Optional[pd.DataFrame] = None) -> Dict[str, Any]:
    """
    Compare current games against stored hashes to detect changes.
    
    Args:
        slice_key: Slice identifier
        current_games_df: Optional current games DataFrame (if None, will load from latest build)
        
    Returns:
        Dictionary with integrity check results:
        - modified_games: List of games that changed
        - deleted_games: List of games that disappeared  
        - new_games: List of new games
        - needs_refresh: Boolean flag to trigger re-scrape
        - integrity_score: Percentage of unchanged games
    """
    logger.info(f"Checking game integrity for {slice_key}")
    
    # Load stored hashes
    stored_data = load_game_hashes(slice_key)
    if not stored_data:
        logger.warning(f"No stored hashes found for {slice_key}")
        return {
            'modified_games': [],
            'deleted_games': [],
            'new_games': [],
            'needs_refresh': True,
            'integrity_score': 0.0,
            'total_stored': 0,
            'total_current': 0
        }
    
    # Load current games if not provided
    if current_games_df is None:
        current_games_df = _load_current_games(slice_key)
        if current_games_df is None or current_games_df.empty:
            logger.warning(f"No current games found for {slice_key}")
            return {
                'modified_games': [],
                'deleted_games': list(stored_data['games'].keys()),
                'new_games': [],
                'needs_refresh': True,
                'integrity_score': 0.0,
                'total_stored': stored_data['total_games'],
                'total_current': 0
            }
    
    # Generate current hashes
    current_hashes = {}
    for idx, game_row in current_games_df.iterrows():
        game_id = f"game_{game_row.get('team_id_source', 'unknown')}_{game_row.get('game_date', 'unknown')}"
        game_hash = generate_game_hash(game_row)
        current_hashes[game_id] = game_hash
    
    # Compare hashes
    stored_hashes = stored_data['games']
    stored_game_ids = set(stored_hashes.keys())
    current_game_ids = set(current_hashes.keys())
    
    # Find differences
    modified_games = []
    deleted_games = []
    new_games = []
    
    # Check for modified games
    for game_id in stored_game_ids & current_game_ids:
        if stored_hashes[game_id] != current_hashes[game_id]:
            modified_games.append(game_id)
    
    # Check for deleted games
    deleted_games = list(stored_game_ids - current_game_ids)
    
    # Check for new games
    new_games = list(current_game_ids - stored_game_ids)
    
    # Calculate integrity score
    unchanged_games = len(stored_game_ids & current_game_ids) - len(modified_games)
    total_comparison_games = len(stored_game_ids | current_game_ids)
    integrity_score = (unchanged_games / total_comparison_games * 100) if total_comparison_games > 0 else 100.0
    
    # Determine if refresh is needed
    # Refresh if more than 5% of games changed or any games deleted
    change_threshold = 0.05
    needs_refresh = (
        len(modified_games) > len(stored_game_ids) * change_threshold or
        len(deleted_games) > 0 or
        integrity_score < 95.0
    )
    
    result = {
        'modified_games': modified_games,
        'deleted_games': deleted_games,
        'new_games': new_games,
        'needs_refresh': needs_refresh,
        'integrity_score': round(integrity_score, 2),
        'total_stored': len(stored_game_ids),
        'total_current': len(current_game_ids),
        'last_checked': datetime.now(timezone.utc).isoformat()
    }
    
    logger.info(f"Integrity check complete: {len(modified_games)} modified, {len(deleted_games)} deleted, {len(new_games)} new")
    logger.info(f"Integrity score: {integrity_score:.2f}%, needs_refresh: {needs_refresh}")
    
    return result


def _load_current_games(slice_key: str) -> Optional[pd.DataFrame]:
    """
    Load current games for a slice from the latest build.
    
    Args:
        slice_key: Slice identifier
        
    Returns:
        DataFrame with current games or None if not found
    """
    try:
        from src.registry.registry import get_registry
        
        registry = get_registry()
        latest_build = registry.get_latest_build(slice_key)
        
        if not latest_build:
            logger.warning(f"No latest build found for {slice_key}")
            return None
        
        # Construct games file path
        games_file = Path(f"data/games/{latest_build}/games_gotsport_{slice_key}.csv")
        
        if not games_file.exists():
            logger.warning(f"Games file not found: {games_file}")
            return None
        
        return pd.read_csv(games_file)
        
    except Exception as e:
        logger.error(f"Failed to load current games for {slice_key}: {e}")
        return None


def trigger_slice_refresh(slice_key: str) -> bool:
    """
    Auto-trigger a full refresh for affected slice.
    
    Args:
        slice_key: Slice identifier
        
    Returns:
        True if refresh was triggered successfully
    """
    logger.warning(f"Triggering full refresh for {slice_key} due to data integrity issues")
    
    try:
        # Update registry to force refresh
        from src.registry.registry import get_registry
        
        registry = get_registry()
        
        # Remove the slice from build registry to force re-scraping
        build_registry = registry._load_build_registry()
        if slice_key in build_registry:
            del build_registry[slice_key]
            registry._save_build_registry(build_registry)
            logger.info(f"Removed {slice_key} from build registry to trigger refresh")
        
        # Log the refresh trigger
        logger.warning(f"DATA INTEGRITY ALERT: {slice_key} marked for full refresh")
        
        return True
        
    except Exception as e:
        logger.error(f"Failed to trigger refresh for {slice_key}: {e}")
        return False


def check_all_slices() -> Dict[str, Any]:
    """
    Check integrity for all slices in the registry.
    
    Returns:
        Dictionary with overall integrity status
    """
    logger.info("Checking integrity for all slices")
    
    try:
        from src.registry.registry import get_registry
        
        registry = get_registry()
        build_registry = registry._load_build_registry()
        
        results = {}
        total_slices = len(build_registry)
        slices_needing_refresh = 0
        
        for slice_key in build_registry.keys():
            integrity_result = check_game_integrity(slice_key)
            results[slice_key] = integrity_result
            
            if integrity_result['needs_refresh']:
                slices_needing_refresh += 1
                logger.warning(f"Slice {slice_key} needs refresh (integrity: {integrity_result['integrity_score']}%)")
        
        overall_integrity = {
            'total_slices': total_slices,
            'slices_needing_refresh': slices_needing_refresh,
            'overall_integrity_score': round(sum(r['integrity_score'] for r in results.values()) / total_slices, 2) if total_slices > 0 else 100.0,
            'check_timestamp': datetime.now(timezone.utc).isoformat(),
            'slice_results': results
        }
        
        logger.info(f"Overall integrity check: {slices_needing_refresh}/{total_slices} slices need refresh")
        
        # Send Slack notification if there are issues
        try:
            from src.utils.notifier import notify_game_integrity_issues
            notify_game_integrity_issues(slices_needing_refresh, total_slices)
        except Exception as e:
            logger.warning(f"Failed to send Slack notification: {e}")
        
        return overall_integrity
        
    except Exception as e:
        logger.error(f"Failed to check all slices: {e}")
        return {'error': str(e)}


def main():
    """CLI entry point for game hash checker."""
    parser = argparse.ArgumentParser(description="Game Hash Integrity Checker")
    parser.add_argument("--slice", type=str, help="Check specific slice (e.g., AZ_M_U10)")
    parser.add_argument("--all-slices", action="store_true", help="Check all slices")
    parser.add_argument("--trigger-refresh", action="store_true", help="Trigger refresh for problematic slices")
    
    args = parser.parse_args()
    
    # Set up logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    try:
        if args.all_slices:
            # Check all slices
            results = check_all_slices()
            
            print(f"\nGame Integrity Check Results:")
            print(f"Total Slices: {results['total_slices']}")
            print(f"Slices Needing Refresh: {results['slices_needing_refresh']}")
            print(f"Overall Integrity Score: {results['overall_integrity_score']}%")
            
            if results['slices_needing_refresh'] > 0:
                print(f"\nWARNING: {results['slices_needing_refresh']} slices need attention!")
                
                if args.trigger_refresh:
                    for slice_key, slice_result in results['slice_results'].items():
                        if slice_result['needs_refresh']:
                            trigger_slice_refresh(slice_key)
            else:
                print("\nAll slices have good data integrity")
                
        elif args.slice:
            # Check specific slice
            result = check_game_integrity(args.slice)
            
            print(f"\nIntegrity Check for {args.slice}:")
            print(f"Modified Games: {len(result['modified_games'])}")
            print(f"Deleted Games: {len(result['deleted_games'])}")
            print(f"New Games: {len(result['new_games'])}")
            print(f"Integrity Score: {result['integrity_score']}%")
            print(f"Needs Refresh: {result['needs_refresh']}")
            
            if result['needs_refresh'] and args.trigger_refresh:
                trigger_slice_refresh(args.slice)
                
        else:
            print("Please specify --slice or --all-slices")
            parser.print_help()
    
    except Exception as e:
        logger.exception(f"Game hash check failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
