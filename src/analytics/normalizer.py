#!/usr/bin/env python3
"""
Game data normalization pipeline for the v53E ranking engine.

Consolidates games from build directories and normalizes schema for ranking analysis.
"""

import pandas as pd
import numpy as np
from pathlib import Path
from typing import List, Dict, Any, Optional
import logging
from datetime import datetime
import argparse

logger = logging.getLogger(__name__)


def normalize_build_games(build_dir: Path) -> pd.DataFrame:
    """
    Normalize games CSV from a single build directory.
    
    Args:
        build_dir: Path to build directory containing games CSV
        
    Returns:
        Normalized DataFrame with consistent schema
    """
    # Find games CSV in build directory
    games_files = list(build_dir.glob("games_*.csv"))
    if not games_files:
        raise FileNotFoundError(f"No games CSV found in {build_dir}")
    
    # Use the first games file found
    games_file = games_files[0]
    logger.info(f"Loading games from {games_file}")
    
    # Load games data
    df = pd.read_csv(games_file)
    
    # Map schema columns to normalized names
    column_mapping = {
        'team_name': 'team',
        'goals_for': 'gf', 
        'goals_against': 'ga',
        'game_date': 'date',
        'opponent_name': 'opponent',
        'opponent_id': 'opponent_id_master',
        'club_name': 'club'
    }
    
    # Rename columns if they exist
    for old_col, new_col in column_mapping.items():
        if old_col in df.columns:
            df = df.rename(columns={old_col: new_col})
    
    # Validate required columns exist
    required_cols = ['team_id_master', 'opponent_id_master', 'team', 'opponent', 
                     'club', 'state', 'gender', 'age_group', 'date', 'gf', 'ga']
    
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")
    
    # Convert date to datetime
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    
    # Drop rows with invalid dates
    initial_count = len(df)
    df = df.dropna(subset=['date'])
    if len(df) < initial_count:
        logger.warning(f"Dropped {initial_count - len(df)} rows with invalid dates")
    
    # Ensure numeric columns are properly typed
    df['gf'] = pd.to_numeric(df['gf'], errors='coerce')
    df['ga'] = pd.to_numeric(df['ga'], errors='coerce')
    
    # Drop rows with invalid goal data
    initial_count = len(df)
    df = df.dropna(subset=['gf', 'ga'])
    if len(df) < initial_count:
        logger.warning(f"Dropped {initial_count - len(df)} rows with invalid goal data")
    
    logger.info(f"Normalized {len(df)} games from {games_file}")
    return df


def consolidate_builds(input_root: Path, states: List[str], genders: List[str], 
                      ages: List[str], refresh: bool = False) -> pd.DataFrame:
    """
    Consolidate games from multiple build directories.
    
    Args:
        input_root: Root directory containing build subdirectories
        states: List of states to include
        genders: List of genders to include  
        ages: List of age groups to include
        refresh: If True, process all builds; if False, use only latest build
        
    Returns:
        Consolidated DataFrame with all games
    """
    # Find build directories
    build_dirs = [d for d in input_root.iterdir() if d.is_dir() and d.name.startswith('build_')]
    if not build_dirs:
        raise FileNotFoundError(f"No build directories found in {input_root}")
    
    # Sort by directory name (timestamp)
    build_dirs.sort(key=lambda x: x.name)
    
    if refresh:
        # Process all builds for refresh mode
        logger.info(f"Processing {len(build_dirs)} build directories for refresh...")
        builds_to_process = build_dirs
    else:
        # Use only latest build for normal mode
        latest_build = build_dirs[-1]
        logger.info(f"Using latest build directory: {latest_build.name}")
        builds_to_process = [latest_build]
    
    all_games = []
    
    # Process each build directory
    for build_dir in builds_to_process:
        logger.info(f"Processing build: {build_dir.name}")
        
        # Process each (state, gender, age) combination
        for state in states:
            for gender in genders:
                for age in ages:
                    # Look for games file matching pattern
                    pattern = f"games_*_{state}_{gender}_{age}.csv"
                    games_files = list(build_dir.glob(pattern))
                    
                    if not games_files:
                        logger.warning(f"No games file found for {state}_{gender}_{age} in {build_dir.name}")
                        continue
                    
                    games_file = games_files[0]
                    logger.info(f"Processing {games_file}")
                    
                    try:
                        # Load and normalize games from the specific file
                        df = pd.read_csv(games_file)
                        
                        # Map schema columns to normalized names
                        column_mapping = {
                            'team_name': 'team',
                            'goals_for': 'gf', 
                            'goals_against': 'ga',
                            'game_date': 'date',
                            'opponent_name': 'opponent',
                            'opponent_id': 'opponent_id_master',
                            'club_name': 'club'
                        }
                        
                        # Rename columns if they exist
                        for old_col, new_col in column_mapping.items():
                            if old_col in df.columns:
                                df = df.rename(columns={old_col: new_col})
                        
                        # Validate required columns exist
                        required_cols = ['team_id_master', 'opponent_id_master', 'team', 'opponent', 
                                         'club', 'state', 'gender', 'age_group', 'date', 'gf', 'ga']
                        
                        missing_cols = [col for col in required_cols if col not in df.columns]
                        if missing_cols:
                            raise ValueError(f"Missing required columns: {missing_cols}")
                        
                        # Convert date to datetime
                        df['date'] = pd.to_datetime(df['date'], errors='coerce')
                        
                        # Drop rows with invalid dates
                        initial_count = len(df)
                        df = df.dropna(subset=['date'])
                        if len(df) < initial_count:
                            logger.warning(f"Dropped {initial_count - len(df)} rows with invalid dates")
                        
                        # Ensure numeric columns are properly typed
                        df['gf'] = pd.to_numeric(df['gf'], errors='coerce')
                        df['ga'] = pd.to_numeric(df['ga'], errors='coerce')
                        
                        # Drop rows with invalid goal data
                        initial_count = len(df)
                        df = df.dropna(subset=['gf', 'ga'])
                        if len(df) < initial_count:
                            logger.warning(f"Dropped {initial_count - len(df)} rows with invalid goal data")
                        
                        logger.info(f"Normalized {len(df)} games from {games_file}")
                        
                        if not df.empty:
                            all_games.append(df)
                            logger.info(f"Added {len(df)} games for {state}_{gender}_{age} from {build_dir.name}")
                        
                    except Exception as e:
                        logger.error(f"Error processing {games_file}: {e}")
                        continue
    
    if not all_games:
        raise ValueError("No games found for any of the requested slices")
    
    # Concatenate all games
    consolidated = pd.concat(all_games, ignore_index=True)
    
    # Deduplicate by (team_id_master, opponent_id_master, date, gf, ga)
    initial_count = len(consolidated)
    consolidated = consolidated.drop_duplicates(
        subset=['team_id_master', 'opponent_id_master', 'date', 'gf', 'ga']
    )
    
    if len(consolidated) < initial_count:
        logger.info(f"Removed {initial_count - len(consolidated)} duplicate games")
    
    # Sort by date descending
    consolidated = consolidated.sort_values('date', ascending=False)
    
    logger.info(f"Consolidated {len(consolidated)} total games")
    return consolidated


def save_normalized(df: pd.DataFrame, output_dir: Path, timestamp: str) -> None:
    """
    Save normalized games data to parquet and CSV formats.
    
    Args:
        df: Normalized DataFrame
        output_dir: Output directory
        timestamp: Timestamp string for filename
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Save as parquet (preferred format)
    parquet_file = output_dir / f"games_normalized_{timestamp}.parquet"
    df.to_parquet(parquet_file, index=False)
    logger.info(f"Saved normalized games to {parquet_file}")
    
    # Save as CSV for debugging
    csv_file = output_dir / f"games_normalized_{timestamp}.csv"
    df.to_csv(csv_file, index=False)
    logger.info(f"Saved normalized games CSV to {csv_file}")
    
    # Log statistics
    logger.info(f"Normalized games statistics:")
    logger.info(f"  Total games: {len(df)}")
    logger.info(f"  Unique teams: {df['team_id_master'].nunique()}")
    logger.info(f"  Date range: {df['date'].min()} to {df['date'].max()}")
    logger.info(f"  States: {sorted(df['state'].unique())}")
    logger.info(f"  Genders: {sorted(df['gender'].unique())}")
    logger.info(f"  Age groups: {sorted(df['age_group'].unique())}")


def main():
    """CLI entry point for game normalization."""
    parser = argparse.ArgumentParser(description="Normalize game data for ranking engine")
    parser.add_argument("--input-root", type=Path, default=Path("data/games"),
                       help="Root directory containing build subdirectories")
    parser.add_argument("--states", type=str, default="AZ,NV",
                       help="Comma-separated list of states")
    parser.add_argument("--genders", type=str, default="M,F", 
                       help="Comma-separated list of genders")
    parser.add_argument("--ages", type=str, default="U10,U11,U12,U13,U14,U15,U16,U17,U18,U19",
                       help="Comma-separated list of age groups")
    parser.add_argument("--refresh", action="store_true",
                       help="Refresh normalized data from latest builds")
    parser.add_argument("--output-dir", type=Path, default=Path("data/games/normalized"),
                       help="Output directory for normalized data")
    
    args = parser.parse_args()
    
    # Parse comma-separated lists
    states = [s.strip() for s in args.states.split(',')]
    genders = [g.strip() for g in args.genders.split(',')]
    ages = [a.strip() for a in args.ages.split(',')]
    
    # Set up logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    try:
        # Consolidate games from builds
        consolidated = consolidate_builds(args.input_root, states, genders, ages, args.refresh)
        
        # Generate timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M")
        
        # Save normalized data
        save_normalized(consolidated, args.output_dir, timestamp)
        
        print(f"Normalization complete! Saved {len(consolidated)} games")
        
    except Exception as e:
        logger.error(f"Normalization failed: {e}")
        raise


if __name__ == "__main__":
    main()
