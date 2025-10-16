#!/usr/bin/env python3
"""
Master Slice Generator

Creates pre-filtered slice CSVs from the master team index for game history scraping.
Each slice contains teams for a specific state/gender/age_group combination.
"""

import pandas as pd
import argparse
import logging
from pathlib import Path
from typing import List, Tuple
import sys
import os

# Add src to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

# from schema.master_team_schema import validate_dataframe
from schema.master_team_schema import validate_dataframe


def setup_logging():
    """Setup logging configuration."""
    # Ensure log directory exists
    log_file_path = 'data/logs/generate_master_slices.log'
    os.makedirs(os.path.dirname(log_file_path), exist_ok=True)
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file_path),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)


def load_master_index(master_file: Path) -> pd.DataFrame:
    """
    Load and validate the master team index.
    
    Args:
        master_file: Path to master team index CSV
        
    Returns:
        Validated DataFrame
    """
    logger = logging.getLogger(__name__)
    
    if not master_file.exists():
        raise FileNotFoundError(f"Master index file not found: {master_file}")
    
    logger.info(f"Loading master index from {master_file}")
    df = pd.read_csv(master_file)
    
    logger.info(f"Loaded {len(df)} teams from master index")
    logger.info(f"Columns: {list(df.columns)}")
    
    # Validate against schema
    try:
        validated_df = validate_dataframe(df)
        logger.info("Master index validation passed")
        return validated_df
    except Exception as e:
        logger.exception(f"Master index validation failed: {e}")
        logger.warning("Continuing without validation - data quality may be compromised")
        logger.warning("Continuing without validation - data quality may be compromised")
        return df


def generate_slice_combinations(states: List[str], genders: List[str], ages: List[str]) -> List[Tuple[str, str, str]]:
    """
    Generate all combinations of state/gender/age_group.
    
    Args:
        states: List of state codes
        genders: List of genders (M/F)
        ages: List of age groups (U10, U11, etc.)
        
    Returns:
        List of (state, gender, age_group) tuples
    """
    combinations = []
    for state in states:
        for gender in genders:
            for age in ages:
                combinations.append((state, gender, age))
    return combinations


def create_slice(df: pd.DataFrame, state: str, gender: str, age_group: str, output_dir: Path) -> Path:
    """
    Create a slice CSV for specific state/gender/age_group combination.
    
    Args:
        df: Master team index DataFrame
        state: State code (e.g., 'AZ')
        gender: Gender (M/F)
        age_group: Age group (e.g., 'U10')
        output_dir: Output directory for slice files
        
    Returns:
        Path to created slice file
    """
    logger = logging.getLogger(__name__)
    
    # Filter teams for this combination
    slice_df = df[
        (df['state'] == state) & 
        (df['gender'] == gender) & 
        (df['age_group'] == age_group)
    ].copy()
    
    if len(slice_df) == 0:
        logger.warning(f"No teams found for {state} {gender} {age_group}")
        return None
    
    # Select and rename columns for game scraping
    slice_df = slice_df[[
        'team_id', 'provider_team_id', 'team_name', 'club_name', 
        'state', 'gender', 'age_group', 'provider'
    ]].copy()
    
    # Rename columns for game scraping context
    slice_df = slice_df.rename(columns={
        'team_id': 'team_id_master',
        'provider_team_id': 'team_id_source'
    })
    
    # Create output filename
    filename = f"{state}_{gender}_{age_group}_master.csv"
    output_path = output_dir / filename
    
    # Write slice CSV
    slice_df.to_csv(output_path, index=False)
    
    logger.info(f"Created slice {filename}: {len(slice_df)} teams")
    logger.info(f"  Teams: {slice_df['team_name'].tolist()[:3]}{'...' if len(slice_df) > 3 else ''}")
    
    return output_path


def main():
    """Main function."""
    parser = argparse.ArgumentParser(description="Generate master team slices for game history scraping")
    parser.add_argument('--master-file', 
                       default='data/master/master_team_index_migrated_20251014_1717.csv',
                       help='Path to master team index CSV')
    parser.add_argument('--states', 
                       default='AZ',
                       help='Comma-separated state codes (e.g., AZ,CA,TX)')
    parser.add_argument('--genders', 
                       default='M,F',
                       help='Comma-separated genders (M,F)')
    parser.add_argument('--ages', 
                       default='U10',
                       help='Comma-separated age groups (U10,U11,U12)')
    parser.add_argument('--output-dir', 
                       default='data/master/slices',
                       help='Output directory for slice files')
    
    args = parser.parse_args()
    
    # Setup logging
    logger = setup_logging()
    
    # Parse arguments
    states = [s.strip().upper() for s in args.states.split(',')]
    genders = [g.strip().upper() for g in args.genders.split(',')]
    ages = [a.strip().upper() for a in args.ages.split(',')]
    
    logger.info(f"Generating slices for:")
    logger.info(f"  States: {states}")
    logger.info(f"  Genders: {genders}")
    logger.info(f"  Ages: {ages}")
    
    # Create output directory
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Load master index
    master_file = Path(args.master_file)
    df = load_master_index(master_file)
    
    # Generate combinations
    combinations = generate_slice_combinations(states, genders, ages)
    logger.info(f"Will create {len(combinations)} slice files")
    
    # Create slices
    created_slices = []
    for state, gender, age_group in combinations:
        try:
            slice_path = create_slice(df, state, gender, age_group, output_dir)
            if slice_path:
                created_slices.append(slice_path)
        except Exception as e:
            logger.error(f"Failed to create slice {state}_{gender}_{age_group}: {e}")
    
    # Summary
    logger.info(f"Created {len(created_slices)} slice files:")
    for slice_path in created_slices:
        logger.info(f"  {slice_path}")
    
    # Show team counts by slice
    logger.info("\nSlice team counts:")
    for slice_path in created_slices:
        slice_df = pd.read_csv(slice_path)
        logger.info(f"  {slice_path.name}: {len(slice_df)} teams")


if __name__ == "__main__":
    main()
