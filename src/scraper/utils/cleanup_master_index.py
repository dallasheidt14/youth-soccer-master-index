#!/usr/bin/env python3
"""
Cleanup Master Index - Remove Duplicates and Create Clean Dataset

This script removes duplicate teams from the master index and creates a clean
dataset with unique teams only.
"""

import pandas as pd
import logging
import sys
import os
from pathlib import Path
from typing import Optional

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))

from src.scraper.utils.logger import get_logger
from src.scraper.utils.file_utils import get_timestamp, safe_write_csv

def cleanup_master_index(input_file: Path, output_file: Optional[Path] = None, logger: Optional[logging.Logger] = None):
    """
    Clean up the master index by removing duplicates and keeping the latest entry.
    
    Args:
        input_file: Path to the input master index CSV
        output_file: Path to save the cleaned CSV (optional)
        logger: Logger instance for output
    
    Returns:
        Path to the cleaned CSV file
    """
    if logger is None:
        logger = get_logger(__name__)
    
    logger.info("🧹 Starting master index cleanup...")
    
    # Load the data
    logger.info(f"📂 Loading data from: {input_file}")
    df = pd.read_csv(input_file)
    logger.info(f"📊 Original data: {len(df):,} rows")
    
    # Check for duplicates
    duplicate_cols = ['team_name', 'age_group', 'gender', 'state']
    duplicates = df.duplicated(subset=duplicate_cols, keep=False)
    duplicate_count = duplicates.sum()
    
    logger.info(f"🔍 Found {duplicate_count:,} duplicate rows")
    
    if duplicate_count == 0:
        logger.info("✅ No duplicates found - data is already clean!")
        return input_file
    
    # Remove duplicates, keeping the row WITH team_id (not NaN)
    logger.info("🧹 Removing duplicates (keeping entries with team_id)...")
    
    # Sort by team_id (NaN values go to the end)
    df_sorted = df.sort_values('team_id', na_position='last')
    
    # Now drop duplicates, keeping the first occurrence (which will have team_id if available)
    df_clean = df_sorted.drop_duplicates(subset=duplicate_cols, keep='first')
    
    logger.info(f"📊 After cleanup: {len(df_clean):,} rows")
    logger.info(f"🗑️ Removed: {len(df) - len(df_clean):,} duplicate rows")
    
    # Check team_id coverage
    teams_with_id = df_clean['team_id'].notna().sum()
    teams_without_id = df_clean['team_id'].isna().sum()
    
    logger.info(f"🆔 Teams with team_id: {teams_with_id:,}")
    logger.info(f"❌ Teams without team_id: {teams_without_id:,}")
    
    # Generate output filename if not provided
    if output_file is None:
        timestamp = get_timestamp()
        output_file = input_file.parent / f"master_team_index_clean_{timestamp}.csv"
    
    # Save cleaned data
    logger.info(f"💾 Saving cleaned data to: {output_file}")
    safe_write_csv(df_clean, output_file)
    
    logger.info("✅ Master index cleanup completed!")
    logger.info(f"📁 Clean file: {output_file}")
    
    return output_file

def main():
    """Main function for command-line usage."""
    logger = get_logger(__name__)
    
    # Find the latest master index file
    master_dir = Path("data/master")
    master_files = list(master_dir.glob("master_team_index_USAonly_*.csv"))
    
    if not master_files:
        logger.error("❌ No master index files found!")
        return
    
    # Get the latest file
    latest_file = max(master_files, key=lambda x: x.stat().st_mtime)
    logger.info(f"📂 Using latest file: {latest_file}")
    
    # Clean up the master index
    clean_file = cleanup_master_index(latest_file, logger=logger)
    
    logger.info("🎉 Cleanup completed successfully!")

if __name__ == "__main__":
    main()
