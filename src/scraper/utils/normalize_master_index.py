#!/usr/bin/env python3
"""
Master Index Normalization Tool

A standalone command-line utility to reorganize and clean an existing master_team_index file.
No scraping required - operates on existing CSV files.
"""

import pandas as pd
from pathlib import Path
import sys
import os
import json
from collections import defaultdict
from datetime import datetime

# Add project root to Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))
sys.path.append(project_root)

try:
    from src.scraper.utils.logger import get_logger
    from src.scraper.utils.file_utils import safe_write_csv, get_timestamp
    from src.scraper.utils.state_normalizer import normalize_states
except ImportError as e:
    print(f"Import error: {e}")
    print(f"Project root: {project_root}")
    sys.exit(1)


def find_latest_master_index(data_dir: str = "data/master") -> Path:
    """
    Auto-detect the latest master index file.
    
    Args:
        data_dir: Directory to search for master index files
        
    Returns:
        Path to the latest master index file
        
    Raises:
        FileNotFoundError: If no master index files found
    """
    data_path = Path(data_dir)
    
    if not data_path.exists():
        raise FileNotFoundError(f"Data directory not found: {data_path}")
    
    # Find all CSV files starting with "master_team_index_"
    master_files = list(data_path.glob("master_team_index_*.csv"))
    
    if not master_files:
        raise FileNotFoundError(f"No master index files found in {data_path}")
    
    # Sort by modification time (newest first)
    latest_file = max(master_files, key=lambda f: f.stat().st_mtime)
    
    return latest_file


def main(input_path: str = None):
    """
    Main normalization function.
    
    Args:
        input_path: Optional path to input CSV file. If None, auto-detects latest.
    """
    # Initialize logger
    logger = get_logger("normalize_master_index.log")
    
    try:
        # Determine input file
        if input_path is None:
            logger.info("🔍 Auto-detecting latest master index file...")
            input_file = find_latest_master_index()
            logger.info(f"📁 Found latest file: {input_file}")
        else:
            input_file = Path(input_path)
            if not input_file.exists():
                raise FileNotFoundError(f"Input file not found: {input_file}")
            logger.info(f"📁 Using specified file: {input_file}")
        
        # Load the DataFrame
        logger.info(f"📊 Loading data from {input_file}")
        df = pd.read_csv(input_file)
        
        original_count = len(df)
        original_states = df['state'].nunique() if 'state' in df.columns else 0
        
        logger.info(f"📈 Original data:")
        logger.info(f"   • Rows: {original_count:,}")
        logger.info(f"   • Unique states: {original_states}")
        logger.info(f"   • Columns: {list(df.columns)}")
        
        # Normalize and clean states
        logger.info("🔧 Starting normalization process...")
        df_clean = normalize_states(df, logger)
        
        # Define desired column order
        desired_columns = [
            "team_name", "age_group", "gender", "state", "rank",
            "points", "source", "provider", "url"
        ]
        
        # Keep only existing columns and reorder
        existing_columns = [col for col in desired_columns if col in df_clean.columns]
        df_clean = df_clean[existing_columns]
        
        logger.info(f"📋 Reordered columns: {existing_columns}")
        
        # Sort the data
        sort_columns = []
        if 'state' in df_clean.columns:
            sort_columns.append('state')
        if 'age_group' in df_clean.columns:
            sort_columns.append('age_group')
        if 'gender' in df_clean.columns:
            sort_columns.append('gender')
        if 'rank' in df_clean.columns:
            sort_columns.append('rank')
        
        if sort_columns:
            df_clean = df_clean.sort_values(sort_columns).reset_index(drop=True)
            logger.info(f"🔄 Sorted by: {sort_columns}")
        
        # Generate output filename
        timestamp = get_timestamp()
        output_path = Path(f"data/master/master_team_index_USAonly_{timestamp}.csv")
        
        # Ensure output directory exists
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Save the cleaned data
        logger.info(f"💾 Saving cleaned data to {output_path}")
        safe_write_csv(df_clean, output_path, logger)
        
        # Generate per-state summary statistics
        logger.info("📊 Generating per-state summary statistics...")
        
        # Initialize summary structure
        summary = defaultdict(lambda: {"Male": {}, "Female": {}, "Total": 0})
        
        # Compute per-state, per-gender, per-age-group counts
        for (state, gender, age_group), group_df in df_clean.groupby(["state", "gender", "age_group"]):
            summary[state][gender][age_group] = len(group_df)
            summary[state]["Total"] += len(group_df)
        
        # Add overall national summary
        summary["USA"] = {"Total": len(df_clean)}
        for gender in df_clean["gender"].unique():
            summary["USA"][gender] = (
                df_clean[df_clean["gender"] == gender]["age_group"]
                .value_counts()
                .to_dict()
            )
        
        # Write JSON summary file
        summary_path = Path(f"data/master/state_summaries_{timestamp}.json")
        with open(summary_path, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2)
        
        logger.info(f"✅ State summaries saved → {summary_path}")
        
        # Log final summary
        final_count = len(df_clean)
        final_states = df_clean['state'].nunique() if 'state' in df_clean.columns else 0
        
        logger.info("🏁 Normalization complete!")
        logger.info(f"✅ Clean USA-only master index saved → {output_path}")
        logger.info(f"📊 Rows before: {original_count:,} → after: {final_count:,}")
        logger.info(f"🌍 States before: {original_states} → after: {final_states}")
        logger.info(f"📈 States covered: {len(summary)-1}")
        logger.info(f"⚽ Total teams nationwide: {summary['USA']['Total']:,}")
        
        # Additional statistics
        if not df_clean.empty:
            age_groups = sorted(df_clean['age_group'].unique()) if 'age_group' in df_clean.columns else []
            genders = sorted(df_clean['gender'].unique()) if 'gender' in df_clean.columns else []
            
            logger.info(f"📈 Final dataset summary:")
            logger.info(f"   • Age groups: {age_groups}")
            logger.info(f"   • Genders: {genders}")
            logger.info(f"   • File size: {output_path.stat().st_size / (1024*1024):.1f} MB")
        
        return output_path
        
    except Exception as e:
        logger.error(f"❌ Error during normalization: {e}")
        raise


if __name__ == "__main__":
    # CLI entrypoint
    input_path = sys.argv[1] if len(sys.argv) > 1 else None
    
    if len(sys.argv) > 2:
        print("Usage: python normalize_master_index.py [input_path]")
        print("  input_path: Optional path to input CSV file")
        print("  If not provided, auto-detects latest master index file")
        sys.exit(1)
    
    try:
        output_path = main(input_path)
        print(f"\nNormalization completed successfully!")
        print(f"Output file: {output_path}")
    except Exception as e:
        print(f"\nNormalization failed: {e}")
        sys.exit(1)
