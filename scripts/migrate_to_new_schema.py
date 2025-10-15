#!/usr/bin/env python3
"""
Data Migration Script

One-time script to migrate the existing clean master index to the new schema
with deterministic team IDs and proper field mappings.
"""

import pandas as pd
import sys
import os
from pathlib import Path
from datetime import datetime
import logging

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.utils.team_id_generator import make_team_id, normalize_gender, extract_age_from_group
from src.schema.master_team_schema import validate_dataframe, MasterTeamSchema
from src.scraper.utils.logger import get_logger
from src.io.safe_write import safe_write_csv
from src.scraper.utils.file_utils import get_timestamp


def migrate_master_index(input_file: Path, output_file: Path = None, logger: logging.Logger = None):
    """
    Migrate existing clean master index to new schema.
    
    Args:
        input_file: Path to existing clean master index CSV
        output_file: Path to save migrated data (optional)
        logger: Logger instance for output
        
    Returns:
        Path to the migrated file
    """
    if logger is None:
        logger = get_logger(__name__)
    
    logger.info("🔄 Starting master index migration to new schema...")
    
    # Load existing data
    logger.info(f"📂 Loading data from: {input_file}")
    df = pd.read_csv(input_file)
    logger.info(f"📊 Original data: {len(df):,} rows, {len(df.columns)} columns")
    logger.info(f"📋 Original columns: {list(df.columns)}")
    
    # Step 1: Rename team_id to provider_team_id
    logger.info("🔄 Step 1: Renaming team_id → provider_team_id")
    if 'team_id' in df.columns:
        df = df.rename(columns={'team_id': 'provider_team_id'})
        logger.info("✅ Renamed team_id column to provider_team_id")
    else:
        logger.warning("⚠️ No 'team_id' column found, creating empty provider_team_id")
        df['provider_team_id'] = None
    
    # Step 2: Filter out invalid data and generate team_id
    logger.info("🔄 Step 2: Filtering invalid data and generating deterministic team_id")
    
    original_count = len(df)
    
    # Filter out null team names
    null_names = df['team_name'].isna()
    if null_names.sum() > 0:
        logger.warning(f"⚠️ Found {null_names.sum()} rows with null team names")
        df = df[~null_names]
    
    # Count invalid states
    invalid_states = df[~df['state'].str.match(r'^[A-Z]{2}$', na=False)]
    if len(invalid_states) > 0:
        logger.warning(f"⚠️ Found {len(invalid_states)} rows with invalid state codes:")
        state_counts = invalid_states['state'].value_counts()
        for state, count in state_counts.head(10).items():
            logger.warning(f"   {state}: {count} rows")
        if len(state_counts) > 10:
            logger.warning(f"   ... and {len(state_counts) - 10} more state codes")
    
    # Filter to only valid 2-character state codes
    df = df[df['state'].str.match(r'^[A-Z]{2}$', na=False)]
    filtered_count = len(df)
    removed_count = original_count - filtered_count
    
    if removed_count > 0:
        logger.info(f"🗑️ Removed {removed_count} rows with invalid data")
        logger.info(f"📊 Remaining data: {filtered_count:,} rows")
    
    # Generate team_id for remaining valid rows
    team_ids = []
    errors = []
    
    for idx, row in df.iterrows():
        try:
            team_id = make_team_id(
                row['team_name'],
                row['state'],
                row['age_group'],
                row['gender']
            )
            team_ids.append(team_id)
        except Exception as e:
            errors.append(f"Row {idx}: {e}")
            team_ids.append(None)  # Placeholder for failed rows
    
    df['team_id'] = team_ids
    
    if errors:
        logger.warning(f"⚠️ Failed to generate team_id for {len(errors)} rows:")
        for error in errors[:5]:  # Show first 5 errors
            logger.warning(f"   {error}")
        if len(errors) > 5:
            logger.warning(f"   ... and {len(errors) - 5} more")
    
    successful_ids = sum(1 for tid in team_ids if tid is not None)
    logger.info(f"✅ Generated team_id for {successful_ids:,} teams ({successful_ids/len(df)*100:.1f}%)")
    
    # Step 3: Extract age_u from age_group
    logger.info("🔄 Step 3: Extracting numeric age_u from age_group")
    age_u_values = []
    age_errors = []
    
    for idx, row in df.iterrows():
        try:
            age_u = extract_age_from_group(row['age_group'])
            age_u_values.append(age_u)
        except Exception as e:
            age_errors.append(f"Row {idx}: {e}")
            age_u_values.append(None)
    
    df['age_u'] = age_u_values
    
    if age_errors:
        logger.warning(f"⚠️ Failed to extract age_u for {len(age_errors)} rows")
    
    successful_ages = sum(1 for age in age_u_values if age is not None)
    logger.info(f"✅ Extracted age_u for {successful_ages:,} teams ({successful_ages/len(df)*100:.1f}%)")
    
    # Step 4: Normalize gender to M/F
    logger.info("🔄 Step 4: Normalizing gender to M/F")
    gender_values = []
    gender_errors = []
    
    for idx, row in df.iterrows():
        try:
            gender = normalize_gender(row['gender'])
            gender_values.append(gender)
        except Exception as e:
            gender_errors.append(f"Row {idx}: {e}")
            gender_values.append(None)
    
    df['gender'] = gender_values
    
    if gender_errors:
        logger.warning(f"⚠️ Failed to normalize gender for {len(gender_errors)} rows")
    
    successful_genders = sum(1 for g in gender_values if g is not None)
    logger.info(f"✅ Normalized gender for {successful_genders:,} teams ({successful_genders/len(df)*100:.1f}%)")
    
    # Step 5: Add missing schema fields and rename existing ones
    logger.info("🔄 Step 5: Adding missing schema fields and renaming existing ones")
    
    # Rename url to source_url if it exists
    if 'url' in df.columns and 'source_url' not in df.columns:
        df = df.rename(columns={'url': 'source_url'})
        logger.info("✅ Renamed 'url' column to 'source_url'")
    
    # Add club_name (nullable)
    if 'club_name' not in df.columns:
        df['club_name'] = None
        logger.info("✅ Added club_name column (null)")
    
    # Add created_at timestamp
    current_timestamp = datetime.utcnow().isoformat()
    df['created_at'] = current_timestamp
    logger.info(f"✅ Added created_at timestamp: {current_timestamp}")
    
    # Step 6: Reorder columns to match schema
    logger.info("🔄 Step 6: Reordering columns to match schema")
    schema_columns = [
        'team_id', 'provider_team_id', 'team_name', 'age_group', 'age_u',
        'gender', 'state', 'provider', 'club_name', 'source_url', 'created_at'
    ]
    
    # Add any extra columns that exist but aren't in schema
    extra_columns = [col for col in df.columns if col not in schema_columns]
    final_columns = schema_columns + extra_columns
    
    df = df[final_columns]
    logger.info(f"✅ Reordered columns: {list(df.columns)}")
    
    # Step 7: Validate with schema
    logger.info("🔄 Step 7: Validating with MasterTeamSchema")
    try:
        validated_df = validate_dataframe(df)
        logger.info("✅ Schema validation passed!")
    except Exception as e:
        logger.exception(f"❌ Schema validation failed: {e}")
        raise
    
    # Step 8: Save migrated data
    if output_file is None:
        timestamp = get_timestamp()
        output_file = input_file.parent / f"master_team_index_migrated_{timestamp}.csv"
    
    logger.info(f"💾 Saving migrated data to: {output_file}")
    write_result = safe_write_csv(validated_df, output_file, logger)
    
    logger.info("✅ Migration completed successfully!")
    logger.info(f"📁 Migrated file: {output_file}")
    logger.info(f"📊 Final data: {len(validated_df):,} rows, {len(validated_df.columns)} columns")
    logger.info(f"🔐 File checksum: {write_result['checksum']}")
    logger.info(f"📏 File size: {write_result['size_bytes']:,} bytes")
    
    return output_file


def main():
    """Main function for command-line usage."""
    logger = get_logger(__name__)
    
    # Find the clean master index file
    master_dir = Path("data/master")
    clean_files = list(master_dir.glob("master_team_index_USAonly_clean_*.csv"))
    
    if not clean_files:
        logger.error("❌ No clean master index files found!")
        logger.info("Expected file pattern: master_team_index_USAonly_clean_*.csv")
        return
    
    # Use the latest clean file
    latest_file = max(clean_files, key=lambda x: x.stat().st_mtime)
    logger.info(f"📂 Using clean file: {latest_file}")
    
    # Run migration
    try:
        migrated_file = migrate_master_index(latest_file, logger=logger)
        
        logger.info("🎉 Migration completed successfully!")
        logger.info(f"📁 Migrated file: {migrated_file}")
        
        # Show summary statistics
        df = pd.read_csv(migrated_file)
        logger.info("📊 Migration Summary:")
        logger.info(f"   Total teams: {len(df):,}")
        logger.info(f"   Teams with team_id: {df['team_id'].notna().sum():,}")
        logger.info(f"   Teams with provider_team_id: {df['provider_team_id'].notna().sum():,}")
        logger.info(f"   Teams with age_u: {df['age_u'].notna().sum():,}")
        logger.info(f"   Teams with normalized gender: {df['gender'].notna().sum():,}")
        logger.info(f"   Unique states: {df['state'].nunique()}")
        logger.info(f"   Age groups: {sorted(df['age_group'].unique())}")
        logger.info(f"   Genders: {sorted(df['gender'].unique())}")
        
    except Exception as e:
        logger.exception(f"❌ Migration failed: {e}")
        raise


if __name__ == "__main__":
    main()
