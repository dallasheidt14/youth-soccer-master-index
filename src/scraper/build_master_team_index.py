"""
build_master_team_index.py
--------------------------
Scalable ingestion pipeline for the Youth Soccer Master Index project.

This orchestrator script coordinates all data providers (GotSport, Modular11, AthleteOne),
merges their outputs, and organizes results by state. It provides a unified interface
for building comprehensive team rankings across multiple sources.

Features:
- Auto-discovery of provider CSV files
- Dynamic merging and deduplication
- Per-state organization
- Comprehensive logging and error handling
- Scalable architecture for future providers
"""

import pandas as pd
from pathlib import Path
import logging
import time
import sys
import os
from datetime import datetime
from typing import List, Tuple, Optional

# Add project root to Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

# Import scrapers and utilities
from src.scraper.providers.gotsport_scraper import GotSportScraper
from src.scraper.utils.logger import get_logger
from src.scraper.utils.file_utils import (
    get_timestamp,
    ensure_dir,
    safe_write_csv,
    list_csvs,
)
from src.registry.registry import get_registry, create_build_entry
from src.scraper.utils.state_normalizer import normalize_states
from src.scraper.utils.delta_tracker import compare_builds, save_deltas_to_csv
from src.validators.verify_master_index import validate_master_index_with_schema, ValidationError
from src.utils.metrics_snapshot import write_metrics_snapshot
from src.utils.state_summary_builder import build_state_summaries
from src.utils.multi_provider_merge import merge_provider_data
from src.io.safe_write import safe_write_csv as atomic_write_csv


def ensure_data_tree() -> None:
    """
    Ensure all required data directories exist.
    
    Creates the complete directory structure needed for the master index:
    - data/
    - data/master/
    - data/master/sources/
    - data/master/states/
    - data/logs/
    - data/temp/
    - data/metrics/
    - data/aliases/
    - data/archive/
    - tests/fixtures/
    """
    base_dirs = [
        "data",
        "data/master",
        "data/master/sources",
        "data/master/states",
        "data/logs",
        "data/temp",
        "data/metrics",
        "data/aliases",
        "data/archive",
        "tests/fixtures"
    ]
    
    for dir_path in base_dirs:
        ensure_dir(Path(dir_path))


def run_gotsport_scraper(logger: logging.Logger, incremental: bool = False) -> Tuple[pd.DataFrame, Path]:
    """
    Run the GotSport scraper and return results.
    
    Args:
        logger: Logger instance for operation logging
        incremental: If True, only return new teams not in baseline
        
    Returns:
        Tuple of (DataFrame with team data, path to CSV file)
        
    Raises:
        Exception: If scraper fails completely
    """
    try:
        logger.info("📡 Initializing GotSport scraper")
        scraper = GotSportScraper(logger, use_zenrows=True)
        
        if incremental:
            logger.info("🔄 Running GotSport scraper (incremental mode)")
        else:
            logger.info("🔄 Running GotSport scraper")
        
        df_gotsport, gotsport_csv = scraper.run(incremental=incremental)
        
        if df_gotsport.empty:
            logger.warning("⚠️ GotSport scraper returned empty data")
            return df_gotsport, gotsport_csv
        
        logger.info(f"✅ GotSport scraper completed: {len(df_gotsport)} teams → {gotsport_csv}")
        return df_gotsport, gotsport_csv
        
    except Exception as e:
        logger.error(f"❌ GotSport scraper failed: {e}")
        raise


def merge_incremental_data(new_teams_df: pd.DataFrame, logger: logging.Logger) -> pd.DataFrame:
    """
    Merge new teams with baseline master index.
    
    Args:
        new_teams_df: DataFrame with new teams
        logger: Logger instance for operation logging
        
    Returns:
        Combined DataFrame with baseline + new teams
    """
    try:
        from src.scraper.utils.incremental_detector import load_baseline_master, detect_new_teams
        
        logger.info("📂 Loading baseline master index")
        baseline_df = load_baseline_master()
        
        logger.info(f"📊 Baseline: {len(baseline_df):,} teams")
        logger.info(f"🆕 New teams: {len(new_teams_df):,} teams")
        
        # Detect actual new teams (not just concatenate everything)
        actual_new_teams = detect_new_teams(new_teams_df, baseline_df, logger)
        
        if actual_new_teams.empty:
            logger.info("✅ No new teams detected - master index is up to date")
            return baseline_df
        
        # Combine baseline and actual new teams
        df_combined = pd.concat([baseline_df, actual_new_teams], ignore_index=True)
        
        logger.info(f"✅ Combined dataset: {len(df_combined):,} teams ({len(actual_new_teams)} new teams added)")
        
        return df_combined
        
    except Exception as e:
        logger.error(f"❌ Failed to merge incremental data: {e}")
        raise


def merge_provider_data(logger: logging.Logger) -> pd.DataFrame:
    """
    Auto-discover and merge all provider CSV files.
    
    Args:
        logger: Logger instance for operation logging
        
    Returns:
        Merged DataFrame with all provider data
        
    Raises:
        Exception: If no provider data can be loaded
    """
    try:
        sources_dir = Path("data/master/sources")
        
        # Auto-discover provider CSV files
        provider_files = list_csvs(sources_dir, "*.csv")
        
        if not provider_files:
            logger.warning("⚠️ No provider CSV files found in sources directory")
            return pd.DataFrame()
        
        logger.info(f"🔍 Found {len(provider_files)} provider CSV files")
        
        # Load and merge all provider DataFrames
        dfs = []
        total_rows = 0
        
        for csv_path in provider_files:
            try:
                df = pd.read_csv(csv_path)
                
                # Infer provider from filename
                filename_lower = csv_path.name.lower()
                if "gotsport" in filename_lower:
                    provider = "GotSport"
                elif "modular11" in filename_lower:
                    provider = "Modular11"
                elif "athleteone" in filename_lower:
                    provider = "AthleteOne"
                else:
                    provider = "Unknown"
                
                # Add provider column if not present
                if "provider" not in df.columns:
                    df["provider"] = provider
                
                dfs.append(df)
                total_rows += len(df)
                
                logger.info(f"📄 Loaded {len(df)} teams from {provider} → {csv_path.name}")
                
            except Exception as e:
                logger.error(f"❌ Failed to load {csv_path.name}: {e}")
                continue
        
        if not dfs:
            logger.error("❌ No provider data could be loaded")
            return pd.DataFrame()
        
        # Concatenate all DataFrames
        logger.info(f"🔄 Merging {len(dfs)} provider datasets ({total_rows} total rows)")
        df_all = pd.concat(dfs, ignore_index=True)
        
        # Drop duplicates based on key fields
        initial_count = len(df_all)
        df_all = df_all.drop_duplicates(
            subset=["team_name", "age_group", "gender", "state", "source", "rank", "points"],
            keep="first"
        )
        final_count = len(df_all)
        
        logger.info(f"📊 Deduplication: {initial_count} → {final_count} teams ({initial_count - final_count} duplicates removed)")
        
        # Normalize state codes and filter to USA-only teams
        logger.info("🌎 Starting U.S.-only normalization...")
        df_all = normalize_states(df_all, logger)
        logger.info(f"🌎 U.S.-only normalization complete → {len(df_all)} teams remain")
        
        # Sort by state, age group, gender, rank, team name
        df_all = df_all.sort_values(by=["state", "age_group", "gender", "rank", "team_name"])
        
        logger.info(f"✅ Successfully merged {len(df_all)} unique teams from {len(dfs)} providers")
        return df_all
        
    except Exception as e:
        logger.error(f"❌ Failed to merge provider data: {e}")
        raise


def save_incremental_master(df_all: pd.DataFrame, logger: logging.Logger) -> Path:
    """
    Save incremental master CSV file.
    
    Args:
        df_all: Combined DataFrame with baseline + new teams
        logger: Logger instance for operation logging
        
    Returns:
        Path to the saved master CSV file
    """
    try:
        timestamp = get_timestamp()
        master_path = Path(f"data/master/master_team_index_USAonly_incremental_{timestamp}.csv")
        
        atomic_write_csv(df_all, master_path, logger=logger)
        logger.info(f"✅ Incremental master index saved: {master_path}")
        
        return master_path
        
    except Exception as e:
        logger.error(f"❌ Failed to save incremental master: {e}")
        raise


def save_national_master(df_all: pd.DataFrame, logger: logging.Logger) -> Path:
    """
    Save the national master team index CSV.
    
    Args:
        df_all: Combined DataFrame with all team data
        logger: Logger instance for operation logging
        
    Returns:
        Path to the saved master CSV file
    """
    try:
        timestamp = get_timestamp()
        master_path = Path(f"data/master/master_team_index_USAonly_{timestamp}.csv")
        
        atomic_write_csv(df_all, master_path, logger=logger)
        logger.info(f"✅ National master index saved: {master_path}")
        
        return master_path
        
    except Exception as e:
        logger.error(f"❌ Failed to save national master CSV: {e}")
        raise


def save_per_state_csvs(df_all: pd.DataFrame, logger: logging.Logger) -> List[Tuple[str, Path]]:
    """
    Group data by state and save per-state CSV files.
    
    Args:
        df_all: Combined DataFrame with all team data
        logger: Logger instance for operation logging
        
    Returns:
        List of tuples (state, csv_path) for saved files
    """
    try:
        timestamp = get_timestamp()
        saved_states = []
        
        # Get unique states (excluding NaN values)
        unique_states = df_all["state"].dropna().unique()
        
        if len(unique_states) == 0:
            logger.warning("⚠️ No states found in data")
            return saved_states
        
        logger.info(f"🌎 Processing {len(unique_states)} states")
        
        for state in sorted(unique_states):
            try:
                # Filter data for this state
                df_state = df_all[df_all["state"] == state]
                
                if df_state.empty:
                    logger.warning(f"⚠️ No data found for state: {state}")
                    continue
                
                # Save per-state CSV
                state_path = Path(f"data/master/states/{state}/combined_{state}_{timestamp}.csv")
                safe_write_csv(df_state, state_path, logger=logger)
                
                saved_states.append((state, state_path))
                logger.info(f"📦 {state}: {len(df_state)} teams saved → {state_path}")
                
            except Exception as e:
                logger.error(f"❌ Failed to save state CSV for {state}: {e}")
                continue
        
        logger.info(f"✅ Successfully saved {len(saved_states)} state CSV files")
        return saved_states
        
    except Exception as e:
        logger.error(f"❌ Failed to save per-state CSVs: {e}")
        raise


def main(incremental_only: bool = False):
    """
    Main orchestrator function for building the master team index.
    
    This function coordinates the entire pipeline:
    1. Setup and initialization
    2. Run GotSport scraper
    3. Merge all provider data
    4. Save national master CSV
    5. Save per-state CSV files
    6. Generate final summary
    
    Args:
        incremental_only: If True, only run incremental updates (new teams only)
    """
    start_time = time.time()
    
    try:
        # Setup and initialization
        logger = get_logger("build_master_team_index")
        
        if incremental_only:
            logger.info("🔄 Starting Incremental Master Index Update")
        else:
            logger.info("🚀 Starting Youth Soccer Master Index Build")
        
        ensure_data_tree()
        logger.info("📁 Data directory structure ensured")
        
        # Stage 1: Run GotSport Scraper
        logger.info("=" * 60)
        logger.info("📡 STAGE 1: Running GotSport Scraper")
        logger.info("=" * 60)
        
        try:
            df_gotsport, gotsport_csv = run_gotsport_scraper(logger, incremental=incremental_only)
        except Exception as e:
            logger.error(f"❌ Stage 1 failed: {e}")
            logger.info("🔄 Continuing with existing provider data...")
            df_gotsport = pd.DataFrame()
        
        # Stage 2: Merge Provider Data
        logger.info("=" * 60)
        if incremental_only:
            logger.info("🔄 STAGE 2: Processing Incremental Data")
        else:
            logger.info("🔄 STAGE 2: Merging Provider Data")
        logger.info("=" * 60)
        
        try:
            if incremental_only:
                # For incremental mode, merge new teams with baseline
                if df_gotsport.empty:
                    logger.info("✅ No new teams to process - master index is up to date")
                    return
                
                logger.info("🔄 Merging new teams with baseline master")
                df_all = merge_incremental_data(df_gotsport, logger)
                
                # Delta tracking for incremental builds
                try:
                    logger.info("🔍 Performing delta analysis...")
                    from src.scraper.utils.incremental_detector import load_baseline_master
                    
                    baseline_df = load_baseline_master()
                    
                    # For delta tracking, we want to compare the new teams against baseline
                    # to detect what changed, not the final merged dataset
                    new_teams_only = df_gotsport  # This contains only the new teams
                    deltas = compare_builds(new_teams_only, baseline_df, logger)
                    
                    # Save delta CSVs
                    timestamp = get_timestamp()
                    delta_files = save_deltas_to_csv(deltas, timestamp)
                    
                    if delta_files:
                        logger.info("📁 Delta files saved:")
                        for delta_type, filepath in delta_files.items():
                            logger.info(f"   • {delta_type}: {filepath.name}")
                    
                    # Update history registry
                    build_info = {
                        "timestamp": timestamp,
                        "build_file": f"data/master/master_team_index_USAonly_incremental_{timestamp}.csv",
                        "teams_total": len(df_all),
                        "notes": "Incremental + delta tracked build",
                        "build_type": "incremental",
                        "duration_seconds": int(time.time() - start_time),
                        "providers": ["GotSport"],
                        "states_covered": len(df_all['state'].unique()) if not df_all.empty else 0
                    }
                    
                    delta_counts = {
                        "added": len(deltas["added"]),
                        "removed": len(deltas["removed"]),
                        "renamed": len(deltas["renamed"])
                    }
                    
                    registry = get_registry()
                    registry.add_history_entry(build_info, delta_counts)
                    
                    # Log delta summary
                    logger.info("📈 Delta Summary")
                    logger.info(f"🆕 {delta_counts['added']} added | ❌ {delta_counts['removed']} removed | ✏️ {delta_counts['renamed']} renamed")
                    
                except Exception as e:
                    logger.warning(f"⚠️ Delta tracking failed: {e}")
                    logger.info("🔄 Continuing without delta tracking...")
            else:
                df_all = merge_provider_data(logger)
            
            if df_all.empty:
                logger.error("❌ No data available from any provider")
                return
                
        except Exception as e:
            logger.error(f"❌ Stage 2 failed: {e}")
            return
        
        # Stage 3: Schema Validation
        logger.info("=" * 60)
        logger.info("🔍 STAGE 3: Schema Validation")
        logger.info("=" * 60)
        
        try:
            logger.info("📊 Running comprehensive validation...")
            validation_results = validate_master_index_with_schema(df_all, logger)
            
            if validation_results['overall_status'] == 'failed':
                logger.error("❌ Schema validation failed - stopping build")
                return
            elif validation_results['overall_status'] == 'warnings':
                logger.warning("⚠️ Schema validation passed with warnings")
            else:
                logger.info("✅ Schema validation passed")
                
        except ValidationError as e:
            logger.error(f"❌ Stage 3 failed - Validation error: {e}")
            return
        except Exception as e:
            logger.error(f"❌ Stage 3 failed: {e}")
            return
        
        # Stage 4: Save National Master CSV
        logger.info("=" * 60)
        if incremental_only:
            logger.info("💾 STAGE 4: Saving Incremental Master CSV")
        else:
            logger.info("💾 STAGE 4: Saving National Master CSV")
        logger.info("=" * 60)
        
        try:
            if incremental_only:
                master_path = save_incremental_master(df_all, logger)
            else:
                master_path = save_national_master(df_all, logger)
        except Exception as e:
            logger.error(f"❌ Stage 4 failed: {e}")
            return
        
        # Stage 5: Save Per-State CSV Files
        logger.info("=" * 60)
        logger.info("🌎 STAGE 5: Saving Per-State CSV Files")
        logger.info("=" * 60)
        
        try:
            saved_states = save_per_state_csvs(df_all, logger)
        except Exception as e:
            logger.error(f"❌ Stage 5 failed: {e}")
            return
        
        # Stage 6: Generate Metrics and State Summaries
        logger.info("=" * 60)
        logger.info("📊 STAGE 6: Generate Metrics and State Summaries")
        logger.info("=" * 60)
        
        try:
            # Generate metrics snapshot
            timestamp = get_timestamp()
            metrics_data = {
                'build_id': timestamp,
                'team_count': len(df_all),
                'states_covered': len(df_all['state'].unique()) if not df_all.empty else 0,
                'data_quality_score': validation_results.get('data_quality_score', 0),
                'build_duration_seconds': int(time.time() - start_time),
                'providers': df_all['provider'].unique().tolist() if not df_all.empty else [],
                'age_distribution': df_all['age_group'].value_counts().to_dict() if not df_all.empty else {},
                'gender_distribution': df_all['gender'].value_counts().to_dict() if not df_all.empty else {},
                'state_distribution': df_all['state'].value_counts().to_dict() if not df_all.empty else {}
            }
            
            if incremental_only and 'deltas' in locals():
                metrics_data.update({
                    'new_teams': len(deltas["added"]),
                    'removed_teams': len(deltas["removed"]),
                    'renamed_teams': len(deltas["renamed"])
                })
            
            write_metrics_snapshot(metrics_data, logger)
            logger.info("✅ Metrics snapshot generated")
            
            # Generate state summaries
            build_state_summaries(df_all, timestamp, logger)
            logger.info("✅ State summaries generated")
            
        except Exception as e:
            logger.warning(f"⚠️ Stage 6 failed: {e}")
            logger.info("🔄 Continuing without metrics...")
        
        # Final Summary
        end_time = time.time()
        duration = end_time - start_time
        
        logger.info("=" * 60)
        logger.info("🏁 MASTER INDEX BUILD COMPLETED SUCCESSFULLY!")
        logger.info("=" * 60)
        logger.info(f"📊 Total teams: {len(df_all)}")
        logger.info(f"🌎 States processed: {df_all['state'].nunique()}")
        logger.info(f"📁 Master saved: {master_path}")
        logger.info(f"⏱️ Build duration: {duration:.2f} seconds")
        
        if saved_states:
            logger.info(f"📦 State files saved: {len(saved_states)}")
            for state, path in saved_states:
                logger.info(f"   • {state}: {path}")
        
        # Data summary
        if not df_all.empty:
            age_groups = sorted(df_all['age_group'].unique())
            genders = sorted(df_all['gender'].unique())
            sources = sorted(df_all['source'].unique())
            
            logger.info(f"📈 Data Summary:")
            logger.info(f"   • Age groups: {age_groups}")
            logger.info(f"   • Genders: {genders}")
            logger.info(f"   • Sources: {sources}")
        
        logger.info("🎉 Master team index build completed successfully!")
        
        # Final summary based on mode
        if incremental_only:
            logger.info("🚀 Incremental update complete")
            logger.info(f"🆕 {len(df_gotsport)} new teams added")
            logger.info(f"📦 New master saved: {master_path.name}")
        else:
            logger.info("🎉 Master team index build completed successfully!")
        
        # Register build metadata
        try:
            registry = get_registry()
            build_entry = create_build_entry(
                teams_total=len(df_all),
                states_total=len(df_all['state'].unique()) if not df_all.empty else 0,
                data_quality=96.7,  # TODO: Calculate actual quality score
                source_file=str(gotsport_csv),
                master_file=str(master_path),
                build_duration_seconds=int(time.time() - start_time),
                providers=["GotSport"],
                age_groups=sorted(df_all['age_group'].unique()) if not df_all.empty else [],
                genders=sorted(df_all['gender'].unique()) if not df_all.empty else [],
                notes="Comprehensive nationwide build with pagination"
            )
            registry.add_metadata_entry(build_entry)
            logger.info("🧾 Build metadata registered successfully")
        except Exception as e:
            logger.warning(f"⚠️ Could not register build metadata: {e}")
        
    except Exception as e:
        logger.error(f"❌ Critical error in main orchestrator: {e}")
        raise


if __name__ == "__main__":
    import sys
    
    # Check for incremental mode flag
    incremental_only = "--incremental" in sys.argv or "-i" in sys.argv
    
    if incremental_only:
        print("Running in incremental mode - only new teams will be processed")
    
    main(incremental_only=incremental_only)