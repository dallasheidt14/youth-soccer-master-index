#!/usr/bin/env python3
"""
Incremental Detection Utility

Detects new teams by comparing scraped data against the existing master index.
Only captures teams that are not already present in the baseline.
"""

import pandas as pd
import logging
from pathlib import Path
from typing import Optional, Tuple, Dict
import sys
import os
from datetime import datetime

# Add project root to Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))
sys.path.append(project_root)

try:
    from src.scraper.utils.file_utils import safe_write_csv, get_timestamp
except ImportError as e:
    print(f"Import error: {e}")
    sys.exit(1)


def load_baseline_master(data_dir: str = "data/master", sample_mode: bool = False, 
                        logger: Optional[logging.Logger] = None) -> pd.DataFrame:
    """
    Load the baseline master CSV using metadata registry lookup.
    
    Args:
        data_dir: Directory to search for master index files
        sample_mode: If True, limit to first 5000 rows for testing
        logger: Optional logger instance for output
        
    Returns:
        DataFrame with baseline master data
        
    Raises:
        FileNotFoundError: If no baseline master file found
    """
    try:
        from src.registry.metadata_registry import load_registry, get_latest_entry
    except ImportError:
        # Fallback to old method if registry not available
        return _load_baseline_master_fallback(data_dir, sample_mode, logger)
    
    if logger is None:
        from src.scraper.utils.logger import get_logger
        logger = get_logger(__name__)
    
    logger.info("📋 Loading baseline master from metadata registry...")
    
    # Load registry and get latest entry
    registry = load_registry()
    if not registry:
        logger.warning("⚠️ No entries in metadata registry, falling back to file search")
        return _load_baseline_master_fallback(data_dir, sample_mode, logger)
    
    latest_entry = get_latest_entry()
    if not latest_entry or 'baseline_file' not in latest_entry:
        logger.warning("⚠️ No baseline_file in latest registry entry, falling back to file search")
        return _load_baseline_master_fallback(data_dir, sample_mode, logger)
    
    baseline_file = Path(latest_entry['baseline_file'])
    if not baseline_file.exists():
        logger.warning(f"⚠️ Baseline file not found: {baseline_file}, falling back to file search")
        return _load_baseline_master_fallback(data_dir, sample_mode, logger)
    
    logger.info(f"📂 Loading baseline from registry: {baseline_file}")
    
    # Load the DataFrame
    df = pd.read_csv(baseline_file)
    
    # Apply sample mode if requested
    if sample_mode and len(df) > 5000:
        logger.info(f"🔬 Sample mode: limiting to first 5000 rows")
        df = df.head(5000)
    
    logger.info(f"✅ Baseline loaded: {len(df):,} teams from {baseline_file}")
    return df


def _load_baseline_master_fallback(data_dir: str = "data/master", sample_mode: bool = False,
                                  logger: Optional[logging.Logger] = None) -> pd.DataFrame:
    """
    Fallback method to load baseline master using file globbing.
    
    Args:
        data_dir: Directory to search for master index files
        sample_mode: If True, limit to first 5000 rows for testing
        logger: Optional logger instance for output
        
    Returns:
        DataFrame with baseline master data
        
    Raises:
        FileNotFoundError: If no master index files found
    """
    if logger is None:
        from src.scraper.utils.logger import get_logger
        logger = get_logger(__name__)
    
    logger.info("📋 Loading baseline master using fallback file search...")
    
    data_path = Path(data_dir)
    
    if not data_path.exists():
        raise FileNotFoundError(f"Data directory not found: {data_path}")
    
    # Find all CSV files starting with "master_team_index_USAonly_" but exclude incremental files
    master_files = list(data_path.glob("master_team_index_USAonly_*.csv"))
    
    # Filter out incremental files (files with "incremental" in the name)
    master_files = [f for f in master_files if "incremental" not in f.name]
    
    if not master_files:
        raise FileNotFoundError(f"No USA-only master index files found in {data_path}")
    
    # Sort by modification time (newest first)
    latest_file = max(master_files, key=lambda f: f.stat().st_mtime)
    
    logger.info(f"📂 Loading baseline from file search: {latest_file}")
    
    # Load the DataFrame
    df = pd.read_csv(latest_file)
    
    # Apply sample mode if requested
    if sample_mode and len(df) > 5000:
        logger.info(f"🔬 Sample mode: limiting to first 5000 rows")
        df = df.head(5000)
    
    logger.info(f"✅ Baseline loaded: {len(df):,} teams from {latest_file}")
    return df


def detect_new_teams_by_provider(new_df: pd.DataFrame, baseline_df: pd.DataFrame, 
                                logger: Optional[logging.Logger] = None) -> Dict[str, Dict[str, int]]:
    """
    Detect new teams grouped by provider.
    
    Args:
        new_df: DataFrame with newly scraped data
        baseline_df: DataFrame with baseline master data
        logger: Optional logger instance for output
        
    Returns:
        Dictionary with provider-specific delta counts
        Format: {provider: {added: N, removed: M, renamed: K}}
    """
    if logger is None:
        from src.scraper.utils.logger import get_logger
        logger = get_logger(__name__)
    
    logger.info("🔍 Detecting new teams by provider...")
    
    # Get unique providers in new data
    providers = new_df['provider'].unique() if 'provider' in new_df.columns else ['unknown']
    
    provider_deltas = {}
    
    for provider in providers:
        logger.info(f"📊 Processing provider: {provider}")
        
        # Filter data for this provider
        provider_new_df = new_df[new_df['provider'] == provider] if 'provider' in new_df.columns else new_df
        provider_baseline_df = baseline_df[baseline_df['provider'] == provider] if 'provider' in baseline_df.columns else pd.DataFrame()
        
        # Detect new teams for this provider
        new_teams = detect_new_teams(provider_new_df, provider_baseline_df, logger)
        
        provider_deltas[provider] = {
            'added': len(new_teams),
            'removed': 0,  # Not implemented yet
            'renamed': 0   # Not implemented yet
        }
        
        logger.info(f"   {provider}: {len(new_teams)} new teams")
    
    logger.info(f"📈 Provider delta summary: {provider_deltas}")
    return provider_deltas


def detect_new_teams(new_df: pd.DataFrame, baseline_df: pd.DataFrame, logger: Optional[logging.Logger] = None) -> pd.DataFrame:
    """
    Detect new teams by comparing against baseline master index.
    
    Args:
        new_df: DataFrame with newly scraped data
        baseline_df: DataFrame with baseline master data
        logger: Optional logger instance for output
        
    Returns:
        DataFrame containing only new teams not present in baseline
    """
    if new_df.empty:
        if logger:
            logger.warning("⚠️ New DataFrame is empty")
        return new_df
    
    if baseline_df.empty:
        if logger:
            logger.warning("⚠️ Baseline DataFrame is empty - all teams are considered new")
        return new_df
    
    # Define comparison columns
    comparison_cols = ["team_name", "age_group", "gender", "state"]
    
    # Check if all comparison columns exist in both DataFrames
    missing_cols_new = [col for col in comparison_cols if col not in new_df.columns]
    missing_cols_baseline = [col for col in comparison_cols if col not in baseline_df.columns]
    
    if missing_cols_new:
        raise ValueError(f"Missing columns in new DataFrame: {missing_cols_new}")
    
    if missing_cols_baseline:
        raise ValueError(f"Missing columns in baseline DataFrame: {missing_cols_baseline}")
    
    if logger:
        logger.info(f"📂 Baseline loaded: {len(baseline_df):,} rows")
        logger.info(f"🧮 Comparing with new scrape: {len(new_df):,} rows")
    
    # Create comparison keys for both DataFrames
    new_keys = new_df[comparison_cols].apply(lambda x: '|'.join(x.astype(str)), axis=1)
    baseline_keys = baseline_df[comparison_cols].apply(lambda x: '|'.join(x.astype(str)), axis=1)
    
    # Find new teams (not in baseline)
    new_mask = ~new_keys.isin(baseline_keys)
    new_teams_df = new_df[new_mask].copy()
    
    # Reset index
    new_teams_df = new_teams_df.reset_index(drop=True)
    
    # Log results
    new_count = len(new_teams_df)
    total_new = len(new_df)
    
    if logger:
        logger.info(f"🆕 {new_count:,} new teams detected out of {total_new:,} total")
        
        if new_count > 0:
            # Get state distribution of new teams
            state_counts = new_teams_df['state'].value_counts()
            top_states = state_counts.head(5)
            
            logger.info(f"📊 New teams by state: {top_states.to_dict()}")
            
            # Get age group distribution
            age_counts = new_teams_df['age_group'].value_counts()
            logger.info(f"🎂 New teams by age: {age_counts.to_dict()}")
            
            # Get gender distribution
            gender_counts = new_teams_df['gender'].value_counts()
            logger.info(f"⚽ New teams by gender: {gender_counts.to_dict()}")
        else:
            logger.info("✅ No new teams detected - all teams already exist in baseline")
    
    return new_teams_df


def save_incremental(df: pd.DataFrame, timestamp: Optional[str] = None, logger: Optional[logging.Logger] = None) -> Path:
    """
    Save incremental teams to CSV file.
    
    Args:
        df: DataFrame with new teams
        timestamp: Optional timestamp override
        logger: Optional logger instance for output
        
    Returns:
        Path to saved file
    """
    if df.empty:
        if logger:
            logger.warning("⚠️ No new teams to save")
        return None
    
    # Generate timestamp if not provided
    if timestamp is None:
        timestamp = get_timestamp()
    
    # Create incremental directory
    incremental_dir = Path("data/master/incremental")
    incremental_dir.mkdir(parents=True, exist_ok=True)
    
    # Generate output path
    output_path = incremental_dir / f"new_teams_{timestamp}.csv"
    
    # Save the DataFrame
    safe_write_csv(df, output_path, logger)
    
    if logger:
        logger.info(f"✅ Incremental file saved: {output_path}")
    
    return output_path


def get_incremental_summary(new_teams_df: pd.DataFrame) -> dict:
    """
    Get summary statistics for incremental teams.
    
    Args:
        new_teams_df: DataFrame with new teams
        
    Returns:
        Dictionary with summary statistics
    """
    if new_teams_df.empty:
        return {
            'total_new_teams': 0,
            'states': {},
            'age_groups': {},
            'genders': {},
            'top_states': [],
            'top_age_groups': []
        }
    
    state_counts = new_teams_df['state'].value_counts()
    age_counts = new_teams_df['age_group'].value_counts()
    gender_counts = new_teams_df['gender'].value_counts()
    
    return {
        'total_new_teams': len(new_teams_df),
        'states': state_counts.to_dict(),
        'age_groups': age_counts.to_dict(),
        'genders': gender_counts.to_dict(),
        'top_states': state_counts.head(5).to_dict(),
        'top_age_groups': age_counts.head(5).to_dict()
    }


if __name__ == "__main__":
    # Test the incremental detector
    import sys
    from pathlib import Path
    
    # Add project root to path
    project_root = Path(__file__).parent.parent.parent
    sys.path.append(str(project_root))
    
    try:
        from src.scraper.utils.logger import get_logger
        logger = get_logger("incremental_detector_test.log")
    except ImportError:
        # Fallback logger for testing
        import logging
        logging.basicConfig(level=logging.INFO)
        logger = logging.getLogger("test")
    
    logger.info("🧪 Testing incremental detector")
    
    try:
        # Load baseline
        baseline_df = load_baseline_master()
        logger.info(f"📂 Loaded baseline: {len(baseline_df):,} teams")
        
        # Create test new data (subset of baseline + some new teams)
        test_new_df = baseline_df.head(1000).copy()
        
        # Add some "new" teams
        new_teams = pd.DataFrame({
            'team_name': ['Test Team A', 'Test Team B', 'Test Team C'],
            'age_group': ['U12', 'U13', 'U14'],
            'gender': ['Male', 'Female', 'Male'],
            'state': ['CA', 'TX', 'FL'],
            'rank': [1, 2, 3],
            'points': [100, 95, 90],
            'source': ['GotSport Rankings'] * 3,
            'provider': ['GotSport'] * 3,
            'url': ['http://test.com'] * 3
        })
        
        test_new_df = pd.concat([test_new_df, new_teams], ignore_index=True)
        
        # Detect new teams
        new_teams_df = detect_new_teams(test_new_df, baseline_df, logger)
        
        # Get summary
        summary = get_incremental_summary(new_teams_df)
        logger.info(f"📊 Summary: {summary}")
        
        # Save incremental (if any new teams)
        if not new_teams_df.empty:
            output_path = save_incremental(new_teams_df, logger=logger)
            logger.info(f"💾 Saved to: {output_path}")
        
        logger.info("✅ Incremental detector test completed")
        
    except Exception as e:
        logger.error(f"❌ Test failed: {e}")
        raise
