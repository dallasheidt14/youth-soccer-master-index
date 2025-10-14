#!/usr/bin/env python3
"""
State Normalization Utility

Normalizes state/region codes in the master team index and removes non-U.S. records.
Handles regional variations and filters to USA-only teams.
"""

import pandas as pd
import logging
from typing import List, Optional, Dict


# State mapping for regional variations
STATE_MAP = {
    "CAS": "CA",  # California South
    "CAN": "CA",  # California North
    "TXN": "TX",  # Texas North
    "TXS": "TX",  # Texas South
    "PAE": "PA",  # Pennsylvania East
    "PAW": "PA",  # Pennsylvania West
    "NYE": "NY",  # New York East
    "NYW": "NY",  # New York West
}

# Non-U.S. country/region codes to remove
NON_US_CODES = [
    "CAN",    # Canada
    "MEX",    # Mexico
    "GBR",    # Great Britain
    "FRA",    # France
    "JAM",    # Jamaica
    "JPN",    # Japan
    "IRL",    # Ireland
    "ARG",    # Argentina
    "COL",    # Colombia
    "PUR",    # Puerto Rico
    "QC",     # Quebec
    "ON",     # Ontario
    "BC",     # British Columbia
    "AB",     # Alberta
    "CND",    # Canada (alternative)
    "Surrey", # Surrey, BC
    "OTH"     # Other/Unknown
]


def normalize_states(df: pd.DataFrame, logger: Optional[logging.Logger] = None) -> pd.DataFrame:
    """
    Normalize state codes and filter to USA-only teams.
    
    Args:
        df: DataFrame with 'state' column
        logger: Optional logger instance for output
        
    Returns:
        Cleaned DataFrame with normalized state codes and USA-only teams
    """
    if df.empty:
        if logger:
            logger.warning("⚠️ Empty DataFrame provided to normalize_states")
        return df
    
    # Create a copy to avoid modifying original
    df_clean = df.copy()
    original_count = len(df_clean)
    
    if logger:
        logger.info(f"🔧 Starting state normalization for {original_count:,} teams")
    
    # Log original state distribution
    if 'state' in df_clean.columns:
        original_states = df_clean['state'].value_counts()
        if logger:
            logger.info(f"📊 Original states found: {len(original_states)}")
            logger.debug(f"Top 10 states: {original_states.head(10).to_dict()}")
    else:
        if logger:
            logger.warning("⚠️ No 'state' column found in DataFrame")
        return df_clean
    
    # Apply state mapping for regional variations
    mapping_count = 0
    for old_code, new_code in STATE_MAP.items():
        mask = df_clean['state'] == old_code
        count = mask.sum()
        if count > 0:
            df_clean.loc[mask, 'state'] = new_code
            mapping_count += count
            if logger:
                logger.info(f"🔄 Mapped {count:,} teams: {old_code} → {new_code}")
    
    if logger and mapping_count > 0:
        logger.info(f"✅ Total regional mappings applied: {mapping_count:,}")
    
    # Filter out non-U.S. teams
    non_us_mask = df_clean['state'].isin(NON_US_CODES)
    non_us_count = non_us_mask.sum()
    
    if non_us_count > 0:
        if logger:
            logger.info(f"🌍 Removing {non_us_count:,} non-U.S. teams")
            removed_states = df_clean[non_us_mask]['state'].value_counts()
            logger.debug(f"Removed states: {removed_states.to_dict()}")
        
        df_clean = df_clean[~non_us_mask].copy()
    
    # Remove teams with invalid/empty state codes
    invalid_mask = (
        df_clean['state'].isna() | 
        (df_clean['state'] == '') | 
        (df_clean['state'].str.len() < 2)
    )
    invalid_count = invalid_mask.sum()
    
    if invalid_count > 0:
        if logger:
            logger.info(f"⚠️ Removing {invalid_count:,} teams with invalid state codes")
        df_clean = df_clean[~invalid_mask].copy()
    
    # Reset index after filtering
    df_clean = df_clean.reset_index(drop=True)
    
    # Log final results
    final_count = len(df_clean)
    removed_count = original_count - final_count
    
    if logger:
        logger.info(f"✅ State normalization complete:")
        logger.info(f"   • Original teams: {original_count:,}")
        logger.info(f"   • Final teams: {final_count:,}")
        logger.info(f"   • Removed: {removed_count:,} ({removed_count/original_count*100:.1f}%)")
        
        # Log final state distribution
        if final_count > 0:
            final_states = df_clean['state'].value_counts()
            logger.info(f"📊 Final U.S. states: {len(final_states)}")
            logger.info(f"   • Top 5 states: {final_states.head(5).to_dict()}")
    
    return df_clean


def get_valid_states(df: pd.DataFrame) -> List[str]:
    """
    Get a sorted list of unique U.S. states remaining in the DataFrame.
    
    Args:
        df: DataFrame with 'state' column
        
    Returns:
        Sorted list of unique state codes
    """
    if df.empty or 'state' not in df.columns:
        return []
    
    # Get unique states, excluding NaN values
    states = df['state'].dropna().unique()
    
    # Filter out non-U.S. codes
    us_states = [state for state in states if state not in NON_US_CODES]
    
    # Sort alphabetically
    return sorted(us_states)


def get_state_statistics(df: pd.DataFrame) -> Dict[str, any]:
    """
    Get comprehensive statistics about state distribution.
    
    Args:
        df: DataFrame with 'state' column
        
    Returns:
        Dictionary with state statistics
    """
    if df.empty or 'state' not in df.columns:
        return {
            'total_states': 0,
            'total_teams': 0,
            'state_distribution': {},
            'top_states': [],
            'regional_mappings': {},
            'non_us_removed': 0
        }
    
    state_counts = df['state'].value_counts()
    
    # Count regional mappings that were applied
    regional_mappings = {}
    for old_code, new_code in STATE_MAP.items():
        if old_code in state_counts.index:
            regional_mappings[f"{old_code}→{new_code}"] = state_counts[old_code]
    
    # Count non-U.S. teams that were removed
    non_us_removed = 0
    for code in NON_US_CODES:
        if code in state_counts.index:
            non_us_removed += state_counts[code]
    
    return {
        'total_states': len(state_counts),
        'total_teams': len(df),
        'state_distribution': state_counts.to_dict(),
        'top_states': state_counts.head(10).to_dict(),
        'regional_mappings': regional_mappings,
        'non_us_removed': non_us_removed
    }


if __name__ == "__main__":
    # Test the state normalizer
    import sys
    from pathlib import Path
    
    # Add project root to path
    project_root = Path(__file__).parent.parent.parent
    sys.path.append(str(project_root))
    
    try:
        from src.scraper.utils.logger import get_logger
        logger = get_logger("state_normalizer_test.log")
    except ImportError:
        # Fallback logger for testing
        import logging
        logging.basicConfig(level=logging.INFO)
        logger = logging.getLogger("test")
    
    # Create test data
    test_data = {
        'team_name': [
            'Team CA', 'Team CAS', 'Team CAN', 'Team TXN', 'Team TXS',
            'Team PAE', 'Team PAW', 'Team NYE', 'Team NYW', 'Team CAN',
            'Team MEX', 'Team GBR', 'Team FRA', 'Team JAM', 'Team JPN'
        ],
        'state': [
            'CA', 'CAS', 'CAN', 'TXN', 'TXS',
            'PAE', 'PAW', 'NYE', 'NYW', 'CAN',
            'MEX', 'GBR', 'FRA', 'JAM', 'JPN'
        ],
        'age_group': ['U12'] * 15,
        'gender': ['Male'] * 15
    }
    
    df_test = pd.DataFrame(test_data)
    logger.info("🧪 Testing state normalizer with sample data")
    logger.info(f"Original data:\n{df_test}")
    
    df_normalized = normalize_states(df_test, logger)
    logger.info(f"Normalized data:\n{df_normalized}")
    
    valid_states = get_valid_states(df_normalized)
    logger.info(f"Valid U.S. states: {valid_states}")
    
    stats = get_state_statistics(df_normalized)
    logger.info(f"Statistics: {stats}")