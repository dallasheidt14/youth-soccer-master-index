#!/usr/bin/env python3
"""
Game Writers Utilities

Handles writing game history data to CSV files with atomic operations.
Creates both games CSV and club lookup CSV outputs.
"""

import pandas as pd
import logging
from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import datetime

from src.io.safe_write import safe_write_csv
from src.schema.game_history_schema import validate_games_dataframe, validate_club_lookup_dataframe, GAMES_COLUMNS, CLUB_COLUMNS


def write_games_csv(games_df: pd.DataFrame, provider: str, state: str, gender: str, age_group: str, build_id: str, incremental: bool = False, existing_file: Optional[Path] = None) -> Path:
    """
    Write games data to CSV file with atomic operation.
    
    Args:
        games_df: DataFrame with game data
        provider: Provider name (e.g., 'gotsport')
        state: State code (e.g., 'AZ')
        gender: Gender ('M' or 'F')
        age_group: Age group (e.g., 'U10')
        build_id: Build identifier
        incremental: If True, append to existing file instead of overwriting
        existing_file: Path to existing file for incremental mode
        
    Returns:
        Path to written CSV file
    """
    logger = logging.getLogger(__name__)
    
    # Validate build_id format and consistency
    if not build_id.startswith("build_"):
        logger.warning(f"Build ID '{build_id}' does not follow expected format 'build_YYYYMMDD_HHMM'")
    
    logger.debug(f"Writing games CSV with build_id: {build_id}")
    
    if games_df.empty:
        logger.warning(f"No games data to write for {provider}_{state}_{gender}_{age_group}")
        return None
    
    # Validate data against schema
    try:
        validated_df = validate_games_dataframe(games_df)
        logger.info(f"Validated {len(validated_df)} games for {provider}_{state}_{gender}_{age_group}")
    except Exception as e:
        logger.exception("Schema validation failed")
        raise
    
    # Determine output path based on mode
    if incremental and existing_file and existing_file.exists():
        # Use existing file for incremental mode
        output_path = existing_file
        logger.info(f"Incremental mode: appending to existing file {output_path}")
    else:
        # Create new file in build directory
        output_dir = Path(f"data/games/{build_id}")
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Create filename
        filename = f"games_{provider}_{state}_{gender}_{age_group}.csv"
        output_path = output_dir / filename
    
    # Ensure all required columns are present
    for col in GAMES_COLUMNS:
        if col not in validated_df.columns:
            validated_df[col] = None
    
    # Reorder columns to match schema
    validated_df = validated_df[GAMES_COLUMNS]
    
    # Write CSV with atomic operation
    try:
        if incremental and existing_file and existing_file.exists():
            # Append mode: read existing data, combine, and write
            existing_df = pd.read_csv(existing_file)
            combined_df = pd.concat([existing_df, validated_df], ignore_index=True)
            # Remove duplicates based on game_id if it exists
            if 'game_id' in combined_df.columns:
                combined_df = combined_df.drop_duplicates(subset=['game_id'], keep='last')
            
            # For incremental mode, write to a new file in the current build directory
            # to avoid file locking issues
            incremental_output_dir = Path(f"data/games/{build_id}")
            incremental_output_dir.mkdir(parents=True, exist_ok=True)
            incremental_filename = f"games_{provider}_{state}_{gender}_{age_group}_incremental.csv"
            incremental_output_path = incremental_output_dir / incremental_filename
            
            safe_write_csv(combined_df, incremental_output_path)
            logger.info(f"Appended {len(validated_df)} new games to {incremental_output_path} (total: {len(combined_df)})")
            return incremental_output_path
        else:
            # Overwrite mode: write new file
            safe_write_csv(validated_df, output_path)
            logger.info(f"Wrote {len(validated_df)} games to {output_path}")
            return output_path
    except Exception as e:
        logger.exception("Failed to write games CSV")
        raise


def write_club_lookup_csv(games_df: pd.DataFrame, provider: str, state: str, gender: str, age_group: str, build_id: str, incremental: bool = False, existing_file: Optional[Path] = None) -> Path:
    """
    Write club lookup data to CSV file with atomic operation.
    
    Args:
        games_df: DataFrame with game data (used to extract clubs)
        provider: Provider name (e.g., 'gotsport')
        state: State code (e.g., 'AZ')
        gender: Gender ('M' or 'F')
        age_group: Age group (e.g., 'U10')
        build_id: Build identifier
        incremental: If True, append to existing file instead of overwriting
        existing_file: Path to existing file for incremental mode
        
    Returns:
        Path to written CSV file
    """
    logger = logging.getLogger(__name__)
    
    # Validate build_id format and consistency
    if not build_id.startswith("build_"):
        logger.warning(f"Build ID '{build_id}' does not follow expected format 'build_YYYYMMDD_HHMM'")
    
    logger.debug(f"Writing club lookup CSV with build_id: {build_id}")
    
    if games_df.empty:
        logger.warning(f"No games data to extract clubs from for {provider}_{state}_{gender}_{age_group}")
        return None
    
    # Extract unique clubs from games
    clubs_data = extract_clubs_from_games(games_df, provider)
    
    if not clubs_data:
        logger.warning(f"No clubs found in games for {provider}_{state}_{gender}_{age_group}")
        return None
    
    # Create DataFrame
    clubs_df = pd.DataFrame(clubs_data)
    
    # Validate data against schema
    try:
        validated_df = validate_club_lookup_dataframe(clubs_df)
        logger.info(f"Validated {len(validated_df)} clubs for {provider}_{state}_{gender}_{age_group}")
    except Exception as e:
        logger.exception("Club lookup schema validation failed")
        raise
    
    # Determine output path based on mode
    if incremental and existing_file and existing_file.exists():
        # Use existing file for incremental mode
        output_path = existing_file
        logger.info(f"Incremental mode: appending to existing club file {output_path}")
    else:
        # Create new file in build directory
        output_dir = Path(f"data/games/{build_id}")
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Create filename
        filename = f"club_lookup_{provider}_{state}_{gender}_{age_group}.csv"
        output_path = output_dir / filename
    
    # Ensure all required columns are present
    for col in CLUB_COLUMNS:
        if col not in validated_df.columns:
            validated_df[col] = None
    
    # Reorder columns to match schema
    validated_df = validated_df[CLUB_COLUMNS]
    
    # Write CSV with atomic operation
    try:
        if incremental and existing_file and existing_file.exists():
            # Append mode: read existing data, combine, and write
            existing_df = pd.read_csv(existing_file)
            combined_df = pd.concat([existing_df, validated_df], ignore_index=True)
            # Remove duplicates based on club_id if it exists
            if 'club_id' in combined_df.columns:
                combined_df = combined_df.drop_duplicates(subset=['club_id'], keep='last')
            
            # For incremental mode, write to a new file in the current build directory
            # to avoid file locking issues
            incremental_output_dir = Path(f"data/games/{build_id}")
            incremental_output_dir.mkdir(parents=True, exist_ok=True)
            incremental_filename = f"club_lookup_{provider}_{state}_{gender}_{age_group}_incremental.csv"
            incremental_output_path = incremental_output_dir / incremental_filename
            
            safe_write_csv(combined_df, incremental_output_path)
            logger.info(f"Appended {len(validated_df)} new clubs to {incremental_output_path} (total: {len(combined_df)})")
            return incremental_output_path
        else:
            # Overwrite mode: write new file
            safe_write_csv(validated_df, output_path)
            logger.info(f"Wrote {len(validated_df)} clubs to {output_path}")
            return output_path
    except Exception as e:
        logger.exception("Failed to write club lookup CSV")
        raise


def extract_clubs_from_games(games_df: pd.DataFrame, provider: str) -> List[Dict[str, Any]]:
    """
    Extract unique clubs from games data.
    
    Args:
        games_df: DataFrame with game data
        provider: Provider name
        
    Returns:
        List of club dictionaries
    """
    logger = logging.getLogger(__name__)
    
    clubs = {}
    current_time = datetime.utcnow().isoformat()
    
    # Process each game to extract club information
    for _, game in games_df.iterrows():
        club_name = game.get('club_name')
        if pd.isna(club_name) or not club_name:
            continue
        
        club_name = str(club_name).strip()
        if not club_name:
            continue
        
        # Create club key (name + state for uniqueness)
        club_key = f"{club_name}_{game.get('state', '')}"
        
        if club_key not in clubs:
            clubs[club_key] = {
                'provider': provider,
                'club_id': None,  # Not available from games
                'club_name': club_name,
                'state': game.get('state', ''),
                'city': game.get('city', ''),
                'website': None,  # Not available from games
                'first_seen_at': current_time,
                'last_seen_at': current_time,
                'source_url': game.get('source_url', '')
            }
        else:
            # Update last_seen_at
            clubs[club_key]['last_seen_at'] = current_time
            
            # Update city if available and different
            game_city = game.get('city')
            if game_city and pd.notna(game_city) and not clubs[club_key]['city']:
                clubs[club_key]['city'] = str(game_city)
    
    clubs_list = list(clubs.values())
    logger.info(f"Extracted {len(clubs_list)} unique clubs from {len(games_df)} games")
    
    return clubs_list


def write_slice_summary(games_df: pd.DataFrame, provider: str, state: str, gender: str, age_group: str, build_id: str, 
                       teams_processed: int, games_scraped: int, skipped_inactive: int, clubs_data: Optional[List[Dict[str, Any]]] = None) -> Path:
    """
    Write slice summary to JSON file.
    
    Args:
        games_df: DataFrame with game data
        provider: Provider name
        state: State code
        gender: Gender
        age_group: Age group
        build_id: Build identifier
        teams_processed: Number of teams processed
        games_scraped: Number of games scraped
        skipped_inactive: Number of teams skipped due to inactivity
        
    Returns:
        Path to written summary file
    """
    logger = logging.getLogger(__name__)
    
    # Create summary data
    summary = {
        'slice_key': f"{provider}_{state}_{gender}_{age_group}",
        'provider': provider,
        'state': state,
        'gender': gender,
        'age_group': age_group,
        'build_id': build_id,
        'teams_processed': teams_processed,
        'games_scraped': games_scraped,
        'games_written': len(games_df),
        'skipped_inactive': skipped_inactive,
        'clubs_found': len(clubs_data) if clubs_data else (len(extract_clubs_from_games(games_df, provider)) if not games_df.empty else 0),
        'completed_at': datetime.utcnow().isoformat()
    }
    
    # Create output directory
    output_dir = Path(f"data/games/{build_id}")
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Create filename
    filename = f"summary_{provider}_{state}_{gender}_{age_group}.json"
    output_path = output_dir / filename
    
    # Write JSON file
    try:
        import json
        with open(output_path, 'w') as f:
            json.dump(summary, f, indent=2)
        
        logger.info(f"Wrote slice summary to {output_path}")
        return output_path
    except Exception as e:
        logger.exception("Failed to write slice summary")
        raise


def get_output_paths(build_id: str, provider: str, state: str, gender: str, age_group: str) -> Dict[str, Path]:
    """
    Get expected output paths for a slice.
    
    Args:
        build_id: Build identifier
        provider: Provider name
        state: State code
        gender: Gender
        age_group: Age group
        
    Returns:
        Dictionary with expected output paths
    """
    base_dir = Path(f"data/games/{build_id}")
    
    return {
        'games_csv': base_dir / f"games_{provider}_{state}_{gender}_{age_group}.csv",
        'club_lookup_csv': base_dir / f"club_lookup_{provider}_{state}_{gender}_{age_group}.csv",
        'summary_json': base_dir / f"summary_{provider}_{state}_{gender}_{age_group}.json"
    }


def cleanup_failed_writes(build_id: str) -> None:
    """
    Clean up any partial files from failed writes.
    
    Args:
        build_id: Build identifier
    """
    logger = logging.getLogger(__name__)
    
    build_dir = Path(f"data/games/{build_id}")
    if not build_dir.exists():
        return
    
    # Look for temporary files
    temp_files = list(build_dir.glob("*.tmp"))
    
    for temp_file in temp_files:
        try:
            temp_file.unlink()
            logger.info(f"Cleaned up temporary file: {temp_file}")
        except Exception as e:
            logger.warning(f"Failed to clean up temporary file {temp_file}: {e}")


def validate_output_files(output_paths: Dict[str, Path]) -> Dict[str, bool]:
    """
    Validate that output files exist and are readable.
    
    Args:
        output_paths: Dictionary of expected output paths
        
    Returns:
        Dictionary with validation results
    """
    logger = logging.getLogger(__name__)
    results = {}
    
    for file_type, path in output_paths.items():
        try:
            if path.exists() and path.stat().st_size > 0:
                results[file_type] = True
                logger.debug(f"Output file validated: {path}")
            else:
                results[file_type] = False
                logger.warning(f"Output file missing or empty: {path}")
        except Exception as e:
            results[file_type] = False
            logger.exception(f"Error validating output file {path}")
    
    return results
