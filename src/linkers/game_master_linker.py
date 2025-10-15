#!/usr/bin/env python3
"""
Game ↔ Master Team Linker

Post-processing module that links scraped game history files to their canonical 
team entries in the master index and identity map, enriching games with master metadata.
"""

import pandas as pd
import json
import logging
from pathlib import Path
from typing import Optional

IDENTITY_MAP_PATH = Path("data/master/team_identity_map.json")


def latest_master_index() -> Path:
    """Auto-detect the most recent master_team_index_*.csv file."""
    # Prioritize migrated files as they have the correct schema
    migrated_files = sorted(Path("data/master").glob("master_team_index_migrated_*.csv"))
    if migrated_files:
        return migrated_files[-1]
    
    # Fallback to any master_team_index file
    files = sorted(Path("data/master").glob("master_team_index_*.csv"))
    if not files:
        raise FileNotFoundError("No master_team_index_*.csv found in data/master/")
    return files[-1]


def load_identity_map() -> dict:
    """Load the team identity map from JSON."""
    if not IDENTITY_MAP_PATH.exists():
        return {}
    
    try:
        with open(IDENTITY_MAP_PATH, 'r', encoding='utf-8') as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError) as e:
        logger = logging.getLogger(__name__)
        logger.warning(f"Failed to load identity map: {e}")
        return {}


def link_games_to_master(
    games_path: str,
    master_path: str,
    output_path: Optional[str] = None
) -> None:
    """
    Link games CSV to master team index, enriching with canonical metadata.
    
    Args:
        games_path: Path to games CSV file
        master_path: Path to master team index CSV
        output_path: Optional custom output path (auto-generated if None)
    """
    logger = logging.getLogger(__name__)
    
    # Load data
    try:
        games_df = pd.read_csv(games_path)
        master_df = pd.read_csv(master_path)
        identity_map = load_identity_map()
    except Exception as e:
        logger.error(f"Failed to load data files: {e}")
        raise
    
    if games_df.empty:
        logger.warning(f"Games CSV is empty: {games_path}")
        return
    
    logger.info(f"Linking {len(games_df)} games from {games_path}")
    
    # Column name normalization for master CSV
    if "team_id_master" not in master_df.columns and "team_id" in master_df.columns:
        master_df.rename(columns={"team_id": "team_id_master"}, inplace=True)
        logger.debug("Renamed 'team_id' to 'team_id_master' in master CSV")
    
    # Ensure team_id_master columns are string type for consistent merging
    games_df["team_id_master"] = games_df["team_id_master"].astype(str)
    master_df["team_id_master"] = master_df["team_id_master"].astype(str)
    
    # Also ensure team_id_source is string for fallback joins
    if "team_id_source" in games_df.columns:
        games_df["team_id_source"] = games_df["team_id_source"].astype(str)
    if "provider_team_id" in master_df.columns:
        master_df["provider_team_id"] = master_df["provider_team_id"].astype(str)
    
    # Validate required columns
    required_games_cols = ["team_id_master", "team_id_source"]
    missing_games_cols = [col for col in required_games_cols if col not in games_df.columns]
    if missing_games_cols:
        logger.error(f"Missing required columns in games CSV: {missing_games_cols}")
        raise ValueError(f"Missing required columns: {missing_games_cols}")
    
    required_master_cols = ["team_id_master"]
    missing_master_cols = [col for col in required_master_cols if col not in master_df.columns]
    if missing_master_cols:
        logger.error(f"Missing required columns in master CSV: {missing_master_cols}")
        raise ValueError(f"Missing required columns: {missing_master_cols}")
    
    # Primary join: Match on team_id_master (canonical)
    master_cols = [
        "team_id_master",
        "team_name",
        "club_name", 
        "state",
        "gender",
        "age_group",
    ]
    
    # Filter to only include columns that exist in master CSV
    available_master_cols = [col for col in master_cols if col in master_df.columns]
    
    merged = games_df.merge(
        master_df[available_master_cols],
        how="left",
        on="team_id_master",
        suffixes=("", "_master"),
    )
    
    # Fallback join: Match on provider_team_id ↔ team_id_source when master link missing
    if "provider_team_id" in master_df.columns:
        # Check for rows where enrichment columns are NaN but team_id_source is notna
        enrichment_cols = [col for col in available_master_cols if col != "team_id_master"]
        mask = merged["team_id_source"].notna()
        for col in enrichment_cols:
            if col in merged.columns:
                mask = mask & merged[col].isna()
        
        if mask.any():
            logger.info(f"Attempting fallback join for {mask.sum()} unmatched games")
            
            fallback = games_df[mask].merge(
                master_df[available_master_cols],
                how="left",
                left_on="team_id_source", 
                right_on="provider_team_id",
                suffixes=("", "_fallback")
            )
            
            # Update merged dataframe with fallback results
            for col in available_master_cols:
                if col != "team_id_master":  # Don't overwrite the primary key
                    # Use fallback values where available, otherwise keep existing values
                    merged.loc[mask, col] = merged.loc[mask, col].fillna(fallback[col])
    
    # Identity map lookup: Fill club names from team_identity_map.json
    def _club_from_map(row):
        """Get club name from identity map if master lacks it."""
        team_id = str(row["team_id_master"])
        if pd.isna(team_id):
            return row.get("club_name")
            
        entry = identity_map.get(team_id)
        if entry and entry.get("club_history"):
            return entry["club_history"][-1]  # Use most recent club
        return row.get("club_name")
    
    # Fill missing club names from identity map
    if "club_name" in merged.columns:
        merged["club_name"] = merged["club_name"].fillna(
            merged.apply(_club_from_map, axis=1)
        )
    
    # Identify unmatched teams
    missing = merged[merged["team_id_master"].isna()]
    if len(missing) > 0:
        logger.warning(f"⚠️  {len(missing)} teams not linked to master. Logged for review.")
        
        # Create unlinked directory
        games_path_obj = Path(games_path)
        unlinked_dir = games_path_obj.parent / "unlinked"
        unlinked_dir.mkdir(parents=True, exist_ok=True)
        
        # Write unlinked teams CSV
        unlinked_filename = games_path_obj.stem + "_unlinked.csv"
        unlinked_path = unlinked_dir / unlinked_filename
        missing.to_csv(unlinked_path, index=False)
        logger.info(f"Unlinked teams saved to: {unlinked_path}")
    
    # Generate output path if not provided
    if not output_path:
        games_path_obj = Path(games_path)
        if "_incremental" in games_path_obj.stem:
            # Handle incremental files
            base_name = games_path_obj.stem.replace("games_", "games_linked_").replace("_incremental", "")
        else:
            base_name = games_path_obj.stem.replace("games_", "games_linked_")
        
        output_path = games_path_obj.parent / f"{base_name}.csv"
    
    # Ensure output directory exists
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    
    # Save linked file
    merged.to_csv(output_path, index=False)
    logger.info(f"✅ Linked file saved → {output_path}")
    
    # Summary statistics
    total = len(merged)
    linked = total - len(missing)
    percentage = (linked / total * 100) if total > 0 else 0
    logger.info(f"📊 Linking complete: {linked}/{total} games linked ({percentage:.1f}%).")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Link games CSV to master team index")
    parser.add_argument("--games", required=True, help="Path to games CSV file")
    parser.add_argument("--master", default=None, help="Path to master CSV file (auto-detected if not provided)")
    parser.add_argument("--output", default=None, help="Output path (auto-generated if not provided)")
    
    args = parser.parse_args()
    
    # Setup logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    # Auto-detect master path if not provided
    master_path = args.master if args.master else str(latest_master_index())
    
    try:
        link_games_to_master(
            games_path=args.games,
            master_path=master_path,
            output_path=args.output
        )
        print("Linking completed successfully!")
    except Exception as e:
        print(f"Linking failed: {e}")
        exit(1)
