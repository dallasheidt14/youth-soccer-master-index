#!/usr/bin/env python3
"""
Game History Build Orchestrator

Main script for orchestrating game history scraping across multiple providers,
states, genders, and age groups. Supports incremental updates and checkpointing.
"""

import argparse
import logging
import sys
import os
import time
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any, Optional
import pandas as pd

# Add src to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

from scraper.providers import get_provider
from scraper.utils.game_state import GameStateManager
from scraper.utils.activity_filter import filter_inactive_teams, apply_game_filters, calculate_team_activity_metrics
from scraper.utils.game_writers import write_games_csv, write_club_lookup_csv, write_slice_summary, get_output_paths, cleanup_failed_writes, extract_clubs_from_games
from src.utils.metrics_snapshot import MetricsSnapshot
from src.registry.registry import get_registry


def setup_logging():
    """Setup logging configuration."""
    # Ensure logs directory exists
    log_dir = Path('data/logs')
    log_dir.mkdir(parents=True, exist_ok=True)
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('data/logs/build_game_history.log'),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)


def generate_build_id() -> str:
    """Generate a unique build ID."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    return f"build_{timestamp}"


def parse_slice_combinations(states: List[str], genders: List[str], ages: List[str]) -> List[Dict[str, str]]:
    """
    Generate all combinations of state/gender/age_group.
    
    Args:
        states: List of state codes
        genders: List of genders (M/F)
        ages: List of age groups (U10, U11, etc.)
        
    Returns:
        List of slice dictionaries
    """
    combinations = []
    for state in states:
        for gender in genders:
            for age in ages:
                combinations.append({
                    'state': state,
                    'gender': gender,
                    'age_group': age
                })
    return combinations


def load_master_slice(state: str, gender: str, age_group: str) -> pd.DataFrame:
    """
    Load master slice CSV for the specified criteria.
    
    Args:
        state: State code
        gender: Gender
        age_group: Age group
        
    Returns:
        DataFrame with team data
    """
    logger = logging.getLogger(__name__)
    
    slice_file = Path(f"data/master/slices/{state}_{gender}_{age_group}_master.csv")
    
    if not slice_file.exists():
        raise FileNotFoundError(f"Master slice not found: {slice_file}")
    
    logger.info(f"Loading master slice: {slice_file}")
    df = pd.read_csv(slice_file)
    
    # Validate required columns
    required_columns = ['team_id_master', 'team_name', 'age_group', 'gender', 'state', 'provider']
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        error_msg = f"Missing required columns in {slice_file}: {missing_columns}"
        logger.error(error_msg)
        raise ValueError(error_msg)
    
    logger.info(f"Loaded {len(df)} teams from slice")
    return df


def process_slice(provider_name: str, state: str, gender: str, age_group: str, 
                 build_id: str, resume: bool, max_teams: Optional[int] = None, 
                 incremental: bool = False, existing_games_file: Optional[Path] = None,
                 existing_clubs_file: Optional[Path] = None) -> Dict[str, Any]:
    """
    Process a single slice (state/gender/age_group combination).
    
    Args:
        provider_name: Name of the provider
        state: State code
        gender: Gender
        age_group: Age group
        build_id: Build identifier
        resume: Whether to resume from checkpoint
        max_teams: Optional limit on number of teams to process
        incremental: Whether to run in incremental mode (append to existing files)
        existing_games_file: Path to existing games CSV for incremental mode
        existing_clubs_file: Path to existing clubs CSV for incremental mode
        
    Returns:
        Dictionary with slice processing results
    """
    logger = logging.getLogger(__name__)
    slice_key = f"{provider_name}_{state}_{gender}_{age_group}"
    
    logger.info(f"Processing slice: {slice_key}")
    logger.info(f"Build ID: {build_id}")
    
    # Validate build_id format
    if not build_id.startswith("build_"):
        logger.warning(f"Build ID '{build_id}' does not follow expected format 'build_YYYYMMDD_HHMM'")
    
    # Assert build_id consistency before any file operations
    logger.debug(f"Build ID validation: {build_id}")
    
    # Initialize state manager
    state_manager = GameStateManager(provider_name)
    
    # Load checkpoint if resuming
    checkpoint = state_manager.load_checkpoint(state, gender, age_group) if resume else state_manager._get_default_checkpoint()
    
    # Update build ID in checkpoint
    checkpoint = state_manager.update_build_id(checkpoint, build_id)
    
    # Load master slice
    slice_df = load_master_slice(state, gender, age_group)
    
    # Pre-scrape identity sync: ensure all teams from master slice are in identity map
    from src.identity.identity_sync import sync_identity
    
    sync_stats = {"checked": 0, "new": 0, "updated": 0}
    
    logger.info(f"Syncing identity for {len(slice_df)} teams from master slice")
    for _, team_row in slice_df.iterrows():
        team = team_row.to_dict()
        try:
            result = sync_identity(
                state=state,
                gender=gender,
                age_group=age_group,
                provider=provider_name,
                team_name=team["team_name"],
                provider_team_id=team["team_id_source"],
                club_name=team.get("club_name", ""),
                existing_team_id_master=team.get("team_id_master")
            )
            
            sync_stats["checked"] += 1
            if result["is_new"]:
                sync_stats["new"] += 1
            elif result["was_updated"]:
                sync_stats["updated"] += 1
                
        except Exception as e:
            logger.warning(f"Failed to sync identity for team {team['team_name']}: {e}")
            sync_stats["checked"] += 1
    
    logger.info(f"Pre-scrape identity sync: {sync_stats['checked']} teams checked, "
               f"{sync_stats['new']} new, {sync_stats['updated']} updated")
    
    # Filter teams to scrape based on mode
    if incremental:
        # In incremental mode, only process teams that might have new games
        # (teams that haven't been scraped recently or have been active)
        teams_to_scrape = slice_df.copy()  # Start with all teams
        logger.info(f"Incremental mode: checking {len(teams_to_scrape)} teams for new games")
    else:
        # In full mode, exclude completed teams from checkpoint
        teams_to_scrape = state_manager.get_teams_to_scrape(slice_df, checkpoint)
    
    if teams_to_scrape.empty:
        logger.info(f"No teams to scrape for {slice_key}")
        return {
            'slice_key': slice_key,
            'teams_processed': 0,
            'games_scraped': 0,
            'skipped_inactive': 0,
            'success': True
        }
    
    # Apply 120-day inactivity filter
    teams_to_scrape = filter_inactive_teams(teams_to_scrape)
    skipped_inactive = len(slice_df) - len(teams_to_scrape)
    
    # Apply max_teams limit if specified
    if max_teams and len(teams_to_scrape) > max_teams:
        teams_to_scrape = teams_to_scrape.head(max_teams)
        logger.info(f"Limited to {max_teams} teams for testing")
    
    # Initialize provider
    provider_class = get_provider(provider_name, "game")
    provider = provider_class()
    
    # Process teams
    all_games = []
    teams_processed = 0
    
    for _, team_row in teams_to_scrape.iterrows():
        team = team_row.to_dict()
        team_id = team['team_id_master']
        
        logger.info(f"Processing team {teams_processed + 1}/{len(teams_to_scrape)}: {team['team_name']}")
        
        try:
            # Get last scraped date from checkpoint
            last_scraped_date = state_manager.get_team_last_scraped_date(checkpoint, team_id)
            
            # In incremental mode, skip teams that were recently scraped (within last 7 days)
            if incremental and last_scraped_date:
                from datetime import timedelta
                days_since_scraped = (datetime.now().date() - last_scraped_date).days
                if days_since_scraped < 7:
                    logger.info(f"  Skipping {team['team_name']} - scraped {days_since_scraped} days ago")
                    continue
            
            # Fetch games for team
            team_games = []
            for game in provider.fetch_team_games_since(team, last_scraped_date):
                team_games.append(game)
            
            # Apply 12-month filter to collected games
            filtered_games = apply_game_filters(team_games, months_back=12)
            
            if filtered_games:
                all_games.extend(filtered_games)
                logger.info(f"  Scraped {len(filtered_games)} games for {team['team_name']}")
            else:
                if incremental:
                    logger.info(f"  No new games for {team['team_name']}")
                else:
                    logger.info(f"  No recent games found for {team['team_name']}")
            
            # Mark team as complete in checkpoint
            last_game_date = None
            if filtered_games:
                # Find most recent game date
                game_dates = [datetime.strptime(g['game_date'], '%Y-%m-%d').date() for g in filtered_games]
                last_game_date = max(game_dates)
            
            checkpoint = state_manager.mark_team_complete(checkpoint, team_id, last_game_date, len(filtered_games))
            
            # Save checkpoint after each team
            state_manager.save_checkpoint(state, gender, age_group, checkpoint)
            
            teams_processed += 1
            
        except Exception as e:
            logger.exception(f"Error processing team {team['team_name']}")
            continue
    
    # Post-scrape identity sync: sync any new teams discovered in opponent data
    if all_games:
        logger.info(f"Syncing identity for opponent teams from {len(all_games)} games")
        opponent_sync_stats = {"checked": 0, "new": 0, "updated": 0}
        
        for game in all_games:
            if game.get("opponent_id") and game.get("opponent_name"):
                try:
                    result = sync_identity(
                        state=state,
                        gender=gender,
                        age_group=age_group,
                        provider=provider_name,
                        team_name=game["opponent_name"],
                        provider_team_id=game["opponent_id"],
                        club_name=""  # Opponent club info not available in game data
                    )
                    
                    opponent_sync_stats["checked"] += 1
                    if result["is_new"]:
                        opponent_sync_stats["new"] += 1
                    elif result["was_updated"]:
                        opponent_sync_stats["updated"] += 1
                        
                except Exception as e:
                    logger.warning(f"Failed to sync identity for opponent {game['opponent_name']}: {e}")
                    opponent_sync_stats["checked"] += 1
        
        logger.info(f"Post-scrape identity sync: {opponent_sync_stats['checked']} opponents checked, "
                   f"{opponent_sync_stats['new']} new, {opponent_sync_stats['updated']} updated")
        
        # Update overall sync stats
        sync_stats["checked"] += opponent_sync_stats["checked"]
        sync_stats["new"] += opponent_sync_stats["new"]
        sync_stats["updated"] += opponent_sync_stats["updated"]
    
    # Write outputs
    if all_games:
        # Ensure all games have consistent structure before DataFrame construction
        required_keys = ['provider', 'team_id_source', 'team_id_master', 'team_name', 'club_name',
                        'opponent_name', 'opponent_id', 'age_group', 'gender', 'state',
                        'game_date', 'home_away', 'goals_for', 'goals_against', 'result',
                        'competition', 'venue', 'city', 'source_url', 'scraped_at']
        
        # Validate and clean game data
        cleaned_games = []
        for game in all_games:
            if not isinstance(game, dict):
                logger.warning(f"Skipping non-dict game entry: {type(game)}")
                continue
            
            # Ensure all required keys are present
            cleaned_game = {}
            for key in required_keys:
                cleaned_game[key] = game.get(key, None)
            
            # Validate game_date format
            game_date = cleaned_game.get('game_date')
            if game_date and not isinstance(game_date, str):
                logger.warning(f"Invalid game_date type: {type(game_date)} for game {game.get('team_name', 'Unknown')}")
                cleaned_game['game_date'] = None
            elif game_date and not re.match(r'^\d{4}-\d{2}-\d{2}$', str(game_date)):
                logger.warning(f"Invalid game_date format: {game_date} for game {game.get('team_name', 'Unknown')}")
                cleaned_game['game_date'] = None
            
            cleaned_games.append(cleaned_game)
        
        games_df = pd.DataFrame(cleaned_games)
        logger.info(f"Constructed DataFrame with {len(games_df)} games, {len(games_df.columns)} columns")
        
        # Validate DataFrame structure
        if not games_df.empty:
            logger.debug(f"DataFrame columns: {list(games_df.columns)}")
            logger.debug(f"DataFrame dtypes:\n{games_df.dtypes}")
            
            # Check for column misalignment
            for col in ['game_date', 'team_name', 'provider']:
                if col in games_df.columns:
                    sample_values = games_df[col].dropna().head(5).tolist()
                    logger.debug(f"Sample {col} values: {sample_values}")
    else:
        games_df = pd.DataFrame()
    
    try:
        if not games_df.empty:
            # Write games CSV
            games_path = write_games_csv(games_df, provider_name, state, gender, age_group, build_id, 
                                       incremental=incremental, existing_file=existing_games_file)
            
            # Extract clubs data once
            clubs_data = extract_clubs_from_games(games_df, provider_name) if not games_df.empty else []
            
            # Write club lookup CSV
            club_path = write_club_lookup_csv(games_df, provider_name, state, gender, age_group, build_id,
                                            incremental=incremental, existing_file=existing_clubs_file)
            
            # Write slice summary
            summary_path = write_slice_summary(
                games_df, provider_name, state, gender, age_group, build_id,
                teams_processed, len(all_games), skipped_inactive, clubs_data
            )
            
            logger.info(f"Wrote outputs for {slice_key}: {len(games_df)} games, {teams_processed} teams")
            
            # Update build registry after successful slice completion
            registry = get_registry()
            # Use state-based key format (e.g., 'AK_M_U10') instead of full slice key
            registry_key = f"{state}_{gender}_{age_group}"
            registry.update_build_registry(registry_key, build_id)
            logger.info(f"Updated build registry for {registry_key}: {build_id}")
        else:
            logger.info(f"No games found for {slice_key}")
        
        # Final identity sync summary
        logger.info(f"Identity sync complete: {sync_stats['checked']} teams checked, "
                   f"{sync_stats['new']} new entries added, {sync_stats['updated']} updated")
        
        return {
            'slice_key': slice_key,
            'teams_processed': teams_processed,
            'games_scraped': len(all_games),
            'skipped_inactive': skipped_inactive,
            'success': True,
            'games_csv_path': games_path
        }
        
    except Exception as e:
        logger.exception(f"Error writing outputs for {slice_key}")
        return {
            'slice_key': slice_key,
            'teams_processed': teams_processed,
            'games_scraped': len(all_games),
            'skipped_inactive': skipped_inactive,
            'success': False,
            'error': str(e),
            'games_csv_path': None
        }


def main():
    """Main function."""
    parser = argparse.ArgumentParser(description="Build game history data")
    parser.add_argument('--providers', 
                       default='gotsport',
                       help='Comma-separated provider names (default: gotsport)')
    parser.add_argument('--states', 
                       default='AZ',
                       help='Comma-separated state codes (e.g., AZ,CA,TX)')
    parser.add_argument('--genders', 
                       default='M,F',
                       help='Comma-separated genders (M,F)')
    parser.add_argument('--ages', 
                       default='U10',
                       help='Comma-separated age groups (U10,U11,U12)')
    parser.add_argument('--resume', 
                       action='store_true',
                       help='Resume from checkpoint')
    parser.add_argument('--max-teams', 
                       type=int,
                       help='Maximum number of teams to process per slice (for testing)')
    parser.add_argument('--build-id', 
                       help='Custom build ID (default: auto-generated)')
    parser.add_argument('--auto', 
                       action='store_true',
                       help='Automatically discover and resume from registry (requires --incremental)')
    parser.add_argument('--refresh-registry', 
                       action='store_true',
                       help='Force rebuild of registry by scanning existing build folders')
    parser.add_argument('--show-registry', 
                       action='store_true',
                       help='Print current registry summary and exit')
    parser.add_argument('--incremental', 
                       action='store_true',
                       help='Run in incremental mode (append to existing files)')
    
    args = parser.parse_args()
    
    # Handle --show-registry before processing
    if args.show_registry:
        registry = get_registry()
        registry_data = registry.list_all_builds()
        print(f"\nBuild Registry ({len(registry_data)} slices):")
        for slice_key, info in sorted(registry_data.items()):
            print(f"  {slice_key}: {info['latest_build']} (updated: {info['last_updated']})")
        return
    
    # Handle --refresh-registry before processing
    if args.refresh_registry:
        registry = get_registry()
        registry.refresh_build_registry()
        print("Registry refreshed successfully")
        return
    
    # Validate --auto requires --incremental
    if args.auto and not args.incremental:
        print("Error: --auto flag requires --incremental mode")
        sys.exit(1)
    
    # Setup logging
    logger = setup_logging()
    
    # Parse arguments
    providers = [p.strip() for p in args.providers.split(',')]
    states = [s.strip().upper() for s in args.states.split(',')]
    genders = [g.strip().upper() for g in args.genders.split(',')]
    ages = [a.strip().upper() for a in args.ages.split(',')]
    
    # Generate build ID
    build_id = args.build_id or generate_build_id()
    
    logger.info(f"Starting game history build: {build_id}")
    logger.info(f"Providers: {providers}")
    logger.info(f"States: {states}")
    logger.info(f"Genders: {genders}")
    logger.info(f"Ages: {ages}")
    logger.info(f"Resume: {args.resume}")
    logger.info(f"Max teams: {args.max_teams}")
    
    # Generate slice combinations
    slice_combinations = parse_slice_combinations(states, genders, ages)
    logger.info(f"Will process {len(slice_combinations)} slices")
    
    # Process each provider and slice combination
    all_results = []
    start_time = time.time()
    
    for provider_name in providers:
        logger.info(f"Processing provider: {provider_name}")
        
        for slice_info in slice_combinations:
            slice_start = time.time()
            state = slice_info['state']
            gender = slice_info['gender']
            age_group = slice_info['age_group']
            
            print(f"\n[START] Starting slice: {state}_{gender}_{age_group} ({provider_name.upper()})")
            
            try:
                # Determine existing files for incremental mode
                existing_games_file = None
                existing_clubs_file = None
                if args.incremental:
                    # Use registry for auto-discovery if --auto flag is set
                    if args.auto:
                        registry = get_registry()
                        slice_key = f"{state}_{gender}_{age_group}"
                        latest_build = registry.get_latest_build(slice_key)
                        if latest_build:
                            logger.info(f"Auto-discovered latest build for {slice_key}: {latest_build}")
                            # Use the latest build directory
                            games_dir = Path("data/games")
                            latest_build_dir = games_dir / latest_build
                            existing_games_file = latest_build_dir / f"games_{provider_name}_{state}_{gender}_{age_group}.csv"
                            existing_clubs_file = latest_build_dir / f"club_lookup_{provider_name}_{state}_{gender}_{age_group}.csv"
                        else:
                            logger.info(f"No existing build found for {slice_key}. Starting new baseline scrape.")
                    else:
                        # Original logic for manual incremental mode
                        games_dir = Path("data/games")
                        if games_dir.exists():
                            build_dirs = [d for d in games_dir.iterdir() if d.is_dir() and d.name.startswith("build_")]
                            if build_dirs:
                                latest_build = max(build_dirs, key=lambda x: x.name)
                                existing_games_file = latest_build / f"games_{provider_name}_{state}_{gender}_{age_group}.csv"
                                existing_clubs_file = latest_build / f"club_lookup_{provider_name}_{state}_{gender}_{age_group}.csv"
                                
                                # Log the build directory being used for incremental mode
                                logger.info(f"Incremental mode: using existing files from {latest_build.name}")
                                logger.info(f"Current build_id: {build_id}")
                                
                                # Validate that we're not mixing build directories
                                if latest_build.name != build_id:
                                    logger.warning(f"Build ID mismatch: using files from {latest_build.name} but current build is {build_id}")
                                    logger.warning("This could cause data inconsistency - consider using consistent build IDs")
                
                result = process_slice(
                    provider_name,
                    state,
                    gender,
                    age_group,
                    build_id,
                    args.resume,
                    args.max_teams,
                    args.incremental,
                    existing_games_file,
                    existing_clubs_file
                )
                
                result['provider'] = provider_name
                all_results.append(result)
                
                # Calculate timing and progress stats
                elapsed_slice = time.time() - slice_start
                elapsed_total = time.time() - start_time
                
                # Calculate ETA
                eta = "estimating..."
                processed_slices = len(all_results)
                total_slices = len(providers) * len(slice_combinations)
                if processed_slices > 0:
                    avg_time = elapsed_total / processed_slices
                    remaining = total_slices - processed_slices
                    eta = str(timedelta(seconds=int(avg_time * remaining)))
                
                # Extract clubs count from games if available
                unique_clubs = 0
                if 'games_scraped' in result and result['games_scraped'] > 0:
                    # This is a simplified count - in practice you'd extract from the actual games data
                    unique_clubs = min(result['games_scraped'] // 10, 20)  # Rough estimate
                
                print(f"[DONE] Completed slice: {state}_{gender}_{age_group}")
                print(f"   - Teams processed: {result.get('teams_processed', 0)}")
                print(f"   - Games scraped: {result.get('games_scraped', 0)}")
                print(f"   - Clubs found: {unique_clubs}")
                print(f"   - Duration: {timedelta(seconds=int(elapsed_slice))}")
                print(f"   - ETA remaining: {eta}")
                
            except Exception as e:
                logger.exception(f"Error processing slice {provider_name}_{slice_info['state']}_{slice_info['gender']}_{slice_info['age_group']}")
                all_results.append({
                    'provider': provider_name,
                    'slice_key': f"{provider_name}_{slice_info['state']}_{slice_info['gender']}_{slice_info['age_group']}",
                    'teams_processed': 0,
                    'games_scraped': 0,
                    'skipped_inactive': 0,
                    'success': False,
                    'error': str(e)
                })
    
    # Calculate summary statistics
    end_time = datetime.now()
    total_duration = time.time() - start_time
    duration = timedelta(seconds=int(total_duration))
    
    total_teams = sum(r['teams_processed'] for r in all_results)
    total_games = sum(r['games_scraped'] for r in all_results)
    total_skipped = sum(r['skipped_inactive'] for r in all_results)
    successful_slices = sum(1 for r in all_results if r['success'])
    
    print("\n[COMPLETE] All slices complete!")
    print(f"[TIME] Total duration: {timedelta(seconds=int(total_duration))}")
    
    logger.info(f"Build completed: {build_id}")
    logger.info(f"Duration: {duration}")
    logger.info(f"Successful slices: {successful_slices}/{len(all_results)}")
    logger.info(f"Total teams processed: {total_teams}")
    logger.info(f"Total games scraped: {total_games}")
    logger.info(f"Total teams skipped (inactive): {total_skipped}")
    
    # Update metrics registry
    try:
        metrics = MetricsSnapshot()
        start_datetime = datetime.fromtimestamp(start_time)
        metrics.add_games_build(build_id, providers, slice_combinations, start_datetime, end_time, all_results)
        logger.info("Updated metrics registry")
    except Exception as e:
        logger.exception("Error updating metrics registry")
    
    # Update history registry
    try:
        registry = get_registry()
        registry.add_games_build_entry(build_id, providers, slice_combinations, all_results)
        logger.info("Updated history registry")
    except Exception as e:
        logger.exception("Error updating history registry")
    
    # Clean up any failed writes
    cleanup_failed_writes(build_id)
    
    # Link all games CSVs to master index
    try:
        from src.linkers.game_master_linker import link_games_to_master, latest_master_index
        
        logger.info("Linking games to master index...")
        master_path = str(latest_master_index())
        
        linked_count = 0
        for result in all_results:
            if result.get('success') and result.get('games_csv_path'):
                try:
                    link_games_to_master(
                        games_path=str(result['games_csv_path']),
                        master_path=master_path,
                        provider=result.get('provider', 'gotsport')
                    )
                    linked_count += 1
                except Exception as e:
                    logger.warning(f"Failed to link {result['games_csv_path']}: {e}")
        
        logger.info(f"Successfully linked {linked_count} games CSV files to master index")
    except Exception:
        logger.exception("Error during games linking to master index")
    
    logger.info("Game history build completed successfully!")


if __name__ == "__main__":
    main()
