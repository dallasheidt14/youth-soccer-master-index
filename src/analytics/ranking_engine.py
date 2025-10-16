#!/usr/bin/env python3
"""
v53E Ranking Engine Implementation

Implements the complete v53E methodology for ranking youth soccer teams
based on game history data with sophisticated statistical adjustments.
"""

import pandas as pd
import numpy as np
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple
import logging
import argparse
import yaml
from datetime import datetime, timedelta
import networkx as nx

from src.analytics.utils_stats import (
    robust_minmax, exp_decay, tapered_weights, clip_zscore_per_team,
    cap_goal_diff, safe_merge, compute_adaptive_k, apply_performance_multiplier,
    compute_bayesian_shrinkage
)
from src.analytics.sos_iterative import refine_iterative_sos, compute_baseline_sos, build_opponent_edges

logger = logging.getLogger(__name__)


def _load_latest_normalized(input_root: Path) -> pd.DataFrame:
    """
    Load the latest normalized parquet file.
    
    Args:
        input_root: Root directory for input data
        
    Returns:
        Loaded DataFrame from latest normalized file
    """
    normalized_dir = input_root / "games" / "normalized"
    if not normalized_dir.exists():
        raise FileNotFoundError(f"Normalized data directory not found: {normalized_dir}")
    
    parquet_files = list(normalized_dir.glob("games_normalized_*.parquet"))
    if not parquet_files:
        raise FileNotFoundError(f"No normalized parquet files found in {normalized_dir}")
    
    # Use latest file
    latest_file = max(parquet_files, key=lambda x: x.name)
    logger.info(f"Loading normalized games from {latest_file}")
    return pd.read_parquet(latest_file)


def load_games(input_root: Path, normalized: str, state: str, genders: List[str], 
               ages: List[str]) -> pd.DataFrame:
    """
    Load games data with auto-detection of schema and format.
    
    Args:
        input_root: Root directory for input data
        normalized: Input preference ("latest", "raw", "legacy")
        state: State to filter
        genders: List of genders to include
        ages: List of age groups to include
        
    Returns:
        Loaded and filtered DataFrame
    """
    if normalized == "latest":
        # Load latest normalized parquet
        df = _load_latest_normalized(input_root)
        
    elif normalized == "raw":
        # Load from raw build directories
        from src.analytics.normalizer import consolidate_builds
        df = consolidate_builds(input_root / "games", [state], genders, ages)
    else:
        # Default to latest normalized
        df = _load_latest_normalized(input_root)
    
    # Detect and normalize schema
    if 'team_name' in df.columns and 'goals_for' in df.columns:
        # Raw schema - map to normalized
        column_mapping = {
            'team_name': 'team',
            'goals_for': 'gf',
            'goals_against': 'ga', 
            'game_date': 'date',
            'opponent_name': 'opponent',
            'opponent_id': 'opponent_id_master',
            'club_name': 'club'
        }
        for old_col, new_col in column_mapping.items():
            if old_col in df.columns:
                df = df.rename(columns={old_col: new_col})
    
    # Ensure required columns exist
    required_cols = ['team_id_master', 'opponent_id_master', 'team', 'opponent', 
                     'club', 'state', 'gender', 'age_group', 'date', 'gf', 'ga']
    
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")
    
    # Filter to requested state/genders/ages
    mask = (
        (df['state'] == state) &
        (df['gender'].isin(genders)) &
        (df['age_group'].isin(ages))
    )
    df = df[mask].copy()
    
    # Convert date and ensure numeric types
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    df['gf'] = pd.to_numeric(df['gf'], errors='coerce')
    df['ga'] = pd.to_numeric(df['ga'], errors='coerce')
    
    # Drop invalid rows
    initial_count = len(df)
    df = df.dropna(subset=['date', 'gf', 'ga'])
    if len(df) < initial_count:
        logger.warning(f"Dropped {initial_count - len(df)} rows with invalid data")
    
    logger.info(f"Loaded {len(df)} games for {state} {genders} {ages}")
    return df


def run_ranking(state: str, genders: List[str], ages: List[str], config: Dict[str, Any],
                input_root: str, output_root: str, provider: str, 
                emit_connectivity: bool = False) -> pd.DataFrame:
    """
    Run the complete v53E ranking pipeline.
    
    This is the core pure function that implements all 12 layers of the v53E methodology.
    
    Args:
        state: State to rank
        genders: List of genders to include
        ages: List of age groups to include
        config: Configuration dictionary
        input_root: Input data root directory
        output_root: Output directory (not used in pure function)
        provider: Data provider name
        emit_connectivity: Whether to compute connectivity metrics
        
    Returns:
        DataFrame with rankings and all metrics
    """
    input_path = Path(input_root)
    
    # Layer 1: Load & filter
    logger.info("Layer 1: Loading and filtering games data")
    df = load_games(input_path, config.get('PRIMARY_INPUT', 'normalized'), 
                   state, genders, ages)
    
    if df.empty:
        logger.warning(f"No games found for {state} {genders} {ages}")
        return pd.DataFrame()
    
    # Filter to time window
    cutoff_date = datetime.now() - timedelta(days=config['WINDOW_DAYS'])
    df = df[df['date'] >= cutoff_date].copy()
    
    logger.info(f"After time filtering: {len(df)} games")
    
    # Layer 2: Per-team game selection
    logger.info("Layer 2: Per-team game selection and preparation")
    
    team_data = []
    
    for team_id in df['team_id_master'].unique():
        if pd.isna(team_id):
            continue
            
        team_games = df[df['team_id_master'] == team_id].copy()
        team_games = team_games.sort_values('date', ascending=False)
        
        # Keep only MAX_GAMES_FOR_RANK most recent games
        team_games = team_games.head(config['MAX_GAMES_FOR_RANK'])
        
        # Compute goal difference and cap it
        team_games['goal_diff'] = team_games['gf'] - team_games['ga']
        team_games['goal_diff'] = cap_goal_diff(team_games['goal_diff'], config['GOAL_DIFF_CAP'])
        
        # Track recency
        team_games['game_index'] = range(len(team_games))
        team_games['days_since'] = (datetime.now() - team_games['date']).dt.days
        
        # Determine if team is active
        last_game_date = team_games['date'].max()
        days_since_last = (datetime.now() - last_game_date).days
        is_active = days_since_last <= config['INACTIVE_HIDE_DAYS']
        
        team_data.append({
            'team_id_master': team_id,
            'team': team_games['team'].iloc[0],
            'club': team_games['club'].iloc[0],
            'state': team_games['state'].iloc[0],
            'gender': team_games['gender'].iloc[0],
            'age_group': team_games['age_group'].iloc[0],
            'games_df': team_games,
            'last_game_date': last_game_date,
            'is_active': is_active
        })
    
    logger.info(f"Processed {len(team_data)} teams")
    
    # Layer 3: Recency weights
    logger.info("Layer 3: Computing recency weights")
    
    for team_info in team_data:
        games_df = team_info['games_df']
        n_games = len(games_df)
        
        if n_games > 0:
            # Generate tapered weights
            tail_cfg = {
                'tail_start': config['DAMPEN_TAIL_START'],
                'tail_end': config['DAMPEN_TAIL_END'],
                'tail_start_weight': config['DAMPEN_TAIL_START_WEIGHT'],
                'tail_end_weight': config['DAMPEN_TAIL_END_WEIGHT']
            }
            
            weights = tapered_weights(
                n_games, config['RECENT_K'], config['RECENT_SHARE'], tail_cfg
            )
            
            games_df['weight'] = weights
        else:
            games_df['weight'] = 0.0
    
    # Layer 4: Raw offensive & defensive metrics
    logger.info("Layer 4: Computing raw offensive and defensive metrics")
    
    for team_info in team_data:
        games_df = team_info['games_df']
        
        if len(games_df) > 0:
            # Raw offensive: weighted sum of goals for
            off_raw = (games_df['weight'] * games_df['gf']).sum()
            
            # Raw defensive: weighted sum of defensive performance (inverse of goals against)
            # Using max(0, 3 - ga) as defensive metric
            def_raw = (games_df['weight'] * np.maximum(0, 3 - games_df['ga'])).sum()
            
            team_info['off_raw'] = off_raw
            team_info['def_raw'] = def_raw
            team_info['gp_used'] = len(games_df)
        else:
            team_info['off_raw'] = 0.0
            team_info['def_raw'] = 0.0
            team_info['gp_used'] = 0
    
    # Layer 5: Opponent strength adjustments
    logger.info("Layer 5: Opponent strength adjustments")
    
    # Compute league means
    all_off_raw = [t['off_raw'] for t in team_data if t['gp_used'] > 0]
    all_def_raw = [t['def_raw'] for t in team_data if t['gp_used'] > 0]
    
    mean_off = np.mean(all_off_raw) if all_off_raw else 0.0
    mean_def = np.mean(all_def_raw) if all_def_raw else 0.0
    
    # Create team strength lookup
    team_strengths = {}
    for team_info in team_data:
        team_strengths[team_info['team_id_master']] = {
            'off_raw': team_info['off_raw'],
            'def_raw': team_info['def_raw']
        }
    
    # Apply opponent strength adjustments
    for team_info in team_data:
        games_df = team_info['games_df']
        
        if len(games_df) > 0:
            sao_raw = 0.0
            sad_raw = 0.0
            
            for _, game in games_df.iterrows():
                opponent_id = game['opponent_id_master']
                
                if pd.notna(opponent_id) and opponent_id in team_strengths:
                    opp_strength = team_strengths[opponent_id]
                    
                    # Scale team contribution by opponent strength
                    off_contribution = game['weight'] * game['gf'] * (opp_strength['def_raw'] / mean_def)
                    def_contribution = game['weight'] * np.maximum(0, 3 - game['ga']) * (opp_strength['off_raw'] / mean_off)
                    
                    sao_raw += off_contribution
                    sad_raw += def_contribution
                else:
                    # Fallback to raw values if opponent strength unknown
                    sao_raw += game['weight'] * game['gf']
                    sad_raw += game['weight'] * np.maximum(0, 3 - game['ga'])
            
            team_info['sao_raw'] = sao_raw
            team_info['sad_raw'] = sad_raw
        else:
            team_info['sao_raw'] = 0.0
            team_info['sad_raw'] = 0.0
    
    # Layer 6: Adaptive K-factor & outlier guard
    logger.info("Layer 6: Adaptive K-factor and outlier protection")
    
    for team_info in team_data:
        games_df = team_info['games_df']
        
        if len(games_df) > 0:
            sao_adjusted = 0.0
            sad_adjusted = 0.0
            
            for _, game in games_df.iterrows():
                opponent_id = game['opponent_id_master']
                
                if pd.notna(opponent_id) and opponent_id in team_strengths:
                    opp_strength = team_strengths[opponent_id]
                    
                    # Compute strength gap
                    team_off = team_info['off_raw']
                    opp_def = opp_strength['def_raw']
                    gap = team_off - opp_def
                    
                    # Compute adaptive K-factor
                    adaptive_k = compute_adaptive_k(
                        gap, team_info['gp_used'], 
                        config['ADAPTIVE_K_ALPHA'], config['ADAPTIVE_K_BETA']
                    )
                    
                    # Apply K-factor to contributions
                    off_contribution = game['weight'] * game['gf'] * adaptive_k
                    def_contribution = game['weight'] * np.maximum(0, 3 - game['ga']) * adaptive_k
                    
                    sao_adjusted += off_contribution
                    sad_adjusted += def_contribution
                else:
                    sao_adjusted += game['weight'] * game['gf']
                    sad_adjusted += game['weight'] * np.maximum(0, 3 - game['ga'])
            
            team_info['sao_adjusted'] = sao_adjusted
            team_info['sad_adjusted'] = sad_adjusted
        else:
            team_info['sao_adjusted'] = 0.0
            team_info['sad_adjusted'] = 0.0
    
    # Apply outlier clipping per team
    team_df = pd.DataFrame(team_data)
    if not team_df.empty:
        team_df = clip_zscore_per_team(team_df, 'team_id_master', 'sao_adjusted', config['OUTLIER_GUARD_ZSCORE'])
        team_df = clip_zscore_per_team(team_df, 'team_id_master', 'sad_adjusted', config['OUTLIER_GUARD_ZSCORE'])
        
        # Update team_data with clipped values
        for i, team_info in enumerate(team_data):
            team_info['sao_adjusted'] = team_df.iloc[i]['sao_adjusted']
            team_info['sad_adjusted'] = team_df.iloc[i]['sad_adjusted']
    
    # Layer 7: Performance layer
    logger.info("Layer 7: Performance layer adjustments")
    
    for team_info in team_data:
        games_df = team_info['games_df']
        
        if len(games_df) > 0:
            sao_perf = 0.0
            sad_perf = 0.0
            
            for idx, (_, game) in enumerate(games_df.iterrows()):
                opponent_id = game['opponent_id_master']
                
                if pd.notna(opponent_id) and opponent_id in team_strengths:
                    opp_strength = team_strengths[opponent_id]
                    
                    # Compute expected vs actual goal difference
                    expected_gd = team_info['off_raw'] - opp_strength['def_raw']
                    actual_gd = game['goal_diff']
                    perf = actual_gd - expected_gd
                    
                    # Apply performance multiplier if significant
                    multiplier = apply_performance_multiplier(
                        perf, config['PERFORMANCE_K'], config['PERFORMANCE_DECAY_RATE'], idx
                    )
                    
                    off_contribution = game['weight'] * game['gf'] * multiplier
                    def_contribution = game['weight'] * np.maximum(0, 3 - game['ga']) * multiplier
                    
                    sao_perf += off_contribution
                    sad_perf += def_contribution
                else:
                    sao_perf += game['weight'] * game['gf']
                    sad_perf += game['weight'] * np.maximum(0, 3 - game['ga'])
            
            team_info['sao_perf'] = sao_perf
            team_info['sad_perf'] = sad_perf
        else:
            team_info['sao_perf'] = 0.0
            team_info['sad_perf'] = 0.0
    
    # Layer 8: Bayesian shrinkage
    logger.info("Layer 8: Bayesian shrinkage")
    
    for team_info in team_data:
        gp = team_info['gp_used']
        
        if gp > 0:
            sao_shrunk = compute_bayesian_shrinkage(
                team_info['sao_perf'], gp, mean_off, config['SHRINK_TAU']
            )
            sad_shrunk = compute_bayesian_shrinkage(
                team_info['sad_perf'], gp, mean_def, config['SHRINK_TAU']
            )
        else:
            sao_shrunk = mean_off
            sad_shrunk = mean_def
        
        team_info['sao_shrunk'] = sao_shrunk
        team_info['sad_shrunk'] = sad_shrunk
    
    # Layer 9: SOS (Strength of Schedule)
    logger.info("Layer 9: Strength of Schedule calculation")
    
    # Build opponent edges for iterative SOS
    edges_df = build_opponent_edges(df)
    
    # Create team strength series for SOS
    team_strength_series = pd.Series({
        t['team_id_master']: t['sao_shrunk'] for t in team_data
    })
    
    # Try iterative SOS refinement
    try:
        sos_refined = refine_iterative_sos(team_strength_series, edges_df)
        logger.info("Used iterative SOS refinement")
    except Exception as e:
        logger.warning(f"Iterative SOS failed: {e}, using baseline")
        sos_refined = compute_baseline_sos(df, team_strength_series)
    
    # Apply SOS stretch
    sos_stretched = sos_refined ** config['SOS_STRETCH_EXPONENT']
    
    # Add SOS to team data
    for team_info in team_data:
        team_id = team_info['team_id_master']
        if team_id in sos_stretched.index:
            team_info['sos_component'] = sos_stretched[team_id]
        else:
            team_info['sos_component'] = 0.0
    
    # Layer 10: Normalization
    logger.info("Layer 10: Data normalization")
    
    # Collect all values for normalization
    sao_values = pd.Series([t['sao_shrunk'] for t in team_data])
    sad_values = pd.Series([t['sad_shrunk'] for t in team_data])
    sos_values = pd.Series([t['sos_component'] for t in team_data])
    
    # Apply robust min-max normalization
    sao_norm = robust_minmax(sao_values)
    sad_norm = robust_minmax(sad_values)
    sos_norm = robust_minmax(sos_values)
    
    # Add normalized values to team data
    for i, team_info in enumerate(team_data):
        team_info['sao_norm'] = sao_norm.iloc[i]
        team_info['sad_norm'] = sad_norm.iloc[i]
        team_info['sos_norm'] = sos_norm.iloc[i]
    
    # Layer 11: PowerScore calculation
    logger.info("Layer 11: PowerScore calculation")
    
    for team_info in team_data:
        # Base PowerScore
        powerscore = (
            config['OFF_WEIGHT'] * team_info['sao_norm'] +
            config['DEF_WEIGHT'] * team_info['sad_norm'] +
            config['SOS_WEIGHT'] * team_info['sos_norm']
        )
        
        # Game count multiplier for provisional teams
        gp_mult = (min(team_info['gp_used'], 20) / 20) ** config['PROVISIONAL_ALPHA']
        powerscore_adj = powerscore * gp_mult
        
        team_info['powerscore'] = powerscore
        team_info['powerscore_adj'] = powerscore_adj
    
    # Layer 12: Status determination
    logger.info("Layer 12: Status determination")
    
    for team_info in team_data:
        if (team_info['gp_used'] >= config['MIN_GAMES_PROVISIONAL'] and 
            team_info['is_active']):
            team_info['status'] = 'Active'
        else:
            team_info['status'] = 'Provisional'
    
    # Connectivity analysis (if requested)
    if emit_connectivity:
        logger.info("Computing connectivity metrics")
        
        # Build NetworkX graph
        G = nx.Graph()
        
        # Add nodes (teams)
        for team_info in team_data:
            G.add_node(team_info['team_id_master'])
        
        # Add edges (games)
        for _, game in df.iterrows():
            team_id = game['team_id_master']
            opponent_id = game['opponent_id_master']
            
            if pd.notna(team_id) and pd.notna(opponent_id):
                G.add_edge(team_id, opponent_id)
        
        # Compute connectivity metrics
        components = list(nx.connected_components(G))
        component_map = {}
        for i, component in enumerate(components):
            for team_id in component:
                component_map[team_id] = i
        
        for team_info in team_data:
            team_id = team_info['team_id_master']
            team_info['component_id'] = component_map.get(team_id, -1)
            team_info['component_size'] = len(components[component_map.get(team_id, -1)]) if team_id in component_map else 1
            team_info['degree'] = G.degree(team_id) if team_id in G else 0
    else:
        # Add default connectivity values
        for team_info in team_data:
            team_info['component_id'] = 0
            team_info['component_size'] = len(team_data)
            team_info['degree'] = 0
    
    # Create final DataFrame
    result_data = []
    
    for team_info in team_data:
        result_data.append({
            'team_id_master': team_info['team_id_master'],
            'team': team_info['team'],
            'club': team_info['club'],
            'state': team_info['state'],
            'gender': team_info['gender'],
            'age_group': team_info['age_group'],
            'powerscore_adj': team_info['powerscore_adj'],
            'powerscore': team_info['powerscore'],
            'sao_norm': team_info['sao_norm'],
            'sad_norm': team_info['sad_norm'],
            'sos_norm': team_info['sos_norm'],
            'gp_used': team_info['gp_used'],
            'is_active': team_info['is_active'],
            'status': team_info['status'],
            'last_game_date': team_info['last_game_date'],
            'component_id': team_info['component_id'],
            'component_size': team_info['component_size'],
            'degree': team_info['degree']
        })
    
    result_df = pd.DataFrame(result_data)
    
    # Sort and rank within each (state, gender, age_group)
    result_df = result_df.sort_values(
        ['state', 'gender', 'age_group', 'powerscore_adj', 'sao_norm', 'sad_norm', 'sos_norm', 'gp_used'],
        ascending=[True, True, True, False, False, False, False, False]
    )
    
    result_df['rank'] = result_df.groupby(['state', 'gender', 'age_group']).cumcount() + 1
    
    # Reorder columns
    column_order = [
        'rank', 'team_id_master', 'team', 'club', 'state', 'gender', 'age_group',
        'powerscore_adj', 'powerscore', 'sao_norm', 'sad_norm', 'sos_norm',
        'gp_used', 'is_active', 'status', 'last_game_date',
        'component_id', 'component_size', 'degree'
    ]
    
    result_df = result_df[column_order]
    
    logger.info(f"Ranking complete: {len(result_df)} teams ranked")
    return result_df


def main():
    """CLI entry point for the ranking engine."""
    parser = argparse.ArgumentParser(description="v53E Ranking Engine")
    parser.add_argument("--input-root", type=str, default="data",
                       help="Root directory for input data")
    parser.add_argument("--normalized", type=str, default="latest",
                       help="Input preference: latest, raw, legacy")
    parser.add_argument("--state", type=str, required=True,
                       help="State to rank")
    parser.add_argument("--genders", type=str, default="M,F",
                       help="Comma-separated genders")
    parser.add_argument("--ages", type=str, default="U10,U11,U12,U13,U14,U15,U16,U17,U18,U19",
                       help="Comma-separated age groups")
    parser.add_argument("--output-root", type=str, default="data/rankings",
                       help="Output directory")
    parser.add_argument("--provider", type=str, default="gotsport",
                       help="Data provider name")
    parser.add_argument("--emit-connectivity", action="store_true",
                       help="Emit connectivity analysis")
    parser.add_argument("--config", type=str, default="src/analytics/ranking_config.yaml",
                       help="Configuration file path")
    
    args = parser.parse_args()
    
    # Parse comma-separated lists
    genders = [g.strip() for g in args.genders.split(',')]
    ages = [a.strip() for a in args.ages.split(',')]
    
    # Load configuration
    with open(args.config, 'r') as f:
        config = yaml.safe_load(f)
    
    # Set up logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    try:
        # Override PRIMARY_INPUT with CLI argument if provided
        if args.normalized != "latest":
            config['PRIMARY_INPUT'] = args.normalized
        
        # Run ranking
        result_df = run_ranking(
            args.state, genders, ages, config,
            args.input_root, args.output_root, args.provider,
            args.emit_connectivity
        )
        
        if result_df.empty:
            logger.warning("No rankings generated")
            return
        
        # Generate output files
        timestamp = datetime.now().strftime("%Y%m%d_%H%M")
        output_dir = Path(args.output_root)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Write rankings CSV
        rankings_file = output_dir / f"rankings_{args.state}_{args.genders}_{args.ages}_{timestamp}.csv"
        result_df.to_csv(rankings_file, index=False)
        logger.info(f"Rankings saved to {rankings_file}")
        
        # Write connectivity CSV if requested
        if args.emit_connectivity:
            connectivity_file = output_dir / f"connectivity_{args.state}_{args.genders}_{args.ages}_{timestamp}.csv"
            connectivity_df = result_df[['team_id_master', 'team', 'state', 'gender', 'age_group',
                                       'component_id', 'component_size', 'degree']].copy()
            connectivity_df.to_csv(connectivity_file, index=False)
            logger.info(f"Connectivity data saved to {connectivity_file}")
        
        # Write summary JSON
        summary = {
            'timestamp': timestamp,
            'state': args.state,
            'genders': genders,
            'ages': ages,
            'provider': args.provider,
            'total_teams': len(result_df),
            'active_teams': len(result_df[result_df['status'] == 'Active']),
            'provisional_teams': len(result_df[result_df['status'] == 'Provisional']),
            'config': config
        }
        
        summary_file = output_dir / f"summary_{timestamp}.json"
        import json
        with open(summary_file, 'w') as f:
            json.dump(summary, f, indent=2, default=str)
        logger.info(f"Summary saved to {summary_file}")
        
        print(f"✅ Ranking complete! {len(result_df)} teams ranked")
        
    except Exception as e:
        logger.error(f"Ranking failed: {e}")
        raise


if __name__ == "__main__":
    main()
