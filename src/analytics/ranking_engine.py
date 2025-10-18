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
    compute_bayesian_shrinkage, performance_adj_factor, robust_scale_logistic
)
from src.analytics.sos_iterative import refine_iterative_sos, compute_baseline_sos, build_opponent_edges

logger = logging.getLogger(__name__)


def _load_latest_normalized(input_root: Path, state: str = None, genders: List[str] = None, ages: List[str] = None) -> pd.DataFrame:
    """
    Load the latest normalized parquet file, preferring per-slice files.
    
    Args:
        input_root: Root directory for input data
        state: State to filter (for per-slice lookup)
        genders: List of genders (for per-slice lookup)
        ages: List of ages (for per-slice lookup)
        
    Returns:
        Loaded DataFrame from latest normalized file
    """
    normalized_dir = input_root / "games" / "normalized"
    if not normalized_dir.exists():
        raise FileNotFoundError(f"Normalized data directory not found: {normalized_dir}")
    
    # First, try to find per-slice normalized file
    if state and genders and ages:
        slice_key = f"{state}_{genders[0]}_{ages[0]}"  # Use first gender/age for slice key
        slice_pattern = f"games_normalized_{slice_key}_*.parquet"
        slice_files = list(normalized_dir.glob(slice_pattern))
        
        if slice_files:
            latest_slice_file = max(slice_files, key=lambda x: x.name)
            logger.info(f"Using per-slice normalized data: {latest_slice_file}")
            return pd.read_parquet(latest_slice_file)
        else:
            logger.info(f"No per-slice file found for {slice_key}, falling back to global normalized file")
    
    # Fallback to global normalized file
    parquet_files = list(normalized_dir.glob("games_normalized_*.parquet"))
    if not parquet_files:
        raise FileNotFoundError(f"No normalized parquet files found in {normalized_dir}")
    
    # Use latest global file
    latest_file = max(parquet_files, key=lambda x: x.name)
    logger.info(f"Using global normalized data: {latest_file}")
    return pd.read_parquet(latest_file)


def load_games(input_root: Path, normalized: str, state: str, genders: List[str], 
               ages: List[str], national_mode: bool = False) -> pd.DataFrame:
    """
    Load games data with auto-detection of schema and format.
    
    Args:
        input_root: Root directory for input data
        normalized: Input preference ("latest", "raw", "legacy")
        state: State to filter (ignored in national mode)
        genders: List of genders to include
        ages: List of age groups to include
        national_mode: If True, load all states for same age/gender
        
    Returns:
        Loaded and filtered DataFrame
    """
    if national_mode:
        # Load all states for this age/gender combination
        logger.info(f"Loading NATIONAL dataset for {genders} {ages}")
        
        # Try to load per-division file first
        division_key = f"ALL_{genders[0]}_{ages[0]}"
        normalized_dir = input_root / "games" / "normalized"
        division_files = list(normalized_dir.glob(f"games_normalized_{division_key}_*.parquet"))
        
        if division_files:
            latest_file = max(division_files, key=lambda x: x.name)
            logger.info(f"Using national division file: {latest_file}")
            df = pd.read_parquet(latest_file)
        else:
            # Fall back to loading global normalized file
            logger.info("Loading global normalized file for national mode")
            df = _load_latest_normalized(input_root, None, genders, ages)
    else:
        # Original per-state loading
        if normalized == "latest":
            # Load latest normalized parquet (prefer per-slice)
            df = _load_latest_normalized(input_root, state, genders, ages)
            
        elif normalized == "raw":
            # Load from raw build directories
            from src.analytics.normalizer import consolidate_builds
            df = consolidate_builds(input_root / "games", [state], genders, ages)
        else:
            # Default to latest normalized (prefer per-slice)
            df = _load_latest_normalized(input_root, state, genders, ages)
    
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
    
    # Filter to requested state/genders/ages (unless in national mode)
    if not national_mode:
        mask = (
            (df['state'] == state) &
            (df['gender'].isin(genders)) &
            (df['age_group'].isin(ages))
        )
        df = df[mask].copy()
    else:
        # In national mode, only filter by gender/age (keep all states)
        mask = (
            (df['gender'].isin(genders)) &
            (df['age_group'].isin(ages))
        )
        df = df[mask].copy()
        logger.info(f"National mode: {df['state'].nunique()} states, "
                   f"{df['team_id_master'].nunique()} teams")
    
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
                emit_connectivity: bool = False, national_mode: bool = False) -> pd.DataFrame:
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
    national_mode = config.get('NATIONAL_MODE', False)
    df = load_games(input_path, config.get('PRIMARY_INPUT', 'normalized'), 
                   state, genders, ages, national_mode=national_mode)
    
    if df.empty:
        logger.warning(f"No games found for {state} {genders} {ages}")
        return pd.DataFrame()
    
    # Validate data quality - check for linking issues
    missing_team = (df['team_id_master'].isna() | (df['team_id_master'] == '')).mean()
    missing_opp = (df['opponent_id_master'].isna() | (df['opponent_id_master'] == '')).mean()
    id_overlap = df['opponent_id_master'].isin(df['team_id_master'].unique()).mean()
    
    logger.info(f"Data quality validation:")
    logger.info(f"  Missing team IDs: {missing_team:.1%}")
    logger.info(f"  Missing opponent IDs: {missing_opp:.1%}")
    logger.info(f"  Opponent overlap with teams: {id_overlap:.1%}")
    
    if missing_opp > 0.05:
        raise ValueError(f"Opponent IDs missing for {missing_opp:.1%} of rows — linking is broken.")
    
    # For state-only data, low overlap is expected (opponents from other states)
    # For national data, we expect higher overlap, but still allow some flexibility
    if national_mode:
        if id_overlap < 0.30:
            logger.warning(f"Low opponent overlap ({id_overlap:.1%}) in national mode - this may indicate linking issues")
        # Don't fail in national mode since opponents from different states may have different ID formats
    else:
        # For state data, just warn if overlap is very low
        if id_overlap < 0.20:
            logger.warning(f"Low opponent overlap ({id_overlap:.1%}) - this may indicate linking issues")
    
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
        
        # Legacy ±7 cap to prevent blowouts from distorting metrics
        team_games['goal_diff'] = team_games['goal_diff'].clip(-7, 7)
        
        # Per-game outlier guard: clip GF/GA to ±2.5σ before weighting
        if len(team_games) > 1:  # Need at least 2 games for std
            gf_mean, gf_std = team_games['gf'].mean(), team_games['gf'].std()
            ga_mean, ga_std = team_games['ga'].mean(), team_games['ga'].std()
            z_threshold = config.get('OUTLIER_GUARD_ZSCORE', 2.5)
            
            if gf_std > 0:
                gf_clipped = np.clip(team_games['gf'], gf_mean - z_threshold * gf_std, gf_mean + z_threshold * gf_std)
                team_games['gf'] = gf_clipped
            
            if ga_std > 0:
                ga_clipped = np.clip(team_games['ga'], ga_mean - z_threshold * ga_std, ga_mean + z_threshold * ga_std)
                team_games['ga'] = ga_clipped
        
        # Track recency
        team_games['game_index'] = range(len(team_games))
        team_games['days_since'] = (datetime.now() - team_games['date']).dt.days
        
        # Determine if team is active
        # Compute last game date with fallback
        if len(team_games) > 0 and 'date' in team_games.columns:
            last_game_date = team_games['date'].max()
        else:
            last_game_date = datetime.now() - timedelta(days=365)  # Default to 1 year ago
        
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
            'opponents': team_games['opponent_id_master'].unique().tolist(),
            'last_game_date': last_game_date,
            'is_active': is_active
        })
    
    logger.info(f"Processed {len(team_data)} teams")
    
    # Data quality validation
    if len(team_data) == 0:
        logger.error("No teams found after processing - check data quality")
        return pd.DataFrame()
    
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
            # Using max(0, 3 - ga) as defensive metric with ridge regularization
            ridge_ga = config.get('RIDGE_GA', 0.25)
            def_raw = (games_df['weight'] * np.maximum(0, 3 - games_df['ga'])).sum() + ridge_ga
            
            team_info['off_raw'] = off_raw
            team_info['def_raw'] = def_raw
            team_info['gp_used'] = len(games_df)
        else:
            team_info['off_raw'] = 0.0
            team_info['def_raw'] = 0.0
            team_info['gp_used'] = 0
    
    # Validate teams with games after Layer 4
    teams_with_games = [t for t in team_data if t['gp_used'] > 0]
    if len(teams_with_games) == 0:
        logger.error("No teams have any games - check data filtering")
        return pd.DataFrame()
    
    logger.info(f"Teams with games: {len(teams_with_games)}/{len(team_data)}")
    
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
    
    # Create normalized strength lookups with clipping (0.15-0.95)
    off_norm = pd.Series({t['team_id_master']: t['off_raw'] for t in team_data})
    def_norm = pd.Series({t['team_id_master']: t['def_raw'] for t in team_data})

    if len(off_norm) > 0:
        off_norm = (off_norm - off_norm.min()) / (off_norm.max() - off_norm.min() + 1e-9)
        off_norm = off_norm.clip(0.15, 0.95)
        
    if len(def_norm) > 0:
        def_norm = (def_norm - def_norm.min()) / (def_norm.max() - def_norm.min() + 1e-9)
        def_norm = def_norm.clip(0.15, 0.95)
    
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
                    
                    # Scale team contribution by opponent strength with clipping
                    opp_def_scale = np.clip(opp_strength['def_raw'] / mean_def, 0.67, 1.50)
                    opp_off_scale = np.clip(opp_strength['off_raw'] / mean_off, 0.67, 1.50)

                    off_contribution = game['weight'] * game['gf'] * opp_def_scale
                    def_contribution = game['weight'] * np.maximum(0, 3 - game['ga']) * opp_off_scale
                    
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
    
    # Before Layer 6: simpler normalization for K context
    off_vals = pd.Series({t['team_id_master']: t['off_raw'] for t in team_data})
    def_vals = pd.Series({t['team_id_master']: t['def_raw'] for t in team_data})
    
    # Safe 0..1 scaling for K context
    eps = 1e-9
    off_norm = (off_vals - off_vals.min()) / max(eps, (off_vals.max() - off_vals.min()))
    def_norm = (def_vals - def_vals.min()) / max(eps, (def_vals.max() - def_vals.min()))
    
    for team_info in team_data:
        games_df = team_info['games_df']
        
        if len(games_df) > 0:
            sao_adjusted = 0.0
            sad_adjusted = 0.0
            
            for _, game in games_df.iterrows():
                opponent_id = game['opponent_id_master']
                
                if pd.notna(opponent_id) and opponent_id in team_strengths:
                    opp_strength = team_strengths[opponent_id]
                    
                    # Get normalized strengths
                    team_strength = float(off_norm.get(team_info['team_id_master'], 0.5))
                    opp_strength_norm = float(def_norm.get(opponent_id, 0.5))
                    
                    # Compute adaptive K-factor
                    adaptive_k = compute_adaptive_k(
                        team_strength, opp_strength_norm, team_info['gp_used'],
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
                    
                    # Get normalized strengths for expected GD
                    team_off_norm = float(off_norm.get(team_info['team_id_master'], 0.5))
                    opp_def_norm = float(def_norm.get(opponent_id, 0.5))
                    
                    # Expected vs actual GD, legacy-style
                    expected_gd = team_off_norm - opp_def_norm
                    actual_gd = float(game['goal_diff'])
                    perf_delta = actual_gd - expected_gd
                    
                    # Get performance adjustment factor
                    adj_factor = performance_adj_factor(
                        perf_delta, 
                        config['PERFORMANCE_K'], 
                        config['PERFORMANCE_DECAY_RATE'], 
                        idx,
                        threshold=config.get('PERFORMANCE_THRESHOLD', 1.0)
                    )
                    
                    # Legacy v5.3E performance gate (simplified, linear)
                    perf_mult = np.clip(perf_delta, -2.0, 2.0)
                    adj_factor *= (1 + 0.05 * perf_mult)
                    
                    # Apply separately to GF and GA
                    gf_scaled = game['gf'] * adj_factor
                    ga_scaled = game['ga'] * (2.0 - adj_factor)
                    
                    # Compute contributions with adjusted values
                    off_contribution = game['weight'] * gf_scaled
                    def_contribution = game['weight'] * np.maximum(0, 3 - ga_scaled)
                    
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
    
    # Layer 9: SOS (Strength of Schedule) - Fixed Implementation
    logger.info("Layer 9: Strength of Schedule calculation")
    
    # 1) Load alias map + canonicalize helper
    import os, json
    
    def _canonize_id(x):
        if x is None:
            return None
        s = str(x).strip()
        if s.endswith(".0"):
            s = s[:-2]
        return s
    
    alias_map = {}
    slice_key = f"{state}_{genders[0]}_{ages[0]}" if not national_mode else f"ALL_{genders[0]}_{ages[0]}"
    alias_path = os.path.join("data", "derived", f"id_alias_map_{slice_key}.json")
    if os.path.exists(alias_path):
        with open(alias_path, "r", encoding="utf-8") as f:
            alias_map = json.load(f)
        logger.info(f"Loaded alias map with {len(alias_map)} mappings from {alias_path}")
    
    def _resolve_opp_id(opp_id):
        cid = _canonize_id(opp_id)
        if cid in alias_map:
            return alias_map[cid]
        return cid
    
    # 2) Build a strength map keyed by canonical team ids
    def _baseline_strength(ti):
        # Use post-shrink offensive/defensive blend (no SOS circularity on first pass)
        return 0.5 * ti["sao_shrunk"] + 0.5 * ti["sad_shrunk"]
    
    team_strength_map = {}
    for ti in team_data:
        tid = _canonize_id(ti["team_id_master"])
        strength = _baseline_strength(ti)
        team_strength_map[tid] = strength
        
        # Also add any aliases for this team ID to the map
        for alias_id, canonical_id in alias_map.items():
            if canonical_id == tid:
                team_strength_map[alias_id] = strength
    
    # 3) Compute SOS from unique, resolved opponents, no self-loops, capped repeat weight
    BASELINE = float(config.get("UNRANKED_SOS_BASE", 0.35))
    MAX_REPEAT_WEIGHT = int(config.get("SOS_REPEAT_CAP", 2))  # cap repeat games per opponent
    
    for ti in team_data:
        tid = _canonize_id(ti["team_id_master"])
        
        # Resolve and sanitize the opponent id list
        raw_opps = ti.get("opponents", []) or []
        resolved = []
        for oid in raw_opps:
            rid = _resolve_opp_id(oid)
            if not rid:
                continue
            if rid == tid:
                continue  # remove self-loop after aliasing
            resolved.append(rid)
        
        # Limit repeat weight per opponent (prevents over-crediting the same strong opponent 5+ times)
        counts = {}
        capped = []
        for rid in resolved:
            c = counts.get(rid, 0)
            if c < MAX_REPEAT_WEIGHT:
                capped.append(rid)
                counts[rid] = c + 1
        
        # Use unique opponents for the baseline mean (legacy-style fairness)
        opp_ids = sorted(set(capped))
        
        strengths = []
        for rid in opp_ids:
            s = team_strength_map.get(rid)
            if s is None:
                s = BASELINE  # never zero-credit an unranked/unknown opponent
            strengths.append(float(s))
        
        ti["sos_component"] = float(np.mean(strengths)) if strengths else BASELINE
        
        # Quick diagnostics for Copper
        if "State 48 FC Avondale 16 Copper" in ti["team"]:
            logger.info(f"[COPPER SOS] raw={ti['sos_component']:.3f} "
                       f"opp_count={len(raw_opps)} unique_opps={len(opp_ids)} "
                       f"missing={sum(1 for o in raw_opps if team_strength_map.get(_resolve_opp_id(o)) is None)}")
            logger.info(f"[COPPER DEBUG] raw_opps={raw_opps[:5]}...")  # Show first 5 opponents
            logger.info(f"[COPPER DEBUG] resolved_opps={resolved[:5]}...")  # Show first 5 resolved
            logger.info(f"[COPPER DEBUG] opp_ids={opp_ids[:5]}...")  # Show first 5 final opponent IDs
    
    # 4) Normalize SOS with logistic (no floor), then optional stretch
    def robust_scale(series: pd.Series) -> pd.Series:
        if series.empty:
            return series
        p1, p99 = np.nanpercentile(series, [1, 99])
        s = series.clip(lower=p1, upper=p99)
        z = (s - s.mean()) / (s.std() or 1.0)
        return 1.0 / (1.0 + np.exp(-z))
    
    sos_raw_series = pd.Series([t["sos_component"] for t in team_data])
    sos_norm_series = robust_scale(sos_raw_series)
    stretch = float(config.get("SOS_STRETCH_EXPONENT", 1.5))
    if stretch and stretch != 1.0:
        sos_norm_series = sos_norm_series ** stretch
    
    for t, v in zip(team_data, sos_norm_series.values):
        t["sos_norm"] = float(v)
    
    # Global distribution logging
    vals = np.array([t["sos_norm"] for t in team_data])
    logger.info(f"SOS_norm range={vals.min():.3f}-{vals.max():.3f} mean={vals.mean():.3f} std={vals.std():.3f}")
    
    # SOS computation completed in Layer 9 above (lines 566-674)
    logger.info("Layer 9 SOS computation completed")
    
    # Layer 10: Data normalization (v5.3E style)
    logger.info("Layer 10: Data normalization (v5.3E style)")
    
    from src.analytics.utils_stats import robust_scale

    # Extract raw values from team_data
    sao_raw = pd.Series([t['sao_shrunk'] for t in team_data], dtype=float)
    sad_raw = pd.Series([t['sad_shrunk'] for t in team_data], dtype=float)
    sos_raw = pd.Series([t['sos_component'] for t in team_data], dtype=float)

    # Log raw values
    logger.info(f"Raw values - SAO: [{sao_raw.min():.3f}, {sao_raw.max():.3f}] unique%={sao_raw.round(6).nunique()/len(sao_raw):.1%}")
    logger.info(f"Raw values - SAD: [{sad_raw.min():.3f}, {sad_raw.max():.3f}] unique%={sad_raw.round(6).nunique()/len(sad_raw):.1%}")
    logger.info(f"Raw values - SOS: [{sos_raw.min():.3f}, {sos_raw.max():.3f}] unique%={sos_raw.round(6).nunique()/len(sos_raw):.1%}")

    # SAO & SAD: v5.3E-style robust_scale (winsorize 1-99%, z-score, logistic)
    sao_norm = robust_scale(sao_raw)
    sad_norm = robust_scale(sad_raw)

    # SOS: use same robust logistic scaling as SAO/SAD (no hard floor)
    sos_norm = robust_scale(sos_raw)

    # Log normalized values
    logger.info(f"Normalized - SAO: [{sao_norm.min():.3f}, {sao_norm.max():.3f}] unique%={sao_norm.round(6).nunique()/len(sao_norm):.1%}")
    logger.info(f"Normalized - SAD: [{sad_norm.min():.3f}, {sad_norm.max():.3f}] unique%={sad_norm.round(6).nunique()/len(sad_norm):.1%}")
    logger.info(f"Normalized - SOS: [{sos_norm.min():.3f}, {sos_norm.max():.3f}] unique%={sos_norm.round(6).nunique()/len(sos_norm):.1%}")

    # Write back to team_data
    for i, team_info in enumerate(team_data):
        team_info['sao_norm'] = float(sao_norm.iloc[i])
        team_info['sad_norm'] = float(sad_norm.iloc[i])
        team_info['sos_norm'] = float(sos_norm.iloc[i])

    # Recalculate PowerScore with proper weights
    logger.info("Recalculating PowerScore with v5.3E normalization")
    for team_info in team_data:
        powerscore = (
            config['OFF_WEIGHT'] * team_info['sao_norm']
            + config['DEF_WEIGHT'] * team_info['sad_norm']
            + config['SOS_WEIGHT'] * team_info['sos_norm']
        )

        # --- v53E realism controls ---
        gp = team_info["gp_used"]
        adj = 1.0

        # 1. Stepwise provisional floor (softened for better differentiation)
        if gp < 8:
            adj *= 0.85
        elif gp < 15:
            adj *= 0.95

        # 2. Connectivity dampener disabled - national mode provides full connectivity
        # if emit_connectivity and team_info.get("component_size", 50) < 25:
        #     adj *= 0.95

        # 3. Recency boost removed for strict legacy parity
        # Legacy v5.3E had no recency multiplier
        # days_since = (datetime.now() - team_info["last_game_date"]).days
        # if days_since < 30:
        #     adj *= 1.01

        # 4. Apply game-count multiplier (soft shrink)
        gp_mult = (min(gp, 20) / 20.0) ** config["PROVISIONAL_ALPHA"]

        powerscore_adj = powerscore * gp_mult * adj

        team_info['powerscore'] = powerscore
        team_info['powerscore_adj'] = powerscore_adj
        team_info['gp_mult'] = gp_mult

    # Log PowerScore distribution
    powerscores = [t['powerscore'] for t in team_data]
    logger.info(f"PowerScore range: [{min(powerscores):.3f}, {max(powerscores):.3f}]")
    logger.info(f"Top 20 PowerScore range: [{sorted(powerscores, reverse=True)[:20][-1]:.3f}, {max(powerscores):.3f}]")
    
    # Layer 11: Status determination
    logger.info("Layer 11: Status determination")
    
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
    
    # Diagnostic: Log connectivity statistics
    if emit_connectivity or config.get('DEBUG_CONNECTIVITY', False):
        logger.info("=" * 60)
        logger.info("CONNECTIVITY DIAGNOSTICS:")
        logger.info(f"  Total teams: {len(team_data)}")
        logger.info(f"  Component sizes: min={min(t['component_size'] for t in team_data)}, "
                   f"max={max(t['component_size'] for t in team_data)}")
        
        # Show teams with low connectivity
        low_connectivity = [t for t in team_data if t['component_size'] < 50]
        if low_connectivity:
            logger.warning(f"  {len(low_connectivity)} teams with component_size < 50")
            for t in low_connectivity[:5]:
                logger.warning(f"    {t['team']}: component_size={t['component_size']}")
        logger.info("=" * 60)
    
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
            # Raw metrics (Layer 4)
            'off_raw': team_info['off_raw'],
            'def_raw': team_info['def_raw'],
            # Strength-adjusted raw (Layer 5)
            'sao_raw': team_info['sao_raw'],
            'sad_raw': team_info['sad_raw'],
            # Adaptive K-factor adjusted (Layer 6)
            'sao_adjusted': team_info['sao_adjusted'],
            'sad_adjusted': team_info['sad_adjusted'],
            # Performance layer (Layer 7)
            'sao_perf': team_info['sao_perf'],
            'sad_perf': team_info['sad_perf'],
            # Bayesian shrunk (Layer 8)
            'sao_shrunk': team_info['sao_shrunk'],
            'sad_shrunk': team_info['sad_shrunk'],
            # SOS component (Layer 9)
            'sos_component': team_info['sos_component'],
            # Normalized metrics (Layer 10)
            'sao_norm': team_info['sao_norm'],
            'sad_norm': team_info['sad_norm'],
            'sos_norm': team_info['sos_norm'],
            # PowerScore calculations (Layer 11)
            'powerscore': team_info['powerscore'],
            'powerscore_adj': team_info['powerscore_adj'],
            'gp_mult': team_info['gp_mult'],
            # Game count and status
            'gp_used': team_info['gp_used'],
            'is_active': team_info['is_active'],
            'status': team_info['status'],
            'last_game_date': team_info['last_game_date'],
            # Connectivity metrics
            'component_id': team_info['component_id'],
            'component_size': team_info['component_size'],
            'degree': team_info['degree']
        })
    
    result_df = pd.DataFrame(result_data)
    
    # Add national rank before filtering
    result_df = result_df.sort_values(
        ['powerscore_adj', 'sao_norm', 'sad_norm', 'sos_norm', 'gp_used'],
        ascending=[False, False, False, False, False]
    )
    result_df['rank_national'] = range(1, len(result_df) + 1)
    
    # STATE FILTERING: Apply AFTER all calculations
    if national_mode and state != "ALL":
        original_count = len(result_df)
        result_df = result_df[result_df['state'] == state].copy()
        logger.info(f"Filtered to state {state}: {len(result_df)}/{original_count} teams")
    
    # Compute national rank (recalculate after any filtering)
    result_df = result_df.sort_values(
        ['powerscore_adj', 'sao_norm', 'sad_norm', 'sos_norm', 'gp_used'],
        ascending=[False, False, False, False, False]
    )
    result_df['rank_national'] = range(1, len(result_df) + 1)
    
    # Use rank_national as primary rank column for backward compatibility
    result_df['rank'] = result_df['rank_national']
    
    # Reorder columns to show national rank
    cols = result_df.columns.tolist()
    # Move rank columns after team name
    rank_cols = ['rank', 'rank_national']
    other_cols = [c for c in cols if c not in rank_cols]
    team_idx = other_cols.index('team') if 'team' in other_cols else 0
    result_df = result_df[other_cols[:team_idx+1] + rank_cols + other_cols[team_idx+1:]]
    
    # Reorder columns
    column_order = [
        'rank', 'rank_national', 'team_id_master', 'team', 'club', 'state', 'gender', 'age_group',
        # Raw metrics (Layer 4)
        'off_raw', 'def_raw',
        # Strength-adjusted raw (Layer 5)
        'sao_raw', 'sad_raw',
        # Adaptive K-factor adjusted (Layer 6)
        'sao_adjusted', 'sad_adjusted',
        # Performance layer (Layer 7)
        'sao_perf', 'sad_perf',
        # Bayesian shrunk (Layer 8)
        'sao_shrunk', 'sad_shrunk',
        # SOS component (Layer 9)
        'sos_component',
        # Normalized metrics (Layer 10)
        'sao_norm', 'sad_norm', 'sos_norm',
        # PowerScore calculations (Layer 11)
        'powerscore', 'powerscore_adj', 'gp_mult',
        # Game count and status
        'gp_used', 'is_active', 'status', 'last_game_date',
        # Connectivity metrics
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
    parser.add_argument("--national-mode", action="store_true",
                       help="Enable national SOS computation mode")
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
        
        # Override NATIONAL_MODE with CLI argument if provided
        if args.national_mode:
            config['NATIONAL_MODE'] = True
        
        # Run ranking
        result_df = run_ranking(
            args.state, genders, ages, config,
            args.input_root, args.output_root, args.provider,
            args.emit_connectivity, args.national_mode
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
        
        # Export per-state views if in national mode
        if config.get('NATIONAL_MODE', False) and args.state == "ALL":
            state_views_dir = output_dir / "state_views"
            state_views_dir.mkdir(parents=True, exist_ok=True)
            
            # Get full national result (before any filtering)
            full_result = result_df.copy()
            
            for st in sorted(full_result['state'].dropna().unique()):
                state_df = full_result[full_result['state'] == st].copy()
                # Recalculate state rank
                state_df = state_df.sort_values(
                    ['powerscore_adj', 'sao_norm', 'sad_norm', 'sos_norm'],
                    ascending=[False, False, False, False]
                )
                state_df['rank_state'] = range(1, len(state_df) + 1)
                state_df['rank'] = state_df['rank_state']
                
                state_path = state_views_dir / f"rankings_{st}_{args.genders}_{args.ages}_{timestamp}.csv"
                state_df.to_csv(state_path, index=False)
            
            logger.info(f"Exported {len(full_result['state'].unique())} state views to {state_views_dir}")
            
            # Optionally generate summary aggregation
            if config.get('AUTO_SUMMARIZE', True):
                try:
                    from scripts.summary_state_rankings import build_master_summary
                    summary_output = output_dir / "summary_state_rankings.csv"
                    summary_df = build_master_summary(state_views_dir, summary_output)
                    logger.info(f"Generated master summary with {len(summary_df)} state records")
                except Exception as e:
                    logger.warning(f"Failed to generate summary aggregation: {e}")
        
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
        
        try:
            print(f"✅ Ranking complete! {len(result_df)} teams ranked")
        except UnicodeEncodeError:
            print(f"Ranking complete! {len(result_df)} teams ranked")
        
    except Exception as e:
        logger.error(f"Ranking failed: {e}")
        raise


if __name__ == "__main__":
    # Suppress the RuntimeWarning about module import
    import warnings
    warnings.filterwarnings("ignore", category=RuntimeWarning, module="runpy")
    main()
