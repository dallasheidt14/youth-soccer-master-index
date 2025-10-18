#!/usr/bin/env python3
"""
Team Analysis Debug Tool

Analyzes detailed game history and calculations for specific teams
to verify ranking accuracy.
"""

import pandas as pd
import numpy as np
from pathlib import Path
import sys
import yaml
from datetime import datetime, timedelta

# Add project root to path
sys.path.append(str(Path(__file__).parent))

from src.analytics.ranking_engine import load_games, run_ranking
from src.analytics.utils_stats import (
    robust_minmax, exp_decay, tapered_weights, clip_zscore_per_team,
    cap_goal_diff, safe_merge, compute_adaptive_k, apply_performance_multiplier,
    compute_bayesian_shrinkage, performance_adj_factor, robust_scale_logistic
)
from src.analytics.sos_iterative import refine_iterative_sos, compute_baseline_sos, build_opponent_edges

def analyze_team_games(team_name, state="AZ", gender="M", age="U10"):
    """Analyze detailed game history for a specific team."""
    
    # Load config
    with open("src/analytics/ranking_config.yaml", "r") as f:
        config = yaml.safe_load(f)
    
    # Load games data
    input_root = Path("data")
    df = load_games(input_root, "latest", state, [gender], [age])
    
    # Filter to specific team - try by name first, then by team_id_master if it looks like an ID
    if len(team_name) == 12 and team_name.replace('-', '').isalnum():
        # Looks like a team ID
        team_games = df[df['team_id_master'] == team_name].copy()
    else:
        # Search by team name
        team_games = df[df['team'].str.contains(team_name, case=False, na=False)].copy()
    
    if team_games.empty:
        print(f"No games found for team containing '{team_name}'")
        return
    
    # Get team ID
    team_id = team_games['team_id_master'].iloc[0]
    team_name_full = team_games['team'].iloc[0]
    
    print(f"\nDETAILED ANALYSIS: {team_name_full}")
    print(f"Team ID: {team_id}")
    print(f"Total Games: {len(team_games)}")
    print("=" * 80)
    
    # Sort by date (most recent first)
    team_games = team_games.sort_values('date', ascending=False)
    
    # Show recent games
    print(f"\nRECENT GAMES (Last 10):")
    print("-" * 80)
    recent_games = team_games.head(10)
    
    for _, game in recent_games.iterrows():
        date_str = game['date'].strftime('%Y-%m-%d') if pd.notna(game['date']) else 'N/A'
        gf = game['gf'] if pd.notna(game['gf']) else 0
        ga = game['ga'] if pd.notna(game['ga']) else 0
        opponent = game['opponent'] if pd.notna(game['opponent']) else 'Unknown'
        gd = gf - ga
        
        print(f"{date_str:12} | {gf:2}-{ga:2} vs {opponent:30} | GD: {gd:+3}")
    
    # Calculate basic stats
    print(f"\nBASIC STATISTICS:")
    print("-" * 40)
    
    gf_total = team_games['gf'].sum()
    ga_total = team_games['ga'].sum()
    gd_total = gf_total - ga_total
    
    wins = len(team_games[team_games['gf'] > team_games['ga']])
    losses = len(team_games[team_games['gf'] < team_games['ga']])
    ties = len(team_games[team_games['gf'] == team_games['ga']])
    
    print(f"Goals For:     {gf_total:6.1f}")
    print(f"Goals Against: {ga_total:6.1f}")
    print(f"Goal Diff:     {gd_total:+6.1f}")
    print(f"Record:        {wins}-{losses}-{ties}")
    print(f"Win %:         {(wins/len(team_games)*100):6.1f}%")
    
    # Calculate raw offensive/defensive metrics
    print(f"\nRAW METRICS CALCULATION:")
    print("-" * 40)
    
    # Apply goal difference cap
    team_games['goal_diff'] = team_games['gf'] - team_games['ga']
    team_games['goal_diff'] = cap_goal_diff(team_games['goal_diff'], config['GOAL_DIFF_CAP'])
    
    # Calculate recency weights
    n_games = len(team_games)
    if n_games > 0:
        weights = tapered_weights(
            n_games, 
            config['RECENT_K'], 
            config['RECENT_SHARE'],
            {
                'tail_start': config['DAMPEN_TAIL_START'],
                'tail_end': config['DAMPEN_TAIL_END'],
                'tail_start_weight': config['DAMPEN_TAIL_START_WEIGHT'],
                'tail_end_weight': config['DAMPEN_TAIL_END_WEIGHT']
            }
        )
        
        # Apply weights (most recent games get higher weights)
        team_games['weight'] = weights
        
        # Raw offensive metric
        off_raw = (team_games['weight'] * team_games['gf']).sum()
        
        # Raw defensive metric (3 - goals against, capped at 0)
        def_raw = (team_games['weight'] * np.maximum(0, 3 - team_games['ga'])).sum()
        
        print(f"Offensive Raw: {off_raw:8.3f}")
        print(f"Defensive Raw: {def_raw:8.3f}")
        print(f"Games Used:    {n_games:8d}")
        
        # Show weight distribution
        print(f"\nRECENCY WEIGHTS (Recent -> Old):")
        print("-" * 40)
        for i, (_, game) in enumerate(team_games.head(10).iterrows()):
            weight = game['weight']
            gf = game['gf']
            ga = game['ga']
            opponent = game['opponent'][:20] if pd.notna(game['opponent']) else 'Unknown'
            print(f"Game {i+1:2d}: Weight {weight:6.4f} | {gf}-{ga} vs {opponent}")
    
    # Check if team is in current rankings
    print(f"\nCURRENT RANKING STATUS:")
    print("-" * 40)
    
    try:
        # Run ranking to get current status
        result_df = run_ranking(state, [gender], [age], config, str(input_root), "data/rankings", "gotsport")
        
        team_rank = result_df[result_df['team_id_master'] == team_id]
        if not team_rank.empty:
            rank_info = team_rank.iloc[0]
            print(f"Current Rank:  {rank_info['rank']:8d}")
            print(f"PowerScore:    {rank_info['powerscore_adj']:8.3f}")
            print(f"SAO Norm:      {rank_info['sao_norm']:8.3f}")
            print(f"SAD Norm:      {rank_info['sad_norm']:8.3f}")
            print(f"SOS Norm:      {rank_info['sos_norm']:8.3f}")
            print(f"Status:        {rank_info['status']:8s}")
            print(f"Last Game:     {rank_info['last_game_date']}")
        else:
            print("Team not found in current rankings")
            
    except Exception as e:
        print(f"Error getting ranking status: {e}")

def compare_teams(team1_name, team2_name, state="AZ", gender="M", age="U10"):
    """Compare two teams side by side."""
    
    print(f"\nTEAM COMPARISON: {team1_name} vs {team2_name}")
    print("=" * 80)
    
    # Load games data
    input_root = Path("data")
    df = load_games(input_root, "latest", state, [gender], [age])
    
    # Get both teams - try by ID first, then by name
    if len(team1_name) == 12 and team1_name.replace('-', '').isalnum():
        team1_games = df[df['team_id_master'] == team1_name]
    else:
        team1_games = df[df['team'].str.contains(team1_name, case=False, na=False)]
        
    if len(team2_name) == 12 and team2_name.replace('-', '').isalnum():
        team2_games = df[df['team_id_master'] == team2_name]
    else:
        team2_games = df[df['team'].str.contains(team2_name, case=False, na=False)]
    
    if team1_games.empty or team2_games.empty:
        print("One or both teams not found")
        return
    
    team1_id = team1_games['team_id_master'].iloc[0]
    team2_id = team2_games['team_id_master'].iloc[0]
    
    print(f"Team 1: {team1_games['team'].iloc[0]} (ID: {team1_id})")
    print(f"Team 2: {team2_games['team'].iloc[0]} (ID: {team2_id})")
    
    # Basic stats comparison
    print(f"\nSTATS COMPARISON:")
    print("-" * 50)
    
    for name, games in [("Team 1", team1_games), ("Team 2", team2_games)]:
        gf_total = games['gf'].sum()
        ga_total = games['ga'].sum()
        gd_total = gf_total - ga_total
        wins = len(games[games['gf'] > games['ga']])
        losses = len(games[games['gf'] < games['ga']])
        ties = len(games[games['gf'] == games['ga']])
        
        print(f"{name:8}: GF={gf_total:5.1f} GA={ga_total:5.1f} GD={gd_total:+5.1f} Record={wins}-{losses}-{ties}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python debug_team_analysis.py <team_name> [team2_name]")
        print("Examples:")
        print("  python debug_team_analysis.py '16B KChacon'")
        print("  python debug_team_analysis.py '2016 Boys Blue'")
        print("  python debug_team_analysis.py '16B KChacon' '2016 Boys Blue'")
        sys.exit(1)
    
    team1 = sys.argv[1]
    
    if len(sys.argv) >= 3:
        team2 = sys.argv[2]
        compare_teams(team1, team2)
    else:
        analyze_team_games(team1)
