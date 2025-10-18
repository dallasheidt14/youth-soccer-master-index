#!/usr/bin/env python3
"""
SOS Analysis Tool - Investigate Strength of Schedule calculations
"""

import pandas as pd
import numpy as np
from pathlib import Path
import sys
import yaml

# Add project root to path
sys.path.append(str(Path(__file__).parent))

from src.analytics.ranking_engine import load_games, run_ranking
from src.analytics.sos_iterative import refine_iterative_sos, compute_baseline_sos, build_opponent_edges

def analyze_sos_for_team(team_id, team_name, state="AZ", gender="M", age="U10"):
    """Analyze SOS calculation for a specific team."""
    
    print(f"\nSOS ANALYSIS: {team_name}")
    print(f"Team ID: {team_id}")
    print("=" * 80)
    
    # Load config
    with open("src/analytics/ranking_config.yaml", "r") as f:
        config = yaml.safe_load(f)
    
    # Load games data
    input_root = Path("data")
    df = load_games(input_root, "latest", state, [gender], [age])
    
    # Get team's games
    team_games = df[df['team_id_master'] == team_id].copy()
    
    if team_games.empty:
        print(f"No games found for team {team_id}")
        return
    
    print(f"Total Games: {len(team_games)}")
    
    # Get unique opponents
    opponents = team_games['opponent_id_master'].dropna().unique()
    print(f"Unique Opponents: {len(opponents)}")
    
    # Show opponent breakdown
    print(f"\nOPPONENT BREAKDOWN:")
    print("-" * 60)
    
    opponent_stats = []
    for opp_id in opponents:
        opp_games = team_games[team_games['opponent_id_master'] == opp_id]
        opp_name = opp_games['opponent'].iloc[0] if not opp_games.empty else 'Unknown'
        games_vs_opp = len(opp_games)
        
        # Get opponent's overall record
        opp_all_games = df[df['team_id_master'] == opp_id]
        if not opp_all_games.empty:
            opp_gf = opp_all_games['gf'].sum()
            opp_ga = opp_all_games['ga'].sum()
            opp_record = f"{opp_gf:.0f}-{opp_ga:.0f}"
            opp_gd = opp_gf - opp_ga
        else:
            opp_record = "N/A"
            opp_gd = 0
            
        opponent_stats.append({
            'opponent': opp_name,
            'games_vs': games_vs_opp,
            'opp_record': opp_record,
            'opp_gd': opp_gd
        })
    
    # Sort by opponent strength (goal differential)
    opponent_stats.sort(key=lambda x: x['opp_gd'], reverse=True)
    
    print(f"{'Opponent':<40} {'Games':<6} {'Opp Record':<10} {'Opp GD':<8}")
    print("-" * 70)
    
    strong_opponents = 0
    for stat in opponent_stats:
        print(f"{stat['opponent']:<40} {stat['games_vs']:<6} {stat['opp_record']:<10} {stat['opp_gd']:+8.0f}")
        if stat['opp_gd'] > 0:
            strong_opponents += 1
    
    print(f"\nStrong Opponents (Positive GD): {strong_opponents}/{len(opponent_stats)}")
    
    # Now let's see what the SOS calculation produces
    print(f"\nSOS CALCULATION ANALYSIS:")
    print("-" * 40)
    
    # Build opponent edges
    edges_df = build_opponent_edges(df)
    print(f"Total opponent edges: {len(edges_df)}")
    
    # Get team strength series (we'll use a dummy one for this analysis)
    team_strength_series = pd.Series({team_id: 1.0})  # Dummy strength
    
    # Compute baseline SOS
    baseline_sos = compute_baseline_sos(df, team_strength_series)
    print(f"Baseline SOS for {team_name}: {baseline_sos.get(team_id, 'N/A')}")
    
    # Try iterative SOS
    try:
        sos_iterative = refine_iterative_sos(team_strength_series, edges_df)
        print(f"Iterative SOS for {team_name}: {sos_iterative.get(team_id, 'N/A')}")
        
        # Combined SOS
        sos_combined = sos_iterative.combine_first(baseline_sos)
        print(f"Combined SOS for {team_name}: {sos_combined.get(team_id, 'N/A')}")
        
    except Exception as e:
        print(f"Iterative SOS failed: {e}")
        print(f"Using baseline SOS: {baseline_sos.get(team_id, 'N/A')}")
    
    # Check connectivity
    print(f"\nCONNECTIVITY ANALYSIS:")
    print("-" * 30)
    
    # Find teams connected to this team through opponent chains
    connected_teams = set()
    connected_teams.add(team_id)
    
    # Add direct opponents
    for opp_id in opponents:
        connected_teams.add(opp_id)
    
    # Add opponents of opponents (2-hop)
    for opp_id in opponents:
        opp_opponents = df[df['team_id_master'] == opp_id]['opponent_id_master'].dropna().unique()
        for opp_opp_id in opp_opponents:
            connected_teams.add(opp_opp_id)
    
    print(f"Teams in 2-hop network: {len(connected_teams)}")
    print(f"Total teams in dataset: {df['team_id_master'].nunique()}")
    print(f"Connectivity ratio: {len(connected_teams)/df['team_id_master'].nunique():.3f}")
    
    # Show some connected teams
    print(f"\nSample of connected teams:")
    connected_df = df[df['team_id_master'].isin(list(connected_teams)[:10])]
    if not connected_df.empty:
        for team_id, team in connected_df.groupby('team_id_master').first().iterrows():
            print(f"  {team['team']} ({team_id})")
    else:
        print("  No connected teams found in current dataset")

if __name__ == "__main__":
    # Analyze 16B KChacon
    analyze_sos_for_team("1dfe64f51e32", "16B KChacon")
