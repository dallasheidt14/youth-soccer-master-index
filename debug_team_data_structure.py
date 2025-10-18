#!/usr/bin/env python3
"""
Debug script to investigate the team_data structure and why Copper's opponents aren't being found.
"""

import pandas as pd
import json
from pathlib import Path

def main():
    # Load the normalized games data
    games_file = "data/games/normalized/games_normalized_ALL_M_U10_20251017_1923.parquet"
    df = pd.read_parquet(games_file)
    
    print("=== TEAM DATA STRUCTURE INVESTIGATION ===")
    
    # Find Copper team
    copper_team_name = "State 48 FC Avondale 16 Copper"
    copper_games = df[df['team'] == copper_team_name]
    
    print(f"Copper games: {len(copper_games)}")
    
    if len(copper_games) > 0:
        # Get Copper's team ID
        copper_team_id = copper_games['team_id_master'].iloc[0]
        print(f"Copper team ID: {copper_team_id}")
        
        # Get all opponent IDs from Copper's games
        opponent_ids = copper_games['opponent_id_master'].unique()
        print(f"Copper opponent IDs: {list(opponent_ids)}")
        
        # Check if these opponent IDs appear as team IDs elsewhere
        print("\n=== OPPONENT ID ANALYSIS ===")
        for opp_id in opponent_ids:
            # Find games where this opponent ID appears as a team
            opp_as_team = df[df['team_id_master'] == opp_id]
            print(f"Opponent ID {opp_id}: {len(opp_as_team)} games as team")
            
            if len(opp_as_team) > 0:
                team_names = opp_as_team['team'].unique()
                print(f"  Team names: {list(team_names)}")
    
    # Check the alias map for these specific IDs
    alias_file = "data/derived/id_alias_map_ALL_M_U10.json"
    if Path(alias_file).exists():
        with open(alias_file, 'r') as f:
            alias_map = json.load(f)
        
        print(f"\n=== ALIAS MAP CHECK ===")
        print(f"Alias map size: {len(alias_map)}")
        
        # Check each opponent ID
        for opp_id in opponent_ids:
            canonical_id = str(opp_id).strip()
            if canonical_id.endswith('.0'):
                canonical_id = canonical_id[:-2]
            
            mapped_id = alias_map.get(canonical_id, "NOT FOUND")
            print(f"  {opp_id} -> {canonical_id} -> {mapped_id}")
    
    # Check if the issue is in how team_data is built
    print(f"\n=== TEAM DATA BUILDING SIMULATION ===")
    
    # Simulate how team_data would be built
    all_teams = df['team'].unique()
    copper_team_data = None
    
    for team_name in all_teams:
        if copper_team_name in team_name:
            team_games = df[df['team'] == team_name]
            if len(team_games) > 0:
                team_id = team_games['team_id_master'].iloc[0]
                opponents = team_games['opponent_id_master'].unique().tolist()
                
                copper_team_data = {
                    'team': team_name,
                    'team_id_master': team_id,
                    'opponents': opponents
                }
                break
    
    if copper_team_data:
        print(f"Copper team data:")
        print(f"  Team: {copper_team_data['team']}")
        print(f"  Team ID: {copper_team_data['team_id_master']}")
        print(f"  Opponents: {len(copper_team_data['opponents'])}")
        print(f"  Opponent IDs: {copper_team_data['opponents'][:5]}...")  # Show first 5
    else:
        print("Copper team data not found!")

if __name__ == "__main__":
    main()

