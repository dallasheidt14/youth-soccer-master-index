#!/usr/bin/env python3
"""
Debug script to investigate why State 48 FC Avondale 16 Copper's opponents 
are not being found in the ranking engine.
"""

import pandas as pd
import json
from pathlib import Path

def main():
    # Load the normalized games data
    games_file = "data/games/normalized/games_normalized_ALL_M_U10_20251017_1923.parquet"
    df = pd.read_parquet(games_file)
    
    print("=== STATE 48 FC AVONDALE 16 COPPER INVESTIGATION ===")
    
    # Find the specific Copper team
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
        
        # Check the alias map
        alias_file = "data/derived/id_alias_map_ALL_M_U10.json"
        if Path(alias_file).exists():
            with open(alias_file, 'r') as f:
                alias_map = json.load(f)
            
            print(f"\n=== ALIAS MAP RESOLUTION ===")
            resolved_opponents = []
            for opp_id in opponent_ids:
                canonical_id = str(opp_id).strip()
                if canonical_id.endswith('.0'):
                    canonical_id = canonical_id[:-2]
                
                mapped_id = alias_map.get(canonical_id, "NOT FOUND")
                print(f"  {opp_id} -> {canonical_id} -> {mapped_id}")
                if mapped_id != "NOT FOUND":
                    resolved_opponents.append(mapped_id)
            
            print(f"\nResolved opponent IDs: {resolved_opponents}")
            
            # Check if these resolved IDs appear as team IDs in the games data
            print(f"\n=== RESOLVED OPPONENT VERIFICATION ===")
            for resolved_id in resolved_opponents:
                opp_as_team = df[df['team_id_master'] == resolved_id]
                print(f"Resolved ID {resolved_id}: {len(opp_as_team)} games as team")
                
                if len(opp_as_team) > 0:
                    team_names = opp_as_team['team'].unique()
                    print(f"  Team names: {list(team_names)}")
    
    # Check if there are multiple Copper teams that might be confusing the issue
    print(f"\n=== ALL COPPER TEAMS ===")
    all_copper_teams = df[df['team'].str.contains('Copper', case=False, na=False)]['team'].unique()
    for team in all_copper_teams:
        print(f"  - {team}")

if __name__ == "__main__":
    main()

