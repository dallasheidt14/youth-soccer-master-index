#!/usr/bin/env python3
"""
Opponent Analysis - Check which opponents are missing from dataset
"""

import pandas as pd
from pathlib import Path

def analyze_missing_opponents():
    """Analyze which of KChacon's opponents are missing from the dataset."""
    
    # Load the normalized games data
    df = pd.read_parquet('data/games/normalized/games_normalized_AZ_M_U10_20251017_1134.parquet')
    
    # Get KChacon's games
    kchacon_games = df[df['team_id_master'] == '1dfe64f51e32'].copy()
    
    print(f"16B KChacon Analysis")
    print(f"Total Games: {len(kchacon_games)}")
    print("=" * 80)
    
    # Get unique opponents
    opponents = kchacon_games['opponent_id_master'].dropna().unique()
    print(f"Unique Opponents: {len(opponents)}")
    
    # Check which opponents are in the dataset
    all_teams_in_dataset = set(df['team_id_master'].unique())
    
    print(f"\nOPPONENT STATUS ANALYSIS:")
    print("-" * 80)
    
    missing_opponents = []
    present_opponents = []
    
    for opp_id in opponents:
        opp_games = kchacon_games[kchacon_games['opponent_id_master'] == opp_id]
        opp_name = opp_games['opponent'].iloc[0] if not opp_games.empty else 'Unknown'
        games_vs_opp = len(opp_games)
        
        if opp_id in all_teams_in_dataset:
            # Opponent is in dataset
            opp_all_games = df[df['team_id_master'] == opp_id]
            opp_gf = opp_all_games['gf'].sum()
            opp_ga = opp_all_games['ga'].sum()
            opp_record = f"{opp_gf:.0f}-{opp_ga:.0f}"
            opp_gd = opp_gf - opp_ga
            
            present_opponents.append({
                'name': opp_name,
                'id': opp_id,
                'games_vs': games_vs_opp,
                'record': opp_record,
                'gd': opp_gd
            })
        else:
            # Opponent is NOT in dataset
            missing_opponents.append({
                'name': opp_name,
                'id': opp_id,
                'games_vs': games_vs_opp
            })
    
    print(f"OPPONENTS IN DATASET ({len(present_opponents)}):")
    print("-" * 50)
    if present_opponents:
        for opp in present_opponents:
            print(f"[IN] {opp['name']:<40} | Games: {opp['games_vs']} | Record: {opp['record']} | GD: {opp['gd']:+3.0f}")
    else:
        print("None")
    
    print(f"\nOPPONENTS MISSING FROM DATASET ({len(missing_opponents)}):")
    print("-" * 50)
    if missing_opponents:
        for opp in missing_opponents:
            print(f"[OUT] {opp['name']:<40} | Games: {opp['games_vs']} | ID: {opp['id']}")
    else:
        print("None")
    
    # Analyze why opponents are missing
    print(f"\nMISSING OPPONENT ANALYSIS:")
    print("-" * 40)
    
    # Categorize missing opponents
    other_states = []
    other_ages = []
    other_genders = []
    unknown = []
    
    for opp in missing_opponents:
        name = opp['name'].lower()
        
        # Check for other states
        if any(state in name for state in ['utah', 'california', 'nevada', 'las vegas', 'irvine']):
            other_states.append(opp)
        # Check for other age groups
        elif any(age in name for age in ['2015', '2017', 'u9', 'u11', '15b', '17b']):
            other_ages.append(opp)
        # Check for other genders
        elif any(gender in name for gender in ['girls', '15g', '17g']):
            other_genders.append(opp)
        else:
            unknown.append(opp)
    
    print(f"Other States: {len(other_states)}")
    for opp in other_states:
        print(f"  - {opp['name']}")
    
    print(f"\nOther Age Groups: {len(other_ages)}")
    for opp in other_ages:
        print(f"  - {opp['name']}")
    
    print(f"\nOther Genders: {len(other_genders)}")
    for opp in other_genders:
        print(f"  - {opp['name']}")
    
    print(f"\nUnknown: {len(unknown)}")
    for opp in unknown:
        print(f"  - {opp['name']}")
    
    # Summary
    print(f"\nSUMMARY:")
    print("-" * 20)
    print(f"Total opponents: {len(opponents)}")
    print(f"In dataset: {len(present_opponents)} ({len(present_opponents)/len(opponents)*100:.1f}%)")
    print(f"Missing: {len(missing_opponents)} ({len(missing_opponents)/len(opponents)*100:.1f}%)")
    print(f"  - Other states: {len(other_states)}")
    print(f"  - Other ages: {len(other_ages)}")
    print(f"  - Other genders: {len(other_genders)}")
    print(f"  - Unknown: {len(unknown)}")

if __name__ == "__main__":
    analyze_missing_opponents()
