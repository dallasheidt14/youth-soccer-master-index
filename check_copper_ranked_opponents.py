#!/usr/bin/env python3
"""
Check if Copper's ranked opponents are in their game history
"""

import pandas as pd

def check_copper_ranked_opponents():
    # Load normalized games
    df = pd.read_csv('data/games/normalized/games_normalized_ALL_M_U10_20251017_1842.csv')
    copper_games = df[df['team'].str.contains('Copper', case=False, na=False)]
    
    print(f"Copper has {len(copper_games)} total games")
    print(f"Copper has {copper_games['opponent'].nunique()} unique opponents")
    print(f"Date range: {copper_games['date'].min()} to {copper_games['date'].max()}")
    
    # Check for ranked opponents
    ranked_opponents = [
        'Southeast 2016 Boys Red',
        'Tuzos Royals 2016', 
        'Northwest 2016 Boys Red'
    ]
    
    print("\nChecking if ranked opponents are in Copper's game history:")
    found_opponents = []
    for opp in ranked_opponents:
        matches = copper_games[copper_games['opponent'].str.contains(opp, case=False, na=False)]
        if len(matches) > 0:
            print(f"  {opp}: FOUND ({len(matches)} games)")
            found_opponents.extend(matches['opponent'].unique())
            # Show the game details
            for _, game in matches.iterrows():
                print(f"    {game['date']}: vs {game['opponent']} (ID: {game['opponent_id_master']})")
        else:
            print(f"  {opp}: NOT FOUND")
    
    # Load rankings to verify these teams are actually ranked
    rankings_df = pd.read_csv('data/rankings/state_views/rankings_AZ_M_U10_20251017_1845.csv')
    ranked_teams = set(rankings_df['team'].str.lower())
    
    print(f"\nVerifying these opponents are ranked:")
    for opp in found_opponents:
        is_ranked = opp.lower() in ranked_teams
        print(f"  {opp}: {'RANKED' if is_ranked else 'NOT RANKED'}")
    
    # Check if there are any similar names in Copper's opponents
    print(f"\nCopper's opponents containing 'southeast', 'tuzos', or 'northwest':")
    copper_opponents = copper_games['opponent'].str.lower()
    keywords = ['southeast', 'tuzos', 'northwest']
    for keyword in keywords:
        matches = copper_games[copper_opponents.str.contains(keyword, na=False)]
        if len(matches) > 0:
            print(f"  {keyword}: {len(matches)} matches")
            for _, match in matches.iterrows():
                print(f"    {match['opponent']} (ID: {match['opponent_id_master']})")
        else:
            print(f"  {keyword}: No matches")

if __name__ == "__main__":
    check_copper_ranked_opponents()

