#!/usr/bin/env python3
import pandas as pd
import numpy as np

# Load the normalized data to check Copper's opponents
df = pd.read_csv('data/games/normalized/games_normalized_ALL_M_U10_20251017_1740.csv')

# Find State 48 FC Copper team
copper_matches = df[df['team'].str.contains('State 48.*Copper', case=False, na=False)]
if copper_matches.empty:
    raise ValueError("State 48 FC Copper team not found in dataset")
copper_team_id = copper_matches['team_id_master'].iloc[0]
print(f"State 48 FC Copper team ID: {copper_team_id}")

# Get all games for this team
copper_games = df[df['team_id_master'] == copper_team_id]
print(f"Copper team games: {len(copper_games)}")

# Get unique opponents
copper_opponents = copper_games['opponent_id_master'].unique()
print(f"Copper unique opponents: {len(copper_opponents)}")

# Load the latest rankings to see which opponents are ranked
rankings_df = pd.read_csv('data/rankings/rankings_ALL_M_U10_20251017_1826.csv')
ranked_team_ids = set(rankings_df['team_id_master'].astype(str))

print()
print('=== COPPER OPPONENT ANALYSIS ===')
ranked_opponents = 0
unranked_opponents = 0

for opp_id in copper_opponents:
    if str(opp_id) in ranked_team_ids:
        ranked_opponents += 1
        # Find opponent name
        opp_games = df[df['team_id_master'] == opp_id]
        if len(opp_games) > 0:
            opp_name = opp_games['team'].iloc[0]
            print(f"RANKED: {opp_name} (ID: {opp_id})")
    else:
        unranked_opponents += 1
        # Find opponent name
        opp_games = df[df['opponent_id_master'] == opp_id]
        if len(opp_games) > 0:
            opp_name = opp_games['opponent'].iloc[0]
            print(f"UNRANKED: {opp_name} (ID: {opp_id})")

print()
print(f'Ranked opponents: {ranked_opponents}')
print(f'Unranked opponents: {unranked_opponents}')
print(f'Total opponents: {len(copper_opponents)}')

# Check if Copper's ranked opponents are strong
print()
print('=== COPPER RANKED OPPONENTS STRENGTH ===')
for opp_id in copper_opponents:
    if str(opp_id) in ranked_team_ids:
        opp_ranking = rankings_df[rankings_df['team_id_master'].astype(str) == str(opp_id)]
        if len(opp_ranking) > 0:
            opp_row = opp_ranking.iloc[0]
            print(f"{opp_row['team'][:40]:<40} Rank: #{opp_row['rank']:3d} PowerScore: {opp_row['powerscore']:.4f}")

