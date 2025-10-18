#!/usr/bin/env python3
import pandas as pd
import numpy as np

print("=== COMPREHENSIVE TEAM LINKING ANALYSIS ===")

# Load the normalized data
df = pd.read_csv('data/games/normalized/games_normalized_ALL_M_U10_20251017_1551.csv')

# Load master index
master_df = pd.read_csv('data/master/master_team_index_migrated_20251014_1717.csv')

print(f"Normalized data: {len(df)} games")
print(f"Master index: {len(master_df)} teams")

# Check data types
print(f"\nData types:")
print(f"team_id_master: {df['team_id_master'].dtype}")
print(f"opponent_id_master: {df['opponent_id_master'].dtype}")
print(f"master team_id: {master_df['team_id'].dtype}")

# Convert opponent_id_master to string to match team_id_master
df['opponent_id_master'] = df['opponent_id_master'].astype(str)

# Check overlap after conversion
team_ids_set = set(df['team_id_master'].unique())
opp_ids_set = set(df['opponent_id_master'].unique())
overlap = team_ids_set.intersection(opp_ids_set)
print(f"\nAfter conversion:")
print(f"Overlap between team_id_master and opponent_id_master: {len(overlap)} out of {len(team_ids_set)} teams")

# Check if team IDs exist in master index
master_team_ids = set(master_df['team_id'].astype(str))
teams_in_master = team_ids_set.intersection(master_team_ids)
opponents_in_master = opp_ids_set.intersection(master_team_ids)

print(f"Teams in master index: {len(teams_in_master)} out of {len(team_ids_set)}")
print(f"Opponents in master index: {len(opponents_in_master)} out of {len(opp_ids_set)}")

# Check State 48 FC Copper specifically
copper_games = df[df['team'].str.contains('State 48.*Copper', case=False, na=False)]
if copper_games.empty:
    print("State 48 FC Copper team not found in dataset")
    copper_team_id = None
    copper_opponents = set()
else:
    copper_team_id = copper_games['team_id_master'].iloc[0]
    copper_opponents = set(copper_games['opponent_id_master'].unique())

print(f"\nState 48 FC Copper:")
print(f"Team ID: {copper_team_id}")
if copper_team_id is not None:
    print(f"Team in master: {copper_team_id in master_team_ids}")
    print(f"Opponents: {len(copper_opponents)}")
    print(f"Opponents in master: {len(copper_opponents.intersection(master_team_ids))}")
else:
    print("Team not found - skipping analysis")

# Show the fix needed
print(f"\n=== THE FIX ===")
print("The issue is that opponent_id_master values are float64 but need to be strings.")
print("This conversion should happen in the normalizer or linker.")
print("After conversion, teams can properly find their opponents for SOS calculation.")

# Test the fix
print(f"\n=== TESTING THE FIX ===")
# Convert opponent_id_master to string
df_fixed = df.copy()
df_fixed['opponent_id_master'] = df_fixed['opponent_id_master'].astype(str)

# Now check overlap
team_ids_set_fixed = set(df_fixed['team_id_master'].unique())
opp_ids_set_fixed = set(df_fixed['opponent_id_master'].unique())
overlap_fixed = team_ids_set_fixed.intersection(opp_ids_set_fixed)

print(f"After fix - Overlap: {len(overlap_fixed)} out of {len(team_ids_set_fixed)} teams")

# Check Copper team opponents after fix
copper_games_fixed = df_fixed[df_fixed['team'].str.contains('State 48.*Copper', case=False, na=False)]
copper_opponents_fixed = set(copper_games_fixed['opponent_id_master'].unique())
copper_opponents_in_teams = copper_opponents_fixed.intersection(team_ids_set_fixed)

print(f"Copper opponents after fix: {len(copper_opponents_fixed)}")
print(f"Copper opponents found in team list: {len(copper_opponents_in_teams)}")
