#!/usr/bin/env python3
import pandas as pd
import json

# Load the normalized data
df = pd.read_csv('data/games/normalized/games_normalized_ALL_M_U10_20251017_1551.csv')

# Find State 48 FC Copper team
copper_games = df[df['team'].str.contains('State 48.*Copper', case=False, na=False)]
if copper_games.empty:
    raise ValueError("State 48 FC Copper team not found in dataset")
copper_team_id = copper_games['team_id_master'].iloc[0]

print("=== TEAM ID INVESTIGATION ===")
print(f"State 48 FC Copper team ID: {copper_team_id}")
print(f"Type: {type(copper_team_id)}")

# Check what columns exist
print(f"\nColumns in normalized data: {list(df.columns)}")

# Check team_id_master values
print(f"\nUnique team_id_master values (first 10):")
unique_team_ids = df['team_id_master'].unique()[:10]
for tid in unique_team_ids:
    print(f"  {tid} (type: {type(tid)})")

# Check opponent_id_master values
print(f"\nUnique opponent_id_master values (first 10):")
unique_opp_ids = df['opponent_id_master'].unique()[:10]
for oid in unique_opp_ids:
    print(f"  {oid} (type: {type(oid)})")

# Check if team_id_master and opponent_id_master overlap
team_ids_set = set(df['team_id_master'].unique())
opp_ids_set = set(df['opponent_id_master'].unique())
overlap = team_ids_set.intersection(opp_ids_set)
print(f"\nOverlap between team_id_master and opponent_id_master: {len(overlap)} out of {len(team_ids_set)} teams")

# Check Copper team's opponents specifically
copper_opponents = copper_games['opponent_id_master'].unique()
print(f"\nCopper team opponents: {len(copper_opponents)}")
print(f"Copper opponents in team list: {sum(1 for opp in copper_opponents if opp in team_ids_set)}")

# Check the master team index
try:
    master_df = pd.read_csv('data/master/master_team_index_migrated_20251014_1717.csv')
    print(f"\nMaster team index loaded: {len(master_df)} teams")
    print(f"Master columns: {list(master_df.columns)}")
    
    # Check if Copper team ID exists in master
    copper_in_master = master_df[master_df['team_id_master'] == copper_team_id]
    print(f"Copper team in master index: {len(copper_in_master)} records")
    
    if len(copper_in_master) > 0:
        print(f"Copper team master data: {copper_in_master.iloc[0].to_dict()}")
    
    # Check opponent coverage in master
    copper_opponents_in_master = master_df[master_df['team_id_master'].isin(copper_opponents)]
    print(f"Copper opponents in master index: {len(copper_opponents_in_master)} out of {len(copper_opponents)}")
    
except Exception as e:
    print(f"Error loading master index: {e}")

# Check identity map
try:
    with open('data/master/team_identity_map.json', 'r') as f:
        identity_map = json.load(f)
    print(f"\nIdentity map loaded: {len(identity_map)} entries")
    
    # Check if Copper team ID is in identity map
    copper_in_identity = str(copper_team_id) in identity_map
    print(f"Copper team in identity map: {copper_in_identity}")
    
    if copper_in_identity:
        print(f"Copper team identity: {identity_map[str(copper_team_id)]}")
    
except Exception as e:
    print(f"Error loading identity map: {e}")
