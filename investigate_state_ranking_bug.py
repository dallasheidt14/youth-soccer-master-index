#!/usr/bin/env python3
import pandas as pd

# Load the new rankings
df = pd.read_csv('data/rankings/rankings_ALL_M_U10_20251017_1656.csv')

print('Arizona Teams - National vs State Ranking Analysis:')
az_teams = df[df['state'] == 'AZ'].sort_values('rank_national').head(10)

print('National Rank | State Rank | Team Name                    | PowerScore')
print('-------------|------------|------------------------------|----------')
for i, team_row in az_teams.iterrows():
    print(f'#{team_row["rank_national"]:3d}          | #{team_row["rank_state"]:2d}        | {team_row["team"][:28]:<28} | {team_row["powerscore"]:.6f}')

print('\nChecking if state ranks are just copies of national ranks...')
# Check if rank_state == rank_national for all Arizona teams
az_copy_check = (az_teams['rank_national'] == az_teams['rank_state']).all()
print(f'All Arizona teams have rank_state == rank_national: {az_copy_check}')

if az_copy_check:
    print('❌ PROBLEM: State ranks are just copies of national ranks!')
    print('This means the state ranking logic is broken.')
else:
    print('✅ State ranks are different from national ranks (correct)')

print('\nWhat the state ranking SHOULD look like:')
# Sort Arizona teams by PowerScore to see what state ranking should be
az_by_powerscore = df[df['state'] == 'AZ'].sort_values('powerscore', ascending=False)
print('Correct State Rank | Current State Rank | Team Name                    | PowerScore')
print('------------------|-------------------|------------------------------|----------')
for rank_idx, (i, team_row) in enumerate(az_by_powerscore.head(10).iterrows(), 1):
    correct_state_rank = rank_idx
    print(f'#{correct_state_rank:2d}                | #{team_row["rank_state"]:2d}        | {team_row["team"][:28]:<28} | {team_row["powerscore"]:.6f}')

print('\nState 48 FC Copper Analysis:')
copper = df[df['team'].str.contains('State 48.*Copper', case=False, na=False)]
if len(copper) > 0:
    row = copper.iloc[0]
    print(f'  National Rank: #{row["rank_national"]}')
    print(f'  State Rank: #{row["rank_state"]} (broken - just copied from national)')
    print(f'  PowerScore: {row["powerscore"]:.6f}')
    
    # Find their correct state rank
    az_teams_sorted = df[df['state'] == 'AZ'].sort_values('powerscore', ascending=False).reset_index(drop=True)
    copper_match = az_teams_sorted[az_teams_sorted['team'] == row['team']]
    if not copper_match.empty:
        correct_state_rank = copper_match.index[0] + 1
        print(f'  Correct State Rank: #{correct_state_rank} (based on PowerScore within Arizona)')
    else:
        print('  Could not find Copper team in Arizona rankings')
