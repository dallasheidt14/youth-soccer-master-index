#!/usr/bin/env python3
import pandas as pd

# Load the new rankings
df = pd.read_csv('data/rankings/rankings_ALL_M_U10_20251017_1656.csv')

print('State 48 FC Copper Analysis:')
copper = df[df['team'].str.contains('State 48.*Copper', case=False, na=False)]
if len(copper) > 0:
    row = copper.iloc[0]
    print(f'  National Rank: #{row["rank_national"]}')
    print(f'  State Rank: #{row["rank_state"]}')
    print(f'  PowerScore: {row["powerscore"]:.6f}')
    print(f'  State: {row["state"]}')

print('\nArizona Teams in National Rankings:')
az_teams = df[df['state'] == 'AZ'].sort_values('rank_national').head(20)
print('National Rank | State Rank | Team Name                    | PowerScore')
print('-------------|------------|------------------------------|----------')
for i, team_row in az_teams.iterrows():
    print(f'#{team_row["rank_national"]:3d}          | #{team_row["rank_state"]:2d}        | {team_row["team"][:28]:<28} | {team_row["powerscore"]:.6f}')

print('\nTop 20 National Teams:')
top_20 = df.head(20)
print('National Rank | State | Team Name                    | PowerScore')
print('-------------|-------|------------------------------|----------')
for i, team_row in top_20.iterrows():
    print(f'#{team_row["rank_national"]:3d}          | {team_row["state"]:5s} | {team_row["team"][:28]:<28} | {team_row["powerscore"]:.6f}')

# Check if there's a correlation issue
print('\nRanking Correlation Analysis:')
print(f'Total teams: {len(df)}')
print(f'Arizona teams: {len(df[df["state"] == "AZ"])}')

if len(copper) > 0:
    row = copper.iloc[0]
    print(f'Teams ranked higher than State 48 FC Copper nationally: {len(df[df["rank_national"] < row["rank_national"]])}')
    az_higher = df[(df["state"] == "AZ") & (df["rank_national"] < row["rank_national"])]
    print(f'Arizona teams ranked higher than State 48 FC Copper nationally: {len(az_higher)}')
else:
    print('State 48 FC Copper team not found - skipping correlation analysis')

# Check PowerScore distribution by state
print('\nPowerScore Distribution by State (Top 5 states):')
state_stats = df.groupby('state')['powerscore'].agg(['count', 'min', 'max', 'mean']).sort_values('max', ascending=False)
print(state_stats.head())
