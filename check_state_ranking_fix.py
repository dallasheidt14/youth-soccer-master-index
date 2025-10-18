#!/usr/bin/env python3
import pandas as pd

# Check the Arizona state view file (should have correct state rankings)
az_df = pd.read_csv('data/rankings/state_views/rankings_AZ_M_U10_20251017_1705.csv')

print('Arizona State Rankings (CORRECT - from state view file):')
print('State Rank | National Rank | Team Name                    | PowerScore')
print('-----------|---------------|------------------------------|----------')
for i, row in az_df.head(10).iterrows():
    print(f'#{row["rank_state"]:2d}        | #{row["rank_national"]:3d}          | {row["team"][:28]:<28} | {row["powerscore"]:.6f}')

print('\nState 48 FC Copper (from state view):')
copper = az_df[az_df['team'].str.contains('State 48.*Copper', case=False, na=False)]
if len(copper) > 0:
    row = copper.iloc[0]
    print(f'  State Rank: #{row["rank_state"]}')
    print(f'  National Rank: #{row["rank_national"]}')
    print(f'  PowerScore: {row["powerscore"]:.6f}')

# Check if state ranks are now different from national ranks
print('\nState vs National Rank Analysis:')
state_national_different = (az_df['rank_state'] != az_df['rank_national']).any()
print(f'Some Arizona teams have different state vs national ranks: {state_national_different}')

if state_national_different:
    print('SUCCESS: State ranking is now working correctly!')
    print('\nExamples of different state vs national ranks:')
    different = az_df[az_df['rank_state'] != az_df['rank_national']].head(5)
    for i, row in different.iterrows():
        print(f'  {row["team"][:30]:<30} State: #{row["rank_state"]:2d}, National: #{row["rank_national"]:3d}')
else:
    print('ISSUE: State ranks are still identical to national ranks')
