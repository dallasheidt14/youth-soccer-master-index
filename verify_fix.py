#!/usr/bin/env python3
import pandas as pd

# Load the new rankings
df = pd.read_csv('data/rankings/rankings_ALL_M_U10_20251017_1640.csv')

print('Top 10 Teams PowerScores:')
for i, row in df.head(10).iterrows():
    print(f'#{row["rank_national"]:2d}. {row["team"][:30]:<30} PowerScore: {row["powerscore"]:.6f}')

print('\nState 48 FC Copper:')
copper = df[df['team'].str.contains('State 48.*Copper', case=False, na=False)]
if len(copper) > 0:
    row = copper.iloc[0]
    print(f'  National Rank: #{row["rank_national"]}')
    print(f'  State Rank: #{row["rank_state"]}')
    print(f'  PowerScore: {row["powerscore"]:.6f}')
    print(f'  SOS: {row["sos_norm"]:.6f}')

# Check if PowerScores are now unique
powerscores = df['powerscore'].round(6)
unique_count = powerscores.nunique()
total_count = len(powerscores)
print(f'\nPowerScore Uniqueness: {unique_count}/{total_count} ({unique_count/total_count:.1%})')

# Show some PowerScore distribution
print(f'\nPowerScore Distribution:')
print(f'  Min: {df["powerscore"].min():.6f}')
print(f'  Max: {df["powerscore"].max():.6f}')
print(f'  Mean: {df["powerscore"].mean():.6f}')
print(f'  Std: {df["powerscore"].std():.6f}')
