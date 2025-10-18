#!/usr/bin/env python3
import pandas as pd

# Load the NEW rankings with v5.3E normalization
df = pd.read_csv('data/rankings/rankings_ALL_M_U10_20251017_1656.csv')

print('Top 10 Teams PowerScores (NEW v5.3E):')
for i, row in df.head(10).iterrows():
    print(f'#{row["rank_national"]:2d}. {row["team"][:30]:<30} PowerScore: {row["powerscore"]:.6f}')

print('\nState 48 FC Copper (NEW):')
copper = df[df['team'].str.contains('State 48.*Copper', case=False, na=False)]
if len(copper) > 0:
    row = copper.iloc[0]
    print(f'  National Rank: #{row["rank_national"]}')
    print(f'  State Rank: #{row.get("rank_state", "N/A")}')
    print(f'  PowerScore: {row["powerscore"]:.6f}')
    print(f'  SAO: {row["sao_norm"]:.6f}')
    print(f'  SAD: {row["sad_norm"]:.6f}')
    print(f'  SOS: {row["sos_norm"]:.6f}')

# Check PowerScore uniqueness
powerscores = df['powerscore'].round(6)
unique_count = powerscores.nunique()
total_count = len(powerscores)
print(f'\nPowerScore Uniqueness: {unique_count}/{total_count} ({unique_count/total_count:.1%})')

# Show PowerScore distribution
print(f'\nPowerScore Distribution:')
print(f'  Min: {df["powerscore"].min():.6f}')
print(f'  Max: {df["powerscore"].max():.6f}')
print(f'  Mean: {df["powerscore"].mean():.6f}')
print(f'  Std: {df["powerscore"].std():.6f}')

# Check if top teams have different PowerScores now
print(f'\nTop 10 PowerScore Analysis:')
top_10_powerscores = df.head(10)['powerscore'].round(6)
unique_top_10 = top_10_powerscores.nunique()
print(f'Top 10 unique PowerScores: {unique_top_10}/10')

if unique_top_10 > 1:
    print('SUCCESS: Top teams now have different PowerScores!')
    print('Top 10 PowerScore values:')
    for i, score in enumerate(top_10_powerscores):
        print(f'  #{i+1}: {score:.6f}')
else:
    print('ISSUE: Top teams still have identical PowerScores')
