#!/usr/bin/env python3
import pandas as pd

# Load the normalized games data
df = pd.read_parquet('data/games/normalized/games_normalized_AZ_M_U10_20251017_1134.parquet')

# Filter for 16B KChacon
kchacon_games = df[df['team_id_master'] == '1dfe64f51e32'].copy()

print(f'Found {len(kchacon_games)} games for 16B KChacon')

if len(kchacon_games) == 0:
    print('No games found for 16B KChacon')
    exit()

print(f'Team name: {kchacon_games["team"].iloc[0]}')
print(f'Club: {kchacon_games["club"].iloc[0]}')
print()

# Sort by date (most recent first)
kchacon_games = kchacon_games.sort_values('date', ascending=False)

print('ALL GAMES (Most Recent First):')
print('=' * 80)
for i, (_, game) in enumerate(kchacon_games.iterrows(), 1):
    date_str = game['date'].strftime('%Y-%m-%d') if pd.notna(game['date']) else 'N/A'
    gf = game['gf'] if pd.notna(game['gf']) else 0
    ga = game['ga'] if pd.notna(game['ga']) else 0
    opponent = game['opponent'] if pd.notna(game['opponent']) else 'Unknown'
    gd = gf - ga
    result = 'W' if gd > 0 else 'L' if gd < 0 else 'T'
    
    print(f'{i:2d}. {date_str} | {gf:2.0f}-{ga:2.0f} vs {opponent:40} | GD: {gd:+3.0f} | {result}')
