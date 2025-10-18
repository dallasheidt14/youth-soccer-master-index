#!/usr/bin/env python3
import pandas as pd

# Load the normalized data
df = pd.read_csv('data/games/normalized/games_normalized_ALL_M_U10_20251017_1551.csv')

# Find State 48 FC Copper team games
copper_games = df[df['team'].str.contains('State 48.*Copper', case=False, na=False)]

print('State 48 FC Copper team games:')
print(copper_games[['date', 'team', 'opponent', 'gf', 'ga', 'result']].head(10))
print(f'Total games: {len(copper_games)}')

if len(copper_games) > 0:
    print(f'Average goals for: {copper_games["gf"].mean():.2f}')
    print(f'Average goals against: {copper_games["ga"].mean():.2f}')
    print(f'Win rate: {(copper_games["result"] == "W").mean():.2%}')
    
    # Check for suspicious patterns
    print(f'\nSuspicious patterns:')
    print(f'Games with 0 goals against: {(copper_games["ga"] == 0).sum()}')
    print(f'Games with 8+ goals for: {(copper_games["gf"] >= 8).sum()}')
    print(f'Max goals for: {copper_games["gf"].max()}')
    print(f'Max goals against: {copper_games["ga"].max()}')
    
    # Show all games
    print('\nAll games:')
    all_games = copper_games.sort_values('date', ascending=False)
    print(all_games[['date', 'opponent', 'gf', 'ga', 'result']])
