#!/usr/bin/env python3
import pandas as pd
import numpy as np

# Load the normalized data
df = pd.read_csv('data/games/normalized/games_normalized_ALL_M_U10_20251017_1551.csv')

# Find State 48 FC Copper team games
copper_games = df[df['team'].str.contains('State 48.*Copper', case=False, na=False)]

print('=== STATE 48 FC COPPER SOS ANALYSIS ===')
print(f'Total games: {len(copper_games)}')

if len(copper_games) > 0:
    # Get all unique opponents
    opponents = copper_games['opponent'].unique()
    print(f'Unique opponents: {len(opponents)}')
    
    print('\n=== OPPONENT ANALYSIS ===')
    opponent_stats = []
    
    for opponent in opponents:
        # Get all games for this opponent
        opp_games = df[df['team'] == opponent]
        
        if len(opp_games) > 0:
            opp_wins = (opp_games['result'] == 'W').sum()
            opp_losses = (opp_games['result'] == 'L').sum()
            opp_ties = (opp_games['result'] == 'D').sum()
            opp_gf_avg = opp_games['gf'].mean()
            opp_ga_avg = opp_games['ga'].mean()
            
            opponent_stats.append({
                'opponent': opponent,
                'games': len(opp_games),
                'wins': opp_wins,
                'losses': opp_losses,
                'ties': opp_ties,
                'win_rate': opp_wins / len(opp_games) if len(opp_games) > 0 else 0,
                'gf_avg': opp_gf_avg,
                'ga_avg': opp_ga_avg,
                'goal_diff': opp_gf_avg - opp_ga_avg
            })
    
    # Sort by opponent strength (win rate)
    opponent_stats.sort(key=lambda x: x['win_rate'], reverse=True)
    
    print('Opponents ranked by strength (win rate):')
    print('=' * 80)
    for i, opp in enumerate(opponent_stats[:15]):  # Top 15 opponents
        print(f'{i+1:2d}. {opp["opponent"][:50]:<50} | '
              f'Games: {opp["games"]:2d} | '
              f'WR: {opp["win_rate"]:.1%} | '
              f'GF: {opp["gf_avg"]:.1f} | '
              f'GA: {opp["ga_avg"]:.1f}')
    
    print('\n=== WEAK OPPONENTS ===')
    weak_opponents = [opp for opp in opponent_stats if opp['games'] < 5 or opp['win_rate'] < 0.3]
    print(f'Opponents with <5 games or <30% win rate: {len(weak_opponents)}')
    
    for opp in weak_opponents[:10]:
        print(f'- {opp["opponent"][:60]:<60} | '
              f'Games: {opp["games"]:2d} | '
              f'WR: {opp["win_rate"]:.1%}')
    
    # Calculate average opponent strength
    avg_opponent_games = np.mean([opp['games'] for opp in opponent_stats])
    avg_opponent_win_rate = np.mean([opp['win_rate'] for opp in opponent_stats])
    
    print(f'\n=== SOS SUMMARY ===')
    print(f'Average opponent games played: {avg_opponent_games:.1f}')
    print(f'Average opponent win rate: {avg_opponent_win_rate:.1%}')
    print(f'Opponents with <5 games: {sum(1 for opp in opponent_stats if opp["games"] < 5)}')
    print(f'Opponents with <30% win rate: {sum(1 for opp in opponent_stats if opp["win_rate"] < 0.3)}')
