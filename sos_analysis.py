#!/usr/bin/env python3
import pandas as pd
import numpy as np

# Load the new rankings
df = pd.read_csv('data/rankings/rankings_ALL_M_U10_20251017_1640.csv')

print('SOS Analysis:')
print(f'SOS Range: {df["sos_norm"].min():.6f} to {df["sos_norm"].max():.6f}')
print(f'SOS Mean: {df["sos_norm"].mean():.6f}')
print(f'SOS Std: {df["sos_norm"].std():.6f}')

# Check how many teams have max SOS
max_sos = df['sos_norm'].max()
teams_at_max = (df['sos_norm'] == max_sos).sum()
total_teams = len(df)
print(f'\nTeams at max SOS ({max_sos:.6f}): {teams_at_max}/{total_teams} ({teams_at_max/total_teams:.1%})')

# Check SOS distribution
print(f'\nSOS Distribution:')
percentiles = [5, 10, 25, 50, 75, 90, 95, 99]
for p in percentiles:
    val = np.percentile(df['sos_norm'], p)
    print(f'  {p:2d}th percentile: {val:.6f}')

# Check if the saturation detection should have triggered
sos_q5, sos_q95 = np.percentile(df['sos_norm'], [5, 95])
print(f'\nSOS 5th-95th percentile range: {sos_q5:.6f} to {sos_q95:.6f}')
print(f'SOS span: {sos_q95 - sos_q5:.6f}')

# Check PowerScore components for top teams
print(f'\nTop 5 Teams Component Breakdown:')
for i, row in df.head(5).iterrows():
    sao_contrib = row['sao_norm'] * 0.20
    sad_contrib = row['sad_norm'] * 0.20
    sos_contrib = row['sos_norm'] * 0.60
    total = sao_contrib + sad_contrib + sos_contrib
    print(f'#{row["rank_national"]:2d}. {row["team"][:25]:<25}')
    print(f'    SAO: {row["sao_norm"]:.6f} * 0.20 = {sao_contrib:.6f}')
    print(f'    SAD: {row["sad_norm"]:.6f} * 0.20 = {sad_contrib:.6f}')
    print(f'    SOS: {row["sos_norm"]:.6f} * 0.60 = {sos_contrib:.6f}')
    print(f'    Total: {total:.6f}')
    print()
