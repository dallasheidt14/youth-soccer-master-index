#!/usr/bin/env python3
import pandas as pd
import numpy as np

# Load the new rankings
df = pd.read_csv('data/rankings/rankings_ALL_M_U10_20251017_1647.csv')

print('Top 5 Teams Component Analysis:')
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

# Check if the components are actually different
print('Component Uniqueness Check:')
sao_unique = df['sao_norm'].round(6).nunique()
sad_unique = df['sad_norm'].round(6).nunique()
sos_unique = df['sos_norm'].round(6).nunique()
total_teams = len(df)

print(f'SAO unique values: {sao_unique}/{total_teams} ({sao_unique/total_teams:.1%})')
print(f'SAD unique values: {sad_unique}/{total_teams} ({sad_unique/total_teams:.1%})')
print(f'SOS unique values: {sos_unique}/{total_teams} ({sos_unique/total_teams:.1%})')

# Check the top teams specifically
print('\nTop 10 Teams Component Values:')
top_10 = df.head(10)
print('Rank | Team                    | SAO      | SAD      | SOS      | PowerScore')
print('-----|-------------------------|----------|----------|----------|----------')
for i, row in top_10.iterrows():
    print(f'#{row["rank_national"]:2d}  | {row["team"][:23]:<23} | {row["sao_norm"]:.6f} | {row["sad_norm"]:.6f} | {row["sos_norm"]:.6f} | {row["powerscore"]:.6f}')
