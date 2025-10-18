#!/usr/bin/env python3
import pandas as pd
import numpy as np

# Load the new rankings
df = pd.read_csv('data/rankings/rankings_ALL_M_U10_20251017_1647.csv')

print('Top 10 Teams Raw Values Analysis:')
top_10 = df.head(10)

print('Rank | Team                    | SAO Raw  | SAD Raw  | SOS Raw  | Games')
print('-----|-------------------------|----------|----------|----------|-------')
for i, row in top_10.iterrows():
    print(f'#{row["rank_national"]:2d}  | {row["team"][:23]:<23} | {row["sao_shrunk"]:.6f} | {row["sad_shrunk"]:.6f} | {row["sos_component"]:.6f} | {row["gp_used"]:3d}')

# Check if raw values are identical
print('\nRaw Value Uniqueness Check:')
sao_raw_unique = df['sao_shrunk'].round(6).nunique()
sad_raw_unique = df['sad_shrunk'].round(6).nunique()
sos_raw_unique = df['sos_component'].round(6).nunique()
total_teams = len(df)

print(f'SAO raw unique values: {sao_raw_unique}/{total_teams} ({sao_raw_unique/total_teams:.1%})')
print(f'SAD raw unique values: {sad_raw_unique}/{total_teams} ({sad_raw_unique/total_teams:.1%})')
print(f'SOS raw unique values: {sos_raw_unique}/{total_teams} ({sos_raw_unique/total_teams:.1%})')

# Check the distribution of raw values
print('\nRaw Value Distribution:')
print(f'SAO raw: min={df["sao_shrunk"].min():.6f}, max={df["sao_shrunk"].max():.6f}, mean={df["sao_shrunk"].mean():.6f}')
print(f'SAD raw: min={df["sad_shrunk"].min():.6f}, max={df["sad_shrunk"].max():.6f}, mean={df["sad_shrunk"].mean():.6f}')
print(f'SOS raw: min={df["sos_component"].min():.6f}, max={df["sos_component"].max():.6f}, mean={df["sos_component"].mean():.6f}')

# Check if top teams have identical raw values
print('\nTop 10 Raw Value Analysis:')
top_10_sao = top_10['sao_shrunk'].round(6).nunique()
top_10_sad = top_10['sad_shrunk'].round(6).nunique()
top_10_sos = top_10['sos_component'].round(6).nunique()

print(f'Top 10 SAO raw unique: {top_10_sao}/10')
print(f'Top 10 SAD raw unique: {top_10_sad}/10')
print(f'Top 10 SOS raw unique: {top_10_sos}/10')
