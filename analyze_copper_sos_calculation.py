#!/usr/bin/env python3
import pandas as pd
import numpy as np
from src.analytics.sos_iterative import refine_iterative_sos, compute_baseline_sos, build_opponent_edges

# Load the normalized data
df = pd.read_csv('data/games/normalized/games_normalized_ALL_M_U10_20251017_1551.csv')

# Find State 48 FC Copper team
copper_matches = df[df['team'].str.contains('State 48.*Copper', case=False, na=False)]
if copper_matches.empty:
    raise ValueError("State 48 FC Copper team not found in dataset")
copper_team_id = copper_matches['team_id_master'].iloc[0]
print(f"State 48 FC Copper team ID: {copper_team_id}")

# Get all games for this team
copper_games = df[df['team_id_master'] == copper_team_id]
print(f"Copper team games: {len(copper_games)}")

# Build opponent edges
edges_df = build_opponent_edges(df)
print(f"Total opponent edges: {len(edges_df)}")

# Get edges for Copper team
copper_edges = edges_df[edges_df['team'] == copper_team_id]
print(f"Copper team opponent edges: {len(copper_edges)}")

print("\n=== COPPER TEAM OPPONENTS ===")
for _, edge in copper_edges.iterrows():
    opponent_id = edge['opponent']
    opponent_matches = df[df['team_id_master'] == opponent_id]
    opponent_name = opponent_matches['team'].iloc[0] if not opponent_matches.empty else "Unknown"
    print(f"Opponent: {opponent_name} (ID: {opponent_id})")

# Calculate initial team strengths (using goal differential as proxy)
team_strengths = {}
for team_id in df['team_id_master'].unique():
    team_games = df[df['team_id_master'] == team_id]
    if len(team_games) > 0:
        # Simple strength based on goal differential
        goal_diff = (team_games['gf'] - team_games['ga']).mean()
        team_strengths[team_id] = max(0, goal_diff + 2)  # Normalize to positive values

team_strengths_series = pd.Series(team_strengths)

# Calculate baseline SOS for Copper team
baseline_sos = compute_baseline_sos(df, team_strengths_series)
copper_baseline_sos = baseline_sos.get(copper_team_id, 0)
print(f"\nCopper team baseline SOS: {copper_baseline_sos:.4f}")

# Calculate iterative SOS
iterative_sos = refine_iterative_sos(team_strengths_series, edges_df)
copper_iterative_sos = iterative_sos.get(copper_team_id, 0)
print(f"Copper team iterative SOS: {copper_iterative_sos:.4f}")

# Show opponent strengths
print(f"\n=== OPPONENT STRENGTHS ===")
copper_opponent_strengths = []
for _, edge in copper_edges.iterrows():
    opponent_id = edge['opponent']
    opponent_matches = df[df['team_id_master'] == opponent_id]
    opponent_name = opponent_matches['team'].iloc[0] if not opponent_matches.empty else "Unknown"
    opponent_strength = team_strengths_series.get(opponent_id, 0)
    copper_opponent_strengths.append(opponent_strength)
    print(f"{opponent_name}: {opponent_strength:.4f}")

print(f"\nAverage opponent strength: {np.mean(copper_opponent_strengths):.4f}")
print(f"Median opponent strength: {np.median(copper_opponent_strengths):.4f}")

# Show how the iterative refinement works
print(f"\n=== ITERATIVE REFINEMENT PROCESS ===")
print(f"Initial Copper strength: {team_strengths_series[copper_team_id]:.4f}")
print(f"After iterative refinement: {copper_iterative_sos:.4f}")
print(f"Change: {copper_iterative_sos - team_strengths_series[copper_team_id]:.4f}")
