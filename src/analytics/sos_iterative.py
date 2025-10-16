#!/usr/bin/env python3
"""
SOS (Strength of Schedule) iterative refinement module.

Provides iterative refinement of team strengths based on opponent networks,
with fallback to baseline SOS when iterative refinement is not available.
"""

import pandas as pd
import numpy as np
from typing import Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)


def refine_iterative_sos(team_seed_strength: pd.Series, edges_df: pd.DataFrame, 
                        max_iter: int = 3, tol: float = 1e-4) -> pd.Series:
    """
    Refine team strengths iteratively based on opponent network.
    
    This is a PageRank-style iterative refinement where each team's strength
    is updated based on the weighted average of their opponents' strengths.
    
    Args:
        team_seed_strength: Initial team strengths (Series indexed by team_id_master)
        edges_df: DataFrame with columns [team, opponent, weight] representing games
        max_iter: Maximum number of iterations
        tol: Convergence tolerance
        
    Returns:
        Refined team strengths (Series indexed by team_id_master)
    """
    if edges_df.empty or team_seed_strength.empty:
        logger.warning("Empty input data for iterative SOS refinement")
        return team_seed_strength
    
    # Validate required columns
    required_cols = ['team', 'opponent', 'weight']
    if not all(col in edges_df.columns for col in required_cols):
        logger.warning(f"Missing required columns in edges_df: {required_cols}")
        return team_seed_strength
    
    try:
        # Initialize strengths
        strengths = team_seed_strength.copy()
        
        # Create opponent mapping
        opponent_map = {}
        for _, row in edges_df.iterrows():
            team = row['team']
            opponent = row['opponent']
            weight = row['weight']
            
            if team not in opponent_map:
                opponent_map[team] = []
            opponent_map[team].append((opponent, weight))
        
        # Iterative refinement
        for iteration in range(max_iter):
            new_strengths = strengths.copy()
            max_change = 0.0
            
            for team in strengths.index:
                if team in opponent_map:
                    # Compute weighted average of opponent strengths
                    opponent_strengths = []
                    opponent_weights = []
                    
                    for opponent, weight in opponent_map[team]:
                        if opponent in strengths.index:
                            opponent_strengths.append(strengths[opponent])
                            opponent_weights.append(weight)
                    
                    if opponent_strengths:
                        # Weighted average of opponent strengths
                        weighted_avg = np.average(opponent_strengths, weights=opponent_weights)
                        
                        # Blend with current strength (damping factor)
                        damping = 0.85  # PageRank-style damping
                        new_strength = damping * weighted_avg + (1 - damping) * strengths[team]
                        
                        new_strengths[team] = new_strength
                        max_change = max(max_change, abs(new_strength - strengths[team]))
            
            strengths = new_strengths
            
            # Check convergence
            if max_change < tol:
                logger.info(f"SOS iterative refinement converged after {iteration + 1} iterations")
                break
        
        if max_change >= tol:
            logger.warning(f"SOS iterative refinement did not converge after {max_iter} iterations")
        
        return strengths
        
    except Exception as e:
        logger.exception("Error in iterative SOS refinement")
        logger.warning("Falling back to baseline SOS")
        return team_seed_strength


def compute_baseline_sos(games_df: pd.DataFrame, team_strengths: pd.Series) -> pd.Series:
    """
    Compute baseline SOS as average opponent strength.
    
    Args:
        games_df: DataFrame with game data
        team_strengths: Team strength values (Series indexed by team_id_master)
        
    Returns:
        Baseline SOS values (Series indexed by team_id_master)
    """
    if games_df.empty or team_strengths.empty:
        return pd.Series(dtype=float)
    
    sos_values = {}
    
    for team in team_strengths.index:
        # Find all opponents for this team
        team_games = games_df[
            (games_df['team_id_master'] == team) | 
            (games_df['opponent_id_master'] == team)
        ]
        
        opponent_strengths = []
        
        for _, game in team_games.iterrows():
            if game['team_id_master'] == team:
                opponent = game['opponent_id_master']
            else:
                opponent = game['team_id_master']
            
            if opponent in team_strengths.index:
                opponent_strengths.append(team_strengths[opponent])
        
        if opponent_strengths:
            # Use median for robustness against outliers
            sos_values[team] = np.median(opponent_strengths)
        else:
            # Fallback to league average if no opponents found
            sos_values[team] = team_strengths.median()
    
    return pd.Series(sos_values)


def build_opponent_edges(games_df: pd.DataFrame) -> pd.DataFrame:
    """
    Build opponent edge list from games data.
    
    Args:
        games_df: DataFrame with game data
        
    Returns:
        DataFrame with columns [team, opponent, weight] representing game connections
    """
    if games_df.empty:
        return pd.DataFrame(columns=['team', 'opponent', 'weight'])
    
    edges = []
    
    for _, game in games_df.iterrows():
        team = game['team_id_master']
        opponent = game['opponent_id_master']
        
        # Skip if missing team or opponent IDs
        if pd.isna(team) or pd.isna(opponent):
            continue
        
        # Weight based on recency (more recent games have higher weight)
        # This is a simple implementation - could be enhanced with actual date weighting
        weight = 1.0
        
        edges.append({
            'team': team,
            'opponent': opponent,
            'weight': weight
        })
    
    return pd.DataFrame(edges)


if __name__ == "__main__":
    # Test the SOS functions
    print("Testing SOS iterative refinement...")
    
    # Create test data
    teams = ['team1', 'team2', 'team3', 'team4']
    seed_strengths = pd.Series([0.8, 0.6, 0.4, 0.2], index=teams)
    
    edges_data = [
        {'team': 'team1', 'opponent': 'team2', 'weight': 1.0},
        {'team': 'team1', 'opponent': 'team3', 'weight': 1.0},
        {'team': 'team2', 'opponent': 'team3', 'weight': 1.0},
        {'team': 'team2', 'opponent': 'team4', 'weight': 1.0},
        {'team': 'team3', 'opponent': 'team4', 'weight': 1.0},
    ]
    edges_df = pd.DataFrame(edges_data)
    
    # Test iterative refinement
    refined = refine_iterative_sos(seed_strengths, edges_df)
    print(f"Seed strengths: {seed_strengths.tolist()}")
    print(f"Refined strengths: {refined.tolist()}")
    
    # Test baseline SOS
    baseline = compute_baseline_sos(pd.DataFrame(), seed_strengths)
    print(f"Baseline SOS: {baseline.tolist()}")
    
    print("SOS tests completed!")
