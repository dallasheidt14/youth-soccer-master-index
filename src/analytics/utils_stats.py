#!/usr/bin/env python3
"""
Statistical utilities for the v53E ranking engine.

Provides helper functions for data normalization, weighting, and statistical operations
used throughout the ranking pipeline.
"""

import pandas as pd
import numpy as np
from typing import Dict, Any, Tuple
import logging

logger = logging.getLogger(__name__)


def robust_minmax(x: pd.Series, q_low: float = 0.05, q_high: float = 0.95) -> pd.Series:
    """
    Apply robust min-max scaling with gentle logistic stretch.
    
    Args:
        x: Input series to normalize
        q_low: Lower quantile for clipping (default 0.05)
        q_high: Upper quantile for clipping (default 0.95)
        
    Returns:
        Series scaled to [0, 1] with logistic stretch applied
    """
    if x.empty:
        return x
    
    # Compute robust bounds using quantiles
    q_low_val = x.quantile(q_low)
    q_high_val = x.quantile(q_high)
    
    # Handle edge case where quantiles are equal
    if q_low_val == q_high_val:
        return pd.Series(0.5, index=x.index)
    
    # Clip to robust bounds
    clipped = x.clip(lower=q_low_val, upper=q_high_val)
    
    # Scale to [0, 1]
    scaled = (clipped - q_low_val) / (q_high_val - q_low_val)
    
    # Apply gentle logistic stretch: 1 / (1 + exp(-6*(scaled - 0.5)))
    stretched = 1 / (1 + np.exp(-6 * (scaled - 0.5)))
    
    return stretched


def exp_decay(series: pd.Series, rate: float) -> pd.Series:
    """
    Apply exponential decay to a series based on index position.
    
    Args:
        series: Input series
        rate: Decay rate (higher = faster decay)
        
    Returns:
        Series with exponential decay weights, normalized to sum=1
    """
    if series.empty:
        return series
    
    # Create index-based decay weights
    indices = np.arange(len(series))
    weights = np.exp(-rate * indices)
    
    # Normalize to sum=1
    weights = weights / weights.sum()
    
    return pd.Series(weights, index=series.index)


def tapered_weights(n: int, recent_k: int, recent_share: float, tail_cfg: Dict[str, Any]) -> np.ndarray:
    """
    Generate tapered weights for game recency with tail dampening.
    
    Args:
        n: Total number of games
        recent_k: Number of recent games to emphasize
        recent_share: Fraction of total weight for recent games
        tail_cfg: Dictionary with tail dampening config
            - tail_start: Start index for tail dampening
            - tail_end: End index for tail dampening  
            - tail_start_weight: Weight at tail start
            - tail_end_weight: Weight at tail end
            
    Returns:
        Array of normalized weights (sum=1)
    """
    if n == 0:
        return np.array([])
    
    weights = np.zeros(n)
    
    # Recent games get recent_share of total weight
    if recent_k > 0 and recent_k <= n:
        recent_weight_per_game = recent_share / recent_k
        weights[:recent_k] = recent_weight_per_game
    
    # Prior games get (1 - recent_share) of total weight
    if n > recent_k:
        prior_weight_per_game = (1 - recent_share) / (n - recent_k)
        weights[recent_k:] = prior_weight_per_game
    
    # Apply tail dampening
    tail_start = tail_cfg.get('tail_start', n)
    tail_end = tail_cfg.get('tail_end', n)
    tail_start_weight = tail_cfg.get('tail_start_weight', 1.0)
    tail_end_weight = tail_cfg.get('tail_end_weight', 1.0)
    
    if tail_start < n and tail_end <= n and tail_start < tail_end:
        # Linear interpolation for tail dampening
        tail_indices = np.arange(tail_start, tail_end)
        if len(tail_indices) > 1:
            dampening = np.linspace(tail_start_weight, tail_end_weight, len(tail_indices))
            weights[tail_indices] *= dampening
    
    # Normalize to sum=1
    if weights.sum() > 0:
        weights = weights / weights.sum()
    
    return weights


def clip_zscore_per_team(df: pd.DataFrame, team_col: str, value_col: str, z: float = 2.5) -> pd.DataFrame:
    """
    Clip outliers per team using z-score threshold.
    
    Args:
        df: DataFrame with team and value columns
        team_col: Column name for team identifier
        value_col: Column name for values to clip
        z: Z-score threshold (default 2.5)
        
    Returns:
        DataFrame with clipped values
    """
    df_clipped = df.copy()
    
    for team in df[team_col].unique():
        mask = df[team_col] == team
        team_values = df.loc[mask, value_col]
        
        if len(team_values) > 1:  # Need at least 2 values for std
            mean_val = team_values.mean()
            std_val = team_values.std()
            
            if std_val > 0:  # Avoid division by zero
                lower_bound = mean_val - z * std_val
                upper_bound = mean_val + z * std_val
                
                df_clipped.loc[mask, value_col] = team_values.clip(
                    lower=lower_bound, upper=upper_bound
                )
    
    return df_clipped


def cap_goal_diff(gd: pd.Series, cap: int) -> pd.Series:
    """
    Cap goal difference to specified range.
    
    Args:
        gd: Goal difference series
        cap: Maximum absolute goal difference
        
    Returns:
        Series with goal differences capped to [-cap, cap]
    """
    return gd.clip(lower=-cap, upper=cap)


def safe_merge(left: pd.DataFrame, right: pd.DataFrame, **kwargs) -> pd.DataFrame:
    """
    Perform safe merge with dtype normalization.
    
    Args:
        left: Left DataFrame
        right: Right DataFrame
        **kwargs: Additional arguments passed to pd.merge
        
    Returns:
        Merged DataFrame
    """
    left_clean = left.copy()
    right_clean = right.copy()
    
    # Normalize common ID columns to string type
    id_columns = ['team_id_master', 'opponent_id_master', 'team_id_source', 'opponent_id']
    
    for col in id_columns:
        if col in left_clean.columns:
            left_clean[col] = left_clean[col].astype(str)
        if col in right_clean.columns:
            right_clean[col] = right_clean[col].astype(str)
    
    # Perform merge
    merged = pd.merge(left_clean, right_clean, **kwargs)
    
    # Log merge statistics
    logger.debug(f"Merge: {len(left)} + {len(right)} → {len(merged)} rows")
    
    return merged


def compute_adaptive_k(gap: float, sample_size: int, alpha: float = 0.5, beta: float = 0.6) -> float:
    """
    Compute adaptive K-factor based on opponent strength gap and sample size.
    
    Args:
        gap: Strength differential between teams
        sample_size: Number of games played
        alpha: Alpha parameter for gap scaling
        beta: Beta parameter for sample scaling
        
    Returns:
        Adaptive K-factor
    """
    # K-factor increases with gap and decreases with sample size
    gap_component = 1 + alpha * abs(gap)
    sample_component = 1 / (1 + beta * np.log(max(1, sample_size)))
    
    return gap_component * sample_component


def apply_performance_multiplier(perf: float, performance_k: float, decay_rate: float, 
                               recency_index: int) -> float:
    """
    Apply performance-based multiplier with recency decay.
    
    Args:
        perf: Performance value (actual - expected)
        performance_k: Base performance multiplier
        decay_rate: Decay rate for recency
        recency_index: Index of game (0 = most recent)
        
    Returns:
        Performance multiplier
    """
    if abs(perf) < 1:
        return 1.0
    
    # Apply performance multiplier
    multiplier = 1 + performance_k * np.sign(perf)
    
    # Apply recency decay
    decay_factor = np.exp(-decay_rate * recency_index)
    
    return multiplier * decay_factor


def compute_bayesian_shrinkage(value: float, sample_size: int, prior_mean: float, 
                              tau: float) -> float:
    """
    Apply Bayesian shrinkage toward prior mean.
    
    Args:
        value: Observed value
        sample_size: Number of observations
        prior_mean: Prior mean to shrink toward
        tau: Shrinkage parameter
        
    Returns:
        Shrunk value
    """
    if sample_size == 0:
        return prior_mean
    
    # Bayesian shrinkage: (n*value + tau*prior) / (n + tau)
    shrunk = (sample_size * value + tau * prior_mean) / (sample_size + tau)
    
    return shrunk


if __name__ == "__main__":
    # Test the statistical utilities
    print("Testing statistical utilities...")
    
    # Test robust_minmax
    test_data = pd.Series([1, 2, 3, 4, 5, 100])  # 100 is outlier
    normalized = robust_minmax(test_data)
    print(f"robust_minmax: {test_data.tolist()} -> {normalized.tolist()}")
    
    # Test tapered_weights
    weights = tapered_weights(10, 3, 0.7, {'tail_start': 8, 'tail_end': 10, 
                                           'tail_start_weight': 0.8, 'tail_end_weight': 0.4})
    print(f"tapered_weights: sum={weights.sum():.3f}, recent={weights[:3].sum():.3f}")
    
    # Test adaptive K
    k_factor = compute_adaptive_k(2.0, 10, 0.5, 0.6)
    print(f"adaptive_k: gap=2.0, sample=10 -> K={k_factor:.3f}")
    
    print("All tests passed!")
