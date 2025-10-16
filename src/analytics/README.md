# v53E Ranking Engine

This module implements the complete v53E methodology for ranking youth soccer teams based on game history data with sophisticated statistical adjustments.

## Overview

The v53E ranking engine processes game data through 12 distinct layers to produce comprehensive team rankings:

1. **Load & Filter**: Load games data and apply time window filtering
2. **Per-team Game Selection**: Select most recent games per team (up to MAX_GAMES_FOR_RANK)
3. **Recency Weights**: Apply tapered weights emphasizing recent games
4. **Raw Metrics**: Compute offensive and defensive metrics
5. **Opponent Strength Adjustments**: Scale metrics by opponent strength
6. **Adaptive K-factor**: Apply adaptive scaling based on strength gaps
7. **Performance Layer**: Adjust for over/under-performance vs expectations
8. **Bayesian Shrinkage**: Shrink toward league averages for stability
9. **SOS (Strength of Schedule)**: Compute and refine opponent strength
10. **Normalization**: Apply robust min-max scaling with logistic stretch
11. **PowerScore**: Combine components with configurable weights
12. **Ranking**: Sort and rank teams within each (state, gender, age_group)

## Files

### Core Components

- `ranking_engine.py`: Main ranking engine implementation
- `ranking_config.yaml`: Configuration parameters for the v53E methodology
- `utils_stats.py`: Statistical utility functions
- `sos_iterative.py`: Strength of Schedule iterative refinement
- `normalizer.py`: Game data normalization pipeline
- `ranking_tuner.py`: Parameter tuning harness
- `tuning_scenarios.yaml`: Tuning scenario definitions

### Data Flow

1. **Input**: Game data from build directories or normalized parquet files
2. **Processing**: Apply v53E methodology with configurable parameters
3. **Output**: Ranked CSV files, optional connectivity analysis, summary JSON

## Usage

### Basic Ranking

```bash
# Rank teams for Arizona, Male, U12
python -m src.analytics.ranking_engine --state AZ --genders M --ages U12

# Rank multiple age groups with connectivity analysis
python -m src.analytics.ranking_engine --state AZ --genders M,F --ages U10,U11,U12 --emit-connectivity
```

### Data Normalization

```bash
# Normalize games from build directories
python -m src.analytics.normalizer --states AZ,NV --genders M,F --ages U10,U11,U12
```

### Parameter Tuning

```bash
# Run parameter tuning scenarios
python -m src.analytics.ranking_tuner --state AZ --genders M --ages U12
```

## Configuration

The ranking engine is configured via `ranking_config.yaml` with parameters for:

- **Time Windows**: `WINDOW_DAYS`, `MAX_GAMES_FOR_RANK`, `INACTIVE_HIDE_DAYS`
- **Recency**: `RECENT_K`, `RECENT_SHARE`, tail dampening parameters
- **Performance**: `PERFORMANCE_K`, `PERFORMANCE_DECAY_RATE`
- **Adaptive Scaling**: `ADAPTIVE_K_ALPHA`, `ADAPTIVE_K_BETA`
- **Outlier Protection**: `OUTLIER_GUARD_ZSCORE`
- **Bayesian Shrinkage**: `SHRINK_TAU`
- **SOS**: `SOS_STRETCH_EXPONENT`
- **Final Scoring**: `OFF_WEIGHT`, `DEF_WEIGHT`, `SOS_WEIGHT`

## Output Format

### Rankings CSV

Columns in the output rankings:
- `rank`: Team rank within (state, gender, age_group)
- `team_id_master`: Canonical team identifier
- `team`: Team name
- `club`: Club name
- `state`, `gender`, `age_group`: Team demographics
- `powerscore_adj`: Final adjusted PowerScore
- `powerscore`: Base PowerScore before game count adjustment
- `sao_norm`: Normalized Strength-Adjusted Offensive metric
- `sad_norm`: Normalized Strength-Adjusted Defensive metric
- `sos_norm`: Normalized Strength of Schedule metric
- `gp_used`: Games played used for ranking
- `is_active`: Whether team is considered active
- `status`: "Active" or "Provisional"
- `last_game_date`: Date of most recent game
- `component_id`, `component_size`, `degree`: Connectivity metrics (if requested)

### Connectivity Analysis

When `--emit-connectivity` is used, additional CSV with network analysis:
- `component_id`: Connected component identifier
- `component_size`: Size of connected component
- `degree`: Number of unique opponents played

## Data Requirements

### Input Data Schema

The engine expects games data with these columns:
- `team_id_master`: Canonical team identifier
- `opponent_id_master`: Canonical opponent identifier
- `team`: Team name
- `opponent`: Opponent name
- `club`: Club name
- `state`: State code
- `gender`: Gender (M/F)
- `age_group`: Age group (U10, U11, etc.)
- `date`: Game date
- `gf`: Goals for
- `ga`: Goals against
- `competition`: Competition name (optional)

### Data Sources

1. **Normalized Parquet**: Preferred format in `data/games/normalized/`
2. **Raw Build Directories**: Fallback to `data/games/build_*/` directories
3. **Legacy Formats**: Automatic schema detection and mapping

## Tuning Harness

The tuning harness compares different parameter configurations against a baseline:

### Metrics Computed

- **Spearman Correlation**: Rank correlation between scenarios
- **Kendall Correlation**: Alternative rank correlation measure
- **Top-K Overlap**: Fraction of teams in common top-k rankings
- **Rank Deltas**: Median, 90th percentile, and maximum rank changes

### Scenarios Included

- `baseline`: Standard v53E parameters
- `sweep_window_days`: Shorter time window
- `sweep_max_games`: Fewer games per team
- `sweep_recent_emphasis`: Higher weight on recent games
- `sweep_performance_sensitivity`: Higher performance adjustments
- `sweep_adaptive_k`: More aggressive adaptive scaling
- `sweep_outlier_protection`: Stricter outlier clipping
- `sweep_bayesian_shrinkage`: More shrinkage toward priors
- `sweep_provisional_threshold`: Higher threshold for active status
- `sweep_sos_stretch`: Stronger SOS stretching
- `sweep_scoring_weights`: Different component weights
- `sweep_conservative`: Conservative parameter set
- `sweep_aggressive`: Aggressive parameter set

## Dependencies

- `pandas>=2.2.2`: Data manipulation
- `numpy`: Numerical computations
- `networkx>=3.0`: Graph analysis for connectivity
- `scipy>=1.11.0`: Statistical functions
- `pyyaml>=6.0`: Configuration management

## Implementation Notes

- **Vectorized Operations**: Uses pandas vectorized operations for performance
- **Memory Efficient**: Processes teams individually to manage memory usage
- **Robust Error Handling**: Graceful handling of missing data and edge cases
- **Configurable**: All parameters externalized to YAML configuration
- **Testable**: Pure functions enable comprehensive testing
- **Extensible**: Modular design allows easy addition of new metrics or adjustments
