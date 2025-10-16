#!/usr/bin/env python3
"""
Ranking Engine Parameter Tuning Harness

Compares different parameter configurations against a baseline to evaluate
ranking stability and parameter sensitivity.
"""

import pandas as pd
import numpy as np
from pathlib import Path
from typing import List, Dict, Any, Tuple
import logging
import argparse
import yaml
import json
from scipy.stats import spearmanr, kendalltau

from src.analytics.ranking_engine import run_ranking

logger = logging.getLogger(__name__)


def load_yaml(path: Path) -> Dict[str, Any]:
    """
    Load YAML configuration file.
    
    Args:
        path: Path to YAML file
        
    Returns:
        Configuration dictionary
    """
    with open(path, 'r') as f:
        return yaml.safe_load(f)


def apply_overrides(base_cfg: Dict[str, Any], overrides: Dict[str, Any]) -> Dict[str, Any]:
    """
    Apply parameter overrides to base configuration.
    
    Args:
        base_cfg: Base configuration dictionary
        overrides: Override parameters
        
    Returns:
        New configuration with overrides applied
    """
    config = base_cfg.copy()
    config.update(overrides)
    return config


def compare_rankings(df_base: pd.DataFrame, df_test: pd.DataFrame) -> Dict[str, float]:
    """
    Compare two ranking DataFrames and compute stability metrics.
    
    Args:
        df_base: Baseline rankings DataFrame
        df_test: Test rankings DataFrame
        
    Returns:
        Dictionary of comparison metrics
    """
    if df_base.empty or df_test.empty:
        return {
            'spearman_correlation': 0.0,
            'kendall_correlation': 0.0,
            'top5_overlap': 0.0,
            'top10_overlap': 0.0,
            'top20_overlap': 0.0,
            'median_rank_delta': 0.0,
            'p90_rank_delta': 0.0,
            'max_rank_delta': 0.0
        }
    
    # Align teams on (state, gender, age_group, team_id_master)
    merge_cols = ['state', 'gender', 'age_group', 'team_id_master']
    merged = pd.merge(
        df_base[merge_cols + ['rank']], 
        df_test[merge_cols + ['rank']], 
        on=merge_cols, 
        suffixes=('_base', '_test')
    )
    
    if merged.empty:
        return {
            'spearman_correlation': 0.0,
            'kendall_correlation': 0.0,
            'top5_overlap': 0.0,
            'top10_overlap': 0.0,
            'top20_overlap': 0.0,
            'median_rank_delta': 0.0,
            'p90_rank_delta': 0.0,
            'max_rank_delta': 0.0
        }
    
    # Compute rank correlations
    spearman_corr, _ = spearmanr(merged['rank_base'], merged['rank_test'])
    kendall_corr, _ = kendalltau(merged['rank_base'], merged['rank_test'])
    
    # Compute top-k overlaps
    def top_k_overlap(base_ranks, test_ranks, k):
        base_top_k = set(base_ranks.nsmallest(k).index)
        test_top_k = set(test_ranks.nsmallest(k).index)
        return len(base_top_k.intersection(test_top_k)) / k
    
    base_ranks = pd.Series(merged['rank_base'].values, index=merged.index)
    test_ranks = pd.Series(merged['rank_test'].values, index=merged.index)
    
    top5_overlap = top_k_overlap(base_ranks, test_ranks, min(5, len(merged)))
    top10_overlap = top_k_overlap(base_ranks, test_ranks, min(10, len(merged)))
    top20_overlap = top_k_overlap(base_ranks, test_ranks, min(20, len(merged)))
    
    # Compute rank deltas
    rank_deltas = np.abs(merged['rank_test'] - merged['rank_base'])
    median_delta = rank_deltas.median()
    p90_delta = rank_deltas.quantile(0.9)
    max_delta = rank_deltas.max()
    
    return {
        'spearman_correlation': spearman_corr,
        'kendall_correlation': kendall_corr,
        'top5_overlap': top5_overlap,
        'top10_overlap': top10_overlap,
        'top20_overlap': top20_overlap,
        'median_rank_delta': median_delta,
        'p90_rank_delta': p90_delta,
        'max_rank_delta': max_delta,
        'teams_compared': len(merged)
    }


def save_report(name: str, base_name: str, metrics: Dict[str, float], 
               df_base: pd.DataFrame, df_test: pd.DataFrame, output_dir: Path) -> None:
    """
    Save tuning report with metrics and rank deltas.
    
    Args:
        name: Scenario name
        base_name: Baseline scenario name
        metrics: Comparison metrics
        df_base: Baseline rankings
        df_test: Test rankings
        output_dir: Output directory
    """
    scenario_dir = output_dir / name
    scenario_dir.mkdir(parents=True, exist_ok=True)
    
    # Save metrics JSON
    metrics_file = scenario_dir / "metrics.json"
    with open(metrics_file, 'w') as f:
        json.dump(metrics, f, indent=2)
    
    # Save rank deltas CSV
    if not df_base.empty and not df_test.empty:
        merge_cols = ['state', 'gender', 'age_group', 'team_id_master']
        merged = pd.merge(
            df_base[merge_cols + ['rank', 'team', 'powerscore_adj']], 
            df_test[merge_cols + ['rank', 'team', 'powerscore_adj']], 
            on=merge_cols, 
            suffixes=('_base', '_test')
        )
        
        merged['rank_delta'] = merged['rank_test'] - merged['rank_base']
        merged['abs_rank_delta'] = np.abs(merged['rank_delta'])
        merged['powerscore_delta'] = merged['powerscore_adj_test'] - merged['powerscore_adj_base']
        
        deltas_file = scenario_dir / "rank_deltas.csv"
        merged.to_csv(deltas_file, index=False)
        
        logger.info(f"Saved {len(merged)} team comparisons to {deltas_file}")
    
    logger.info(f"Saved tuning report for {name} to {scenario_dir}")


def main():
    """CLI entry point for the ranking tuner."""
    parser = argparse.ArgumentParser(description="v53E Ranking Engine Parameter Tuner")
    parser.add_argument("--input-root", type=str, default="data",
                       help="Root directory for input data")
    parser.add_argument("--state", type=str, required=True,
                       help="State to rank")
    parser.add_argument("--genders", type=str, default="M,F",
                       help="Comma-separated genders")
    parser.add_argument("--ages", type=str, default="U10,U11,U12,U13,U14,U15,U16,U17,U18,U19",
                       help="Comma-separated age groups")
    parser.add_argument("--output-root", type=str, default="data/rankings/tuning",
                       help="Output directory for tuning results")
    parser.add_argument("--provider", type=str, default="gotsport",
                       help="Data provider name")
    parser.add_argument("--scenarios", type=str, default="src/analytics/tuning_scenarios.yaml",
                       help="Tuning scenarios configuration file")
    parser.add_argument("--config", type=str, default="src/analytics/ranking_config.yaml",
                       help="Base configuration file")
    
    args = parser.parse_args()
    
    # Parse comma-separated lists
    genders = [g.strip() for g in args.genders.split(',')]
    ages = [a.strip() for a in args.ages.split(',')]
    
    # Load configurations
    base_config = load_yaml(Path(args.config))
    scenarios = load_yaml(Path(args.scenarios))
    
    # Set up logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    # Create output directory
    output_dir = Path(args.output_root)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    try:
        # Run baseline ranking
        logger.info("Running baseline ranking...")
        baseline_config = apply_overrides(base_config, scenarios['baseline'])
        df_baseline = run_ranking(
            args.state, genders, ages, baseline_config,
            args.input_root, args.output_root, args.provider,
            emit_connectivity=False
        )
        
        if df_baseline.empty:
            logger.error("Baseline ranking produced no results")
            return
        
        logger.info(f"Baseline ranking complete: {len(df_baseline)} teams")
        
        # Run each scenario
        scenario_results = {}
        
        for scenario_name, scenario_overrides in scenarios.items():
            if scenario_name == 'baseline':
                continue
                
            logger.info(f"Running scenario: {scenario_name}")
            
            try:
                # Apply scenario overrides
                scenario_config = apply_overrides(base_config, scenario_overrides)
                
                # Run ranking
                df_scenario = run_ranking(
                    args.state, genders, ages, scenario_config,
                    args.input_root, args.output_root, args.provider,
                    emit_connectivity=False
                )
                
                if df_scenario.empty:
                    logger.warning(f"Scenario {scenario_name} produced no results")
                    continue
                
                # Compare to baseline
                metrics = compare_rankings(df_baseline, df_scenario)
                
                # Save report
                save_report(scenario_name, 'baseline', metrics, 
                           df_baseline, df_scenario, output_dir)
                
                scenario_results[scenario_name] = metrics
                
                logger.info(f"Scenario {scenario_name} complete:")
                logger.info(f"  Spearman correlation: {metrics['spearman_correlation']:.3f}")
                logger.info(f"  Top-10 overlap: {metrics['top10_overlap']:.3f}")
                logger.info(f"  Median rank delta: {metrics['median_rank_delta']:.1f}")
                
            except Exception as e:
                logger.error(f"Scenario {scenario_name} failed: {e}")
                continue
        
        # Save overall summary
        summary = {
            'timestamp': pd.Timestamp.now().isoformat(),
            'state': args.state,
            'genders': genders,
            'ages': ages,
            'provider': args.provider,
            'baseline_teams': len(df_baseline),
            'scenarios_run': len(scenario_results),
            'scenario_results': scenario_results
        }
        
        summary_file = output_dir / "tuning_summary.json"
        with open(summary_file, 'w') as f:
            json.dump(summary, f, indent=2, default=str)
        
        logger.info(f"Tuning complete! Results saved to {output_dir}")
        logger.info(f"Summary saved to {summary_file}")
        
        # Print summary table
        print("\n" + "="*80)
        print("TUNING RESULTS SUMMARY")
        print("="*80)
        print(f"{'Scenario':<20} {'Spearman':<10} {'Kendall':<10} {'Top-10':<10} {'Med Δ':<10}")
        print("-"*80)
        
        for name, metrics in scenario_results.items():
            print(f"{name:<20} {metrics['spearman_correlation']:<10.3f} "
                  f"{metrics['kendall_correlation']:<10.3f} "
                  f"{metrics['top10_overlap']:<10.3f} "
                  f"{metrics['median_rank_delta']:<10.1f}")
        
        print("="*80)
        
    except Exception as e:
        logger.error(f"Tuning failed: {e}")
        raise


if __name__ == "__main__":
    main()
