#!/usr/bin/env python3
"""
Pipeline Runner - Weekly Data Refresh Orchestrator

Automates the complete pipeline:
1. Scraper (--incremental --auto)
2. Linker (--relink-latest)
3. Normalizer (optional --refresh)
4. Ranking Engine (default config)
5. Tuner (optional)
"""

import subprocess
import logging
import sys
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any
import argparse

# Global logger instances
logger = logging.getLogger(__name__)
error_logger = logging.getLogger('pipeline_errors')


def setup_logging():
    """Configure dual logging (info + errors)"""
    log_dir = Path("data/logs")
    log_dir.mkdir(parents=True, exist_ok=True)
    
    # Main log file
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('data/logs/pipeline_runner.log'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    # Error log file
    error_handler = logging.FileHandler('data/logs/pipeline_errors.log')
    error_handler.setLevel(logging.ERROR)
    error_logger.addHandler(error_handler)


def execute_step(step_name: str, cmd: List[str], dry_run: bool = False) -> bool:
    """
    Execute a pipeline step with fail-fast error handling.
    
    Args:
        step_name: Name of the pipeline step for logging
        cmd: Command to execute as list of strings
        dry_run: If True, only print command without executing
        
    Returns:
        True if successful, False on error (stops pipeline)
    """
    logger.info(f"[START] {step_name}")
    logger.info(f"Command: {' '.join(cmd)}")
    
    if dry_run:
        logger.info(f"[DRY RUN] Would execute: {' '.join(cmd)}")
        return True
    
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=True
        )
        logger.info(f"[SUCCESS] {step_name}")
        logger.debug(f"Output: {result.stdout}")
        return True
        
    except subprocess.CalledProcessError as e:
        logger.error(f"[FAILED] {step_name}")
        logger.error(f"Exit code: {e.returncode}")
        logger.error(f"Error output: {e.stderr}")
        
        # Write detailed error to error log
        error_logger.error(f"""
=== Pipeline Failure ===
Timestamp: {datetime.now().isoformat()}
Step: {step_name}
Command: {' '.join(cmd)}
Exit Code: {e.returncode}
Error Output:
{e.stderr}
========================
""")
        return False


def run_scraper(states: List[str], genders: List[str], ages: List[str], dry_run: bool = False) -> bool:
    """Run game history scraper with --incremental --auto flags"""
    cmd = [
        'python', '-m', 'src.scraper.build_game_history',
        '--providers', 'gotsport',
        '--states', ','.join(states),
        '--genders', ','.join(genders),
        '--ages', ','.join(ages),
        '--incremental',
        '--auto'
    ]
    return execute_step('Scraper', cmd, dry_run)


def run_linker(dry_run: bool = False) -> bool:
    """Link games to master index with --relink-latest"""
    cmd = [
        'python', '-m', 'src.linkers.game_master_linker',
        '--relink-latest'
    ]
    return execute_step('Linker', cmd, dry_run)


def run_normalizer(states: List[str], genders: List[str], ages: List[str], refresh: bool, dry_run: bool = False) -> bool:
    """Run normalizer with per-slice build awareness"""
    from src.registry.registry import get_registry
    
    # Show build status before normalizing
    registry = get_registry()
    logger.info("Per-slice build status:")
    for state in states:
        for gender in genders:
            for age in ages:
                slice_key = f"{state}_{gender}_{age}"
                build = registry.get_latest_build(slice_key)
                logger.info(f"  {slice_key}: {build or 'NOT FOUND'}")
    
    cmd = [
        'python', '-m', 'src.analytics.normalizer',
        '--states', ','.join(states),
        '--genders', ','.join(genders),
        '--ages', ','.join(ages)
    ]
    if refresh:
        cmd.append('--refresh')
    return execute_step('Normalizer', cmd, dry_run)


def run_ranking_engine(states: List[str], genders: List[str], ages: List[str], dry_run: bool = False) -> bool:
    """Run ranking engine for each state"""
    for state in states:
        cmd = [
            'python', '-m', 'src.analytics.ranking_engine',
            '--state', state,
            '--genders', ','.join(genders),
            '--ages', ','.join(ages),
            '--config', 'src/analytics/ranking_config.yaml'
        ]
        if not execute_step(f'Ranking Engine ({state})', cmd, dry_run):
            return False
    return True


def run_tuner(states: List[str], genders: List[str], ages: List[str], dry_run: bool = False) -> bool:
    """Run parameter tuner (optional)"""
    for state in states:
        cmd = [
            'python', '-m', 'src.analytics.ranking_tuner',
            '--state', state,
            '--genders', ','.join(genders),
            '--ages', ','.join(ages)
        ]
        if not execute_step(f'Tuner ({state})', cmd, dry_run):
            return False
    return True


def check_registry_health() -> bool:
    """Check registry health before starting pipeline."""
    try:
        import sys
        from pathlib import Path
        
        # Add project root to path for imports
        project_root = Path(__file__).resolve().parents[1]
        if str(project_root) not in sys.path:
            sys.path.insert(0, str(project_root))
        
        from src.registry.registry import get_registry
        from src.utils.notifier import notify_registry_health
        
        registry = get_registry()
        stats = registry.get_registry_stats()
        
        logger.info(f"Registry Health: {stats['registry_health']}%")
        logger.info(f"Total Slices: {stats['total_slices']}")
        
        if stats['stale_slices']:
            logger.warning(f"Stale slices detected: {', '.join(stats['stale_slices'][:5])}")
        
        # Send Slack notification about registry health
        try:
            notify_registry_health(stats)
        except Exception as e:
            logger.warning(f"Failed to send Slack notification: {e}")
        
        if stats['registry_health'] < 70:
            logger.error(f"Registry health too low: {stats['registry_health']}%")
            return False
        
        return True
    except Exception as e:
        logger.warning(f"Could not check registry health: {e}")
        return True  # Don't fail pipeline on health check error


def main():
    """Main orchestrator function"""
    parser = argparse.ArgumentParser(description="Pipeline Runner - Weekly Data Refresh Orchestrator")
    parser.add_argument('--states', required=True, help='Comma-separated state codes (e.g., AZ,NV)')
    parser.add_argument('--genders', default='M,F', help='Comma-separated genders (default: M,F)')
    parser.add_argument('--ages', default='U10', help='Comma-separated age groups (default: U10)')
    parser.add_argument('--refresh-normalized', action='store_true', help='Rebuild normalized data from all builds')
    parser.add_argument('--with-tuner', action='store_true', help='Run parameter tuner after ranking')
    parser.add_argument('--dry-run', action='store_true', help='Print commands without executing')
    
    args = parser.parse_args()
    
    # Parse comma-separated lists
    states = [s.strip() for s in args.states.split(',')]
    genders = [g.strip() for g in args.genders.split(',')]
    ages = [a.strip() for a in args.ages.split(',')]
    
    # Setup logging
    setup_logging()
    
    logger.info("=" * 60)
    logger.info("Starting Pipeline Orchestrator")
    logger.info(f"States: {states}")
    logger.info(f"Genders: {genders}")
    logger.info(f"Ages: {ages}")
    logger.info(f"Refresh Normalized: {args.refresh_normalized}")
    logger.info(f"With Tuner: {args.with_tuner}")
    logger.info(f"Dry Run: {args.dry_run}")
    logger.info("=" * 60)
    
    start_time = datetime.now()
    
    # Send pipeline start notification
    try:
        import sys
        from pathlib import Path
        
        # Add project root to path for imports
        project_root = Path(__file__).resolve().parents[1]
        if str(project_root) not in sys.path:
            sys.path.insert(0, str(project_root))
        
        from src.utils.notifier import notify_pipeline_start
        
        if not args.dry_run:
            notify_pipeline_start(states, genders, ages)
    except Exception as e:
        logger.warning(f"Failed to send pipeline start notification: {e}")
    
    # Check registry health
    if not check_registry_health():
        logger.error("Registry health check failed")
        sys.exit(1)
    
    # Run pipeline steps (fail-fast)
    if not run_scraper(states, genders, ages, args.dry_run):
        logger.error("Pipeline failed at Scraper step")
        sys.exit(1)
    
    if not run_linker(args.dry_run):
        logger.error("Pipeline failed at Linker step")
        sys.exit(1)
    
    if not run_normalizer(states, genders, ages, args.refresh_normalized, args.dry_run):
        logger.error("Pipeline failed at Normalizer step")
        sys.exit(1)
    
    if not run_ranking_engine(states, genders, ages, args.dry_run):
        logger.error("Pipeline failed at Ranking Engine step")
        sys.exit(1)
    
    if args.with_tuner:
        if not run_tuner(states, genders, ages, args.dry_run):
            logger.error("Pipeline failed at Tuner step")
            sys.exit(1)
    
    # Success
    duration = datetime.now() - start_time
    logger.info("=" * 60)
    logger.info(f"Pipeline completed successfully in {duration}")
    logger.info("=" * 60)
    
    # Send pipeline completion notification
    try:
        import sys
        from pathlib import Path
        
        # Add project root to path for imports
        project_root = Path(__file__).resolve().parents[1]
        if str(project_root) not in sys.path:
            sys.path.insert(0, str(project_root))
        
        from src.utils.notifier import notify_pipeline_complete
        
        if not args.dry_run:
            total_slices = len(states) * len(genders) * len(ages)
            notify_pipeline_complete(total_slices, total_slices)  # All successful
    except Exception as e:
        logger.warning(f"Failed to send pipeline completion notification: {e}")


if __name__ == "__main__":
    main()
