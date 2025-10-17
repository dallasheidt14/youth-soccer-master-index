#!/usr/bin/env python3
"""
End-to-End Pipeline Smoke Tests

Tests critical pipeline components to catch regressions before merge.
"""

import pytest
import pandas as pd
from pathlib import Path
import subprocess
import sys
import os


def test_registry_health():
    """Test that registry is healthy and accessible."""
    from src.registry.registry import get_registry
    
    registry = get_registry()
    stats = registry.get_registry_stats()
    
    assert stats['registry_health'] >= 70, f"Registry health too low: {stats['registry_health']}%"
    assert stats['total_slices'] > 0, "No slices in registry"
    assert isinstance(stats['registry_version'], str), "Registry version should be a string"


def test_registry_version_compatibility():
    """Test that registry version is compatible."""
    from src.registry.registry import get_registry
    
    registry = get_registry()
    version_info = registry.check_version_compatibility()
    
    assert version_info['is_compatible'], f"Registry version incompatible: {version_info['registry_version']}"
    assert version_info['current_code_version'] == "v2.0.0", "Code version should be v2.0.0"


def test_scraper_dry_run():
    """Test that scraper runs without errors in dry-run mode."""
    result = subprocess.run(
        ['python', '-m', 'src.scraper.build_game_history',
         '--states', 'AZ', '--genders', 'M', '--ages', 'U10',
         '--max-teams', '5', '--dry-run'],
        capture_output=True,
        text=True,
        timeout=60
    )
    
    assert result.returncode == 0, f"Scraper failed: {result.stderr}"


def test_linker_works():
    """Test that linker can process games and find master index."""
    from src.linkers.game_master_linker import latest_master_index
    
    try:
        master_file = latest_master_index()
        assert master_file.exists(), "Master index file not found"
    except FileNotFoundError:
        # This is OK if no master index exists yet
        pytest.skip("No master index file found (expected in fresh environment)")


def test_normalizer_outputs():
    """Test that normalizer produces valid outputs."""
    normalized_dir = Path("data/games/normalized")
    
    if normalized_dir.exists():
        parquet_files = list(normalized_dir.glob("*.parquet"))
        assert len(parquet_files) > 0, "No normalized parquet files found"
        
        # Test that we can read at least one parquet file
        if parquet_files:
            df = pd.read_parquet(parquet_files[0])
            assert len(df) >= 0, "Parquet file should be readable"
    else:
        pytest.skip("No normalized data directory found (expected in fresh environment)")


def test_ranking_engine_smoke():
    """Test that ranking engine can load config and basic functions work."""
    import yaml
    
    config_path = Path("src/analytics/ranking_config.yaml")
    assert config_path.exists(), "Ranking config not found"
    
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    assert 'WINDOW_DAYS' in config, "Config missing WINDOW_DAYS"
    assert 'RECENT_K' in config, "Config missing RECENT_K"
    assert isinstance(config['WINDOW_DAYS'], int), "WINDOW_DAYS should be integer"
    assert isinstance(config['RECENT_K'], int), "RECENT_K should be integer"


def test_identity_map_exists():
    """Test that identity map is loadable."""
    from src.identity.identity_sync import _load
    
    identity_map = _load()
    assert isinstance(identity_map, dict), "Identity map should be a dict"


def test_identity_audit_imports():
    """Test that identity audit module can be imported and basic functions work."""
    from src.identity.identity_audit import audit_identity_map, get_weekly_review_summary
    
    # Test that functions exist and are callable
    assert callable(audit_identity_map), "audit_identity_map should be callable"
    assert callable(get_weekly_review_summary), "get_weekly_review_summary should be callable"
    
    # Test audit function with empty data (should not crash)
    audit_df = audit_identity_map(threshold=0.85)
    assert isinstance(audit_df, pd.DataFrame), "audit_identity_map should return DataFrame"


def test_game_hash_checker_imports():
    """Test that game hash checker module can be imported."""
    from src.scraper.utils.game_hash_checker import generate_game_hash, check_game_integrity
    
    assert callable(generate_game_hash), "generate_game_hash should be callable"
    assert callable(check_game_integrity), "check_game_integrity should be callable"


def test_pipeline_orchestrator_help():
    """Test that pipeline orchestrator shows help without errors."""
    result = subprocess.run(
        ['python', 'scripts/pipeline_runner.py', '--help'],
        capture_output=True,
        text=True,
        timeout=30
    )
    
    assert result.returncode == 0, f"Pipeline runner help failed: {result.stderr}"
    assert '--states' in result.stdout, "Help should mention --states argument"


def test_pipeline_orchestrator_dry_run():
    """Test that pipeline orchestrator dry run works."""
    result = subprocess.run(
        ['python', 'scripts/pipeline_runner.py', 
         '--states', 'AZ', '--genders', 'M', '--ages', 'U10', '--dry-run'],
        capture_output=True,
        text=True,
        timeout=60
    )
    
    assert result.returncode == 0, f"Pipeline runner dry run failed: {result.stderr}"
    assert 'Pipeline completed successfully' in result.stdout, "Should show success message"


def test_data_directories_exist():
    """Test that required data directories exist or can be created."""
    required_dirs = [
        "data",
        "data/registry", 
        "data/games",
        "data/master",
        "data/logs"
    ]
    
    for dir_path in required_dirs:
        path = Path(dir_path)
        if not path.exists():
            # Try to create it
            path.mkdir(parents=True, exist_ok=True)
        
        assert path.exists(), f"Required directory {dir_path} should exist"


def test_schema_files_exist():
    """Test that required schema files exist."""
    schema_files = [
        "src/schema/master_team_schema.py",
        "src/schema/game_history_schema.py"
    ]
    
    for schema_file in schema_files:
        assert Path(schema_file).exists(), f"Schema file {schema_file} should exist"


def test_config_files_exist():
    """Test that required config files exist."""
    config_files = [
        "src/analytics/ranking_config.yaml"
    ]
    
    for config_file in config_files:
        assert Path(config_file).exists(), f"Config file {config_file} should exist"


def test_imports_work():
    """Test that critical modules can be imported without errors."""
    import_modules = [
        "src.registry.registry",
        "src.scraper.build_game_history", 
        "src.linkers.game_master_linker",
        "src.analytics.normalizer",
        "src.analytics.ranking_engine",
        "src.identity.identity_sync"
    ]
    
    for module_name in import_modules:
        try:
            __import__(module_name)
        except ImportError as e:
            pytest.fail(f"Failed to import {module_name}: {e}")


def test_logging_setup():
    """Test that logging can be configured."""
    import logging
    
    # Test basic logging setup
    logger = logging.getLogger("test_logger")
    logger.setLevel(logging.INFO)
    
    # Should not raise any exceptions
    logger.info("Test log message")
    
    assert logger.level == logging.INFO, "Logger level should be set correctly"


if __name__ == "__main__":
    # Allow running tests directly
    pytest.main([__file__, "-v"])
