#!/usr/bin/env python3
"""
Build Registry - Tracks latest build directories for each slice.

This module provides functionality to track and manage the latest build directory
for each slice (state x gender x age_group), enabling automatic discovery for
incremental scraping and eliminating manual folder management.

Author: Youth Soccer Master Index System
"""

import json
import logging
import os
import time
import uuid
import sys
from pathlib import Path
from typing import Dict, Optional, Any
from datetime import datetime, timezone

from src.utils.json_safety import serialize_paths


def load_registry() -> Dict[str, Any]:
    """
    Load the build registry from JSON file.
    
    Returns:
        Dictionary with registry data, or empty dict if file doesn't exist
    """
    registry_path = Path("data/registry/build_registry.json")
    registry_path.parent.mkdir(parents=True, exist_ok=True)
    
    if not registry_path.exists():
        return {}
    
    try:
        with open(registry_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError) as e:
        logging.getLogger(__name__).warning(f"Could not load build registry: {e}")
        return {}


def save_registry(registry: Dict[str, Any]) -> None:
    """
    Save the build registry to JSON file using atomic write pattern.
    
    Args:
        registry: Dictionary with registry data to save
        
    Raises:
        RuntimeError: If write operation fails after retries
    """
    registry_path = Path("data/registry/build_registry.json")
    registry_path.parent.mkdir(parents=True, exist_ok=True)
    
    logger = logging.getLogger(__name__)
    
    # Create unique temporary file in same directory with process ID and timestamp
    temp_suffix = f".tmp.{os.getpid()}.{int(time.time() * 1000000)}.{uuid.uuid4().hex[:8]}"
    temp_path = registry_path.with_suffix(temp_suffix)
    
    # Ensure temp file doesn't already exist with max retry counter
    MAX_TEMP_RETRIES = 10
    retry_count = 0
    while temp_path.exists():
        retry_count += 1
        if retry_count > MAX_TEMP_RETRIES:
            raise RuntimeError(f"Temp file collision after {MAX_TEMP_RETRIES} attempts, attempted temp_path: {temp_path}")
        temp_suffix = f".tmp.{os.getpid()}.{int(time.time() * 1000000)}.{uuid.uuid4().hex[:8]}"
        temp_path = registry_path.with_suffix(temp_suffix)
    
    try:
        logger.debug(f"Writing registry to temporary file: {temp_path}")
        
        # Write to temporary file
        cleaned_data = serialize_paths(registry)
        
        with open(temp_path, 'w', encoding='utf-8') as f:
            json.dump(cleaned_data, f, indent=2, ensure_ascii=False)
        
        # Atomically rename to final destination with retry logic
        MAX_RENAME_RETRIES = 3
        rename_retry = 0
        while rename_retry < MAX_RENAME_RETRIES:
            try:
                temp_path.replace(registry_path)
                break
            except OSError as e:
                rename_retry += 1
                if rename_retry >= MAX_RENAME_RETRIES:
                    logger.error(f"Failed to rename {temp_path} to {registry_path} after {MAX_RENAME_RETRIES} attempts: {e}")
                    raise
                logger.warning(f"Rename attempt {rename_retry} failed: {e}, retrying...")
                time.sleep(0.1 * rename_retry)  # Exponential backoff
        
        logger.debug(f"Successfully wrote registry: {registry_path}")
        
    except Exception as e:
        # Clean up temporary file on error
        if temp_path.exists():
            temp_path.unlink()
        logger.error(f"Failed to write registry to {registry_path}: {e}")
        raise


def update_registry(slice_key: str, build_dir: str) -> None:
    """
    Add or update the entry for a slice in the registry.
    
    Args:
        slice_key: Slice identifier (e.g., 'AZ_M_U10')
        build_dir: Build directory name (e.g., 'build_20251015_1348')
    """
    registry = load_registry()
    
    registry[slice_key] = {
        "latest_build": build_dir,
        "last_updated": datetime.now(timezone.utc).isoformat()
    }
    
    save_registry(registry)
    
    logger = logging.getLogger(__name__)
    logger.info(f"Updated build registry for {slice_key}: {build_dir}")


def get_latest_build(slice_key: str) -> Optional[str]:
    """
    Return the path to the latest build folder for a given slice.
    
    If not found in registry, auto-detect most recent build matching pattern
    and add to registry.
    
    Args:
        slice_key: Slice identifier (e.g., 'AZ_M_U10')
        
    Returns:
        Build directory name if found, None otherwise
    """
    registry = load_registry()
    
    # Check if slice exists in registry
    if slice_key in registry:
        return registry[slice_key]["latest_build"]
    
    # Auto-detect by scanning build folders
    logger = logging.getLogger(__name__)
    logger.info(f"Slice {slice_key} not in registry, auto-detecting...")
    
    games_dir = Path("data/games")
    if not games_dir.exists():
        return None
    
    # Find all build directories
    build_dirs = []
    for build_dir in games_dir.glob("build_*"):
        if not build_dir.is_dir():
            continue
            
        # Look for matching CSV file
        csv_pattern = f"games_gotsport_{slice_key}.csv"
        csv_file = build_dir / csv_pattern
        
        if csv_file.exists():
            # Extract timestamp from build directory name
            try:
                # Parse build_YYYYMMDD_HHMM format
                timestamp_str = build_dir.name.replace("build_", "")
                timestamp = datetime.strptime(timestamp_str, "%Y%m%d_%H%M")
                build_dirs.append((timestamp, build_dir.name))
            except ValueError:
                logger.warning(f"Could not parse timestamp from {build_dir.name}")
                continue
    
    if not build_dirs:
        logger.info(f"No existing builds found for {slice_key}")
        return None
    
    # Sort by timestamp (most recent first)
    build_dirs.sort(key=lambda x: x[0], reverse=True)
    latest_build = build_dirs[0][1]
    
    # Auto-add to registry
    logger.info(f"Auto-detected latest build for {slice_key}: {latest_build}")
    update_registry(slice_key, latest_build)
    
    return latest_build


def list_all_builds() -> Dict[str, Any]:
    """
    Return dictionary of all slice entries and build timestamps.
    
    Returns:
        Dictionary with all registry entries
    """
    return load_registry()


def refresh_registry() -> None:
    """
    Force rebuild of registry by scanning existing build folders.
    """
    logger = logging.getLogger(__name__)
    logger.info("Refreshing build registry by scanning existing folders...")
    
    registry = {}
    games_dir = Path("data/games")
    
    if not games_dir.exists():
        logger.warning("Games directory does not exist")
        save_registry(registry)
        return
    
    # Scan all build directories
    for build_dir in sorted(games_dir.glob("build_*")):
        if not build_dir.is_dir():
            continue
            
        # Look for all CSV files in this build directory
        for csv_file in build_dir.glob("games_gotsport_*.csv"):
            # Extract slice_key: AZ_M_U10 from games_gotsport_AZ_M_U10.csv
            parts = csv_file.stem.split("_")
            if len(parts) >= 3:
                slice_key = "_".join(parts[2:])  # Skip 'games' and 'gotsport'
                
                # Extract timestamp from build directory name
                try:
                    timestamp_str = build_dir.name.replace("build_", "")
                    timestamp = datetime.strptime(timestamp_str, "%Y%m%d_%H%M")
                    
                    # Only keep the latest build for each slice
                    if slice_key not in registry:
                        registry[slice_key] = {
                            "latest_build": build_dir.name,
                            "last_updated": datetime.now(timezone.utc).isoformat()
                        }
                    else:
                        # Safely parse the stored latest build name
                        latest_name = registry[slice_key].get("latest_build")
                        if latest_name:
                            try:
                                latest_timestamp_str = latest_name.replace("build_", "")
                                latest_timestamp = datetime.strptime(latest_timestamp_str, "%Y%m%d_%H%M")
                                if timestamp > latest_timestamp:
                                    registry[slice_key] = {
                                        "latest_build": build_dir.name,
                                        "last_updated": datetime.now(timezone.utc).isoformat()
                                    }
                            except ValueError:
                                logger.warning(f"Could not parse timestamp from stored build name '{latest_name}', replacing with current build '{build_dir.name}'")
                                registry[slice_key] = {
                                    "latest_build": build_dir.name,
                                    "last_updated": datetime.now(timezone.utc).isoformat()
                                }
                        else:
                            # No stored build name, use current one
                            registry[slice_key] = {
                                "latest_build": build_dir.name,
                                "last_updated": datetime.now(timezone.utc).isoformat()
                            }
                except ValueError:
                    logger.warning(f"Could not parse timestamp from {build_dir.name}")
                    continue
    
    save_registry(registry)
    logger.info(f"Registry refreshed with {len(registry)} slices")


if __name__ == "__main__":
    # Test the build registry functions
    import argparse
    
    parser = argparse.ArgumentParser(description="Build Registry Test")
    parser.add_argument("--refresh", action="store_true", help="Refresh registry from folders")
    parser.add_argument("--show", action="store_true", help="Show current registry")
    parser.add_argument("--test-slice", type=str, help="Test get_latest_build for a slice")
    
    args = parser.parse_args()
    
    # Set up logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    if args.refresh:
        refresh_registry()
    elif args.show:
        registry = list_all_builds()
        print(f"\nBuild Registry ({len(registry)} slices):")
        for slice_key, info in sorted(registry.items()):
            print(f"  {slice_key}: {info['latest_build']} (updated: {info['last_updated']})")
    elif args.test_slice:
        latest = get_latest_build(args.test_slice)
        print(f"Latest build for {args.test_slice}: {latest}")
    else:
        print("Use --help for available options")
