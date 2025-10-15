#!/usr/bin/env python3
"""
Team Identity Synchronization Module

Automatically maintains team_identity_map.json during incremental scraping runs,
tracking provider IDs, team name aliases, and club affiliation history.
"""

import json
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, Any, Optional
import re

IDENTITY_PATH = Path("data/master/team_identity_map.json")


def _load() -> Dict[str, Any]:
    """
    Load existing identity map from JSON with error handling.
    
    Returns:
        Dictionary containing team identity data, or empty dict if file doesn't exist
    """
    if not IDENTITY_PATH.exists():
        return {}
    
    try:
        with open(IDENTITY_PATH, 'r', encoding='utf-8') as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError) as e:
        logger = logging.getLogger(__name__)
        logger.warning(f"Failed to load identity map: {e}, starting with empty map")
        return {}


def _save(data: Dict[str, Any]) -> None:
    """
    Save identity map with atomic write using temp file + replace pattern.
    
    Args:
        data: Dictionary containing team identity data to save
    """
    logger = logging.getLogger(__name__)
    
    # Ensure directory exists
    IDENTITY_PATH.parent.mkdir(parents=True, exist_ok=True)
    
    # Create temporary file
    temp_path = IDENTITY_PATH.with_suffix('.tmp')
    
    try:
        # Write to temporary file
        with open(temp_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        # Atomically replace the original file
        temp_path.replace(IDENTITY_PATH)
        
        logger.debug(f"Saved identity map: {IDENTITY_PATH}")
        
    except Exception as e:
        # Clean up temp file on error
        if temp_path.exists():
            temp_path.unlink()
        logger.exception("Failed to save identity map")
        raise


def sync_identity(state: str, gender: str, age_group: str, provider: str, 
                 team_name: str, provider_team_id: str, club_name: str = "",
                 existing_team_id_master: Optional[str] = None) -> Dict[str, Any]:
    """
    Add or update team + club identity in the master map.
    
    Args:
        state: State code (e.g., 'AZ')
        gender: Gender ('M' or 'F')
        age_group: Age group (e.g., 'U10')
        provider: Provider name (e.g., 'gotsport')
        team_name: Team name
        provider_team_id: Provider-specific team ID
        club_name: Club name (optional)
        existing_team_id_master: Existing team ID to preserve (optional)
        
    Returns:
        Dictionary with sync results: {'team_id_master': str, 'is_new': bool, 'was_updated': bool}
    """
    logger = logging.getLogger(__name__)
    
    # Use existing ID if provided, otherwise generate
    if existing_team_id_master:
        team_id_master = existing_team_id_master
    else:
        from src.utils.team_id_generator import make_team_id
        try:
            team_id_master = make_team_id(team_name, state, age_group, gender)
        except Exception as e:
            logger.warning(f"Failed to generate team ID for {team_name}: {e}")
            return {"team_id_master": None, "is_new": False, "was_updated": False}
    
    # Validate team_id_master format
    if not re.match(r'^[a-f0-9]{12}$', team_id_master):
        logger.warning(f"Invalid team_id_master format: {team_id_master}")
        return {"team_id_master": None, "is_new": False, "was_updated": False}
    
    # Load existing data
    data = _load()
    
    # Check if entry already exists
    entry_existed = team_id_master in data
    entry = data.get(team_id_master, {
        "canonical_name": team_name,
        "provider_ids": {},
        "aliases": [],
        "club_history": [],
        "created_at": datetime.now(timezone.utc).isoformat()
    })
    
    # Track changes
    changed = False
    
    # Update provider link
    if entry["provider_ids"].get(provider) != str(provider_team_id):
        entry["provider_ids"][provider] = str(provider_team_id)
        changed = True
    
    # Add alias if not already present
    if team_name and team_name not in entry["aliases"]:
        entry["aliases"].append(team_name)
        changed = True
    
    # Merge club history from all sources
    club_set = set(entry.get("club_history", []))
    if club_name and club_name.strip():
        club_set.add(club_name.strip())
    
    # Keep only non-empty unique values, sorted for consistency
    new_club_history = sorted([c for c in club_set if c])
    if new_club_history != entry.get("club_history", []):
        entry["club_history"] = new_club_history
        changed = True
    
    # Update timestamp if anything changed
    if changed:
        entry["updated_at"] = datetime.now(timezone.utc).isoformat()
        data[team_id_master] = entry
        _save(data)
    
    # Log debug info
    if not entry_existed:
        logger.debug(f"🔗 New identity: {team_id_master} ({team_name})")
    elif changed:
        logger.debug(f"🔗 Updated identity: {team_id_master} ({team_name})")
    
    return {
        "team_id_master": team_id_master,
        "is_new": not entry_existed,
        "was_updated": changed
    }


def get_identity_summary() -> Dict[str, Any]:
    """
    Get summary statistics about the identity map.
    
    Returns:
        Dictionary with summary statistics
    """
    data = _load()
    
    total_teams = len(data)
    total_providers = sum(len(entry.get("provider_ids", {})) for entry in data.values())
    total_aliases = sum(len(entry.get("aliases", [])) for entry in data.values())
    total_clubs = sum(len(entry.get("club_history", [])) for entry in data.values())
    
    return {
        "total_teams": total_teams,
        "total_providers": total_providers,
        "total_aliases": total_aliases,
        "total_clubs": total_clubs,
        "file_path": str(IDENTITY_PATH),
        "file_exists": IDENTITY_PATH.exists()
    }


if __name__ == "__main__":
    # Test the identity sync system
    logging.basicConfig(level=logging.DEBUG)
    logger = logging.getLogger(__name__)
    
    print("Testing Identity Sync System")
    print("=" * 50)
    
    # Test data
    test_teams = [
        ("AZ", "M", "U10", "gotsport", "FC Elite AZ", "12345", "Elite FC"),
        ("AZ", "M", "U10", "gotsport", "FC Elite Arizona", "12345", "Elite FC"),  # Same team, different name
        ("CA", "F", "U12", "gotsport", "Premier Soccer Club", "67890", "Premier SC"),
    ]
    
    sync_stats = {"checked": 0, "new": 0, "updated": 0}
    
    for state, gender, age_group, provider, team_name, provider_id, club_name in test_teams:
        result = sync_identity(
            state=state,
            gender=gender,
            age_group=age_group,
            provider=provider,
            team_name=team_name,
            provider_team_id=provider_id,
            club_name=club_name
        )
        
        sync_stats["checked"] += 1
        if result["is_new"]:
            sync_stats["new"] += 1
        elif result["was_updated"]:
            sync_stats["updated"] += 1
        
        print(f"Team: {team_name} -> {result['team_id_master']}")
        print(f"  New: {result['is_new']}, Updated: {result['was_updated']}")
    
    print(f"\nSync Summary: {sync_stats['checked']} checked, "
          f"{sync_stats['new']} new, {sync_stats['updated']} updated")
    
    # Show summary
    summary = get_identity_summary()
    print("\nIdentity Map Summary:")
    print(f"  Total teams: {summary['total_teams']}")
    print(f"  Total providers: {summary['total_providers']}")
    print(f"  Total aliases: {summary['total_aliases']}")
    print(f"  Total clubs: {summary['total_clubs']}")
    print(f"  File: {summary['file_path']}")
    
    print("\nIdentity sync test completed successfully!")
