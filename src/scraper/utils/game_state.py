#!/usr/bin/env python3
"""
Game State Checkpoint Management

Manages checkpoint state for game history scraping to enable resumable builds.
Tracks completed teams and per-team progress for incremental updates.
"""

import json
import logging
from datetime import date, datetime
from pathlib import Path
from typing import Dict, Any, Optional, List
import pandas as pd


class GameStateManager:
    """
    Manages checkpoint state for game history scraping.
    
    Checkpoint structure:
    {
        "last_build_id": "build_20251015_1130",
        "completed_teams": ["team_id_1", "team_id_2"],
        "per_team": {
            "team_id_1": {
                "last_scraped_game_date": "2024-10-15",
                "last_seen_active_date": "2024-10-15",
                "games_scraped": 25
            }
        }
    }
    """
    
    def __init__(self, provider: str):
        """
        Initialize game state manager.
        
        Args:
            provider: Provider name (e.g., 'gotsport')
        """
        self.provider = provider
        self.logger = logging.getLogger(__name__)
        self.state_dir = Path(f"data/game_history/state/{provider}")
        self.state_dir.mkdir(parents=True, exist_ok=True)
    
    def load_checkpoint(self, state: str, gender: str, age_group: str) -> Dict[str, Any]:
        """
        Load checkpoint data for a specific slice.
        
        Args:
            state: State code (e.g., 'AZ')
            gender: Gender ('M' or 'F')
            age_group: Age group (e.g., 'U10')
            
        Returns:
            Checkpoint dictionary or default empty state
        """
        checkpoint_file = self._get_checkpoint_path(state, gender, age_group)
        
        if not checkpoint_file.exists():
            self.logger.info(f"No checkpoint found for {state}_{gender}_{age_group}")
            return self._get_default_checkpoint()
        
        try:
            with open(checkpoint_file, 'r') as f:
                checkpoint = json.load(f)
            
            self.logger.info(f"Loaded checkpoint for {state}_{gender}_{age_group}")
            self.logger.debug(f"Checkpoint: {checkpoint}")
            
            return checkpoint
            
        except Exception:
            self.logger.exception(f"Error loading checkpoint {checkpoint_file}")
            return self._get_default_checkpoint()
    
    def save_checkpoint(self, state: str, gender: str, age_group: str, checkpoint: Dict[str, Any]) -> None:
        """
        Save checkpoint data for a specific slice.
        
        Args:
            state: State code (e.g., 'AZ')
            gender: Gender ('M' or 'F')
            age_group: Age group (e.g., 'U10')
            checkpoint: Checkpoint data to save
        """
        checkpoint_file = self._get_checkpoint_path(state, gender, age_group)
        
        try:
            # Ensure directory exists
            checkpoint_file.parent.mkdir(parents=True, exist_ok=True)
            
            # Write checkpoint with atomic operation
            temp_file = checkpoint_file.with_suffix('.tmp')
            with open(temp_file, 'w') as f:
                json.dump(checkpoint, f, indent=2, default=str)
            
            # Atomic move
            temp_file.replace(checkpoint_file)
            
            self.logger.debug(f"Saved checkpoint for {state}_{gender}_{age_group}")
            
        except Exception:
            self.logger.exception(f"Error saving checkpoint {checkpoint_file}")
            raise
    
    def mark_team_complete(self, checkpoint: Dict[str, Any], team_id: str, last_game_date: Optional[date], games_scraped: int = 0) -> Dict[str, Any]:
        """
        Mark a team as completed in the checkpoint.
        
        Args:
            checkpoint: Current checkpoint data
            team_id: Team ID to mark complete
            last_game_date: Date of last scraped game
            games_scraped: Number of games scraped for this team
            
        Returns:
            Updated checkpoint dictionary
        """
        checkpoint = checkpoint.copy()
        
        # Add to completed teams if not already there
        if 'completed_teams' not in checkpoint:
            checkpoint['completed_teams'] = []
        
        if team_id not in checkpoint['completed_teams']:
            checkpoint['completed_teams'].append(team_id)
        
        # Update per-team data
        if 'per_team' not in checkpoint:
            checkpoint['per_team'] = {}
        
        checkpoint['per_team'][team_id] = {
            'last_scraped_game_date': last_game_date.isoformat() if last_game_date else None,
            'last_seen_active_date': datetime.now().date().isoformat(),
            'games_scraped': games_scraped
        }
        
        self.logger.debug(f"Marked team {team_id} as complete with {games_scraped} games")
        
        return checkpoint
    
    def get_teams_to_scrape(self, slice_df: pd.DataFrame, checkpoint: Dict[str, Any]) -> pd.DataFrame:
        """
        Filter teams to scrape based on checkpoint data.
        
        Args:
            slice_df: DataFrame with team data from master slice
            checkpoint: Current checkpoint data
            
        Returns:
            Filtered DataFrame with teams that need scraping
        """
        completed_teams = set(checkpoint.get('completed_teams', []))
        
        # Filter out completed teams
        teams_to_scrape = slice_df[~slice_df['team_id_master'].isin(completed_teams)].copy()
        
        self.logger.info(f"Teams to scrape: {len(teams_to_scrape)} (completed: {len(completed_teams)})")
        
        return teams_to_scrape
    
    def get_team_last_scraped_date(self, checkpoint: Dict[str, Any], team_id: str) -> Optional[date]:
        """
        Get the last scraped game date for a team.
        
        Args:
            checkpoint: Current checkpoint data
            team_id: Team ID
            
        Returns:
            Last scraped date or None
        """
        per_team = checkpoint.get('per_team', {})
        team_data = per_team.get(team_id, {})
        
        last_date_str = team_data.get('last_scraped_game_date')
        if last_date_str:
            try:
                return datetime.fromisoformat(last_date_str).date()
            except (ValueError, TypeError):
                self.logger.warning(f"Invalid last_scraped_game_date for team {team_id}: {last_date_str}")
        
        return None
    
    def update_build_id(self, checkpoint: Dict[str, Any], build_id: str) -> Dict[str, Any]:
        """
        Update the build ID in checkpoint.
        
        Args:
            checkpoint: Current checkpoint data
            build_id: New build ID
            
        Returns:
            Updated checkpoint dictionary
        """
        checkpoint = checkpoint.copy()
        checkpoint['last_build_id'] = build_id
        return checkpoint
    
    def _get_checkpoint_path(self, state: str, gender: str, age_group: str) -> Path:
        """Get the checkpoint file path for a slice."""
        filename = f"{state}_{gender}_{age_group}.json"
        return self.state_dir / filename
    
    def _get_default_checkpoint(self) -> Dict[str, Any]:
        """Get default empty checkpoint structure."""
        return {
            "last_build_id": None,
            "completed_teams": [],
            "per_team": {}
        }
    
    def cleanup_old_checkpoints(self, keep_days: int = 30) -> None:
        """
        Clean up old checkpoint files.
        
        Args:
            keep_days: Number of days to keep checkpoints
        """
        cutoff_time = datetime.now().timestamp() - (keep_days * 24 * 60 * 60)
        
        for checkpoint_file in self.state_dir.glob("*.json"):
            if checkpoint_file.stat().st_mtime < cutoff_time:
                self.logger.info(f"Removing old checkpoint: {checkpoint_file}")
                checkpoint_file.unlink()


def load_checkpoint(provider: str, state: str, gender: str, age_group: str) -> Dict[str, Any]:
    """
    Load checkpoint data for a specific slice.
    
    Args:
        provider: Provider name
        state: State code
        gender: Gender
        age_group: Age group
        
    Returns:
        Checkpoint dictionary
    """
    manager = GameStateManager(provider)
    return manager.load_checkpoint(state, gender, age_group)


def save_checkpoint(provider: str, state: str, gender: str, age_group: str, data: Dict[str, Any]) -> None:
    """
    Save checkpoint data for a specific slice.
    
    Args:
        provider: Provider name
        state: State code
        gender: Gender
        age_group: Age group
        data: Checkpoint data to save
    """
    manager = GameStateManager(provider)
    manager.save_checkpoint(state, gender, age_group, data)


def mark_team_complete(checkpoint: Dict[str, Any], team_id: str, last_game_date: Optional[date], games_scraped: int = 0) -> Dict[str, Any]:
    """
    Mark a team as completed in the checkpoint.
    
    Args:
        checkpoint: Current checkpoint data
        team_id: Team ID to mark complete
        last_game_date: Date of last scraped game
        games_scraped: Number of games scraped for this team
        
    Returns:
        Updated checkpoint dictionary
    """
    manager = GameStateManager("dummy")  # Provider not needed for this operation
    return manager.mark_team_complete(checkpoint, team_id, last_game_date, games_scraped)


def get_teams_to_scrape(slice_df: pd.DataFrame, checkpoint: Dict[str, Any]) -> pd.DataFrame:
    """
    Filter teams to scrape based on checkpoint data.
    
    Args:
        slice_df: DataFrame with team data from master slice
        checkpoint: Current checkpoint data
        
    Returns:
        Filtered DataFrame with teams that need scraping
    """
    manager = GameStateManager("dummy")  # Provider not needed for this operation
    return manager.get_teams_to_scrape(slice_df, checkpoint)
