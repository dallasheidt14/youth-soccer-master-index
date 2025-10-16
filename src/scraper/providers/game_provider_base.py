#!/usr/bin/env python3
"""
Game History Provider Base Interface

Abstract base class defining the interface for game history providers.
All game providers must implement this interface to ensure consistency.
"""

from abc import ABC, abstractmethod
from datetime import date
from typing import Iterable, Dict, Any, Optional
import logging


class GameHistoryProvider(ABC):
    """
    Abstract base class for game history providers.
    
    This interface ensures all providers implement consistent methods
    for scraping team game history data.
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize the provider.
        
        Args:
            config: Optional configuration dictionary
        """
        self.config = config or {}
        self.logger = logging.getLogger(self.__class__.__name__)
    
    @abstractmethod
    def iter_teams(self, state: str, gender: str, age_group: str) -> Iterable[Dict[str, Any]]:
        """
        Yield team dictionaries for the specified criteria.
        
        Args:
            state: 2-letter state code (e.g., 'AZ')
            gender: Gender code ('M' or 'F')
            age_group: Age group (e.g., 'U10')
            
        Yields:
            Dict containing team information with keys:
            - team_id_source: Original provider team ID
            - team_id_master: Master team index ID
            - team_name: Team name
            - club_name: Club/organization name
            - state: State code
            - gender: Gender code
            - age_group: Age group
        """
        pass
    
    @abstractmethod
    def fetch_team_games_since(self, team: Dict[str, Any], since: Optional[date]) -> Iterable[Dict[str, Any]]:
        """
        Fetch games for a team since the specified date.
        
        Args:
            team: Team dictionary from iter_teams()
            since: Optional date to fetch games since (inclusive)
                  If None, fetch all available games
                  
        Yields:
            Dict containing game information with keys:
            - provider: Provider name
            - team_id_source: Original provider team ID
            - team_id_master: Master team index ID
            - team_name: Team name
            - opponent_name: Opponent team name
            - game_date: Date of the game
            - goals_for: Goals scored by team
            - goals_against: Goals scored by opponent
            - venue: Game venue/location
            - source_url: URL where game data was found
        """
        pass
    
    def get_provider_name(self) -> str:
        """
        Get the provider name.
        
        Returns:
            Provider name string
        """
        return self.__class__.__name__.replace('Provider', '').lower()
    
    def validate_team_data(self, team: Dict[str, Any]) -> bool:
        """
        Validate team data structure.
        
        Args:
            team: Team dictionary to validate
            
        Returns:
            True if valid, False otherwise
        """
        required_keys = {
            'team_id_source', 'team_id_master', 'team_name', 
            'club_name', 'state', 'gender', 'age_group'
        }
        
        if not isinstance(team, dict):
            self.logger.error(f"Team data must be a dictionary, got {type(team)}")
            return False
        
        missing_keys = required_keys - set(team.keys())
        if missing_keys:
            self.logger.error(f"Team data missing required keys: {missing_keys}")
            return False
        
        return True
    
    def validate_game_data(self, game: Dict[str, Any]) -> bool:
        """
        Validate game data structure.
        
        Args:
            game: Game dictionary to validate
            
        Returns:
            True if valid, False otherwise
        """
        required_keys = {
            'provider', 'team_id_source', 'team_id_master', 'team_name',
            'opponent_name', 'game_date', 'goals_for', 'goals_against'
        }
        
        if not isinstance(game, dict):
            self.logger.error(f"Game data must be a dictionary, got {type(game)}")
            return False
        
        missing_keys = required_keys - set(game.keys())
        if missing_keys:
            self.logger.error(f"Game data missing required keys: {missing_keys}")
            return False
        
        return True
    
    def normalize_game_data(self, game: Dict[str, Any]) -> Dict[str, Any]:
        """
        Normalize game data to standard format.
        
        Args:
            game: Raw game data dictionary
            
        Returns:
            Normalized game data dictionary
        """
        normalized = game.copy()
        
        # Ensure provider is set
        if 'provider' not in normalized:
            normalized['provider'] = self.get_provider_name()
        
        # Ensure numeric fields are properly typed
        if 'goals_for' in normalized:
            try:
                normalized['goals_for'] = int(normalized['goals_for']) if normalized['goals_for'] is not None else None
            except (ValueError, TypeError):
                normalized['goals_for'] = None
        
        if 'goals_against' in normalized:
            try:
                normalized['goals_against'] = int(normalized['goals_against']) if normalized['goals_against'] is not None else None
            except (ValueError, TypeError):
                normalized['goals_against'] = None
        
        # Ensure string fields are strings
        string_fields = ['team_name', 'opponent_name', 'venue']
        for field in string_fields:
            if field in normalized and normalized[field] is not None:
                normalized[field] = str(normalized[field]).strip()
        
        return normalized


class GameProviderError(Exception):
    """Base exception for game provider errors."""
    pass


class GameProviderConfigError(GameProviderError):
    """Configuration error in game provider."""
    pass


class GameProviderAPIError(GameProviderError):
    """API error in game provider."""
    pass


class GameProviderDataError(GameProviderError):
    """Data parsing error in game provider."""
    pass

