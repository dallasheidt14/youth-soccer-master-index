#!/usr/bin/env python3
"""
Activity Filter Utilities

Implements filtering logic for team activity and game recency.
Applies 12-month game filter and 120-day inactivity filter.
"""

import logging
from datetime import date, datetime, timedelta
from typing import List, Dict, Any, Optional
import pandas as pd
from dateutil.relativedelta import relativedelta


def is_team_active(last_active_date: Optional[date], threshold_days: int = 120) -> bool:
    """
    Check if a team is considered active based on last activity date.
    
    Args:
        last_active_date: Date of last team activity (e.g., last game)
        threshold_days: Number of days threshold for inactivity (default: 120)
        
    Returns:
        True if team is active, False if inactive
    """
    if last_active_date is None:
        return True  # Assume active if no activity date
    
    cutoff_date = date.today() - timedelta(days=threshold_days)
    return last_active_date >= cutoff_date


def filter_recent_games(games_list: List[Dict[str, Any]], months_back: int = 12) -> List[Dict[str, Any]]:
    """
    Filter games to only include those within the specified time period.
    
    Args:
        games_list: List of game dictionaries
        months_back: Number of months back to include games (default: 12)
        
    Returns:
        Filtered list of games within the time period
    """
    if not games_list:
        return games_list
    
    cutoff_date = date.today() - relativedelta(months=months_back)
    filtered_games = []
    
    for game in games_list:
        try:
            # Parse game date
            game_date_str = game.get('game_date')
            if not game_date_str:
                continue
            
            # Handle different date formats
            if isinstance(game_date_str, str):
                try:
                    game_date = datetime.strptime(game_date_str, '%Y-%m-%d').date()
                except ValueError:
                    try:
                        game_date = datetime.fromisoformat(game_date_str).date()
                    except ValueError:
                        continue
            elif isinstance(game_date_str, date):
                game_date = game_date_str
            else:
                continue
            
            # Include game if within time period
            if game_date >= cutoff_date:
                filtered_games.append(game)
                
        except Exception as e:
            logging.getLogger(__name__).warning(f"Error filtering game {game}: {e}")
            continue
    
    return filtered_games


def filter_inactive_teams(teams_df: pd.DataFrame, threshold_days: int = 120) -> pd.DataFrame:
    """
    Filter out teams that have been inactive for more than threshold days.
    
    Args:
        teams_df: DataFrame with team data
        threshold_days: Number of days threshold for inactivity (default: 120)
        
    Returns:
        Filtered DataFrame with only active teams
    """
    if teams_df.empty:
        return teams_df
    
    logger = logging.getLogger(__name__)
    cutoff_date = date.today() - timedelta(days=threshold_days)
    
    # Check if we have activity date column
    if 'last_seen_active_date' not in teams_df.columns:
        logger.warning("No 'last_seen_active_date' column found, skipping inactivity filter")
        return teams_df
    
    # Convert activity dates to date objects
    active_teams = []
    for _, team in teams_df.iterrows():
        try:
            last_active_str = team.get('last_seen_active_date')
            if pd.isna(last_active_str) or last_active_str is None:
                # No activity date, assume active
                active_teams.append(True)
                continue
            
            # Parse activity date
            if isinstance(last_active_str, str):
                try:
                    last_active_date = datetime.strptime(last_active_str, '%Y-%m-%d').date()
                except ValueError:
                    try:
                        last_active_date = datetime.fromisoformat(last_active_str).date()
                    except ValueError:
                        logger.warning(f"Invalid activity date format: {last_active_str}")
                        active_teams.append(True)  # Assume active
                        continue
            elif isinstance(last_active_str, date):
                last_active_date = last_active_str
            else:
                active_teams.append(True)  # Assume active
                continue
            
            # Check if team is active
            is_active = last_active_date >= cutoff_date
            active_teams.append(is_active)
            
        except Exception as e:
            logger.warning(f"Error checking team activity: {e}")
            active_teams.append(True)  # Assume active on error
    
    # Filter teams
    filtered_df = teams_df[active_teams].copy()
    
    logger.info(f"Filtered teams: {len(filtered_df)} active out of {len(teams_df)} total")
    
    return filtered_df


def get_team_last_activity_date(team_data: Dict[str, Any]) -> Optional[date]:
    """
    Extract the last activity date from team data.
    
    Args:
        team_data: Team dictionary
        
    Returns:
        Last activity date or None
    """
    # Try different possible field names
    activity_fields = ['last_seen_active_date', 'last_active_date', 'last_game_date']
    
    for field in activity_fields:
        if field in team_data:
            try:
                date_str = team_data[field]
                if pd.isna(date_str) or date_str is None:
                    continue
                
                if isinstance(date_str, str):
                    try:
                        return datetime.strptime(date_str, '%Y-%m-%d').date()
                    except ValueError:
                        try:
                            return datetime.fromisoformat(date_str).date()
                        except ValueError:
                            continue
                elif isinstance(date_str, date):
                    return date_str
                    
            except (ValueError, TypeError) as e:
                logger = logging.getLogger(__name__)
                logger.warning(f"Error parsing date '{date_str}': {e}")
                continue
    
    return None


def apply_game_filters(games_list: List[Dict[str, Any]], 
                      months_back: int = 12,
                      min_games: int = 1) -> List[Dict[str, Any]]:
    """
    Apply multiple filters to game list.
    
    Args:
        games_list: List of game dictionaries
        months_back: Number of months back to include games (default: 12)
        min_games: Minimum number of games required (default: 1)
        
    Returns:
        Filtered list of games
    """
    logger = logging.getLogger(__name__)
    
    # Apply time filter
    filtered_games = filter_recent_games(games_list, months_back)
    
    logger.info(f"Time filter: {len(filtered_games)} games after {months_back} months")
    
    # Apply minimum games filter
    if len(filtered_games) < min_games:
        logger.debug(f"Skipping team with only {len(filtered_games)} games (min: {min_games})")
        return []
    
    return filtered_games


def calculate_team_activity_metrics(teams_df: pd.DataFrame) -> Dict[str, Any]:
    """
    Calculate activity metrics for teams.
    
    Args:
        teams_df: DataFrame with team data
        
    Returns:
        Dictionary with activity metrics
    """
    logger = logging.getLogger(__name__)
    
    metrics = {
        'total_teams': len(teams_df),
        'active_teams': 0,
        'inactive_teams': 0,
        'teams_without_activity_date': 0
    }
    
    if teams_df.empty:
        return metrics
    
    cutoff_date = date.today() - timedelta(days=120)
    
    for _, team in teams_df.iterrows():
        last_active_date = get_team_last_activity_date(team.to_dict())
        
        if last_active_date is None:
            metrics['teams_without_activity_date'] += 1
        elif last_active_date >= cutoff_date:
            metrics['active_teams'] += 1
        else:
            metrics['inactive_teams'] += 1
    
    # Calculate percentages
    if metrics['total_teams'] > 0:
        metrics['active_percentage'] = (metrics['active_teams'] / metrics['total_teams']) * 100
        metrics['inactive_percentage'] = (metrics['inactive_teams'] / metrics['total_teams']) * 100
    else:
        metrics['active_percentage'] = 0
        metrics['inactive_percentage'] = 0
    
    logger.info(f"Activity metrics: {metrics['active_teams']} active ({metrics['active_percentage']:.1f}%), "
                f"{metrics['inactive_teams']} inactive ({metrics['inactive_percentage']:.1f}%)")
    
    return metrics
