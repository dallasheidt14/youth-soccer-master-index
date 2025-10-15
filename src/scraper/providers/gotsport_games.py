#!/usr/bin/env python3
"""
GotSport Game History Provider

Implements game history scraping for GotSport teams using their API.
Scrapes team match history from the GotSport system.
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import random
import re
import logging
from datetime import date, datetime, timedelta
from typing import Iterable, Dict, Any, Optional
from pathlib import Path
import json

from .game_provider_base import GameHistoryProvider, GameProviderAPIError, GameProviderDataError
from src.scraper.utils.zenrows_client import fetch_with_zenrows


class GotSportGameProvider(GameHistoryProvider):
    """
    GotSport game history provider.
    
    Scrapes team game history from GotSport's API endpoint:
    https://system.gotsport.com/api/v1/teams/{team_id}/matches?past=true
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize GotSport game provider.
        
        Args:
            config: Configuration dictionary with optional keys:
                - delay_min: Minimum delay between requests (default: 1.5)
                - delay_max: Maximum delay between requests (default: 2.5)
                - max_retries: Maximum retry attempts (default: 3)
                - timeout: Request timeout in seconds (default: 30)
        """
        super().__init__(config)
        
        self.delay_min = self.config.get('delay_min', 1.5)
        self.delay_max = self.config.get('delay_max', 2.5)
        self.max_retries = self.config.get('max_retries', 3)
        self.timeout = self.config.get('timeout', 30)
        self.retry_delay = self.config.get('retry_delay', 2.0)
        
        # API configuration
        self.base_url = "https://system.gotsport.com/api/v1"
        self.session = requests.Session()
        
        # Set headers to mimic browser requests
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Origin': 'https://rankings.gotsport.com',
            'Referer': 'https://rankings.gotsport.com/',
            'Connection': 'keep-alive',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'cross-site'
        })
        
        self.logger.info(f"Initialized GotSportGameProvider with delay {self.delay_min}-{self.delay_max}s")
    
    def iter_teams(self, state: str, gender: str, age_group: str) -> Iterable[Dict[str, Any]]:
        """
        Load teams from master slice CSV for the specified criteria.
        
        Args:
            state: 2-letter state code (e.g., 'AZ')
            gender: Gender code ('M' or 'F')
            age_group: Age group (e.g., 'U10')
            
        Yields:
            Dict containing team information
        """
        slice_file = Path(f"data/master/slices/{state}_{gender}_{age_group}_master.csv")
        
        if not slice_file.exists():
            self.logger.error(f"Slice file not found: {slice_file}")
            return
        
        self.logger.info(f"Loading teams from {slice_file}")
        
        try:
            df = pd.read_csv(slice_file)
            self.logger.info(f"Loaded {len(df)} teams from slice")
            
            for _, row in df.iterrows():
                team = {
                    'team_id_source': str(row['team_id_source']),
                    'team_id_master': str(row['team_id_master']),
                    'team_name': str(row['team_name']),
                    'club_name': str(row['club_name']) if pd.notna(row['club_name']) else None,
                    'state': str(row['state']),
                    'gender': str(row['gender']),
                    'age_group': str(row['age_group'])
                }
                
                if self.validate_team_data(team):
                    yield team
                else:
                    self.logger.warning(f"Invalid team data: {team}")
                    
        except Exception as e:
            self.logger.exception(f"Error loading teams from {slice_file}")
            raise GameProviderDataError(f"Failed to load teams: {e}") from e
    
    def fetch_team_games_since(self, team: Dict[str, Any], since: Optional[date]) -> Iterable[Dict[str, Any]]:
        """
        Fetch games for a team since the specified date using the GotSport API.
        
        Args:
            team: Team dictionary from iter_teams()
            since: Optional date to fetch games since (inclusive)
                  
        Yields:
            Dict containing game information
        """
        raw_team_id = team['team_id_source']
        team_name = team['team_name']
        
        # Normalize team id like "126693.0" -> 126693
        try:
            normalized_team_id = int(float(str(raw_team_id)))
        except (ValueError, TypeError):
            self.logger.error(f"Invalid team_id_source for team {team_name}: {raw_team_id}")
            return
        
        # Apply 12-month filter baseline
        cutoff_date = date.today() - timedelta(days=365)
        if since:
            since = max(since, cutoff_date)
        else:
            since = cutoff_date
        
        # Use the proven API endpoint and headers
        api_url = f"https://system.gotsport.com/api/v1/teams/{normalized_team_id}/matches"
        params = {'past': 'true'}
        
        # Use the exact headers that worked in the past
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json',
            'Origin': 'https://rankings.gotsport.com',
            'Referer': 'https://rankings.gotsport.com/'
        }
        
        self.logger.info(f"Fetching API matches for team {team_name} (ID: {normalized_team_id}) since {since}")
        
        # First, fetch the club name from the team page
        club_name = self._extract_club_name(normalized_team_id, team_name)
        
        try:
            for attempt in range(self.max_retries):
                try:
                    response = requests.get(api_url, params=params, headers=headers, timeout=30)
                    response.raise_for_status()
                    data = response.json()
                    
                    # API returns a list directly, not a dict with 'matches' key
                    if isinstance(data, list) and data:
                        matches = data
                        self.logger.info(f"API returned {len(matches)} matches")
                        
                        # Process matches using the proven approach
                        for match in matches:
                            game = self._parse_api_match(match, team, since, club_name)
                            if game:
                                yield self.normalize_game_data(game)
                        
                        self.logger.info(f"Successfully processed {len(matches)} games via API")
                        return
                    else:
                        self.logger.info(f"No matches found for team {team_name}")
                        return
                        
                except requests.exceptions.RequestException as e:
                    if attempt < self.max_retries - 1:
                        self.logger.warning(f"API attempt {attempt + 1} failed: {e}, retrying...")
                        time.sleep(self.retry_delay)
                        continue
                    else:
                        self.logger.exception(f"API failed after {self.max_retries} attempts")
                        raise GameProviderAPIError(f"API failed after {self.max_retries} attempts") from e                        
        except Exception as e:
            self.logger.exception(f"Failed to fetch games for team {team_name}")
            raise GameProviderAPIError(f"Failed to fetch games for team {team_name}: {e}") from e
        
        time.sleep(random.uniform(self.delay_min, self.delay_max))
    
    def _extract_club_name(self, team_id: int, team_name: str) -> str:
        """
        Extract club name from the GotSport team details API.
        
        Args:
            team_id: Normalized team ID
            team_name: Team name for logging
            
        Returns:
            Club name or empty string if not found
        """
        try:
            # Use the team details API endpoint
            api_url = f"https://system.gotsport.com/api/v1/team_ranking_data/team_details?team_id={team_id}"
            
            # Use the same headers as the API
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Accept': 'application/json',
                'Origin': 'https://rankings.gotsport.com',
                'Referer': 'https://rankings.gotsport.com/'
            }
            
            self.logger.debug(f"Fetching club name from API: {api_url}")
            
            response = requests.get(api_url, headers=headers, timeout=15)
            response.raise_for_status()
            
            data = response.json()
            
            # Extract club name from API response
            club_name = data.get('club_name', '')
            
            if club_name:
                self.logger.info(f"Extracted club name for {team_name}: '{club_name}'")
            else:
                self.logger.warning(f"Could not extract club name for {team_name} from API")
            
            return club_name
            
        except Exception as e:
            self.logger.warning(f"Failed to extract club name for {team_name}: {e}")
            return ""
    
    def _parse_api_match(self, match: Dict[str, Any], team: Dict[str, Any], since: Optional[date], club_name: str = '') -> Optional[Dict[str, Any]]:
        """
        Parse a match from the GotSport API response.
        
        Args:
            match: Match data from API
            team: Team information
            since: Date filter
            club_name: Club name extracted from team page
            
        Returns:
            Parsed game dictionary or None if invalid
        """
        try:
            # Extract team info
            home_team = match.get('homeTeam', {})
            away_team = match.get('awayTeam', {})
            
            # Determine if our team is home or away
            team_id = team['team_id_source']
            # Normalize team ID for comparison
            try:
                normalized_team_id = int(float(str(team_id)))
            except (ValueError, TypeError):
                self.logger.warning(f"Invalid team_id_source: {team_id}")
                return None
            
            is_home = False
            opponent = {}
            
            if home_team.get('team_id') == normalized_team_id:
                is_home = True
                opponent = away_team
            elif away_team.get('team_id') == normalized_team_id:
                is_home = False
                opponent = home_team
            else:
                # Team not found in match - log debug info
                home_id = home_team.get('team_id')
                away_id = away_team.get('team_id')
                self.logger.info(f"Team {normalized_team_id} not found in match. Home: {home_id}, Away: {away_id}")
                return None
            
            # Parse opponent name from title if available
            title = match.get('title', '')
            opponent_name = ''
            if ' vs. ' in title:
                parts = title.split(' vs. ')
                if len(parts) == 2:
                    # Determine which team is the opponent
                    if team['team_name'] in parts[0]:
                        opponent_name = parts[1]
                    elif team['team_name'] in parts[1]:
                        opponent_name = parts[0]
            
            # Fallback to team name from API
            if not opponent_name:
                opponent_name = opponent.get('full_name', 'Unknown')
            
            # Parse date
            match_date = match.get('match_date', '')
            if not match_date:
                return None
            
            try:
                # Parse date string (assuming ISO format)
                game_date = datetime.fromisoformat(match_date.replace('Z', '+00:00')).date()
            except (ValueError, TypeError):
                self.logger.warning(f"Invalid date format: {match_date}")
                return None
            
            # Apply date filter
            if since and game_date < since:
                return None
            
            # Extract scores
            home_score = match.get('home_score')
            away_score = match.get('away_score')
            
            # Determine goals for/against based on home/away
            if is_home:
                goals_for = home_score
                goals_against = away_score
            else:
                goals_for = away_score
                goals_against = home_score
            
            # Keep None values as None - schema validation will handle conversion to nullable Int64
            # No need to convert None to pd.NA here
            
            # Extract venue info
            venue = match.get('venue', {})
            venue_name = venue.get('name', '') if isinstance(venue, dict) else ''
            
            # Build game dictionary
            game = {
                'provider': 'gotsport',
                'team_id_source': str(team_id),
                'team_id_master': team['team_id_master'],
                'team_name': team['team_name'],
                'club_name': club_name,
                'opponent_name': opponent_name,
                'opponent_id': str(opponent.get('team_id', '')),
                'age_group': team['age_group'],
                'gender': team['gender'],
                'state': team['state'],
                'game_date': game_date.strftime('%Y-%m-%d'),
                'home_away': 'H' if is_home else 'A',
                'goals_for': goals_for,
                'goals_against': goals_against,
                'result': self._determine_result(goals_for, goals_against),
                'competition': match.get('competition_name', ''),
                'venue': venue_name,
                'city': '',
                'source_url': f"https://rankings.gotsport.com/teams/{team_id}/game-history",
                'scraped_at': datetime.utcnow().isoformat()
            }
            
            return game
            
        except Exception as e:
            self.logger.debug(f"Error parsing API match: {e}")
            return None
    
    def _parse_div_game_entry(self, entry, team: Dict[str, Any], since: date) -> Optional[Dict[str, Any]]:
        """
        Parse a div-based game entry from GotSport game history page.
        
        Args:
            entry: BeautifulSoup element containing game data
            team: Team information
            since: Date filter
            
        Returns:
            Parsed game dictionary or None if invalid
        """
        try:
            # Extract text content and parse
            text = entry.get_text(strip=True)
            if not text:
                return None
            
            # Look for date patterns like "Saturday, September 20, 2025"
            import re
            date_match = re.search(r'(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),\s+(\w+)\s+(\d+),\s+(\d{4})', text)
            if not date_match:
                return None
            
            # Parse date
            day_name, month_name, day_num, year = date_match.groups()
            month_map = {
                'January': 1, 'February': 2, 'March': 3, 'April': 4, 'May': 5, 'June': 6,
                'July': 7, 'August': 8, 'September': 9, 'October': 10, 'November': 11, 'December': 12
            }
            try:
                game_date = date(int(year), month_map[month_name], int(day_num))
            except (ValueError, KeyError):
                return None
            
            # Apply date filter
            if game_date < since:
                return None
            
            # Look for score pattern like "0 - 1" or "2 - 4"
            score_match = re.search(r'(\d+)\s*-\s*(\d+)', text)
            if not score_match:
                return None
            
            goals_for = int(score_match.group(1))
            goals_against = int(score_match.group(2))
            
            # Determine if home or away based on team name position
            team_name = team['team_name']
            is_home = team_name in text.split(' - ')[0] if ' - ' in text else False
            
            # Extract opponent name (the other team in the score line)
            score_line = score_match.group(0)
            score_parts = text.split(score_line)
            if len(score_parts) >= 2:
                # Find team names around the score
                before_score = score_parts[0].strip()
                after_score = score_parts[1].strip() if len(score_parts) > 1 else ""
                
                # Clean up the text to find team names
                # Remove common prefixes and suffixes
                clean_before = re.sub(r'^(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),\s+\w+\s+\d+,\s+\d{4}', '', before_score)
                clean_before = re.sub(r'\d{4}[-\w\s]*(?:League|Cup|Tournament|Event|Season)[\w\s]*', '', clean_before)
                clean_after = re.sub(r'\d{4}[-\w\s]*(?:League|Cup|Tournament|Event|Season)[\w\s]*', '', after_score)
                
                # Determine opponent
                if team_name in clean_before:
                    opponent = clean_after.split('\n')[0].strip()
                else:
                    opponent = clean_before.split('\n')[-1].strip()
                
                # Clean up opponent name
                opponent = re.sub(r'\d+:\d+\s*[AP]M', '', opponent).strip()
                opponent = re.sub(r'^\d+\s*-\s*\d+', '', opponent).strip()
            else:
                opponent = "Unknown"
            
            # Extract competition name (look for patterns like "2025-26 MaxinMotion Open League Season 1")
            comp_match = re.search(r'(\d{4}[-\w\s]+(?:League|Cup|Tournament|Event|Season)[\w\s]*)', text)
            if comp_match:
                competition = comp_match.group(1).strip()
                # Clean up competition name
                competition = re.sub(r'\d+:\d+\s*[AP]M', '', competition).strip()
                competition = re.sub(r'\d+\s*-\s*\d+', '', competition).strip()
            else:
                competition = ""
            
            # Determine result
            result = self._determine_result(goals_for, goals_against)
            
            return {
                'provider': 'gotsport',
                'team_id_source': str(team['team_id_source']),
                'team_id_master': team['team_id_master'],
                'team_name': team['team_name'],
                'opponent_name': opponent,
                'opponent_id': None,
                'age_group': team['age_group'],
                'gender': team['gender'],
                'state': team['state'],
                'game_date': game_date.strftime('%Y-%m-%d'),
                'home_away': 'H' if is_home else 'A',
                'goals_for': goals_for,
                'goals_against': goals_against,
                'result': result,
                'competition': competition,
                'venue': '',
                'city': '',
                'source_url': f"https://rankings.gotsport.com/teams/{team['team_id_source']}/game-history",
                'scraped_at': datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            self.logger.debug(f"Error parsing div game entry: {e}")
            return None
    
    def _parse_date_flex(self, text: str) -> Optional[date]:
        """Parse a date string in common formats into a date object."""
        text = (text or '').strip()
        for fmt in ('%Y-%m-%d', '%m/%d/%Y', '%m/%d/%y'):
            try:
                return datetime.strptime(text, fmt).date()
            except ValueError:
                continue
        # Try letting pandas parse if available
        try:
            parsed = pd.to_datetime(text, errors='coerce')
            if pd.notna(parsed):
                return parsed.date()
        except Exception:
            pass
        return None

    def _parse_result_text(self, text: str) -> tuple[Optional[int], Optional[int], str, str]:
        """
        Parse a result text like "W 2-1 (H)" or "L 0-3 (A)" to goals and home/away.
        Returns: (goals_for, goals_against, home_away, result_code)
        """
        text_norm = (text or '').strip().upper()
        # Default values
        goals_for: Optional[int] = None
        goals_against: Optional[int] = None
        home_away = 'H' if '(H)' in text_norm else ('A' if '(A)' in text_norm else 'H')
        result_code = 'U'
        
        # Extract result code W/L/D
        if text_norm.startswith('W'):
            result_code = 'W'
        elif text_norm.startswith('L'):
            result_code = 'L'
        elif text_norm.startswith('D'):
            result_code = 'D'
        
        # Extract score like 2-1
        import re
        m = re.search(r'(\d+)\s*[-:]\s*(\d+)', text_norm)
        if m:
            try:
                goals_for = int(m.group(1))
                goals_against = int(m.group(2))
            except Exception:
                goals_for = None
                goals_against = None
        
        return goals_for, goals_against, home_away, result_code
    
    def _parse_match(self, match: Dict[str, Any], team: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Parse a single match from GotSport API response.
        
        Args:
            match: Raw match data from API
            team: Team information
            
        Returns:
            Parsed game dictionary or None if invalid
        """
        try:
            # Extract basic match info
            match_time = match.get('matchTime')
            if not match_time:
                return None
            
            # Parse match time
            try:
                game_date = datetime.fromisoformat(match_time.replace('Z', '+00:00')).date()
            except (ValueError, AttributeError):
                self.logger.warning(f"Invalid match time format: {match_time}")
                return None
            
            # Extract team info
            home_team = match.get('homeTeam', {})
            away_team = match.get('awayTeam', {})
            
            # Determine if our team is home or away
            team_id = team['team_id_source']
            # Normalize team ID for comparison
            try:
                normalized_team_id = int(float(str(team_id)))
            except (ValueError, TypeError):
                self.logger.warning(f"Invalid team_id_source: {team_id}")
                return None
            
            is_home = False
            
            if home_team.get('team_id') == normalized_team_id:
                is_home = True
                opponent = away_team
            elif away_team.get('team_id') == normalized_team_id:
                is_home = False
                opponent = home_team
            else:
                # Team not found in match - log debug info
                home_id = home_team.get('team_id')
                away_id = away_team.get('team_id')
                self.logger.debug(f"Team {normalized_team_id} not found in match. Home: {home_id}, Away: {away_id}")
                return None
            
            # Extract scores
            home_score = match.get('homeScore')
            away_score = match.get('awayScore')
            
            if is_home:
                goals_for = home_score
                goals_against = away_score
            else:
                goals_for = away_score
                goals_against = home_score
            
            # Extract venue info
            venue_info = match.get('venue', {})
            venue_name = venue_info.get('name', '') if venue_info else ''
            venue_city = venue_info.get('city', '') if venue_info else ''
            
            # Build game dictionary
            game = {
                'provider': 'gotsport',
                'team_id_source': str(team_id),
                'team_id_master': team['team_id_master'],
                'team_name': team['team_name'],
                'opponent_name': opponent.get('full_name', 'Unknown'),
                'opponent_id': str(opponent.get('team_id', '')),
                'age_group': team['age_group'],
                'gender': team['gender'],
                'state': team['state'],
                'game_date': game_date.strftime('%Y-%m-%d'),
                'home_away': 'H' if is_home else 'A',
                'goals_for': goals_for,
                'goals_against': goals_against,
                'result': self._determine_result(goals_for, goals_against),
                'competition': match.get('competition_name', ''),
                'venue': venue_name,
                'city': venue_city,
                'source_url': f"https://system.gotsport.com/api/v1/teams/{team_id}/matches",
                'scraped_at': datetime.utcnow().isoformat()
            }
            
            return game
            
        except Exception as e:
            self.logger.warning(f"Error parsing match: {e}")
            return None
    
    def _determine_result(self, goals_for: Optional[int], goals_against: Optional[int]) -> str:
        """
        Determine game result based on scores.
        
        Args:
            goals_for: Goals scored by team
            goals_against: Goals scored by opponent
            
        Returns:
            Result string: 'W', 'L', 'D', or 'U' (unknown)
        """
        if goals_for is None or goals_against is None:
            return 'U'  # Unknown
        
        if goals_for > goals_against:
            return 'W'  # Win
        elif goals_for < goals_against:
            return 'L'  # Loss
        else:
            return 'D'  # Draw
    
    def get_provider_name(self) -> str:
        """Get the provider name."""
        return 'gotsport'
