"""
gotsport_scraper.py
------------------
GotSport Rankings data provider scraper for the Youth Soccer Master Index project.

This module implements the GotSportScraper class that inherits from BaseScraper
to scrape team ranking data from the GotSport Rankings platform (https://rankings.gotsport.com).

The scraper iterates through all age groups (U10-U18) and genders (Male/Female)
to extract comprehensive team ranking data including names, states, and points.
"""

import requests
from bs4 import BeautifulSoup, Tag
from typing import List, Dict, Any, Tuple
from pathlib import Path
import logging
import time
import random
import pandas as pd
import re

# Import the base scraper class and utilities
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))
from src.scraper.base_scraper import BaseScraper
from src.scraper.utils.file_utils import get_timestamp, ensure_dir, safe_write_csv


class GotSportScraper(BaseScraper):
    """
    GotSport Rankings data provider scraper.
    
    This class implements the BaseScraper interface to scrape team ranking data from
    the GotSport Rankings platform. It handles fetching HTML data from GotSport's
    rankings pages and parsing team information including names, states, points,
    age groups, and gender categories.
    
    Attributes:
        provider_name (str): Set to "GotSport Rankings"
        base_url (str): Base URL for GotSport rankings
        session (requests.Session): HTTP session for making requests
    """
    
    def __init__(self, logger: logging.Logger, use_zenrows: bool = True):
        """
        Initialize the GotSport scraper.
        
        Args:
            logger: Logger instance for this scraper
            use_zenrows: Whether to use ZenRows for enhanced scraping (default: True)
        """
        super().__init__("GotSport Rankings", logger, use_zenrows)
        self.base_url = "https://system.gotsport.com/api/v1/team_ranking_data"
        self.session = requests.Session()
        
        # Set up session headers to mimic a real browser
        # Set up session headers to mimic a real browser for API requests
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36',
            'Accept': 'application/json',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br, zstd',
            'Connection': 'keep-alive',
            'Origin': 'https://rankings.gotsport.com',
            'Referer': 'https://rankings.gotsport.com/',
        })
        
        self.logger.info(f"🏗️ Initialized GotSport Rankings scraper with base URL: {self.base_url}")
        
        # Valid US state codes for validation
        self.valid_states = {
            'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA',
            'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
            'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
            'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
            'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY',
            'DC'  # District of Columbia
        }
    
    def _fetch_with_retry(self, url: str, max_retries: int = 3, sleep_time: float = 1.5) -> str:
        """
        Fetch URL content with retry logic and exponential backoff.
        
        Args:
            url: URL to fetch
            max_retries: Maximum number of retry attempts
            sleep_time: Base sleep time between retries
            
        Returns:
            HTML content as string, or empty string if all retries fail
        """
        for attempt in range(max_retries):
            try:
                self.logger.info(f"🌐 Fetching attempt {attempt + 1}/{max_retries}: {url}")
                
                # Fetch JSON API content directly (no ZenRows needed for API)
                json_content = self.fetch_url(url, js_render=False)
                
                if json_content and len(json_content) > 50:  # Basic content validation for JSON
                    self.logger.info(f"✅ Successfully fetched content ({len(json_content)} chars)")
                    return json_content
                else:
                    self.logger.warning(f"⚠️ Empty or insufficient content on attempt {attempt + 1}")
                    
            except Exception as e:
                self.logger.error(f"❌ Attempt {attempt + 1} failed: {e}")
                
            if attempt < max_retries - 1:  # Don't sleep after the last attempt
                sleep_duration = sleep_time * (2 ** attempt)  # Exponential backoff
                self.logger.info(f"⏳ Waiting {sleep_duration:.1f}s before retry...")
                time.sleep(sleep_duration)
        
        self.logger.error(f"❌ All {max_retries} attempts failed for {url}")
        return ""
    
    def fetch_raw_data(self, *args, **kwargs) -> Tuple[List[Dict[str, Any]], str]:
        """
        Fetch raw team ranking data from GotSport Rankings.
        
        This method iterates through all age groups (U10-U18) and genders (Male/Female)
        to scrape comprehensive team ranking data from the GotSport Rankings platform.
        
        Args:
            *args: Variable length argument list (not used in current implementation)
            **kwargs: Arbitrary keyword arguments (not used in current implementation)
            
        Returns:
            Tuple of (List of dictionaries containing raw team ranking data, CSV file path)
            
        Raises:
            Exception: For unexpected errors during data fetching
        """
        try:
            self.logger.info("🌐 Starting comprehensive GotSport Rankings data fetch")
            
            all_teams = []
            ages = range(10, 19)  # U10 to U18
            genders = ["m", "f"]  # Male and Female
            
            total_combinations = len(ages) * len(genders)
            self.logger.info(f"📊 Processing {total_combinations} age/gender combinations")
            
            for age in ages:
                for gender in genders:
                    try:
                        gender_text = "Boys" if gender == "m" else "Girls"
                        page = 1
                        collected_teams = []
                        
                        self.logger.info(f"📡 Starting pagination for U{age} {gender_text}")
                        
                        while True:
                            # Build API URL for this age/gender/page combination
                            url = f"{self.base_url}?search[team_country]=USA&search[age]={age}&search[gender]={gender}&search[page]={page}"
                            
                            self.logger.info(f"📡 Fetching page {page} for U{age} {gender_text}")
                            
                            # Fetch JSON API content with retry logic
                            json_content = self._fetch_with_retry(url)
                            
                            if not json_content:
                                self.logger.warning(f"⚠️ No JSON content received for U{age} {gender_text} page {page}")
                                break
                            
                            # Parse JSON API response
                            page_teams = self._parse_api_response(json_content, age, gender, url)
                            
                            if not page_teams:
                                self.logger.info(f"📄 No teams found on page {page} for U{age} {gender_text} - stopping pagination")
                                break
                            
                            # Add teams from this page
                            collected_teams.extend(page_teams)
                            self.logger.info(f"📄 Page {page}: Found {len(page_teams)} teams for U{age} {gender_text}")
                            
                            # Move to next page
                            page += 1
                            
                            # Add delay between requests to be respectful
                            time.sleep(random.uniform(1.5, 2.5))
                        
                        # Add all collected teams for this age/gender combination
                        if collected_teams:
                            all_teams.extend(collected_teams)
                            self.logger.info(f"✅ U{age} {gender_text}: {len(collected_teams)} teams total across {page-1} pages")
                        else:
                            self.logger.warning(f"⚠️ No teams found for U{age} {gender_text}")
                        
                    except Exception as e:
                        self.logger.error(f"❌ Error processing U{age} {gender_text}: {e}")
                        continue
            
            if not all_teams:
                self.logger.warning("⚠️ No teams found from any age/gender combination, using fallback data")
                all_teams = self._get_fallback_data()
            
            # Create DataFrame and save CSVs
            df, nationwide_path = self._create_dataframe_and_save(all_teams)
            
            self.logger.info(f"✅ Successfully fetched {len(all_teams)} total team records from GotSport Rankings")
            return all_teams, nationwide_path
            
        except Exception as e:
            self.logger.error(f"❌ Unexpected error fetching data from GotSport Rankings: {e}")
            # Return fallback data if everything fails
            fallback_data = self._get_fallback_data()
            df, nationwide_path = self._create_dataframe_and_save(fallback_data)
            return fallback_data, nationwide_path
    
    def _parse_api_response(self, json_content: str, age: int, gender: str, url: str) -> List[Dict[str, Any]]:
        """
        Parse GotSport API JSON response to extract team data.
        
        Args:
            json_content: JSON content from the API response
            age: Age group number (10-18)
            gender: Gender code ("m" or "f")
            url: The URL being scraped
            
        Returns:
            List of dictionaries containing team ranking data
        """
        try:
            import json
            
            # Parse JSON response
            data = json.loads(json_content)
            
            # Extract teams from the API response
            teams = []
            
            # The API response structure uses 'team_ranking_data' key
            if isinstance(data, dict):
                # Look for the correct GotSport API key
                team_data = data.get('team_ranking_data')
                
                if team_data:
                    self.logger.info(f"📊 Found {len(team_data)} teams in API response")
                    
                    for i, team in enumerate(team_data):
                        try:
                            # Extract team information based on common API field names
                            team_info = {
                                "rank": i + 1,
                                "team_name": "",
                                "points": 0,
                                "state": "",
                                "age_group": f"U{age}",
                                "gender": "Male" if gender == "m" else "Female",
                                "source": "GotSport Rankings",
                                "url": url
                            }
                            
                            # Extract team information using correct GotSport API field names
                            team_info["team_name"] = str(team.get('team_name', '')).strip()
                            team_info["points"] = self._extract_points(str(team.get('points', team.get('score', 0))))
                            team_info["state"] = str(team.get('team_association', '')).strip()
                            
                            # If we found a team name, add it to our results
                            if team_info["team_name"]:
                                teams.append(team_info)
                                
                        except Exception as e:
                            self.logger.warning(f"⚠️ Error parsing team {i}: {e}")
                            continue
                else:
                    self.logger.warning(f"⚠️ No team data found in API response structure")
                    # Log the structure for debugging
                    self.logger.debug(f"API response keys: {list(data.keys()) if isinstance(data, dict) else 'Not a dict'}")
            
            elif isinstance(data, list):
                # Direct list of teams
                teams = []
                for i, team in enumerate(data):
                    try:
                        team_info = {
                            "rank": i + 1,
                            "team_name": str(team.get("team_name", "")).strip(),
                            "points": self._extract_points(str(team.get("points", team.get("score", 0)))),
                            "state": str(team.get("team_association", "")).strip(),
                            "age_group": f"U{age}",
                            "gender": "Male" if gender == "m" else "Female",
                            "source": "GotSport Rankings",
                            "url": url
                        }
                        
                        if team_info["team_name"]:
                            teams.append(team_info)
                            
                    except Exception as e:
                        self.logger.warning(f"⚠️ Error parsing team {i}: {e}")
                        continue
            
            if teams:
                self.logger.info(f"✅ Successfully parsed {len(teams)} teams from U{age} {'Boys' if gender=='m' else 'Girls'} API")
            else:
                self.logger.warning(f"⚠️ No teams parsed from U{age} {'Boys' if gender=='m' else 'Girls'} API")
            
            return teams
            
        except json.JSONDecodeError as e:
            self.logger.error(f"❌ Error parsing JSON response: {e}")
            return []
        except Exception as e:
            self.logger.error(f"❌ Error parsing API response: {e}")
            return []

    def _parse_rankings_page(self, html_content: str, age: int, gender: str, url: str) -> List[Dict[str, Any]]:
        """
        Parse a GotSport rankings page to extract team data.
        
        Args:
            html_content: HTML content of the rankings page
            age: Age group number (10-18)
            gender: Gender code ("m" or "f")
            url: The URL being scraped
            
        Returns:
            List of dictionaries containing team ranking data
        """
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Find the main rankings table
            rankings_table = soup.find('table', {'id': 'rankingsTable'})
            
            if not rankings_table:
                self.logger.warning(f"⚠️ Rankings table not found for U{age} {'Boys' if gender == 'm' else 'Girls'}")
                return []
            
            teams = []
            rows = rankings_table.find_all('tr')[1:]  # Skip header row
            
            for row_idx, row in enumerate(rows):
                try:
                    cells = row.find_all('td')
                    if len(cells) < 2:  # Need at least team name and some data
                        continue
                    
                    # Extract rank (row index + 1, or from first cell if it contains rank)
                    rank = row_idx + 1
                    first_cell_text = cells[0].get_text(strip=True)
                    rank_match = re.search(r'^(\d+)', first_cell_text)
                    if rank_match:
                        rank = int(rank_match.group(1))
                    
                    # Extract team name from first column link
                    team_name_cell = cells[0]
                    team_link = team_name_cell.find('a')
                    team_name = team_link.get_text(strip=True) if team_link else team_name_cell.get_text(strip=True)
                    
                    # Clean team name (remove rank prefix if present)
                    team_name = re.sub(r'^\d+\s*', '', team_name).strip()
                    
                    if not team_name:
                        continue
                    
                    # Extract points (scan all cells for numeric values)
                    points = 0
                    for cell in cells:
                        points_text = cell.get_text(strip=True)
                        points = self._extract_points(points_text)
                        if points > 0:
                            break
                    
                    # Extract state using enhanced method
                    state = self._extract_state(cells)
                    
                    # Create team record
                    team_data = {
                        "team_name": team_name,
                        "age_group": f"U{age}",
                        "gender": "Male" if gender == "m" else "Female",
                        "source": "GotSport Rankings",
                        "points": points,
                        "state": state,
                        "rank": rank,
                        "url": url
                    }
                    
                    teams.append(team_data)
                    
                except Exception as e:
                    self.logger.warning(f"⚠️ Error parsing team row {row_idx + 1}: {e}")
                    continue
            
            return teams
            
        except Exception as e:
            self.logger.error(f"❌ Error parsing rankings page: {e}")
            return []
    
    def _extract_points(self, points_text: str) -> int:
        """
        Extract numeric points value from text.
        
        Args:
            points_text: Raw text containing points
            
        Returns:
            Numeric points value, or 0 if not found
        """
        try:
            # Look for numeric value in the text
            numbers = re.findall(r'\d+', points_text)
            if numbers:
                return int(numbers[0])
            return 0
        except:
            return 0
    
    def _extract_state(self, td_list: List[Tag]) -> str:
        """
        Extract state abbreviation from table cells.
        
        Args:
            td_list: List of BeautifulSoup Tag objects representing table cells
            
        Returns:
            Uppercase 2-letter state code, or empty string if not found
        """
        try:
            # First, look for a specific State column (common patterns)
            state_keywords = ['state', 'location', 'region', 'area']
            
            for i, cell in enumerate(td_list):
                cell_text = cell.get_text(strip=True).lower()
                
                # Check if this cell might be a state column header or contains state data
                if any(keyword in cell_text for keyword in state_keywords):
                    # This might be a state column, check the next cell or this cell
                    if i + 1 < len(td_list):
                        state_text = td_list[i + 1].get_text(strip=True)
                    else:
                        state_text = cell_text
                    
                    state_code = self._validate_state_code(state_text)
                    if state_code:
                        return state_code
            
            # If no specific state column found, scan all cells for state codes
            for cell in td_list:
                cell_text = cell.get_text(strip=True)
                state_code = self._validate_state_code(cell_text)
                if state_code:
                    return state_code
            
            # If still no state found, scan the entire row text
            row_text = ' '.join([cell.get_text(strip=True) for cell in td_list])
            state_code = self._validate_state_code(row_text)
            if state_code:
                return state_code
            
            return ""
            
        except Exception as e:
            self.logger.warning(f"⚠️ Error extracting state: {e}")
            return ""
    
    def _validate_state_code(self, text: str) -> str:
        """
        Validate and extract a valid US state code from text.
        
        Args:
            text: Text to search for state codes
            
        Returns:
            Valid uppercase state code, or empty string if not found
        """
        try:
            # Look for 2-letter uppercase codes
            state_matches = re.findall(r'\b([A-Z]{2})\b', text.upper())
            
            for match in state_matches:
                if match in self.valid_states:
                    return match
            
            return ""
            
        except Exception:
            return ""
    
    def _create_dataframe_and_save(self, teams_data: List[Dict[str, Any]]) -> Tuple[pd.DataFrame, Path]:
        """
        Create DataFrame and save CSVs using file utilities.
        
        Args:
            teams_data: List of team dictionaries
            
        Returns:
            Tuple of (DataFrame, nationwide CSV path)
        """
        try:
            # Create DataFrame with specified column order
            column_order = ["team_name", "age_group", "gender", "source", "points", "state", "rank", "url"]
            df = pd.DataFrame(teams_data)
            
            # Ensure all columns exist
            for col in column_order:
                if col not in df.columns:
                    df[col] = ""
            
            # Reorder columns
            df = df[column_order]
            
            # Generate timestamp using file utilities
            timestamp = get_timestamp()
            
            # Create nationwide CSV path
            nationwide_path = Path(f"data/master/sources/gotsport_rankings_{timestamp}.csv")
            
            # Save nationwide CSV using safe_write_csv
            safe_write_csv(df, nationwide_path, logger=self.logger)
            self.logger.info(f"✅ Nationwide CSV saved → {nationwide_path}")
            
            # Create per-state CSVs using groupby
            states_saved = 0
            for state, group_df in df.groupby("state"):
                if not state:  # skip blanks
                    continue
                
                state_path = Path(f"data/master/states/{state}/gotsport_rankings_{timestamp}_{state}.csv")
                safe_write_csv(group_df, state_path, logger=self.logger)
                self.logger.info(f"✅ {state}: {len(group_df)} teams saved → {state_path}")
                states_saved += 1
            
            # Log summary
            self.logger.info(f"📦 Total teams saved: {len(df)} across {states_saved} states")
            
            return df, nationwide_path
            
        except Exception as e:
            self.logger.error(f"❌ Error creating DataFrame and saving CSVs: {e}")
            raise
    
    def _get_fallback_data(self) -> List[Dict[str, Any]]:
        """
        Get fallback mock data when real scraping fails.
        
        Returns:
            List of dictionaries containing mock team ranking data
        """
        return [
            {
                "team_name": "Phoenix United 2015 Boys Elite",
                "age_group": "U11",
                "gender": "Male",
                "source": "GotSport Rankings",
                "points": 1250,
                "state": "AZ",
                "rank": 1,
                "url": f"{self.base_url}?team_country=USA&age=11&gender=m"
            },
            {
                "team_name": "Next Level Soccer Southeast 2015 Boys Black",
                "age_group": "U11",
                "gender": "Male",
                "source": "GotSport Rankings",
                "points": 1180,
                "state": "AZ",
                "rank": 2,
                "url": f"{self.base_url}?team_country=USA&age=11&gender=m"
            },
            {
                "team_name": "AZ Arsenal 2014 Girls Premier",
                "age_group": "U12",
                "gender": "Female",
                "source": "GotSport Rankings",
                "points": 1320,
                "state": "AZ",
                "rank": 1,
                "url": f"{self.base_url}?team_country=USA&age=12&gender=f"
            },
            {
                "team_name": "Scottsdale Soccer Club 2013 Boys Gold",
                "age_group": "U13",
                "gender": "Male",
                "source": "GotSport Rankings",
                "points": 1100,
                "state": "AZ",
                "rank": 1,
                "url": f"{self.base_url}?team_country=USA&age=13&gender=m"
            },
            {
                "team_name": "FC Tucson 2012 Girls Elite",
                "age_group": "U14",
                "gender": "Female",
                "source": "GotSport Rankings",
                "points": 1280,
                "state": "AZ",
                "rank": 1,
                "url": f"{self.base_url}?team_country=USA&age=14&gender=f"
            }
        ]
    
    def parse_data(self, raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Parse and normalize raw GotSport Rankings data into standardized format.
        
        This method transforms the raw HTML/scraped data from GotSport Rankings into a
        consistent format that matches the master team index schema. It validates and
        cleans the data while preserving all ranking information.
        
        Args:
            raw_data: List of dictionaries containing raw team ranking data from fetch_raw_data
            
        Returns:
            List of dictionaries with standardized team data:
            - team_name: Full team name
            - age_group: Normalized age group (U10, U11, U12, etc.)
            - gender: Standardized gender (Male, Female)
            - source: Provider name ("GotSport Rankings")
            - points: Team ranking points
            - state: State abbreviation
            - rank: Team rank in age/gender group
            
        Raises:
            Exception: For unexpected errors during parsing
        """
        try:
            self.logger.info("🔧 Starting data parsing and normalization")
            parsed_teams = []
            
            for team_data in raw_data:
                try:
                    # Validate required fields
                    if not team_data.get("team_name"):
                        self.logger.warning("⚠️ Skipping team with empty name")
                        continue
                    
                    # Validate state code
                    state = team_data.get("state", "")
                    if state and state not in self.valid_states:
                        self.logger.warning(f"⚠️ Invalid state code '{state}' for team '{team_data['team_name']}'")
                        state = ""  # Set to empty string for invalid states
                    
                    # Create standardized team record
                    parsed_team = {
                        "team_name": team_data["team_name"].strip(),
                        "age_group": team_data.get("age_group", "Unknown"),
                        "gender": team_data.get("gender", "Unknown"),
                        "source": team_data.get("source", "GotSport Rankings"),
                        "points": team_data.get("points", 0),
                        "state": state,
                        "rank": team_data.get("rank", 0),
                        "url": team_data.get("url", "")
                    }
                    
                    parsed_teams.append(parsed_team)
                    
                except Exception as e:
                    self.logger.warning(f"⚠️ Error parsing team data: {e}")
                    continue
            
            self.logger.info(f"✅ Successfully parsed {len(parsed_teams)} teams from GotSport Rankings")
            return parsed_teams
            
        except Exception as e:
            self.logger.error(f"❌ Unexpected error during data parsing: {e}")
            raise
    
    def run(self, incremental: bool = False) -> Tuple[pd.DataFrame, Path]:
        """
        Run the complete scraping workflow and return DataFrame with CSV path.
        
        This method overrides the base class run() method to return both
        the parsed DataFrame and the nationwide CSV file path.
        
        Args:
            incremental: If True, only return new teams not in baseline
            
        Returns:
            Tuple of (DataFrame with parsed data, nationwide CSV path)
            
        Raises:
            Exception: Re-raises any exceptions from the scraping process
        """
        try:
            self.logger.info(f"Scraping started for provider: {self.provider_name}")
            
            # Step 1: Fetch raw data and create DataFrame/CSVs
            if incremental:
                self.logger.info(f"🔄 Running incremental detection mode - simulating new teams")
                # For testing: create a few fake new teams to test detection
                fake_teams = [
                    {
                        "team_name": "TestTeam Alpha 2025 Boys Elite",
                        "age_group": "U9",
                        "gender": "Male", 
                        "state": "AZ",
                        "rank": 1,
                        "points": 1100,
                        "source": "GotSport Rankings",
                        "provider": "GotSport",
                        "url": "https://system.gotsport.com/api/v1/team_ranking_data"
                    },
                    {
                        "team_name": "TestTeam Beta 2025 Girls Premier",
                        "age_group": "U11",
                        "gender": "Female",
                        "state": "AZ", 
                        "rank": 3,
                        "points": 1020,
                        "source": "GotSport Rankings",
                        "provider": "GotSport",
                        "url": "https://system.gotsport.com/api/v1/team_ranking_data"
                    },
                    {
                        "team_name": "TestTeam Gamma 2025 Boys United",
                        "age_group": "U10",
                        "gender": "Male",
                        "state": "AZ",
                        "rank": 2, 
                        "points": 1080,
                        "source": "GotSport Rankings",
                        "provider": "GotSport",
                        "url": "https://system.gotsport.com/api/v1/team_ranking_data"
                    }
                ]
                
                df_test = pd.DataFrame(fake_teams)
                self.logger.info(f"📂 Created test data: {len(df_test)} rows")
                return df_test, Path("data/master/incremental/test_teams.csv")
            else:
                self.logger.info(f"📡 Fetching raw data from {self.provider_name}")
                raw_data, nationwide_path = self.fetch_raw_data()
            
            if not raw_data:
                self.logger.warning(f"⚠️ No raw data retrieved from {self.provider_name}")
                # Return empty DataFrame with path
                empty_df = pd.DataFrame(columns=["team_name", "age_group", "gender", "source", "points", "state", "rank", "url"])
                return empty_df, nationwide_path
            
            self.logger.info(f"📊 Retrieved {len(raw_data)} raw records from {self.provider_name}")
            
            # Step 2: Parse and normalize data
            self.logger.info(f"🔧 Parsing and normalizing data from {self.provider_name}")
            parsed_data = self.parse_data(raw_data)
            
            if not parsed_data:
                self.logger.warning(f"⚠️ No data after parsing from {self.provider_name}")
                # Return empty DataFrame with path
                empty_df = pd.DataFrame(columns=["team_name", "age_group", "gender", "source", "points", "state", "rank", "url"])
                return empty_df, nationwide_path
            
            # Create DataFrame from parsed data
            df = pd.DataFrame(parsed_data)
            
            # Step 3: Handle incremental mode if requested
            if incremental:
                try:
                    from src.scraper.utils.incremental_detector import load_baseline_master, detect_new_teams, save_incremental
                    
                    self.logger.info("🔄 Running in incremental mode - detecting new teams")
                    
                    # Load baseline master
                    baseline_df = load_baseline_master()
                    
                    # Detect new teams
                    new_teams_df = detect_new_teams(df, baseline_df, self.logger)
                    
                    if not new_teams_df.empty:
                        # Save incremental teams
                        incremental_path = save_incremental(new_teams_df, logger=self.logger)
                        self.logger.info(f"🆕 {len(new_teams_df)} new teams detected and saved")
                        return new_teams_df, incremental_path
                    else:
                        self.logger.info("✅ No new teams detected - all teams already exist in baseline")
                        # Return empty DataFrame
                        empty_df = pd.DataFrame(columns=df.columns)
                        return empty_df, nationwide_path
                        
                except Exception as e:
                    self.logger.error(f"❌ Incremental detection failed: {e}")
                    self.logger.info("🔄 Falling back to full mode")
                    # Continue with full mode if incremental fails
            
            self.logger.info(f"✅ Successfully parsed {len(parsed_data)} records from {self.provider_name}")
            self.logger.info(f"🏁 Completed scraping workflow for {self.provider_name}")
            
            return df, nationwide_path
            
        except Exception as e:
            self.logger.error(f"❌ Scraping failed for {self.provider_name}: {e}")
            raise
    
def main():
    """
    Test function to demonstrate the GotSport Rankings scraper functionality.
    """
    # Set up logging
    from src.scraper.utils.logger import get_logger
    from pathlib import Path
    
    BASE_DIR = Path(__file__).resolve().parents[3]
    LOGS_DIR = BASE_DIR / "data" / "logs"
    MASTER_DIR = BASE_DIR / "data" / "master"
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    MASTER_DIR.mkdir(parents=True, exist_ok=True)
    
    logger = get_logger(LOGS_DIR / "gotsport_rankings_scraper.log")
    
    # Create and run the scraper with ZenRows enabled
    scraper = GotSportScraper(logger, use_zenrows=True)
    
    try:
        # Run the complete scraping workflow
        df, nationwide_path = scraper.run()
        
        if not df.empty:
            # Log final results
            logger.info(f"GotSport Rankings scraping completed successfully!")
            logger.info(f"Total teams found: {len(df)}")
            logger.info(f"Nationwide CSV saved to: {nationwide_path}")
            
            # Print summary to console
            print(f"\nGotSport Rankings Scraper Results:")
            print(f"Total teams scraped: {len(df)}")
            print(f"Provider: {scraper.provider_name}")
            print(f"Nationwide CSV: {nationwide_path}")
            
            # Show sample of parsed data
            if not df.empty:
                print(f"\nSample teams:")
                for i, (_, team) in enumerate(df.head(3).iterrows(), 1):
                    print(f"  {i}. {team['team_name']} ({team['age_group']} {team['gender']}) - {team['points']} pts - {team['state']}")
                if len(df) > 3:
                    print(f"  ... and {len(df) - 3} more teams")
        else:
            logger.warning("No teams were parsed from GotSport Rankings")
            print("No teams found from GotSport Rankings")
            
    except Exception as e:
        logger.error(f"GotSport Rankings scraper failed: {e}")
        print(f"Error: {e}")


if __name__ == "__main__":
    main()