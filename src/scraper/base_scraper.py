"""
base_scraper.py
---------------
Abstract base class for all soccer data providers in the Youth Soccer Master Index project.

This module defines the BaseScraper class that provides a common interface and shared
functionality for scraping data from different soccer provider platforms (GotSport, 
Modular11, AthleteOne, etc.).

All provider-specific scrapers should inherit from this base class and implement
the required abstract methods.
"""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import List, Dict, Any, Optional
import pandas as pd
import logging
import requests


class BaseScraper(ABC):
    """
    Abstract base class for soccer data providers.
    
    This class defines the common interface and shared functionality for all
    soccer data scrapers. Child classes must implement the abstract methods
    to provide provider-specific data fetching and parsing logic.
    
    Attributes:
        provider_name (str): Name of the data provider (e.g., "GotSport", "Modular11")
        logger (logging.Logger): Logger instance for this scraper
        use_zenrows (bool): Whether to use ZenRows for enhanced scraping
    """
    
    def __init__(self, provider_name: str, logger: logging.Logger, use_zenrows: bool = False):
        """
        Initialize the base scraper.
        
        Args:
            provider_name: Name of the data provider
            logger: Logger instance for this scraper
            use_zenrows: Whether to use ZenRows for enhanced scraping with JS rendering
        """
        self.provider_name = provider_name
        self.logger = logger
        self.use_zenrows = use_zenrows
        
        if use_zenrows:
            self.logger.info(f"🔧 ZenRows integration enabled for {provider_name}")
        else:
            self.logger.info(f"🌐 Using standard requests for {provider_name}")
    
    def fetch_url(self, url: str, js_render: bool = True, **kwargs) -> str:
        """
        Fetch content from a URL using either ZenRows or standard requests.
        
        This helper method provides a unified interface for fetching web content,
        automatically choosing between ZenRows (for dynamic sites) or standard
        requests based on the scraper configuration.
        
        Args:
            url: The URL to fetch
            js_render: Whether to enable JavaScript rendering (only used with ZenRows)
            **kwargs: Additional parameters for requests or ZenRows
            
        Returns:
            The HTML content as a string, or empty string if request fails
        """
        if self.use_zenrows:
            try:
                # Import ZenRows client
                from src.scraper.utils.zenrows_client import fetch_with_zenrows
                
                self.logger.info(f"🔧 Using ZenRows for rendering: {url}")
                return fetch_with_zenrows(url, js_render=js_render, params=kwargs)
                
            except ImportError:
                self.logger.warning("⚠️ ZenRows client not available, falling back to standard requests")
                return self._fetch_with_requests(url, **kwargs)
            except Exception as e:
                self.logger.error(f"❌ ZenRows request failed: {e}, falling back to standard requests")
                return self._fetch_with_requests(url, **kwargs)
        else:
            return self._fetch_with_requests(url, **kwargs)
    
    def _fetch_with_requests(self, url: str, **kwargs) -> str:
        """
        Fetch content using standard requests library.
        
        Args:
            url: The URL to fetch
            **kwargs: Additional parameters for requests
            
        Returns:
            The HTML content as a string, or empty string if request fails
        """
        try:
            self.logger.info(f"🌐 Fetching with standard requests: {url}")
            
            # Set default headers if not provided
            headers = kwargs.pop('headers', {})
            if not headers:
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
                }
            
            response = requests.get(url, headers=headers, timeout=30, **kwargs)
            response.raise_for_status()
            
            self.logger.info(f"✅ Successfully fetched content (status: {response.status_code})")
            return response.text
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"❌ Request failed for {url}: {e}")
            return ""
        except Exception as e:
            self.logger.error(f"❌ Unexpected error fetching {url}: {e}")
            return ""
    
    @abstractmethod
    def fetch_raw_data(self, *args, **kwargs) -> List[Dict[str, Any]]:
        """
        Fetch raw data from the provider's API or website.
        
        This method should handle the actual data retrieval from the provider's
        data source. Implementation details will vary by provider.
        
        Args:
            *args: Variable length argument list for provider-specific parameters
            **kwargs: Arbitrary keyword arguments for provider-specific parameters
            
        Returns:
            List of dictionaries containing raw data from the provider
            
        Raises:
            NotImplementedError: Must be implemented by child classes
        """
        pass
    
    @abstractmethod
    def parse_data(self, raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Parse and normalize raw data into a standardized format.
        
        This method should transform the raw data from the provider into a
        consistent format that matches the master team index schema.
        
        Args:
            raw_data: List of dictionaries containing raw data from fetch_raw_data
            
        Returns:
            List of dictionaries with standardized team data fields:
            - team_name: Full team name
            - age_group: Age group (e.g., "U11", "U12")
            - gender: Gender category ("Male", "Female", "Coed")
            - source: Provider name
            
        Raises:
            NotImplementedError: Must be implemented by child classes
        """
        pass
    
    def save_to_csv(self, data: List[Dict[str, Any]], path: Path) -> None:
        """
        Save parsed data to a CSV file.
        
        This helper method handles the common task of saving scraped data
        to a CSV file with proper error handling and logging.
        
        Args:
            data: List of dictionaries containing parsed team data
            path: Path where the CSV file should be saved
            
        Raises:
            FileNotFoundError: If the directory path doesn't exist
            PermissionError: If the file cannot be written due to permissions
            Exception: For other unexpected errors during file writing
        """
        try:
            # Ensure the directory exists
            path.parent.mkdir(parents=True, exist_ok=True)
            
            # Convert data to DataFrame and save
            df = pd.DataFrame(data)
            df.to_csv(path, index=False)
            
            self.logger.info(f"✅ Successfully saved {len(data)} records to {path}")
            
        except FileNotFoundError as e:
            self.logger.error(f"❌ Directory not found for path {path}: {e}")
            raise
        except PermissionError as e:
            self.logger.error(f"❌ Permission denied writing to {path}: {e}")
            raise
        except Exception as e:
            self.logger.error(f"❌ Unexpected error saving data to {path}: {e}")
            raise
    
    def run(self) -> None:
        """
        Orchestrate the complete scraping workflow.
        
        This method coordinates the entire scraping process by calling
        fetch_raw_data, parse_data, and save_to_csv in sequence. It includes
        comprehensive error handling and logging throughout the process.
        
        The method is designed to be called by child classes or external
        orchestrators to execute the full scraping pipeline.
        
        Raises:
            Exception: Re-raises any exceptions from the scraping process
        """
        try:
            self.logger.info(f"Scraping started for provider: {self.provider_name}")
            
            # Step 1: Fetch raw data
            self.logger.info(f"📡 Fetching raw data from {self.provider_name}")
            raw_data = self.fetch_raw_data()
            
            if not raw_data:
                self.logger.warning(f"⚠️ No raw data retrieved from {self.provider_name}")
                return
            
            self.logger.info(f"📊 Retrieved {len(raw_data)} raw records from {self.provider_name}")
            
            # Step 2: Parse and normalize data
            self.logger.info(f"🔧 Parsing and normalizing data from {self.provider_name}")
            parsed_data = self.parse_data(raw_data)
            
            if not parsed_data:
                self.logger.warning(f"⚠️ No data after parsing from {self.provider_name}")
                return
            
            self.logger.info(f"✅ Successfully parsed {len(parsed_data)} records from {self.provider_name}")
            
            # Step 3: Save to CSV (this will be handled by the orchestrator)
            self.logger.info(f"🏁 Completed scraping workflow for {self.provider_name}")
            
            # Return the parsed data for the orchestrator to handle saving
            return parsed_data
            
        except Exception as e:
            self.logger.error(f"❌ Scraping failed for {self.provider_name}: {e}")
            raise
