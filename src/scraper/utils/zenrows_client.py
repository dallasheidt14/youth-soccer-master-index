"""
zenrows_client.py
-----------------
ZenRows API client for enhanced web scraping with JavaScript rendering support.

This module provides a utility function to interact with ZenRows API, which offers
advanced web scraping capabilities including JavaScript rendering, proxy rotation,
and anti-bot protection bypass for dynamic websites.

The client handles API key management, request formatting, and error handling
to provide a seamless integration with the scraper framework.
"""

import os
import requests
import logging
from typing import Dict, Any, Optional
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


def fetch_with_zenrows(
    url: str, 
    js_render: bool = True, 
    params: Optional[Dict[str, Any]] = None
) -> str:
    """
    Fetch web content using ZenRows API with optional JavaScript rendering.
    
    This function provides enhanced web scraping capabilities through ZenRows,
    including JavaScript rendering for dynamic websites, proxy rotation,
    and anti-bot protection bypass.
    
    Args:
        url: The target URL to scrape
        js_render: Whether to enable JavaScript rendering (default: True)
        params: Additional ZenRows parameters to pass in the request
        
    Returns:
        The HTML content of the page as a string, or empty string if request fails
        
    Raises:
        ValueError: If ZenRows API key is not configured
        requests.RequestException: For HTTP-related errors
        Exception: For other unexpected errors
    """
    # Get logger for this module
    logger = logging.getLogger("zenrows-client")
    
    try:
        # Load ZenRows API key from environment
        api_key = os.getenv("ZENROWS_API_KEY")
        if not api_key:
            logger.error("❌ ZENROWS_API_KEY not found in environment variables")
            raise ValueError("ZenRows API key not configured. Please set ZENROWS_API_KEY in your .env file")
        
        # Prepare ZenRows API endpoint
        zenrows_url = "https://api.zenrows.com/v1/"
        
        # Build query parameters
        query_params = {
            "url": url,
            "apikey": api_key,
            "js_render": "true" if js_render else "false"
        }
        
        # Add any additional parameters
        if params:
            query_params.update(params)
        
        logger.info(f"🌐 Fetching URL with ZenRows: {url}")
        logger.info(f"🔧 JavaScript rendering: {'enabled' if js_render else 'disabled'}")
        
        # Make request to ZenRows API
        response = requests.get(
            zenrows_url,
            params=query_params,
            timeout=30,  # 30 second timeout
            headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
        )
        
        # Check for successful response
        response.raise_for_status()
        
        logger.info(f"✅ Successfully fetched content via ZenRows (status: {response.status_code})")
        logger.info(f"📊 Response size: {len(response.text)} characters")
        
        return response.text
        
    except requests.exceptions.Timeout:
        logger.error(f"⏰ ZenRows request timed out for URL: {url}")
        return ""
    except requests.exceptions.ConnectionError:
        logger.error(f"🔌 Connection error with ZenRows API for URL: {url}")
        return ""
    except requests.exceptions.HTTPError as e:
        logger.error(f"🌐 HTTP error from ZenRows API: {e.response.status_code} - {e}")
        return ""
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Request error with ZenRows API: {e}")
        return ""
    except ValueError as e:
        logger.error(f"🔑 Configuration error: {e}")
        return ""
    except Exception as e:
        logger.error(f"❌ Unexpected error with ZenRows API: {e}")
        return ""


def test_zenrows_connection() -> bool:
    """
    Test the ZenRows API connection and configuration.
    
    Returns:
        True if connection is successful, False otherwise
    """
    logger = logging.getLogger("zenrows-client")
    
    try:
        # Test with a simple URL
        test_url = "https://httpbin.org/html"
        result = fetch_with_zenrows(test_url, js_render=False)
        
        if result and "html" in result.lower():
            logger.info("✅ ZenRows connection test successful")
            return True
        else:
            logger.warning("⚠️ ZenRows connection test failed - no content received")
            return False
            
    except Exception as e:
        logger.error(f"❌ ZenRows connection test failed: {e}")
        return False


def get_zenrows_status() -> Dict[str, Any]:
    """
    Get the current status of ZenRows configuration and connection.
    
    Returns:
        Dictionary containing status information
    """
    api_key = os.getenv("ZENROWS_API_KEY")
    
    status = {
        "api_key_configured": bool(api_key),
        "api_key_length": len(api_key) if api_key else 0,
        "connection_test": False
    }
    
    if api_key:
        status["connection_test"] = test_zenrows_connection()
    
    return status


if __name__ == "__main__":
    # Test the ZenRows client
    import sys
    from pathlib import Path
    
    # Add project root to path for logger import
    sys.path.append(str(Path(__file__).resolve().parents[3]))
    from src.scraper.utils.logger import get_logger
    
    # Set up logging
    BASE_DIR = Path(__file__).resolve().parents[3]
    LOGS_DIR = BASE_DIR / "data" / "logs"
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    
    logger = get_logger(LOGS_DIR / "zenrows_client.log")
    
    print("Testing ZenRows Client")
    print("=" * 50)
    
    # Test status
    status = get_zenrows_status()
    print(f"API Key Configured: {status['api_key_configured']}")
    print(f"API Key Length: {status['api_key_length']}")
    print(f"Connection Test: {'Passed' if status['connection_test'] else 'Failed'}")
    
    if status['api_key_configured']:
        # Test actual fetch
        test_url = "https://httpbin.org/html"
        print(f"\nTesting fetch with: {test_url}")
        
        content = fetch_with_zenrows(test_url, js_render=False)
        if content:
            print(f"Successfully fetched {len(content)} characters")
            print(f"Content preview: {content[:100]}...")
        else:
            print("Failed to fetch content")
    else:
        print("\nPlease configure ZENROWS_API_KEY in your .env file to test")
