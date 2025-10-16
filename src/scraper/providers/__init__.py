#!/usr/bin/env python3
"""
Scraper Providers Module

Registry for all data providers (team rankings and game history).
"""

from .gotsport_scraper import GotSportScraper
from .gotsport_games import GotSportGameProvider

# Team ranking providers
TEAM_PROVIDERS = {
    "gotsport": GotSportScraper
}

# Game history providers  
GAME_PROVIDERS = {
    "gotsport": GotSportGameProvider
}

def get_provider(provider_name: str, provider_type: str = "team"):
    """
    Get a provider instance by name and type.
    
    Args:
        provider_name: Name of the provider (e.g., 'gotsport')
        provider_type: Type of provider ('team' or 'game')
        
    Returns:
        Provider class
        
    Raises:
        ValueError: If provider not found
    """
    if provider_type == "team":
        providers = TEAM_PROVIDERS
    elif provider_type == "game":
        providers = GAME_PROVIDERS
    else:
        raise ValueError(f"Invalid provider type: {provider_type}")
    
    if provider_name not in providers:
        available = list(providers.keys())
        raise ValueError(f"Provider '{provider_name}' not found. Available: {available}")
    
    return providers[provider_name]

def list_providers(provider_type: str = "all"):
    """
    List available providers.
    
    Args:
        provider_type: Type of providers to list ('team', 'game', or 'all')
        
    Returns:
        Dictionary of provider names and classes
    """
    if provider_type == "team":
        return TEAM_PROVIDERS
    elif provider_type == "game":
        return GAME_PROVIDERS
    elif provider_type == "all":
        return {"team": TEAM_PROVIDERS, "game": GAME_PROVIDERS}
    else:
        raise ValueError(f"Invalid provider type: {provider_type}")
