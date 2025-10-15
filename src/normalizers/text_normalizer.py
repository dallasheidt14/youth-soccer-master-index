#!/usr/bin/env python3
"""
Canonical Name Normalizer

Normalizes soccer team names for consistent identity matching across different
data sources. Removes common stopwords and special characters to enable better
fuzzy matching and duplicate detection.
"""

import re
from typing import Optional


# Common soccer team stopwords to remove for normalization
STOPWORDS = {
    "fc", "sc", "academy", "united", "club", "soccer", "football",
    "athletic", "association", "organization", "team", "sports",
    "youth", "junior", "senior", "elite", "premier", "select",
    "travel", "competitive", "recreation", "recreational"
}


def normalize_name(name: str) -> str:
    """
    Normalize soccer team names for consistent identity matching.
    
    Performs the following normalization steps:
    1. Convert to lowercase
    2. Remove special characters (keep alphanumeric + spaces)
    3. Remove common soccer stopwords
    4. Strip and collapse multiple spaces
    
    Args:
        name: Raw team name string
        
    Returns:
        Normalized team name string
        
    Example:
        >>> normalize_name("FC Elite AZ United Soccer Club")
        'elite az'
        >>> normalize_name("SC Premier Academy 2010")
        'premier 2010'
    """
    if not name or not isinstance(name, str):
        return ""
    
    # Convert to lowercase
    normalized = name.lower().strip()
    
    # Remove special characters, keep alphanumeric and spaces
    normalized = re.sub(r"[^a-z0-9\s]", "", normalized)
    
    # Split into tokens and filter out stopwords
    tokens = normalized.split()
    filtered_tokens = [token for token in tokens if token not in STOPWORDS]
    
    # Join tokens and collapse multiple spaces
    result = " ".join(filtered_tokens)
    
    # Final cleanup - remove extra spaces
    result = re.sub(r"\s+", " ", result).strip()
    
    return result


def normalize_name_with_year(name: str) -> tuple[str, Optional[str]]:
    """
    Normalize team name and extract year if present.
    
    Args:
        name: Raw team name string
        
    Returns:
        Tuple of (normalized_name, extracted_year)
        
    Example:
        >>> normalize_name_with_year("FC Elite AZ 2010 Boys")
        ('elite az boys', '2010')
        >>> normalize_name_with_year("Premier Soccer Club")
        ('premier', None)
    """
    if not name or not isinstance(name, str):
        return "", None
    
    # Extract year (4 digits)
    year_match = re.search(r'\b(19|20)\d{2}\b', name)
    extracted_year = year_match.group(0) if year_match else None
    
    # Remove year from name before normalizing
    name_without_year = re.sub(r'\b(19|20)\d{2}\b', '', name)
    
    # Normalize the name without year
    normalized = normalize_name(name_without_year)
    
    return normalized, extracted_year


def similarity_score(name1: str, name2: str) -> float:
    """
    Calculate similarity score between two normalized team names.
    
    Uses simple token-based similarity (Jaccard similarity).
    
    Args:
        name1: First team name
        name2: Second team name
        
    Returns:
        Similarity score between 0.0 and 1.0
    """
    norm1 = normalize_name(name1)
    norm2 = normalize_name(name2)
    
    if not norm1 or not norm2:
        return 0.0
    
    tokens1 = set(norm1.split())
    tokens2 = set(norm2.split())
    
    intersection = len(tokens1.intersection(tokens2))
    union = len(tokens1.union(tokens2))
    
    return intersection / union if union > 0 else 0.0

def is_likely_same_team(name1: str, name2: str, threshold: float = 0.7) -> bool:
    """
    Determine if two team names likely refer to the same team.
    
    Args:
        name1: First team name
        name2: Second team name
        threshold: Similarity threshold (default 0.7)
        
    Returns:
        True if names are likely the same team
    """
    return similarity_score(name1, name2) >= threshold


if __name__ == "__main__":
    # Test the name normalizer
    test_names = [
        "FC Elite AZ United Soccer Club",
        "SC Premier Academy 2010",
        "Elite AZ FC",
        "Premier Soccer Club",
        "AZ Elite United",
        "Soccer Club Premier",
        "FC Barcelona Academy",
        "Real Madrid CF",
        "Manchester United FC",
        "Chelsea Football Club"
    ]
    
    print("Testing name normalizer:")
    print("-" * 60)
    
    for name in test_names:
        normalized = normalize_name(name)
        normalized_with_year, year = normalize_name_with_year(name)
        print(f"Original: {name}")
        print(f"Normalized: {normalized}")
        print(f"With year extraction: {normalized_with_year} (year: {year})")
        print()
    
    # Test similarity
    print("Testing similarity:")
    print("-" * 30)
    
    pairs = [
        ("FC Elite AZ", "Elite AZ FC"),
        ("Premier Soccer Club", "SC Premier"),
        ("Manchester United FC", "Man United"),
        ("Real Madrid CF", "Real Madrid Club")
    ]
    
    for name1, name2 in pairs:
        score = similarity_score(name1, name2)
        is_same = is_likely_same_team(name1, name2)
        print(f"{name1} vs {name2}: {score:.2f} ({'SAME' if is_same else 'DIFFERENT'})")

