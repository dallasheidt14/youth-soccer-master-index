#!/usr/bin/env python3
"""
Deterministic Team ID Generator

Provides provider-agnostic global team identity for merging & tracking across
different data sources. Uses SHA1 hash of normalized team attributes to create
consistent, deterministic team IDs.
"""

import hashlib
import re
from typing import Union


def normalize_gender(gender: Union[str, int, float]) -> str:
    """
    Normalize gender input to standard M/F format.
    
    Args:
        gender: Gender value (string, int, or float)
        
    Returns:
        Normalized gender as "M" or "F"
        
    Raises:
        ValueError: If gender cannot be normalized
    """
    if gender is None:
        raise ValueError("Gender cannot be None")
    
    gender_str = str(gender).strip().lower()
    
    # Handle various formats
    if gender_str in ['m', 'male', '1', '1.0']:
        return "M"
    elif gender_str in ['f', 'female', '0', '0.0']:
        return "F"
    else:
        raise ValueError(f"Cannot normalize gender: {gender}")


def extract_age_from_group(age_group: str) -> int:
    """
    Extract numeric age from age group string.
    
    Args:
        age_group: Age group string like "U10", "U11", "10", etc.
        
    Returns:
        Numeric age as integer
        
    Raises:
        ValueError: If age cannot be extracted or is invalid
    """
    if age_group is None:
        raise ValueError("Age group cannot be None")
    
    age_str = str(age_group).strip().upper()
    
    # Extract number from U10, U11, etc.
    match = re.search(r'(\d+)', age_str)
    if match:
        age = int(match.group(1))
        if 10 <= age <= 18:
            return age
        else:
            raise ValueError(f"Age {age} is outside valid range [10, 18]")
    else:
        raise ValueError(f"Cannot extract age from: {age_group}")


def make_team_id(name: str, state: str, age: Union[str, int], gender: Union[str, int, float]) -> str:
    """
    Generate a deterministic team ID using SHA1 hash.
    
    Creates a provider-agnostic global team identity by hashing normalized
    team attributes. This enables consistent merging and tracking across
    different data sources.
    
    Args:
        name: Team name
        state: US state code (2 letters)
        age: Age group (string like "U10" or integer)
        gender: Gender (string like "Male" or "M")
        
    Returns:
        12-character hexadecimal team ID
        
    Raises:
        ValueError: If any input cannot be normalized
        
    Example:
        >>> make_team_id("FC Elite AZ", "AZ", "U10", "Male")
        '6c1e02b09d77'
    """
    # Normalize inputs
    normalized_name = str(name).strip().lower() if name else ""
    normalized_state = str(state).strip().upper() if state else ""
    
    # Extract numeric age
    if isinstance(age, int):
        age_int = age
    else:
        age_int = extract_age_from_group(str(age))
    
    # Normalize gender
    gender_code = normalize_gender(gender)
    
    # Validate required fields
    if not normalized_name:
        raise ValueError("Team name cannot be empty")
    if not normalized_state:
        raise ValueError("State cannot be empty")
    if len(normalized_state) != 2:
        raise ValueError(f"State must be 2 characters, got: {normalized_state}")
    
    # Create hash input string
    hash_input = f"{normalized_name}|{normalized_state}|{age_int}|{gender_code}"
    
    # Generate SHA1 hash and take first 12 characters
    hash_obj = hashlib.sha1(hash_input.encode('utf-8'))
    team_id = hash_obj.hexdigest()[:12]
    
    return team_id


def batch_make_team_ids(df, name_col='team_name', state_col='state', 
                       age_col='age_group', gender_col='gender') -> list:
    """
    Generate team IDs for an entire DataFrame.
    
    Args:
        df: pandas DataFrame with team data
        name_col: Column name for team names
        state_col: Column name for states
        age_col: Column name for age groups
        gender_col: Column name for genders
        
    Returns:
        List of team IDs corresponding to DataFrame rows
        
    Raises:
        ValueError: If any row cannot be processed
    """
    team_ids = []
    errors = []
    
    for idx, row in df.iterrows():
        try:
            team_id = make_team_id(
                row[name_col],
                row[state_col], 
                row[age_col],
                row[gender_col]
            )
            team_ids.append(team_id)
        except Exception as e:
            errors.append(f"Row {idx}: {e}")
    
    if errors:
        raise ValueError(f"Failed to generate team IDs:\n" + "\n".join(errors))
    
    return team_ids


if __name__ == "__main__":
    # Test the team ID generator
    test_cases = [
        ("FC Elite AZ", "AZ", "U10", "Male"),
        ("FC Elite AZ", "AZ", "U10", "M"),
        ("FC Elite AZ", "AZ", 10, "Male"),
        ("Soccer Club", "CA", "U12", "Female"),
        ("Soccer Club", "CA", "U12", "F"),
    ]
    
    print("Testing team ID generator:")
    print("-" * 50)
    
    for name, state, age, gender in test_cases:
        try:
            team_id = make_team_id(name, state, age, gender)
            print(f"{name} ({state}, {age}, {gender}) -> {team_id}")
        except Exception as e:
            print(f"ERROR: {name} ({state}, {age}, {gender}) -> {e}")

