#!/usr/bin/env python3
"""
Game History Schema Definition

Defines the canonical schema for game history data using Pandera for validation.
This enforces column consistency and enables drift detection across different
data sources and builds.
"""

import pandera as pa
from pandera.typing import Series, DataFrame
from typing import Optional
import re
import pandas as pd
import logging


def sanitize_error_message(error_msg: str, max_length: int = 500) -> str:
    """
    Sanitize error messages to prevent data leakage.
    
    Args:
        error_msg: Original error message
        max_length: Maximum length for the sanitized message
        
    Returns:
        Sanitized error message
    """
    # Replace sensitive patterns with placeholders
    sanitized = error_msg
    
    # Replace URLs with placeholder
    sanitized = re.sub(r'https?://[^\s]+', '[URL_REDACTED]', sanitized)
    
    # Replace team names with placeholder
    sanitized = re.sub(r'[A-Za-z\s]+(?:SC|FC|United|Academy|Club|Soccer|Futbol)', '[TEAM_NAME_REDACTED]', sanitized)
    
    # Replace numeric IDs with placeholder
    sanitized = re.sub(r'\b\d{6,}\b', '[ID_REDACTED]', sanitized)
    
    # Replace timestamps with placeholder
    sanitized = re.sub(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z?', '[TIMESTAMP_REDACTED]', sanitized)
    
    # Truncate if too long
    if len(sanitized) > max_length:
        sanitized = sanitized[:max_length-50] + "... and more failures"
    
    return sanitized


class GameHistorySchema(pa.DataFrameModel):
    """
    Pandera schema for game history data.
    
    This schema enforces:
    - Column presence and types
    - Data validation rules
    - Consistency across builds
    - Drift detection capabilities
    """
    
    # Core identity fields
    provider: Series[str] = pa.Field(
        description="Data source provider name"
    )
    
    team_id_source: Series[str] = pa.Field(
        description="Original provider team ID"
    )
    
    team_id_master: Series[str] = pa.Field(
        description="Master team index ID (12-character hash)"
    )
    
    team_name: Series[str] = pa.Field(
        description="Team name"
    )
    
    club_name: Optional[Series[str]] = pa.Field(
        description="Club or organization name",
        nullable=True
    )
    
    # Opponent information
    opponent_name: Series[str] = pa.Field(
        description="Opponent team name"
    )
    
    opponent_id: Optional[Series[str]] = pa.Field(
        description="Opponent team ID",
        nullable=True
    )
    
    # Game metadata
    age_group: Series[str] = pa.Field(
        description="Age group display format (U10, U11, etc.)",
        regex=r"^U(1[0-8]|[0-9])$"
    )
    
    gender: Series[str] = pa.Field(
        description="Normalized gender (M or F)",
        isin=["M", "F"]
    )
    
    state: Series[str] = pa.Field(
        description="2-letter US state code",
        regex=r"^[A-Z]{2}$"
    )
    
    # Game details
    game_date: Series[str] = pa.Field(
        description="Game date in YYYY-MM-DD format",
        regex=r"^\d{4}-\d{2}-\d{2}$"
    )
    
    home_away: Series[str] = pa.Field(
        description="Home or away indicator",
        isin=["H", "A"]
    )
    
    goals_for: Optional[Series[pd.Int64Dtype()]] = pa.Field(
        description="Goals scored by team",
        nullable=True,
        ge=0
    )
    
    goals_against: Optional[Series[pd.Int64Dtype()]] = pa.Field(
        description="Goals scored by opponent",
        nullable=True,
        ge=0
    )
    
    result: Series[str] = pa.Field(
        description="Game result",
        isin=["W", "L", "D", "U"]  # Win, Loss, Draw, Unknown
    )
    
    # Competition and venue
    competition: Optional[Series[str]] = pa.Field(
        description="Competition or league name",
        nullable=True
    )
    
    venue: Optional[Series[str]] = pa.Field(
        description="Game venue or location",
        nullable=True
    )
    
    city: Optional[Series[str]] = pa.Field(
        description="City where game was played",
        nullable=True
    )
    
    # Source tracking
    source_url: Series[str] = pa.Field(
        description="Source URL where game data was found"
    )
    
    scraped_at: Series[str] = pa.Field(
        description="ISO timestamp when data was scraped",
        regex=r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z?$"
    )
    
    class Config:
        """Pandera configuration."""
        coerce = True  # Automatically coerce types when possible
        strict = False  # Allow extra columns not in schema
    
    @pa.dataframe_check
    def valid_game_date_format(cls, df: DataFrame) -> Series[bool]:
        """Validate that game_date is in correct format."""
        return valid_game_date_format_check(df)
    
    @pa.check("state")
    def valid_us_state_codes(cls, series: Series[str]) -> Series[bool]:
        """Validate that state codes are valid US states."""
        return valid_us_state_codes_check(series)


# Custom validation functions
def valid_game_date_format_check(df: DataFrame) -> Series[bool]:
    """Validate that game_date is in correct format."""
    try:
        # Try to parse dates to ensure they're valid
        pd.to_datetime(df["game_date"], format="%Y-%m-%d")
        return Series([True] * len(df), index=df.index)
    except (ValueError, TypeError):
        return Series([False] * len(df), index=df.index)
    except Exception as e:
        # Re-raise unexpected exceptions
        raise e


def valid_us_state_codes_check(series: Series[str]) -> Series[bool]:
    """Validate that state codes are valid US states."""
    # US state codes (50 states + DC)
    valid_states = {
        'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA',
        'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
        'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
        'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
        'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY',
        'DC'  # District of Columbia
    }
    
    return series.isin(valid_states)


class ClubLookupSchema(pa.DataFrameModel):
    """
    Pandera schema for club lookup data.
    
    This schema enforces consistency for club information extracted from games.
    """
    
    provider: Series[str] = pa.Field(
        description="Data source provider name"
    )
    
    club_id: Optional[Series[str]] = pa.Field(
        description="Club ID if available",
        nullable=True
    )
    
    club_name: Series[str] = pa.Field(
        description="Club or organization name"
    )
    
    state: Series[str] = pa.Field(
        description="2-letter US state code",
        regex=r"^[A-Z]{2}$"
    )
    
    city: Optional[Series[str]] = pa.Field(
        description="City where club is located",
        nullable=True
    )
    
    website: Optional[Series[str]] = pa.Field(
        description="Club website URL",
        nullable=True
    )
    
    first_seen_at: Series[str] = pa.Field(
        description="ISO timestamp when club was first seen",
        regex=r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z?$"
    )
    
    last_seen_at: Series[str] = pa.Field(
        description="ISO timestamp when club was last seen",
        regex=r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z?$"
    )
    
    source_url: Series[str] = pa.Field(
        description="Source URL where club data was found"
    )
    
    class Config:
        """Pandera configuration."""
        coerce = True
        strict = False


def validate_games_dataframe(df, schema: GameHistorySchema = GameHistorySchema):
    """
    Validate a DataFrame against the GameHistorySchema.
    
    Args:
        df: pandas DataFrame to validate
        schema: Pandera schema class (default: GameHistorySchema)
        
    Returns:
        Validated DataFrame
        
    Raises:
        pa.errors.SchemaError: If validation fails
    """
    logger = logging.getLogger(__name__)
    
    try:
        # Create a copy to avoid modifying the original DataFrame
        df_clean = df.copy()
        
        # Handle nullable integer fields properly
        if 'goals_for' in df_clean.columns:
            # Convert to numeric, coercing errors to NaN, then to nullable Int64
            df_clean['goals_for'] = pd.to_numeric(df_clean['goals_for'], errors='coerce')
            df_clean['goals_for'] = df_clean['goals_for'].astype('Int64')
            
        if 'goals_against' in df_clean.columns:
            # Convert to numeric, coercing errors to NaN, then to nullable Int64
            df_clean['goals_against'] = pd.to_numeric(df_clean['goals_against'], errors='coerce')
            df_clean['goals_against'] = df_clean['goals_against'].astype('Int64')
        
        # Log statistics about missing data
        if 'goals_for' in df_clean.columns:
            missing_goals_for = df_clean['goals_for'].isna().sum()
            logger.info(f"Found {missing_goals_for} rows with missing goals_for data")
            
        if 'goals_against' in df_clean.columns:
            missing_goals_against = df_clean['goals_against'].isna().sum()
            logger.info(f"Found {missing_goals_against} rows with missing goals_against data")
        
        validated_df = schema.validate(df_clean)
        return validated_df
    except (pa.errors.SchemaError, pa.errors.SchemaErrors) as e:
        # Log detailed error information
        logger.exception("Game history schema validation failed")
        logger.debug(f"DataFrame shape: {df.shape}")
        logger.debug(f"DataFrame columns: {list(df.columns)}")
        logger.debug(f"DataFrame dtypes:\n{df.dtypes}")
        
        # Show sample of problematic data (sanitized)
        if hasattr(e, 'failure_cases') and e.failure_cases is not None:
            failure_cases_str = str(e.failure_cases)
            sanitized_failures = sanitize_error_message(failure_cases_str)
            logger.debug(f"Failure cases (sanitized):\n{sanitized_failures}")
        
        # Create a sanitized exception message
        error_msg = str(e)
        sanitized_error = sanitize_error_message(error_msg)
        
        # Re-raise with sanitized message
        raise pa.errors.SchemaError(data=None, message=sanitized_error) from e


def validate_club_lookup_dataframe(df, schema: ClubLookupSchema = ClubLookupSchema):
    """
    Validate a DataFrame against the ClubLookupSchema.
    
    Args:
        df: pandas DataFrame to validate
        schema: Pandera schema class (default: ClubLookupSchema)
        
    Returns:
        Validated DataFrame
        
    Raises:
        pa.errors.SchemaError: If validation fails
    """
    logger = logging.getLogger(__name__)
    
    try:
        validated_df = schema.validate(df)
        return validated_df
    except (pa.errors.SchemaError, pa.errors.SchemaErrors) as e:
        # Log detailed error information
        logger.exception("Club lookup schema validation failed")
        logger.debug(f"DataFrame shape: {df.shape}")
        logger.debug(f"DataFrame columns: {list(df.columns)}")
        logger.debug(f"DataFrame dtypes:\n{df.dtypes}")
        
        # Show sample of problematic data (sanitized)
        if hasattr(e, 'failure_cases') and e.failure_cases is not None:
            failure_cases_str = str(e.failure_cases)
            sanitized_failures = sanitize_error_message(failure_cases_str)
            logger.debug(f"Failure cases (sanitized):\n{sanitized_failures}")
        
        # Create a sanitized exception message
        error_msg = str(e)
        sanitized_error = sanitize_error_message(error_msg)
        
        # Re-raise with sanitized message
        raise pa.errors.SchemaError(data=None, message=sanitized_error) from e


def get_games_schema_summary() -> dict:
    """
    Get a summary of the games schema definition.
    
    Returns:
        Dictionary with schema information
    """
    schema_info = {
        "schema_name": "GameHistorySchema",
        "description": "Canonical schema for game history data",
        "total_fields": 20,
        "required_fields": 12,
        "optional_fields": 8,
        "fields": {
            "provider": {
                "type": "str",
                "description": "Data source provider name",
                "required": True
            },
            "team_id_source": {
                "type": "str",
                "description": "Original provider team ID",
                "required": True
            },
            "team_id_master": {
                "type": "str",
                "description": "Master team index ID (12-character hash)",
                "required": True
            },
            "team_name": {
                "type": "str",
                "description": "Team name",
                "required": True
            },
            "club_name": {
                "type": "str",
                "description": "Club or organization name",
                "required": False
            },
            "opponent_name": {
                "type": "str",
                "description": "Opponent team name",
                "required": True
            },
            "opponent_id": {
                "type": "str",
                "description": "Opponent team ID",
                "required": False
            },
            "age_group": {
                "type": "str",
                "description": "Age group display format (U10, U11, etc.)",
                "required": True,
                "validation": "U10-U18 format"
            },
            "gender": {
                "type": "str",
                "description": "Normalized gender (M or F)",
                "required": True,
                "validation": "M or F only"
            },
            "state": {
                "type": "str",
                "description": "2-letter US state code",
                "required": True,
                "validation": "Valid US state codes"
            },
            "game_date": {
                "type": "str",
                "description": "Game date in YYYY-MM-DD format",
                "required": True,
                "validation": "YYYY-MM-DD format"
            },
            "home_away": {
                "type": "str",
                "description": "Home or away indicator",
                "required": True,
                "validation": "H or A only"
            },
            "goals_for": {
                "type": "int",
                "description": "Goals scored by team",
                "required": False,
                "validation": "Non-negative integer"
            },
            "goals_against": {
                "type": "int",
                "description": "Goals scored by opponent",
                "required": False,
                "validation": "Non-negative integer"
            },
            "result": {
                "type": "str",
                "description": "Game result",
                "required": True,
                "validation": "W, L, D, or U"
            },
            "competition": {
                "type": "str",
                "description": "Competition or league name",
                "required": False
            },
            "venue": {
                "type": "str",
                "description": "Game venue or location",
                "required": False
            },
            "city": {
                "type": "str",
                "description": "City where game was played",
                "required": False
            },
            "source_url": {
                "type": "str",
                "description": "Source URL where game data was found",
                "required": True
            },
            "scraped_at": {
                "type": "str",
                "description": "ISO timestamp when data was scraped",
                "required": True,
                "validation": "ISO timestamp format"
            }
        }
    }
    
    return schema_info


# Column definitions for easy reference
GAMES_COLUMNS = [
    "provider", "team_id_source", "team_id_master", "team_name", "club_name",
    "opponent_name", "opponent_id", "age_group", "gender", "state",
    "game_date", "home_away", "goals_for", "goals_against", "result",
    "competition", "venue", "city", "source_url", "scraped_at"
]

CLUB_COLUMNS = [
    "provider", "club_id", "club_name", "state", "city", "website",
    "first_seen_at", "last_seen_at", "source_url"
]


if __name__ == "__main__":
    # Test the schema
    import pandas as pd
    from datetime import datetime, timezone
    
    # Create test data
    test_data = {
        'provider': ['gotsport', 'gotsport'],
        'team_id_source': ['123', '456'],
        'team_id_master': ['6c1e02b09d77', 'a1b2c3d4e5f6'],
        'team_name': ['FC Elite AZ', 'Premier Soccer Club'],
        'club_name': ['Elite FC', 'Premier SC'],
        'opponent_name': ['Rival Team', 'Competitor FC'],
        'opponent_id': ['789', '101'],
        'age_group': ['U10', 'U12'],
        'gender': ['M', 'F'],
        'state': ['AZ', 'CA'],
        'game_date': ['2024-10-15', '2024-10-16'],
        'home_away': ['H', 'A'],
        'goals_for': [2, 1],
        'goals_against': [1, 3],
        'result': ['W', 'L'],
        'competition': ['League A', 'League B'],
        'venue': ['Stadium 1', 'Stadium 2'],
        'city': ['Phoenix', 'Los Angeles'],
        'source_url': ['https://api.example.com/game/1', 'https://api.example.com/game/2'],
        'scraped_at': [datetime.now(timezone.utc).isoformat(), datetime.now(timezone.utc).isoformat()]
    }
    
    df = pd.DataFrame(test_data)
    
    print("Testing GameHistorySchema:")
    print("-" * 40)
    
    try:
        validated_df = validate_games_dataframe(df)
        print("Schema validation passed!")
        print(f"Validated DataFrame shape: {validated_df.shape}")
        print(f"Validated DataFrame columns: {list(validated_df.columns)}")
    except Exception as e:
        print(f"Schema validation failed: {e}")
    
    # Test schema summary
    print("\nSchema Summary:")
    print("-" * 20)
    summary = get_games_schema_summary()
    print(f"Schema: {summary['schema_name']}")
    print(f"Total fields: {summary['total_fields']}")
    print(f"Required fields: {summary['required_fields']}")
    print(f"Optional fields: {summary['optional_fields']}")
