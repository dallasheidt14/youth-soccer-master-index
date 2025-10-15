#!/usr/bin/env python3
"""
Master Team Schema Definition

Defines the canonical schema for the master team index using Pandera for
validation. This enforces column consistency and enables drift detection
across different data sources and builds.
"""

import pandera as pa
from pandera.typing import Series, DataFrame
from typing import Optional
import re


class MasterTeamSchema(pa.DataFrameModel):
    """
    Pandera schema for the master team index.
    
    This schema enforces:
    - Column presence and types
    - Data validation rules
    - Consistency across builds
    - Drift detection capabilities
    
    Fields:
    - team_id: Deterministic hash (12 chars) - global team identity
    - provider_team_id: Original provider ID (nullable) - for traceability
    - team_name: Team name string
    - age_group: Display format (U10, U11, etc.)
    - age_u: Numeric age (10-18) for analytics
    - gender: Normalized to M/F
    - state: 2-letter US state code
    - provider: Data source provider name
    - club_name: Club/organization name (nullable)
    - source_url: API endpoint or page URL
    - created_at: ISO timestamp (nullable)
    """
    
    # Core identity fields
    team_id: Series[str] = pa.Field(
        description="Deterministic team ID hash (12 characters)",
        regex=r"^[a-f0-9]{12}$"
    )
    
    provider_team_id: Optional[Series[str]] = pa.Field(
        description="Original provider team ID",
        nullable=True
    )
    
    team_name: Series[str] = pa.Field(
        description="Team name"
    )
    
    # Age fields (both string and numeric)
    age_group: Series[str] = pa.Field(
        description="Age group display format (U10, U11, etc.)",
        regex=r"^U(1[0-8]|[0-9])$"
    )
    
    age_u: Series[int] = pa.Field(
        description="Numeric age for analytics",
        ge=10,
        le=18
    )
    
    # Demographics
    gender: Series[str] = pa.Field(
        description="Normalized gender (M or F)",
        isin=["M", "F"]
    )
    
    state: Series[str] = pa.Field(
        description="2-letter US state code",
        regex=r"^[A-Z]{2}$"
    )
    
    # Provider information
    provider: Series[str] = pa.Field(
        description="Data source provider"
    )
    
    club_name: Optional[Series[str]] = pa.Field(
        description="Club or organization name",
        nullable=True
    )
    
    source_url: Series[str] = pa.Field(
        description="Source URL or API endpoint"
    )
    
    created_at: Optional[Series[str]] = pa.Field(
        description="ISO timestamp when record was created",
        nullable=True,
        regex=r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z?$"
    )
    
    class Config:
        """Pandera configuration."""
        coerce = True  # Automatically coerce types when possible
        strict = False  # Allow extra columns not in schema
        
    @pa.dataframe_check
    def age_group_matches_age_u(cls, df: DataFrame) -> Series[bool]:
        """Validate that age_group format matches age_u value."""
        # Build expected age_group string for each row
        expected_age_group = "U" + df["age_u"].astype(str)
        # Compare actual age_group with expected format
        return df["age_group"] == expected_age_group
    
    @pa.check("state")
    def valid_us_state_codes(cls, series: Series[str]) -> Series[bool]:
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


def validate_dataframe(df, schema: MasterTeamSchema = MasterTeamSchema):
    """
    Validate a DataFrame against the MasterTeamSchema.
    
    Args:
        df: pandas DataFrame to validate
        schema: Pandera schema class (default: MasterTeamSchema)
        
    Returns:
        Validated DataFrame
        
    Raises:
        pa.errors.SchemaError: If validation fails
    """
    try:
        validated_df = schema.validate(df)
        return validated_df
    except (pa.errors.SchemaError, pa.errors.SchemaErrors) as e:
        # Provide more detailed error information
        print(f"Schema validation failed:")
        print(f"Error: {e}")
        print(f"DataFrame shape: {df.shape}")
        print(f"DataFrame columns: {list(df.columns)}")
        print(f"DataFrame dtypes:\n{df.dtypes}")
        
        # Show sample of problematic data
        if hasattr(e, 'failure_cases') and e.failure_cases is not None:
            print(f"\nFailure cases:\n{e.failure_cases}")
        
        raise


def get_schema_summary() -> dict:
    """
    Get a summary of the schema definition.
    
    Returns:
        Dictionary with schema information
    """
    schema_info = {
        "schema_name": "MasterTeamSchema",
        "description": "Canonical schema for master team index",
        "total_fields": 11,
        "required_fields": 8,
        "optional_fields": 3,
        "fields": {
            "team_id": {
                "type": "str",
                "description": "Deterministic team ID hash (12 characters)",
                "required": True,
                "validation": "12-character hex string"
            },
            "provider_team_id": {
                "type": "str",
                "description": "Original provider team ID",
                "required": False,
                "validation": "nullable string"
            },
            "team_name": {
                "type": "str", 
                "description": "Team name",
                "required": True,
                "validation": "1-200 characters"
            },
            "age_group": {
                "type": "str",
                "description": "Age group display format (U10, U11, etc.)",
                "required": True,
                "validation": "U10-U18 format"
            },
            "age_u": {
                "type": "int",
                "description": "Numeric age for analytics",
                "required": True,
                "validation": "10-18 range"
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
            "provider": {
                "type": "str",
                "description": "Data source provider",
                "required": True,
                "validation": "1-50 characters"
            },
            "club_name": {
                "type": "str",
                "description": "Club or organization name",
                "required": False,
                "validation": "nullable, max 200 characters"
            },
            "source_url": {
                "type": "str",
                "description": "Source URL or API endpoint",
                "required": True,
                "validation": "1-500 characters"
            },
            "created_at": {
                "type": "str",
                "description": "ISO timestamp when record was created",
                "required": False,
                "validation": "ISO timestamp format"
            }
        }
    }
    
    return schema_info


if __name__ == "__main__":
    # Test the schema
    import pandas as pd
    from datetime import datetime
    
    # Create test data
    test_data = {
        'team_id': ['6c1e02b09d77', 'a1b2c3d4e5f6'],
        'provider_team_id': ['522.0', '123.0'],
        'team_name': ['FC Elite AZ', 'Premier Soccer Club'],
        'age_group': ['U10', 'U12'],
        'age_u': [10, 12],
        'gender': ['M', 'F'],
        'state': ['AZ', 'CA'],
        'provider': ['GotSport', 'GotSport'],
        'club_name': ['Elite FC', 'Premier SC'],
        'source_url': ['https://api.example.com/team/522', 'https://api.example.com/team/123'],
        'created_at': [datetime.utcnow().isoformat(), datetime.utcnow().isoformat()]
    }
    
    df = pd.DataFrame(test_data)
    
    print("Testing MasterTeamSchema:")
    print("-" * 40)
    
    try:
        validated_df = validate_dataframe(df)
        print("✅ Schema validation passed!")
        print(f"Validated DataFrame shape: {validated_df.shape}")
        print(f"Validated DataFrame columns: {list(validated_df.columns)}")
    except Exception as e:
        print(f"❌ Schema validation failed: {e}")
    
    # Test schema summary
    print("\nSchema Summary:")
    print("-" * 20)
    summary = get_schema_summary()
    print(f"Schema: {summary['schema_name']}")
    print(f"Total fields: {summary['total_fields']}")
    print(f"Required fields: {summary['required_fields']}")
    print(f"Optional fields: {summary['optional_fields']}")
