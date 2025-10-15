#!/usr/bin/env python3
"""
Test suite for MasterTeamSchema validation
"""

import pytest
import pandas as pd
from pathlib import Path
import sys
import os

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent))

from src.schema.master_team_schema import MasterTeamSchema, validate_dataframe, get_schema_summary


class TestMasterTeamSchema:
    """Test cases for MasterTeamSchema validation"""
    
    def test_valid_data_passes_validation(self):
        """Test that valid data passes schema validation"""
        valid_data = {
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
            'created_at': ['2025-10-14T12:00:00Z', '2025-10-14T12:00:00Z']
        }
        
        df = pd.DataFrame(valid_data)
        validated_df = validate_dataframe(df)
        
        assert len(validated_df) == 2
        assert list(validated_df.columns) == list(valid_data.keys())
    
    def test_invalid_team_id_fails_validation(self):
        """Test that invalid team_id format fails validation"""
        invalid_data = {
            'team_id': ['invalid_id', 'a1b2c3d4e5f6'],  # First one is invalid
            'provider_team_id': ['522.0', '123.0'],
            'team_name': ['FC Elite AZ', 'Premier Soccer Club'],
            'age_group': ['U10', 'U12'],
            'age_u': [10, 12],
            'gender': ['M', 'F'],
            'state': ['AZ', 'CA'],
            'provider': ['GotSport', 'GotSport'],
            'club_name': ['Elite FC', 'Premier SC'],
            'source_url': ['https://api.example.com/team/522', 'https://api.example.com/team/123'],
            'created_at': ['2025-10-14T12:00:00Z', '2025-10-14T12:00:00Z']
        }
        
        df = pd.DataFrame(invalid_data)
        
        with pytest.raises(Exception):  # Should raise validation error
            validate_dataframe(df)
    
    def test_invalid_age_range_fails_validation(self):
        """Test that invalid age range fails validation"""
        invalid_data = {
            'team_id': ['6c1e02b09d77', 'a1b2c3d4e5f6'],
            'provider_team_id': ['522.0', '123.0'],
            'team_name': ['FC Elite AZ', 'Premier Soccer Club'],
            'age_group': ['U10', 'U12'],
            'age_u': [9, 19],  # Invalid ages
            'gender': ['M', 'F'],
            'state': ['AZ', 'CA'],
            'provider': ['GotSport', 'GotSport'],
            'club_name': ['Elite FC', 'Premier SC'],
            'source_url': ['https://api.example.com/team/522', 'https://api.example.com/team/123'],
            'created_at': ['2025-10-14T12:00:00Z', '2025-10-14T12:00:00Z']
        }
        
        df = pd.DataFrame(invalid_data)
        
        with pytest.raises(Exception):  # Should raise validation error
            validate_dataframe(df)
    
    def test_invalid_gender_fails_validation(self):
        """Test that invalid gender values fail validation"""
        invalid_data = {
            'team_id': ['6c1e02b09d77', 'a1b2c3d4e5f6'],
            'provider_team_id': ['522.0', '123.0'],
            'team_name': ['FC Elite AZ', 'Premier Soccer Club'],
            'age_group': ['U10', 'U12'],
            'age_u': [10, 12],
            'gender': ['Male', 'X'],  # Invalid genders
            'state': ['AZ', 'CA'],
            'provider': ['GotSport', 'GotSport'],
            'club_name': ['Elite FC', 'Premier SC'],
            'source_url': ['https://api.example.com/team/522', 'https://api.example.com/team/123'],
            'created_at': ['2025-10-14T12:00:00Z', '2025-10-14T12:00:00Z']
        }
        
        df = pd.DataFrame(invalid_data)
        
        with pytest.raises(Exception):  # Should raise validation error
            validate_dataframe(df)
    
    def test_invalid_state_code_fails_validation(self):
        """Test that invalid state codes fail validation"""
        invalid_data = {
            'team_id': ['6c1e02b09d77', 'a1b2c3d4e5f6'],
            'provider_team_id': ['522.0', '123.0'],
            'team_name': ['FC Elite AZ', 'Premier Soccer Club'],
            'age_group': ['U10', 'U12'],
            'age_u': [10, 12],
            'gender': ['M', 'F'],
            'state': ['AZ', 'XX'],  # Invalid state
            'provider': ['GotSport', 'GotSport'],
            'club_name': ['Elite FC', 'Premier SC'],
            'source_url': ['https://api.example.com/team/522', 'https://api.example.com/team/123'],
            'created_at': ['2025-10-14T12:00:00Z', '2025-10-14T12:00:00Z']
        }
        
        df = pd.DataFrame(invalid_data)
        
        with pytest.raises(Exception):  # Should raise validation error
            validate_dataframe(df)
    
    def test_missing_required_columns_fails_validation(self):
        """Test that missing required columns fail validation"""
        incomplete_data = {
            'team_id': ['6c1e02b09d77'],
            'team_name': ['FC Elite AZ'],
            # Missing required columns
        }
        
        df = pd.DataFrame(incomplete_data)
        
        with pytest.raises(Exception):  # Should raise validation error
            validate_dataframe(df)
    
    def test_nullable_columns_allow_nulls(self):
        """Test that nullable columns allow null values"""
        data_with_nulls = {
            'team_id': ['6c1e02b09d77'],
            'provider_team_id': [None],  # Nullable
            'team_name': ['FC Elite AZ'],
            'age_group': ['U10'],
            'age_u': [10],
            'gender': ['M'],
            'state': ['AZ'],
            'provider': ['GotSport'],
            'club_name': [None],  # Nullable
            'source_url': ['https://api.example.com/team/522'],
            'created_at': [None]  # Nullable
        }
        
        df = pd.DataFrame(data_with_nulls)
        validated_df = validate_dataframe(df)
        
        assert len(validated_df) == 1
        assert validated_df['provider_team_id'].iloc[0] is None
        assert validated_df['club_name'].iloc[0] is None
        assert validated_df['created_at'].iloc[0] is None
    
    def test_schema_summary(self):
        """Test that schema summary provides correct information"""
        summary = get_schema_summary()
        
        assert summary['schema_name'] == 'MasterTeamSchema'
        assert summary['total_fields'] == 11
        assert summary['required_fields'] == 8
        assert summary['optional_fields'] == 3
        
        # Check that all expected fields are present
        expected_fields = [
            'team_id', 'provider_team_id', 'team_name', 'age_group', 'age_u',
            'gender', 'state', 'provider', 'club_name', 'source_url', 'created_at'
        ]
        
        for field in expected_fields:
            assert field in summary['fields']
    
    def test_fixture_data_validation(self):
        """Test validation with sample fixture data"""
        fixture_path = Path(__file__).parent / 'fixtures' / 'sample_master_index.csv'
        
        if fixture_path.exists():
            df = pd.read_csv(fixture_path)
            validated_df = validate_dataframe(df)
            
            assert len(validated_df) > 0
            assert 'team_id' in validated_df.columns
            assert 'team_name' in validated_df.columns
            assert 'state' in validated_df.columns
            
            # Check that all team_ids are valid format
            for team_id in validated_df['team_id']:
                assert len(team_id) == 12
                assert all(c in '0123456789abcdef' for c in team_id)
    
    def test_age_group_age_u_consistency(self):
        """Test that age_group and age_u are consistent"""
        consistent_data = {
            'team_id': ['6c1e02b09d77', 'a1b2c3d4e5f6'],
            'provider_team_id': ['522.0', '123.0'],
            'team_name': ['FC Elite AZ', 'Premier Soccer Club'],
            'age_group': ['U10', 'U12'],
            'age_u': [10, 12],  # Consistent with age_group
            'gender': ['M', 'F'],
            'state': ['AZ', 'CA'],
            'provider': ['GotSport', 'GotSport'],
            'club_name': ['Elite FC', 'Premier SC'],
            'source_url': ['https://api.example.com/team/522', 'https://api.example.com/team/123'],
            'created_at': ['2025-10-14T12:00:00Z', '2025-10-14T12:00:00Z']
        }
        
        df = pd.DataFrame(consistent_data)
        validated_df = validate_dataframe(df)
        
        # Check consistency
        for _, row in validated_df.iterrows():
            expected_age_group = f"U{row['age_u']}"
            assert row['age_group'] == expected_age_group


if __name__ == "__main__":
    pytest.main([__file__])
