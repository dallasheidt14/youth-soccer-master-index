#!/usr/bin/env python3
"""
Test suite for team ID generator
"""

import pytest
import sys
from pathlib import Path

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent))

from src.utils.team_id_generator import (
    make_team_id, 
    normalize_gender, 
    extract_age_from_group,
    batch_make_team_ids
)


class TestTeamIDGenerator:
    """Test cases for team ID generation"""
    
    def test_make_team_id_basic(self):
        """Test basic team ID generation"""
        team_id = make_team_id("FC Elite AZ", "AZ", "U10", "Male")
        
        assert len(team_id) == 12
        assert all(c in '0123456789abcdef' for c in team_id)
    
    def test_make_team_id_deterministic(self):
        """Test that team ID generation is deterministic"""
        team_id1 = make_team_id("FC Elite AZ", "AZ", "U10", "Male")
        team_id2 = make_team_id("FC Elite AZ", "AZ", "U10", "Male")
        
        assert team_id1 == team_id2
    
    def test_make_team_id_different_inputs(self):
        """Test that different inputs produce different IDs"""
        team_id1 = make_team_id("FC Elite AZ", "AZ", "U10", "Male")
        team_id2 = make_team_id("FC Elite AZ", "CA", "U10", "Male")  # Different state
        team_id3 = make_team_id("FC Elite AZ", "AZ", "U11", "Male")  # Different age
        team_id4 = make_team_id("FC Elite AZ", "AZ", "U10", "Female")  # Different gender
        team_id5 = make_team_id("Premier SC", "AZ", "U10", "Male")  # Different name
        
        assert team_id1 != team_id2
        assert team_id1 != team_id3
        assert team_id1 != team_id4
        assert team_id1 != team_id5
    
    def test_make_team_id_case_insensitive(self):
        """Test that team ID generation is case insensitive"""
        team_id1 = make_team_id("FC Elite AZ", "AZ", "U10", "Male")
        team_id2 = make_team_id("fc elite az", "az", "u10", "male")
        
        assert team_id1 == team_id2
    
    def test_make_team_id_whitespace_insensitive(self):
        """Test that team ID generation handles whitespace"""
        team_id1 = make_team_id("FC Elite AZ", "AZ", "U10", "Male")
        team_id2 = make_team_id("  FC Elite AZ  ", "  AZ  ", "  U10  ", "  Male  ")
        
        assert team_id1 == team_id2
    
    def test_make_team_id_invalid_inputs(self):
        """Test that invalid inputs raise appropriate errors"""
        with pytest.raises(ValueError, match="Team name cannot be empty"):
            make_team_id("", "AZ", "U10", "Male")
        
        with pytest.raises(ValueError, match="State cannot be empty"):
            make_team_id("FC Elite AZ", "", "U10", "Male")
        
        with pytest.raises(ValueError, match="State must be 2 characters"):
            make_team_id("FC Elite AZ", "ARIZONA", "U10", "Male")
    
    def test_normalize_gender(self):
        """Test gender normalization"""
        assert normalize_gender("Male") == "M"
        assert normalize_gender("male") == "M"
        assert normalize_gender("M") == "M"
        assert normalize_gender("1") == "M"
        assert normalize_gender("1.0") == "M"
        
        assert normalize_gender("Female") == "F"
        assert normalize_gender("female") == "F"
        assert normalize_gender("F") == "F"
        assert normalize_gender("0") == "F"
        assert normalize_gender("0.0") == "F"
        
        with pytest.raises(ValueError, match="Cannot normalize gender"):
            normalize_gender("Unknown")
        
        with pytest.raises(ValueError, match="Gender cannot be None"):
            normalize_gender(None)
    
    def test_extract_age_from_group(self):
        """Test age extraction from age group strings"""
        assert extract_age_from_group("U10") == 10
        assert extract_age_from_group("U11") == 11
        assert extract_age_from_group("U18") == 18
        assert extract_age_from_group("10") == 10
        assert extract_age_from_group("11") == 11
        assert extract_age_from_group("18") == 18
        
        with pytest.raises(ValueError, match="Age 9 is outside valid range"):
            extract_age_from_group("U9")
        
        with pytest.raises(ValueError, match="Age 19 is outside valid range"):
            extract_age_from_group("U19")
        
        with pytest.raises(ValueError, match="Cannot extract age from"):
            extract_age_from_group("Invalid")
        
        with pytest.raises(ValueError, match="Age group cannot be None"):
            extract_age_from_group(None)
    
    def test_batch_make_team_ids(self):
        """Test batch team ID generation"""
        import pandas as pd
        
        data = {
            'team_name': ['FC Elite AZ', 'Premier SC', 'United FC'],
            'state': ['AZ', 'CA', 'NY'],
            'age_group': ['U10', 'U12', 'U14'],
            'gender': ['Male', 'Female', 'Male']
        }
        
        df = pd.DataFrame(data)
        team_ids = batch_make_team_ids(df)
        
        assert len(team_ids) == 3
        assert all(len(tid) == 12 for tid in team_ids)
        assert all(tid is not None for tid in team_ids)
        
        # Test that IDs are unique
        assert len(set(team_ids)) == 3
    
    def test_batch_make_team_ids_with_errors(self):
        """Test batch team ID generation with some errors"""
        import pandas as pd
        
        data = {
            'team_name': ['FC Elite AZ', '', 'United FC'],  # Empty name
            'state': ['AZ', 'CA', 'INVALID'],  # Invalid state
            'age_group': ['U10', 'U12', 'U14'],
            'gender': ['Male', 'Female', 'Male']
        }
        
        df = pd.DataFrame(data)
        
        with pytest.raises(ValueError, match="Failed to generate team IDs"):
            batch_make_team_ids(df)
    
    def test_team_id_uniqueness(self):
        """Test that team IDs are unique across different teams"""
        teams = [
            ("FC Elite AZ", "AZ", "U10", "Male"),
            ("FC Elite AZ", "AZ", "U10", "Female"),
            ("FC Elite AZ", "AZ", "U11", "Male"),
            ("FC Elite AZ", "CA", "U10", "Male"),
            ("Premier SC", "AZ", "U10", "Male"),
            ("Premier Soccer Club", "AZ", "U10", "Male"),
            ("FC Elite Arizona", "AZ", "U10", "Male"),
        ]
        
        team_ids = [make_team_id(*team) for team in teams]
        
        # All IDs should be unique
        assert len(set(team_ids)) == len(team_ids)
    
    def test_team_id_stability(self):
        """Test that team IDs are stable across multiple runs"""
        team_id1 = make_team_id("FC Elite AZ", "AZ", "U10", "Male")
        
        # Simulate multiple runs
        for _ in range(10):
            team_id2 = make_team_id("FC Elite AZ", "AZ", "U10", "Male")
            assert team_id1 == team_id2
    
    def test_edge_cases(self):
        """Test edge cases for team ID generation"""
        # Very long team name
        long_name = "A" * 1000
        team_id = make_team_id(long_name, "AZ", "U10", "Male")
        assert len(team_id) == 12
        
        # Special characters in name
        special_name = "FC Elite AZ!@#$%^&*()_+-=[]{}|;':\",./<>?"
        team_id = make_team_id(special_name, "AZ", "U10", "Male")
        assert len(team_id) == 12
        
        # Minimum valid age
        team_id = make_team_id("FC Elite AZ", "AZ", "U10", "Male")
        assert len(team_id) == 12
        
        # Maximum valid age
        team_id = make_team_id("FC Elite AZ", "AZ", "U18", "Male")
        assert len(team_id) == 12


if __name__ == "__main__":
    pytest.main([__file__])
