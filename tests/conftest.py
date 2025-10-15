#!/usr/bin/env python3
"""
Pytest configuration and fixtures
"""

import pytest
import pandas as pd
import tempfile
import shutil
from pathlib import Path
import sys

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent))


@pytest.fixture
def sample_team_data():
    """Sample team data for testing"""
    return {
        'team_id': ['6c1e02b09d77', 'a1b2c3d4e5f6', 'c3d4e5f6a1b2'],
        'provider_team_id': ['522.0', '123.0', '456.0'],
        'team_name': ['FC Elite AZ', 'Premier Soccer Club', 'United FC'],
        'age_group': ['U10', 'U12', 'U14'],
        'age_u': [10, 12, 14],
        'gender': ['M', 'F', 'M'],
        'state': ['AZ', 'CA', 'NY'],
        'provider': ['GotSport', 'GotSport', 'GotSport'],
        'club_name': ['Elite FC', 'Premier SC', 'United FC'],
        'source_url': ['https://api.example.com/team/522', 'https://api.example.com/team/123', 'https://api.example.com/team/456'],
        'created_at': ['2025-10-14T12:00:00Z', '2025-10-14T12:00:00Z', '2025-10-14T12:00:00Z']
    }


@pytest.fixture
def sample_dataframe(sample_team_data):
    """Sample DataFrame for testing"""
    return pd.DataFrame(sample_team_data)


@pytest.fixture
def temp_data_dir():
    """Temporary directory for test data"""
    temp_dir = tempfile.mkdtemp()
    yield Path(temp_dir)
    shutil.rmtree(temp_dir)


@pytest.fixture
def sample_master_index_file(temp_data_dir):
    """Sample master index CSV file for testing"""
    sample_data = {
        'team_id': ['6c1e02b09d77', 'a1b2c3d4e5f6', 'c3d4e5f6a1b2'],
        'provider_team_id': ['522.0', '123.0', '456.0'],
        'team_name': ['FC Elite AZ', 'Premier Soccer Club', 'United FC'],
        'age_group': ['U10', 'U12', 'U14'],
        'age_u': [10, 12, 14],
        'gender': ['M', 'F', 'M'],
        'state': ['AZ', 'CA', 'NY'],
        'provider': ['GotSport', 'GotSport', 'GotSport'],
        'club_name': ['Elite FC', 'Premier SC', 'United FC'],
        'source_url': ['https://api.example.com/team/522', 'https://api.example.com/team/123', 'https://api.example.com/team/456'],
        'created_at': ['2025-10-14T12:00:00Z', '2025-10-14T12:00:00Z', '2025-10-14T12:00:00Z']
    }
    
    df = pd.DataFrame(sample_data)
    file_path = temp_data_dir / 'sample_master_index.csv'
    df.to_csv(file_path, index=False)
    
    return file_path


@pytest.fixture
def sample_baseline_data():
    """Sample baseline data for incremental testing"""
    return {
        'team_name': ['FC Elite AZ', 'Premier SC', 'United FC'],
        'team_id': ['6c1e02b09d77', 'a1b2c3d4e5f6', 'c3d4e5f6a1b2'],
        'age_group': ['U10', 'U12', 'U14'],
        'gender': ['M', 'F', 'M'],
        'state': ['AZ', 'CA', 'NY'],
        'provider': ['GotSport', 'GotSport', 'GotSport'],
        'source_url': ['https://api.example.com/team/1', 'https://api.example.com/team/2', 'https://api.example.com/team/3'],
        'created_at': ['2025-10-14T12:00:00Z', '2025-10-14T12:00:00Z', '2025-10-14T12:00:00Z']
    }


@pytest.fixture
def sample_new_data():
    """Sample new data for incremental testing"""
    return {
        'team_name': ['FC Elite AZ', 'Premier SC', 'Thunder FC', 'Lightning SC'],
        'team_id': ['6c1e02b09d77', 'a1b2c3d4e5f6', 'd4e5f6a1b2c3', 'e5f6a1b2c3d4'],
        'age_group': ['U10', 'U12', 'U13', 'U15'],
        'gender': ['M', 'F', 'M', 'F'],
        'state': ['AZ', 'CA', 'TX', 'FL'],
        'provider': ['GotSport', 'GotSport', 'GotSport', 'GotSport'],
        'source_url': ['https://api.example.com/team/1', 'https://api.example.com/team/2', 'https://api.example.com/team/4', 'https://api.example.com/team/5'],
        'created_at': ['2025-10-14T12:00:00Z', '2025-10-14T12:00:00Z', '2025-10-14T12:00:00Z', '2025-10-14T12:00:00Z']
    }


@pytest.fixture
def sample_baseline_df(sample_baseline_data):
    """Sample baseline DataFrame for incremental testing"""
    return pd.DataFrame(sample_baseline_data)


@pytest.fixture
def sample_new_df(sample_new_data):
    """Sample new DataFrame for incremental testing"""
    return pd.DataFrame(sample_new_data)


@pytest.fixture
def sample_team_names():
    """Sample team names for testing"""
    return [
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


@pytest.fixture
def sample_team_id_inputs():
    """Sample inputs for team ID generation testing"""
    return [
        ("FC Elite AZ", "AZ", "U10", "Male"),
        ("Premier Soccer Club", "CA", "U12", "Female"),
        ("United FC", "NY", "U14", "Male"),
        ("Thunder SC", "TX", "U16", "Female"),
        ("Lightning FC", "FL", "U18", "Male")
    ]


@pytest.fixture
def sample_gender_inputs():
    """Sample gender inputs for testing"""
    return [
        ("Male", "M"),
        ("male", "M"),
        ("M", "M"),
        ("1", "M"),
        ("1.0", "M"),
        ("Female", "F"),
        ("female", "F"),
        ("F", "F"),
        ("0", "F"),
        ("0.0", "F")
    ]


@pytest.fixture
def sample_age_inputs():
    """Sample age inputs for testing"""
    return [
        ("U10", 10),
        ("U11", 11),
        ("U12", 12),
        ("U18", 18),
        ("10", 10),
        ("11", 11),
        ("18", 18)
    ]


@pytest.fixture
def sample_state_codes():
    """Sample US state codes for testing"""
    return [
        "AZ", "CA", "NY", "TX", "FL", "WA", "OR", "CO", "UT", "NV",
        "ID", "MT", "WY", "ND", "SD", "NE", "KS", "OK", "AR", "LA",
        "MS", "AL", "GA", "SC", "NC", "TN", "KY", "WV", "VA", "MD",
        "DE", "NJ", "CT", "RI", "MA", "VT", "NH", "ME", "HI", "AK",
        "DC"
    ]


@pytest.fixture
def sample_invalid_inputs():
    """Sample invalid inputs for testing error handling"""
    return {
        'invalid_genders': ["Unknown", "X", "Other", None],
        'invalid_ages': ["U9", "U19", "Invalid", None],
        'invalid_states': ["XX", "ARIZONA", "123", None],
        'empty_names': ["", "   ", None],
        'empty_states': ["", "   ", None]
    }


@pytest.fixture
def sample_metrics_data():
    """Sample metrics data for testing"""
    return {
        'build_id': '20251014_120000',
        'team_count': 1000,
        'new_teams': 50,
        'removed_teams': 10,
        'renamed_teams': 5,
        'states_covered': 25,
        'data_quality_score': 95.5,
        'build_duration_seconds': 120,
        'providers': ['GotSport'],
        'age_distribution': {'U10': 100, 'U11': 120, 'U12': 150, 'U13': 140, 'U14': 130, 'U15': 120, 'U16': 110, 'U17': 100, 'U18': 90},
        'gender_distribution': {'M': 600, 'F': 400},
        'state_distribution': {'AZ': 50, 'CA': 200, 'NY': 150, 'TX': 100, 'FL': 80}
    }


@pytest.fixture
def sample_registry_data():
    """Sample registry data for testing"""
    return {
        'timestamp': '2025-10-14_120000',
        'build_file': 'master_team_index_20251014_120000.csv',
        'teams_total': 1000,
        'added': 50,
        'removed': 10,
        'renamed': 5,
        'notes': 'Test build',
        'build_type': 'incremental',
        'duration_seconds': 120,
        'providers': ['GotSport'],
        'states_covered': 25
    }


@pytest.fixture
def sample_delta_data():
    """Sample delta data for testing"""
    return {
        'added': pd.DataFrame({
            'team_name': ['Thunder FC', 'Lightning SC'],
            'team_id': ['d4e5f6a1b2c3', 'e5f6a1b2c3d4'],
            'age_group': ['U13', 'U15'],
            'gender': ['M', 'F'],
            'state': ['TX', 'FL'],
            'provider': ['GotSport', 'GotSport']
        }),
        'removed': pd.DataFrame({
            'team_name': ['Old Team'],
            'team_id': ['f6a1b2c3d4e5'],
            'age_group': ['U12'],
            'gender': ['M'],
            'state': ['WA'],
            'provider': ['GotSport']
        }),
        'renamed': pd.DataFrame({
            'old_team_name': ['Old Name'],
            'new_team_name': ['New Name'],
            'age_group': ['U14'],
            'gender': ['F'],
            'state': ['OR'],
            'similarity_score': [85.0]
        })
    }
