#!/usr/bin/env python3
"""
Test suite for incremental detection
"""

import pytest
import pandas as pd
import sys
from pathlib import Path
import tempfile
import shutil

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent))

from src.scraper.utils.incremental_detector import (
    load_baseline_master,
    detect_new_teams,
    merge_incremental_data
)


class TestIncrementalDetector:
    """Test cases for incremental detection"""
    
    def setup_method(self):
        """Set up test data"""
        self.baseline_data = {
            'team_name': ['FC Elite AZ', 'Premier SC', 'United FC'],
            'team_id': ['6c1e02b09d77', 'a1b2c3d4e5f6', 'c3d4e5f6a1b2'],
            'age_group': ['U10', 'U12', 'U14'],
            'gender': ['M', 'F', 'M'],
            'state': ['AZ', 'CA', 'NY'],
            'provider': ['GotSport', 'GotSport', 'GotSport'],
            'source_url': ['https://api.example.com/team/1', 'https://api.example.com/team/2', 'https://api.example.com/team/3'],
            'created_at': ['2025-10-14T12:00:00Z', '2025-10-14T12:00:00Z', '2025-10-14T12:00:00Z']
        }
        
        self.new_data = {
            'team_name': ['FC Elite AZ', 'Premier SC', 'Thunder FC', 'Lightning SC'],
            'team_id': ['6c1e02b09d77', 'a1b2c3d4e5f6', 'd4e5f6a1b2c3', 'e5f6a1b2c3d4'],
            'age_group': ['U10', 'U12', 'U13', 'U15'],
            'gender': ['M', 'F', 'M', 'F'],
            'state': ['AZ', 'CA', 'TX', 'FL'],
            'provider': ['GotSport', 'GotSport', 'GotSport', 'GotSport'],
            'source_url': ['https://api.example.com/team/1', 'https://api.example.com/team/2', 'https://api.example.com/team/4', 'https://api.example.com/team/5'],
            'created_at': ['2025-10-14T12:00:00Z', '2025-10-14T12:00:00Z', '2025-10-14T12:00:00Z', '2025-10-14T12:00:00Z']
        }
        
        self.baseline_df = pd.DataFrame(self.baseline_data)
        self.new_df = pd.DataFrame(self.new_data)
    
    def test_detect_new_teams_basic(self):
        """Test basic new team detection"""
        new_teams = detect_new_teams(self.new_df, self.baseline_df)
        
        # Should detect 2 new teams: Thunder FC and Lightning SC
        assert len(new_teams) == 2
        assert 'Thunder FC' in new_teams['team_name'].values
        assert 'Lightning SC' in new_teams['team_name'].values
        
        # Should not include existing teams
        assert 'FC Elite AZ' not in new_teams['team_name'].values
        assert 'Premier SC' not in new_teams['team_name'].values
    
    def test_detect_new_teams_empty_baseline(self):
        """Test new team detection with empty baseline"""
        empty_baseline = pd.DataFrame(columns=self.baseline_df.columns)
        new_teams = detect_new_teams(self.new_df, empty_baseline)
        
        # All teams should be considered new
        assert len(new_teams) == len(self.new_df)
        assert new_teams.equals(self.new_df)
    
    def test_detect_new_teams_empty_new_data(self):
        """Test new team detection with empty new data"""
        empty_new = pd.DataFrame(columns=self.new_df.columns)
        new_teams = detect_new_teams(empty_new, self.baseline_df)
        
        # No new teams should be detected
        assert len(new_teams) == 0
        assert new_teams.empty
    
    def test_detect_new_teams_duplicate_handling(self):
        """Test that duplicates in new data are handled correctly"""
        # Add duplicate team to new data
        duplicate_data = self.new_data.copy()
        duplicate_data['team_name'].append('Thunder FC')
        duplicate_data['team_id'].append('d4e5f6a1b2c3')
        duplicate_data['age_group'].append('U13')
        duplicate_data['gender'].append('M')
        duplicate_data['state'].append('TX')
        duplicate_data['provider'].append('GotSport')
        duplicate_data['source_url'].append('https://api.example.com/team/4')
        duplicate_data['created_at'].append('2025-10-14T12:00:00Z')
        
        duplicate_df = pd.DataFrame(duplicate_data)
        new_teams = detect_new_teams(duplicate_df, self.baseline_df)
        
        # Should still detect 2 new teams (duplicates removed)
        assert len(new_teams) == 2
        assert 'Thunder FC' in new_teams['team_name'].values
        assert 'Lightning SC' in new_teams['team_name'].values
    
    def test_detect_new_teams_case_insensitive(self):
        """Test that team detection is case insensitive"""
        # Modify new data to have different case
        case_modified_data = self.new_data.copy()
        case_modified_data['team_name'] = ['fc elite az', 'premier sc', 'Thunder FC', 'Lightning SC']
        
        case_modified_df = pd.DataFrame(case_modified_data)
        new_teams = detect_new_teams(case_modified_df, self.baseline_df)
        
        # Should detect 2 new teams (case insensitive matching)
        assert len(new_teams) == 2
        assert 'Thunder FC' in new_teams['team_name'].values
        assert 'Lightning SC' in new_teams['team_name'].values
    
    def test_detect_new_teams_whitespace_handling(self):
        """Test that team detection handles whitespace correctly"""
        # Modify new data to have extra whitespace
        whitespace_data = self.new_data.copy()
        whitespace_data['team_name'] = ['  FC Elite AZ  ', '  Premier SC  ', 'Thunder FC', 'Lightning SC']
        
        whitespace_df = pd.DataFrame(whitespace_data)
        new_teams = detect_new_teams(whitespace_df, self.baseline_df)
        
        # Should detect 2 new teams (whitespace insensitive)
        assert len(new_teams) == 2
        assert 'Thunder FC' in new_teams['team_name'].values
        assert 'Lightning SC' in new_teams['team_name'].values
    
    def test_detect_new_teams_missing_columns(self):
        """Test that missing columns are handled gracefully"""
        # Remove required column
        incomplete_data = {k: v for k, v in self.new_data.items() if k != 'team_name'}
        incomplete_df = pd.DataFrame(incomplete_data)
        
        with pytest.raises(ValueError, match="Missing required columns"):
            detect_new_teams(incomplete_df, self.baseline_df)
    
    def test_detect_new_teams_different_columns(self):
        """Test that different column sets are handled"""
        # Add extra column to new data
        extra_data = self.new_data.copy()
        extra_data['extra_column'] = ['value1', 'value2', 'value3', 'value4']
        extra_df = pd.DataFrame(extra_data)
        
        new_teams = detect_new_teams(extra_df, self.baseline_df)
        
        # Should work with extra columns
        assert len(new_teams) == 2
        assert 'extra_column' in new_teams.columns
    
    def test_merge_incremental_data_basic(self):
        """Test basic incremental data merging"""
        new_teams = detect_new_teams(self.new_df, self.baseline_df)
        merged_df = merge_incremental_data(new_teams)
        
        # Should contain all teams
        assert len(merged_df) == len(self.baseline_df) + len(new_teams)
        
        # Should contain all team names
        all_team_names = set(self.baseline_df['team_name']) | set(new_teams['team_name'])
        assert set(merged_df['team_name']) == all_team_names
    
    def test_merge_incremental_data_empty_new_teams(self):
        """Test merging with empty new teams"""
        empty_new = pd.DataFrame(columns=self.baseline_df.columns)
        merged_df = merge_incremental_data(empty_new)
        
        # Should return original baseline
        assert len(merged_df) == len(self.baseline_df)
        assert merged_df.equals(self.baseline_df)
    
    def test_merge_incremental_data_duplicate_handling(self):
        """Test that duplicates are handled during merging"""
        # Create new teams with some duplicates
        duplicate_new = pd.DataFrame({
            'team_name': ['Thunder FC', 'Lightning SC', 'Thunder FC'],  # Duplicate
            'team_id': ['d4e5f6a1b2c3', 'e5f6a1b2c3d4', 'd4e5f6a1b2c3'],
            'age_group': ['U13', 'U15', 'U13'],
            'gender': ['M', 'F', 'M'],
            'state': ['TX', 'FL', 'TX'],
            'provider': ['GotSport', 'GotSport', 'GotSport'],
            'source_url': ['https://api.example.com/team/4', 'https://api.example.com/team/5', 'https://api.example.com/team/4'],
            'created_at': ['2025-10-14T12:00:00Z', '2025-10-14T12:00:00Z', '2025-10-14T12:00:00Z']
        })
        
        merged_df = merge_incremental_data(duplicate_new)
        
        # Should remove duplicates
        assert len(merged_df) == len(self.baseline_df) + 2  # 2 unique new teams
        assert merged_df['team_name'].value_counts()['Thunder FC'] == 1
    
    def test_merge_incremental_data_column_consistency(self):
        """Test that column consistency is maintained during merging"""
        new_teams = detect_new_teams(self.new_df, self.baseline_df)
        merged_df = merge_incremental_data(new_teams)
        
        # Should have all expected columns
        expected_columns = set(self.baseline_df.columns)
        assert set(merged_df.columns) == expected_columns
        
        # Should maintain data types
        for col in self.baseline_df.columns:
            if col in merged_df.columns:
                assert merged_df[col].dtype == self.baseline_df[col].dtype
    
    def test_merge_incremental_data_sorting(self):
        """Test that merged data is properly sorted"""
        new_teams = detect_new_teams(self.new_df, self.baseline_df)
        merged_df = merge_incremental_data(new_teams)
        
        # Should be sorted by team_name
        assert merged_df['team_name'].is_monotonic_increasing
    
    def test_incremental_detection_integration(self):
        """Test full incremental detection integration"""
        # Test the full flow
        new_teams = detect_new_teams(self.new_df, self.baseline_df)
        merged_df = merge_incremental_data(new_teams)
        
        # Verify results
        assert len(new_teams) == 2
        assert len(merged_df) == 5  # 3 baseline + 2 new
        
        # Verify new teams are in merged data
        new_team_names = set(new_teams['team_name'])
        merged_team_names = set(merged_df['team_name'])
        assert new_team_names.issubset(merged_team_names)
        
        # Verify baseline teams are in merged data
        baseline_team_names = set(self.baseline_df['team_name'])
        assert baseline_team_names.issubset(merged_team_names)
    
    def test_sample_mode(self):
        """Test sample mode functionality"""
        # Create large dataset
        large_data = []
        for i in range(1000):
            large_data.append({
                'team_name': f'Team {i}',
                'team_id': f'team_id_{i}',
                'age_group': 'U10',
                'gender': 'M',
                'state': 'AZ',
                'provider': 'GotSport',
                'source_url': f'https://api.example.com/team/{i}',
                'created_at': '2025-10-14T12:00:00Z'
            })
        
        large_df = pd.DataFrame(large_data)
        
        # Test sample mode
        new_teams = detect_new_teams(large_df, self.baseline_df, sample_mode=True)
        
        # Should limit to 5000 rows
        assert len(new_teams) <= 5000
        
        # Test without sample mode
        new_teams_full = detect_new_teams(large_df, self.baseline_df, sample_mode=False)
        
        # Should process all rows
        assert len(new_teams_full) == 1000


if __name__ == "__main__":
    pytest.main([__file__])
