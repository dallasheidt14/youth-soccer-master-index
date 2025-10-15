#!/usr/bin/env python3
"""
Test suite for text normalizer
"""

import pytest
import sys
from pathlib import Path

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent))

from src.normalizers.text_normalizer import (
    normalize_name,
    normalize_name_with_year,
    similarity_score,
    is_likely_same_team
)


class TestTextNormalizer:
    """Test cases for text normalization"""
    
    def test_normalize_name_basic(self):
        """Test basic name normalization"""
        assert normalize_name("FC Elite AZ United Soccer Club") == "elite az"
        assert normalize_name("SC Premier Academy 2010") == "premier 2010"
        assert normalize_name("Elite AZ FC") == "elite az"
    
    def test_normalize_name_removes_stopwords(self):
        """Test that stopwords are removed"""
        test_cases = [
            ("FC Elite AZ", "elite az"),
            ("SC Premier Club", "premier"),
            ("Academy United FC", "united"),
            ("Soccer Club Elite", "elite"),
            ("Football Association United", "united"),
            ("Youth Soccer Academy", "youth"),
            ("Junior Elite Club", "junior elite"),
            ("Senior Premier SC", "senior premier"),
            ("Travel Competitive Team", "travel competitive"),
            ("Recreation Soccer Club", "recreation")
        ]
        
        for input_name, expected in test_cases:
            result = normalize_name(input_name)
            assert result == expected, f"Expected '{expected}', got '{result}' for input '{input_name}'"
    
    def test_normalize_name_handles_special_characters(self):
        """Test that special characters are removed"""
        assert normalize_name("FC Elite-AZ!@#$%^&*()") == "elite az"
        assert normalize_name("Premier SC (2010)") == "premier 2010"
        assert normalize_name("United FC [Elite]") == "united elite"
    
    def test_normalize_name_handles_whitespace(self):
        """Test that whitespace is normalized"""
        assert normalize_name("  FC   Elite   AZ  ") == "elite az"
        assert normalize_name("Premier\nSC\t2010") == "premier 2010"
        assert normalize_name("United\r\nFC") == "united"
    
    def test_normalize_name_handles_case(self):
        """Test that case is normalized"""
        assert normalize_name("FC ELITE AZ") == "elite az"
        assert normalize_name("fc elite az") == "elite az"
        assert normalize_name("Fc ElItE Az") == "elite az"
    
    def test_normalize_name_handles_empty_input(self):
        """Test that empty input is handled gracefully"""
        assert normalize_name("") == ""
        assert normalize_name("   ") == ""
        assert normalize_name(None) == ""
    
    def test_normalize_name_with_year(self):
        """Test year extraction and normalization"""
        name, year = normalize_name_with_year("FC Elite AZ 2010 Boys")
        assert name == "elite az boys"
        assert year == "2010"
        
        name, year = normalize_name_with_year("Premier Soccer Club")
        assert name == "premier"
        assert year is None
        
        name, year = normalize_name_with_year("United FC 2025 Girls")
        assert name == "united girls"
        assert year == "2025"
        
        name, year = normalize_name_with_year("SC Academy 1999")
        assert name == "academy"
        assert year == "1999"
    
    def test_normalize_name_with_year_handles_multiple_years(self):
        """Test year extraction with multiple years"""
        name, year = normalize_name_with_year("FC Elite AZ 2010 2025")
        assert name == "elite az"
        assert year == "2010"  # Should take first year
    
    def test_normalize_name_with_year_handles_edge_cases(self):
        """Test year extraction edge cases"""
        name, year = normalize_name_with_year("FC Elite AZ 2000")
        assert name == "elite az"
        assert year == "2000"
        
        name, year = normalize_name_with_year("FC Elite AZ 2099")
        assert name == "elite az"
        assert year == "2099"
        
        name, year = normalize_name_with_year("FC Elite AZ 1999")
        assert name == "elite az"
        assert year == "1999"
    
    def test_similarity_score(self):
        """Test similarity score calculation"""
        # Identical names
        assert similarity_score("FC Elite AZ", "FC Elite AZ") == 1.0
        
        # Very similar names
        score = similarity_score("FC Elite AZ", "Elite AZ FC")
        assert score > 0.8
        
        # Different names
        score = similarity_score("FC Elite AZ", "Premier SC")
        assert score < 0.5
        
        # Empty names
        assert similarity_score("", "") == 1.0
        assert similarity_score("FC Elite AZ", "") == 0.0
        assert similarity_score("", "FC Elite AZ") == 0.0
    
    def test_similarity_score_token_based(self):
        """Test that similarity is token-based"""
        # Same tokens, different order
        score1 = similarity_score("FC Elite AZ United", "United Elite AZ FC")
        assert score1 > 0.8
        
        # Different tokens
        score2 = similarity_score("FC Elite AZ", "Premier Soccer Club")
        assert score2 < 0.5
        
        # Partial overlap
        score3 = similarity_score("FC Elite AZ United", "Elite AZ Premier")
        assert 0.3 < score3 < 0.8
    
    def test_is_likely_same_team(self):
        """Test team similarity detection"""
        # Same team
        assert is_likely_same_team("FC Elite AZ", "FC Elite AZ") == True
        assert is_likely_same_team("FC Elite AZ", "Elite AZ FC") == True
        
        # Different teams
        assert is_likely_same_team("FC Elite AZ", "Premier SC") == False
        assert is_likely_same_team("FC Elite AZ", "United FC") == False
        
        # Edge cases
        assert is_likely_same_team("", "") == True
        assert is_likely_same_team("FC Elite AZ", "") == False
    
    def test_is_likely_same_team_custom_threshold(self):
        """Test team similarity detection with custom threshold"""
        # High threshold
        assert is_likely_same_team("FC Elite AZ", "Elite AZ FC", threshold=0.9) == True
        assert is_likely_same_team("FC Elite AZ", "Premier SC", threshold=0.9) == False
        
        # Low threshold
        assert is_likely_same_team("FC Elite AZ", "Elite AZ FC", threshold=0.5) == True
        assert is_likely_same_team("FC Elite AZ", "Premier SC", threshold=0.5) == False
    
    def test_real_world_examples(self):
        """Test with real-world team name examples"""
        test_cases = [
            # Same team, different formats
            ("FC Barcelona Academy", "Barcelona FC Academy", True),
            ("Real Madrid CF", "Real Madrid Club", True),
            ("Manchester United FC", "Man United", True),
            ("Chelsea Football Club", "Chelsea FC", True),
            
            # Different teams
            ("FC Barcelona Academy", "Real Madrid CF", False),
            ("Manchester United FC", "Chelsea FC", False),
            ("FC Elite AZ", "Premier SC", False),
            
            # Edge cases
            ("FC Elite AZ", "FC Elite AZ", True),
            ("", "", True),
            ("FC Elite AZ", "", False),
        ]
        
        for name1, name2, expected in test_cases:
            result = is_likely_same_team(name1, name2)
            assert result == expected, f"Expected {expected}, got {result} for '{name1}' vs '{name2}'"
    
    def test_normalize_name_preserves_important_words(self):
        """Test that important words are preserved"""
        test_cases = [
            ("FC Elite AZ", "elite az"),
            ("Premier Soccer Club", "premier"),
            ("United FC", "united"),
            ("Real Madrid CF", "real madrid"),
            ("Barcelona Academy", "barcelona"),
            ("Manchester United", "manchester united"),
            ("Chelsea FC", "chelsea"),
        ]
        
        for input_name, expected in test_cases:
            result = normalize_name(input_name)
            assert result == expected, f"Expected '{expected}', got '{result}' for input '{input_name}'"
    
    def test_normalize_name_handles_numbers(self):
        """Test that numbers are preserved"""
        assert normalize_name("FC Elite AZ 2010") == "elite az 2010"
        assert normalize_name("Premier SC 2005") == "premier 2005"
        assert normalize_name("United FC 99") == "united 99"
    
    def test_similarity_score_edge_cases(self):
        """Test similarity score edge cases"""
        # Single word vs multiple words
        score1 = similarity_score("Elite", "Elite AZ")
        assert 0.3 < score1 < 0.8
        
        # Very long names
        long_name1 = "FC Elite AZ United Soccer Club Academy Premier"
        long_name2 = "Elite AZ United Soccer Club Academy Premier FC"
        score2 = similarity_score(long_name1, long_name2)
        assert score2 > 0.8
        
        # Very short names
        score3 = similarity_score("FC", "SC")
        assert score3 < 0.5


if __name__ == "__main__":
    pytest.main([__file__])
