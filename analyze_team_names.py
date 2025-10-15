#!/usr/bin/env python3
"""
Analyze team names to see if we can extract club names
"""

import pandas as pd
from pathlib import Path

def analyze_team_names():
    """Analyze team names for club extraction patterns"""
    
    master_file = Path('data/master/master_team_index_migrated_20251014_1717.csv')
    if not master_file.exists():
        print("No migrated master file found")
        return
    
    df = pd.read_csv(master_file)
    
    print('Sample team names to analyze for club extraction:')
    print('=' * 60)
    
    # Show some examples
    sample_teams = df['team_name'].head(20)
    for i, name in enumerate(sample_teams, 1):
        print(f'{i:2d}. {name}')
    
    print(f'\nTotal teams: {len(df):,}')
    print(f'Unique team names: {df["team_name"].nunique():,}')
    
    # Look for common patterns that might indicate club names
    print('\nCommon patterns in team names:')
    print('=' * 40)
    
    # Teams with 'Academy' in the name
    academy_teams = df[df['team_name'].str.contains('Academy', case=False, na=False)]
    print(f'Teams with "Academy": {len(academy_teams):,}')
    
    # Teams with 'FC' in the name
    fc_teams = df[df['team_name'].str.contains(' FC', case=False, na=False)]
    print(f'Teams with " FC": {len(fc_teams):,}')
    
    # Teams with 'SC' in the name
    sc_teams = df[df['team_name'].str.contains(' SC', case=False, na=False)]
    print(f'Teams with " SC": {len(sc_teams):,}')
    
    # Teams with 'United' in the name
    united_teams = df[df['team_name'].str.contains('United', case=False, na=False)]
    print(f'Teams with "United": {len(united_teams):,}')
    
    # Teams with 'Club' in the name
    club_teams = df[df['team_name'].str.contains('Club', case=False, na=False)]
    print(f'Teams with "Club": {len(club_teams):,}')
    
    # Teams with 'Soccer' in the name
    soccer_teams = df[df['team_name'].str.contains('Soccer', case=False, na=False)]
    print(f'Teams with "Soccer": {len(soccer_teams):,}')
    
    # Teams with 'Football' in the name
    football_teams = df[df['team_name'].str.contains('Football', case=False, na=False)]
    print(f'Teams with "Football": {len(football_teams):,}')
    
    print('\nExamples of each pattern:')
    print('=' * 40)
    
    if len(academy_teams) > 0:
        print('\nAcademy teams:')
        for name in academy_teams['team_name'].head(5):
            print(f'  - {name}')
    
    if len(fc_teams) > 0:
        print('\nFC teams:')
        for name in fc_teams['team_name'].head(5):
            print(f'  - {name}')
    
    if len(sc_teams) > 0:
        print('\nSC teams:')
        for name in sc_teams['team_name'].head(5):
            print(f'  - {name}')
    
    if len(united_teams) > 0:
        print('\nUnited teams:')
        for name in united_teams['team_name'].head(5):
            print(f'  - {name}')
    
    if len(club_teams) > 0:
        print('\nClub teams:')
        for name in club_teams['team_name'].head(5):
            print(f'  - {name}')

if __name__ == "__main__":
    analyze_team_names()
