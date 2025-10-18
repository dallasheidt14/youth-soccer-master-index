#!/usr/bin/env python3

import pandas as pd
import os

def create_u11_master_slices():
    """Create master slice files for all states with U11 M teams."""
    
    # Validate input file exists
    master_path = 'data/master/master_team_index_migrated_20251014_1717.csv'
    if not os.path.exists(master_path):
        raise FileNotFoundError(f"Master index not found: {master_path}")
    
    # Load master team index
    try:
        master_df = pd.read_csv(master_path)
    except Exception as e:
        raise RuntimeError(f"Failed to load master index: {e}")
    
    # Validate required columns
    required_cols = ['gender', 'age_group', 'state', 'team_id', 'provider_team_id', 
                     'team_name', 'club_name', 'provider']
    missing_cols = set(required_cols) - set(master_df.columns)
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")
    
    # Validate data quality
    if len(master_df) == 0:
        raise ValueError("Master index is empty")
    
    # Check for null values in critical columns
    critical_cols = ['gender', 'age_group', 'state', 'team_id']
    for col in critical_cols:
        null_count = master_df[col].isnull().sum()
        if null_count > 0:
            print(f"Warning: {null_count} null values found in column '{col}'")
    
    # Filter for U11 M teams
    u11_m_teams = master_df[(master_df['gender'] == 'M') & (master_df['age_group'] == 'U11')]
    
    # Get unique states
    states = sorted(u11_m_teams['state'].unique())
    
    print(f"Found U11 M teams in {len(states)} states")
    print(f"Total U11 M teams: {len(u11_m_teams)}")
    
    # Create slices directory
    try:
        os.makedirs('data/master/slices', exist_ok=True)
    except Exception as e:
        raise RuntimeError(f"Failed to create slices directory: {e}")
    
    created_files = []
    failed_files = []
    
    for state in states:
        try:
            state_teams = u11_m_teams[u11_m_teams['state'] == state]
            
            if len(state_teams) == 0:
                print(f"Warning: No U11 M teams found for state {state}")
                continue
            
            # Create slice dataframe with required columns
            slice_df = state_teams[['team_id', 'provider_team_id', 'team_name', 'club_name', 'state', 'gender', 'age_group', 'provider']].copy()
            slice_df.columns = ['team_id_master', 'team_id_source', 'team_name', 'club_name', 'state', 'gender', 'age_group', 'provider']
            
            # Save to file
            filename = f'data/master/slices/{state}_M_U11_master.csv'
            slice_df.to_csv(filename, index=False)
            
            created_files.append(f'{state}: {len(slice_df)} teams')
            print(f'Created {filename} with {len(slice_df)} teams')
            
        except Exception as e:
            error_msg = f'Failed to create slice for {state}: {e}'
            failed_files.append(error_msg)
            print(f'Error: {error_msg}')
    
    # Print summary
    print(f'\nSummary: Created {len(created_files)} master slice files')
    if failed_files:
        print(f'Failed to create {len(failed_files)} files:')
        for error in failed_files:
            print(f'  - {error}')
    
    if len(created_files) == 0:
        raise RuntimeError("No master slice files were created successfully")
    
    return created_files

if __name__ == "__main__":
    try:
        create_u11_master_slices()
        print("Script completed successfully!")
    except Exception as e:
        print(f"Script failed with error: {e}")
        exit(1)

