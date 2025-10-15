#!/usr/bin/env python3
"""
Test script to explore GotSport game history API and see what data structure we get back.
This will help us determine if we need team_id for the master teams list.
"""

import requests
import json
import time
from pathlib import Path

def test_game_history_api():
    """Test the GotSport game history API to see what data structure we get."""
    
    print("Testing GotSport Game History API...")
    print("=" * 50)
    
    # Test different possible game history endpoints
    possible_endpoints = [
        "https://system.gotsport.com/api/v1/team_matches",
        "https://system.gotsport.com/api/v1/team_games", 
        "https://system.gotsport.com/api/v1/matches",
        "https://system.gotsport.com/api/v1/games",
        "https://system.gotsport.com/api/v1/team_history",
        "https://system.gotsport.com/api/v1/team_results"
    ]
    
    # Test parameters - we'll try different approaches
    test_params = [
        # Try with team_id
        {"team_id": "12345"},
        # Try with team name
        {"team_name": "Southeast 2016 Boys Black"},
        # Try with search parameters
        {"search[team_id]": "12345"},
        {"search[team_name]": "Southeast 2016 Boys Black"},
        # Try with age/gender like rankings
        {"search[age]": "10", "search[gender]": "m"},
        # Try with state
        {"search[state]": "AZ"},
        # Try with page
        {"search[page]": "1"},
        # Try empty params
        {}
    ]
    
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'application/json',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br, zstd',
        'Connection': 'keep-alive',
        'Origin': 'https://rankings.gotsport.com',
        'Referer': 'https://rankings.gotsport.com/',
    })
    
    results = {}
    
    for endpoint in possible_endpoints:
        print(f"\nTesting endpoint: {endpoint}")
        print("-" * 40)
        
        endpoint_results = {}
        
        for i, params in enumerate(test_params):
            print(f"  Test {i+1}: {params}")
            
            try:
                response = session.get(endpoint, params=params, timeout=10)
                
                if response.status_code == 200:
                    try:
                        data = response.json()
                        endpoint_results[f"test_{i+1}"] = {
                            "status": "success",
                            "status_code": response.status_code,
                            "data_keys": list(data.keys()) if isinstance(data, dict) else f"List with {len(data)} items",
                            "sample_data": str(data)[:200] + "..." if len(str(data)) > 200 else str(data)
                        }
                        print(f"    SUCCESS! Keys: {list(data.keys()) if isinstance(data, dict) else f'List with {len(data)} items'}")
                        
                        # If we get actual data, save a sample
                        if data and ((isinstance(data, dict) and data) or (isinstance(data, list) and len(data) > 0)):
                            sample_file = Path(f"game_history_sample_{endpoint.split('/')[-1]}_test_{i+1}.json")
                            with open(sample_file, 'w') as f:
                                json.dump(data, f, indent=2)
                            print(f"    Sample saved to: {sample_file}")
                            
                    except json.JSONDecodeError:
                        endpoint_results[f"test_{i+1}"] = {
                            "status": "json_error",
                            "status_code": response.status_code,
                            "content_type": response.headers.get('content-type', 'unknown'),
                            "sample_content": response.text[:200]
                        }
                        print(f"    WARNING: JSON decode error. Content-Type: {response.headers.get('content-type', 'unknown')}")
                        
                else:
                    endpoint_results[f"test_{i+1}"] = {
                        "status": "http_error",
                        "status_code": response.status_code,
                        "reason": response.reason
                    }
                    print(f"    ERROR: HTTP {response.status_code}: {response.reason}")
                    
            except requests.exceptions.RequestException as e:
                endpoint_results[f"test_{i+1}"] = {
                    "status": "request_error",
                    "error": str(e)
                }
                print(f"    ERROR: Request error: {e}")
            
            # Small delay between requests
            time.sleep(0.5)
        
        results[endpoint] = endpoint_results
    
    # Save comprehensive results
    results_file = Path("game_history_api_test_results.json")
    with open(results_file, 'w') as f:
        json.dump(results, f, indent=2)
    
    print(f"\nTest Results Summary:")
    print("=" * 50)
    
    for endpoint, endpoint_results in results.items():
        print(f"\n{endpoint}")
        successful_tests = [k for k, v in endpoint_results.items() if v.get("status") == "success"]
        if successful_tests:
            print(f"  SUCCESSFUL tests: {successful_tests}")
            for test in successful_tests:
                data_info = endpoint_results[test].get("data_keys", "Unknown")
                print(f"    - {test}: {data_info}")
        else:
            print(f"  NO successful tests")
    
    print(f"\nFull results saved to: {results_file}")
    
    return results

if __name__ == "__main__":
    test_game_history_api()
