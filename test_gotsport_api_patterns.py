#!/usr/bin/env python3
"""
Test script to explore GotSport API endpoints more thoroughly.
Let's try different patterns and see what's available.
"""

import requests
import json
import time
from pathlib import Path

def test_gotsport_api_patterns():
    """Test different GotSport API patterns to find game history endpoints."""
    
    print("Testing GotSport API Patterns...")
    print("=" * 50)
    
    # First, let's get a real team_id from our existing data
    print("Getting real team data from our master index...")
    
    try:
        import pandas as pd
        master_file = Path("data/master/master_team_index_USAonly_incremental_20251014_1237.csv")
        if master_file.exists():
            df = pd.read_csv(master_file)
            sample_team = df.iloc[0]
            print(f"Sample team: {sample_team['team_name']} from {sample_team['state']}")
            print(f"Team URL: {sample_team['url']}")
        else:
            print("No master file found, using dummy data")
            sample_team = {"team_name": "Southeast 2016 Boys Black", "state": "AZ"}
    except Exception as e:
        print(f"Error loading master data: {e}")
        sample_team = {"team_name": "Southeast 2016 Boys Black", "state": "AZ"}
    
    # Test different API patterns
    base_urls = [
        "https://system.gotsport.com/api/v1",
        "https://api.gotsport.com/v1", 
        "https://rankings.gotsport.com/api/v1",
        "https://system.gotsport.com/api",
        "https://api.gotsport.com"
    ]
    
    endpoints = [
        "teams",
        "team",
        "matches", 
        "games",
        "results",
        "history",
        "schedule",
        "fixtures",
        "events",
        "tournaments",
        "competitions",
        "leagues"
    ]
    
    # Test parameters
    test_params = [
        {},  # No params
        {"team": sample_team['team_name']},
        {"team_name": sample_team['team_name']},
        {"state": sample_team['state']},
        {"search": sample_team['team_name']},
        {"q": sample_team['team_name']},
        {"page": "1"},
        {"limit": "10"}
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
    
    for base_url in base_urls:
        print(f"\nTesting base URL: {base_url}")
        print("-" * 40)
        
        base_results = {}
        
        for endpoint in endpoints:
            url = f"{base_url}/{endpoint}"
            print(f"  Testing: {url}")
            
            endpoint_results = {}
            
            for i, params in enumerate(test_params):
                try:
                    response = session.get(url, params=params, timeout=10)
                    
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
                            
                            # Save sample data
                            if data and (isinstance(data, dict) and data) or (isinstance(data, list) and len(data) > 0):
                                sample_file = Path(f"api_sample_{base_url.split('/')[-1]}_{endpoint}_test_{i+1}.json")
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
                            print(f"    JSON decode error. Content-Type: {response.headers.get('content-type', 'unknown')}")
                            
                    elif response.status_code == 404:
                        print(f"    404 Not Found")
                        endpoint_results[f"test_{i+1}"] = {
                            "status": "not_found",
                            "status_code": 404
                        }
                    else:
                        print(f"    HTTP {response.status_code}: {response.reason}")
                        endpoint_results[f"test_{i+1}"] = {
                            "status": "http_error",
                            "status_code": response.status_code,
                            "reason": response.reason
                        }
                        
                except requests.exceptions.RequestException as e:
                    print(f"    Request error: {e}")
                    endpoint_results[f"test_{i+1}"] = {
                        "status": "request_error",
                        "error": str(e)
                    }
                
                time.sleep(0.3)  # Small delay
            
            if endpoint_results:
                base_results[endpoint] = endpoint_results
        
        results[base_url] = base_results
    
    # Save results
    results_file = Path("gotsport_api_patterns_test.json")
    with open(results_file, 'w') as f:
        json.dump(results, f, indent=2)
    
    print(f"\nTest Results Summary:")
    print("=" * 50)
    
    successful_endpoints = []
    for base_url, base_results in results.items():
        if base_results:
            print(f"\n{base_url}")
            for endpoint, endpoint_results in base_results.items():
                successful_tests = [k for k, v in endpoint_results.items() if v.get("status") == "success"]
                if successful_tests:
                    print(f"  SUCCESS: /{endpoint} - {successful_tests}")
                    successful_endpoints.append(f"{base_url}/{endpoint}")
                else:
                    print(f"  FAILED: /{endpoint}")
    
    if successful_endpoints:
        print(f"\nSUCCESSFUL ENDPOINTS FOUND:")
        for endpoint in successful_endpoints:
            print(f"  - {endpoint}")
    else:
        print(f"\nNO SUCCESSFUL ENDPOINTS FOUND")
        print("This suggests GotSport may not have a public API for game history")
        print("or the endpoints are protected/require authentication")
    
    print(f"\nFull results saved to: {results_file}")
    
    return results

if __name__ == "__main__":
    test_gotsport_api_patterns()

