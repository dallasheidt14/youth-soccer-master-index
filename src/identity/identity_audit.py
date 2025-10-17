#!/usr/bin/env python3
"""
Team Identity Audit Module

Audits team identity map for low-similarity merges that could corrupt historical data.
Provides safeguards against accidental team merges across states/seasons.
"""

import json
import logging
import pandas as pd
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional
import argparse
import sys

# Try to import fuzzy matching libraries
try:
    from rapidfuzz import fuzz
    FUZZY_AVAILABLE = True
except ImportError:
    try:
        from fuzzywuzzy import fuzz
        FUZZY_AVAILABLE = True
    except ImportError:
        FUZZY_AVAILABLE = False

logger = logging.getLogger(__name__)

IDENTITY_PATH = Path("data/master/team_identity_map.json")
AUDIT_OUTPUT_DIR = Path("data/audit")
AUDIT_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def _load_identity_map() -> Dict[str, Any]:
    """Load the team identity map."""
    if not IDENTITY_PATH.exists():
        logger.warning(f"Identity map not found: {IDENTITY_PATH}")
        return {}
    
    try:
        with open(IDENTITY_PATH, 'r', encoding='utf-8') as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError) as e:
        logger.error(f"Failed to load identity map: {e}")
        return {}


def _calculate_similarity(name1: str, name2: str) -> float:
    """
    Calculate similarity between two team names.
    
    Args:
        name1: First team name
        name2: Second team name
        
    Returns:
        Similarity score between 0.0 and 1.0
    """
    if not FUZZY_AVAILABLE:
        # Fallback to simple string matching
        name1_clean = name1.lower().strip()
        name2_clean = name2.lower().strip()
        if name1_clean == name2_clean:
            return 1.0
        elif name1_clean in name2_clean or name2_clean in name1_clean:
            return 0.8
        else:
            return 0.0
    
    # Use fuzzy matching
    try:
        # Use token_sort_ratio for better handling of word order differences
        score = fuzz.token_sort_ratio(name1, name2)
        return score / 100.0  # Convert to 0.0-1.0 range
    except Exception as e:
        logger.warning(f"Error calculating similarity between '{name1}' and '{name2}': {e}")
        return 0.0


def audit_identity_map(threshold: float = 0.85) -> pd.DataFrame:
    """
    Audit team identity map for low-similarity matches.
    
    Args:
        threshold: Similarity threshold below which entries are flagged for review
        
    Returns:
        DataFrame with columns:
        - team_id_master: Master team ID
        - canonical_name: Canonical team name
        - alias: Team name alias
        - similarity_score: Similarity score (0.0-1.0)
        - review_flag: True if similarity < threshold
        - provider: Data provider
        - state, gender, age_group: Team demographics
        - first_seen: When alias was first seen
        - last_seen: When alias was last seen
    """
    logger.info(f"Starting identity map audit with threshold {threshold}")
    
    identity_map = _load_identity_map()
    if not identity_map:
        logger.warning("No identity map data to audit")
        return pd.DataFrame()
    
    audit_data = []
    
    for team_id_master, team_data in identity_map.items():
        canonical_name = team_data.get('canonical_name', '')
        aliases = team_data.get('aliases', [])
        provider = team_data.get('provider', 'unknown')
        state = team_data.get('state', '')
        gender = team_data.get('gender', '')
        age_group = team_data.get('age_group', '')
        
        # Check similarity for each alias
        for alias_name in aliases:
            if isinstance(alias_name, str):
                # Alias is a string
                alias_str = alias_name
                first_seen = ''
                last_seen = ''
            elif isinstance(alias_name, dict):
                # Alias is a dictionary with metadata
                alias_str = alias_name.get('name', '')
                first_seen = alias_name.get('first_seen_at', '')
                last_seen = alias_name.get('last_seen_at', '')
            else:
                # Skip invalid alias types
                continue
            
            if alias_str and canonical_name:
                similarity_score = _calculate_similarity(canonical_name, alias_str)
                review_flag = similarity_score < threshold
                
                audit_data.append({
                    'team_id_master': team_id_master,
                    'canonical_name': canonical_name,
                    'alias': alias_str,
                    'similarity_score': similarity_score,
                    'review_flag': review_flag,
                    'provider': provider,
                    'state': state,
                    'gender': gender,
                    'age_group': age_group,
                    'first_seen': first_seen,
                    'last_seen': last_seen
                })
    
    audit_df = pd.DataFrame(audit_data)
    
    if not audit_df.empty:
        flagged_count = len(audit_df[audit_df['review_flag']])
        logger.info(f"Audit complete: {len(audit_df)} entries, {flagged_count} flagged for review")
    else:
        logger.info("Audit complete: No entries found")
    
    return audit_df


def export_audit_report(audit_df: pd.DataFrame, output_path: Optional[str] = None) -> Path:
    """
    Export identity audit to CSV for human review.
    
    Args:
        audit_df: Audit DataFrame from audit_identity_map()
        output_path: Optional custom output path
        
    Returns:
        Path to exported CSV file
    """
    if output_path is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M")
        output_path = AUDIT_OUTPUT_DIR / f"identity_audit_{timestamp}.csv"
    else:
        output_path = Path(output_path)
    
    # Ensure output directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Sort by review_flag (flagged first) and similarity_score (lowest first)
    sorted_df = audit_df.sort_values(['review_flag', 'similarity_score'], ascending=[False, True])
    
    sorted_df.to_csv(output_path, index=False)
    logger.info(f"Audit report exported to: {output_path}")
    
    return output_path


def get_weekly_review_summary() -> Dict[str, Any]:
    """
    Generate weekly summary of identity issues.
    
    Returns:
        Dictionary with:
        - flagged_count: Number of entries needing review
        - low_similarity_entries: List of problematic mappings
        - new_teams_this_week: Count of new canonical IDs
        - total_entries: Total number of identity entries
        - avg_similarity: Average similarity score
    """
    logger.info("Generating weekly identity review summary")
    
    audit_df = audit_identity_map(threshold=0.85)
    
    if audit_df.empty:
        return {
            'flagged_count': 0,
            'low_similarity_entries': [],
            'new_teams_this_week': 0,
            'total_entries': 0,
            'avg_similarity': 0.0,
            'summary_date': datetime.now(timezone.utc).isoformat()
        }
    
    # Count flagged entries
    flagged_df = audit_df[audit_df['review_flag']]
    flagged_count = len(flagged_df)
    
    # Get low similarity entries (top 10 worst)
    low_similarity_entries = flagged_df.nsmallest(10, 'similarity_score')[
        ['team_id_master', 'canonical_name', 'alias', 'similarity_score', 'state', 'gender', 'age_group']
    ].to_dict('records')
    
    # Count new teams this week (rough estimate based on recent last_seen dates)
    week_ago = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    week_ago_str = week_ago.isoformat()
    
    recent_entries = audit_df[audit_df['last_seen'] >= week_ago_str]
    new_teams_this_week = len(recent_entries['team_id_master'].unique())
    
    # Calculate average similarity
    avg_similarity = audit_df['similarity_score'].mean()
    
    summary = {
        'flagged_count': flagged_count,
        'low_similarity_entries': low_similarity_entries,
        'new_teams_this_week': new_teams_this_week,
        'total_entries': len(audit_df),
        'avg_similarity': round(avg_similarity, 3),
        'summary_date': datetime.now(timezone.utc).isoformat()
    }
    
    logger.info(f"Weekly summary: {flagged_count} flagged, {new_teams_this_week} new teams, avg similarity {avg_similarity:.3f}")
    
    # Send Slack notification if there are issues
    try:
        from src.utils.notifier import notify_identity_audit_results
        notify_identity_audit_results(flagged_count, len(audit_df))
    except Exception as e:
        logger.warning(f"Failed to send Slack notification: {e}")
    
    return summary


def print_weekly_summary():
    """Print weekly summary to console."""
    summary = get_weekly_review_summary()
    
    print("\n" + "="*60)
    print("IDENTITY MAP WEEKLY REVIEW SUMMARY")
    print("="*60)
    print(f"Summary Date: {summary['summary_date']}")
    print(f"Total Entries: {summary['total_entries']}")
    print(f"Flagged for Review: {summary['flagged_count']}")
    print(f"New Teams This Week: {summary['new_teams_this_week']}")
    print(f"Average Similarity: {summary['avg_similarity']:.3f}")
    
    if summary['low_similarity_entries']:
        print(f"\nTop {len(summary['low_similarity_entries'])} Low-Similarity Entries:")
        print("-" * 60)
        for entry in summary['low_similarity_entries']:
            print(f"ID: {entry['team_id_master']}")
            print(f"  Canonical: {entry['canonical_name']}")
            print(f"  Alias: {entry['alias']}")
            print(f"  Similarity: {entry['similarity_score']:.3f}")
            print(f"  Demographics: {entry['state']} {entry['gender']} {entry['age_group']}")
            print()
    
    if summary['flagged_count'] > 0:
        print(f"WARNING: {summary['flagged_count']} entries need manual review!")
        print("   Run: python -m src.identity.identity_audit --export")
    else:
        print("All entries have acceptable similarity scores")
    
    print("="*60)


def main():
    """CLI entry point for identity audit."""
    parser = argparse.ArgumentParser(description="Team Identity Audit Tool")
    parser.add_argument("--threshold", type=float, default=0.85,
                       help="Similarity threshold for flagging (default: 0.85)")
    parser.add_argument("--export", action="store_true",
                       help="Export audit report to CSV")
    parser.add_argument("--weekly-summary", action="store_true",
                       help="Generate and print weekly summary")
    parser.add_argument("--output", type=str,
                       help="Custom output path for export")
    
    args = parser.parse_args()
    
    # Set up logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    if not FUZZY_AVAILABLE:
        logger.warning("Fuzzy matching libraries not available. Install rapidfuzz or fuzzywuzzy for better accuracy.")
    
    try:
        if args.weekly_summary:
            print_weekly_summary()
        else:
            # Run audit
            audit_df = audit_identity_map(threshold=args.threshold)
            
            if args.export:
                output_path = export_audit_report(audit_df, args.output)
                print(f"Audit report exported to: {output_path}")
            
            # Print summary
            if not audit_df.empty:
                flagged_count = len(audit_df[audit_df['review_flag']])
                print(f"\nAudit Results:")
                print(f"  Total entries: {len(audit_df)}")
                print(f"  Flagged for review: {flagged_count}")
                print(f"  Threshold: {args.threshold}")
                
                if flagged_count > 0:
                    print(f"\n⚠️  {flagged_count} entries need manual review!")
                    print("   Use --export to generate CSV report")
                else:
                    print("\n✅ All entries have acceptable similarity scores")
            else:
                print("No identity map data found to audit")
    
    except Exception as e:
        logger.exception(f"Identity audit failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
