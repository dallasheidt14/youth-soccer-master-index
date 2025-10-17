#!/usr/bin/env python3
"""
Slack Notification Utility

Provides safe Slack webhook notifications for pipeline events and alerts.
"""

import os
import requests
import json
import logging
from typing import Optional

logger = logging.getLogger(__name__)


def notify_slack(message: str, webhook_url: Optional[str] = None) -> bool:
    """
    Send a notification to Slack via webhook.
    
    Args:
        message: The message to send to Slack
        webhook_url: Optional webhook URL (if not provided, uses SLACK_WEBHOOK_URL env var)
        
    Returns:
        True if notification was sent successfully, False otherwise
    """
    # Get webhook URL from parameter or environment
    url = webhook_url or os.getenv("SLACK_WEBHOOK_URL")
    
    if not url:
        logger.warning("No Slack webhook configured. Set SLACK_WEBHOOK_URL environment variable.")
        return False
    
    # Prepare payload
    payload = {"text": message}
    
    try:
        response = requests.post(url, json=payload, timeout=10)
        response.raise_for_status()
        
        logger.debug(f"Slack notification sent successfully: {message[:50]}...")
        return True
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Slack notification failed: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error sending Slack notification: {e}")
        return False


def notify_registry_health(registry_stats: dict) -> bool:
    """
    Send registry health notification to Slack.
    
    Args:
        registry_stats: Dictionary from registry.get_registry_stats()
        
    Returns:
        True if notification was sent successfully, False otherwise
    """
    health_score = registry_stats.get('registry_health', 0)
    total_slices = registry_stats.get('total_slices', 0)
    stale_count = registry_stats.get('stale_count', 0)
    registry_version = registry_stats.get('registry_version', 'unknown')
    
    if health_score < 80:
        message = (f"[WARNING] Registry health is low: {health_score}% "
                  f"({stale_count} stale slices)")
    else:
        message = (f"[SUCCESS] Registry healthy ({health_score}%) - "
                  f"{total_slices} slices, v{registry_version}")
    
    return notify_slack(message)


def notify_pipeline_start(states: list, genders: list, ages: list) -> bool:
    """
    Send pipeline start notification to Slack.
    
    Args:
        states: List of state codes
        genders: List of genders
        ages: List of age groups
        
    Returns:
        True if notification was sent successfully, False otherwise
    """
    slices_count = len(states) * len(genders) * len(ages)
    message = (f"🚀 Starting pipeline: {slices_count} slices "
              f"({', '.join(states)} x {', '.join(genders)} x {', '.join(ages)})")
    
    return notify_slack(message)


def notify_pipeline_complete(success_count: int, total_count: int, 
                           failed_slices: list = None) -> bool:
    """
    Send pipeline completion notification to Slack.
    
    Args:
        success_count: Number of successful slices
        total_count: Total number of slices processed
        failed_slices: List of failed slice names
        
    Returns:
        True if notification was sent successfully, False otherwise
    """
    if success_count == total_count:
        message = f"[SUCCESS] Pipeline completed successfully: {success_count}/{total_count} slices"
    else:
        failed_list = ', '.join(failed_slices[:5]) if failed_slices else 'unknown'
        if len(failed_slices) > 5:
            failed_list += f" (+{len(failed_slices) - 5} more)"
        
        message = (f"[WARNING] Pipeline completed with issues: {success_count}/{total_count} slices "
                  f"(failed: {failed_list})")
    
    return notify_slack(message)


def notify_identity_audit_results(flagged_count: int, total_entries: int) -> bool:
    """
    Send identity audit results notification to Slack.
    
    Args:
        flagged_count: Number of entries flagged for review
        total_entries: Total number of entries audited
        
    Returns:
        True if notification was sent successfully, False otherwise
    """
    if flagged_count == 0:
        message = f"[SUCCESS] Identity audit clean: {total_entries} entries, 0 flagged"
    else:
        percentage = (flagged_count / total_entries * 100) if total_entries > 0 else 0
        message = (f"[WARNING] Identity audit: {flagged_count}/{total_entries} entries flagged "
                  f"({percentage:.1f}%) - manual review needed")
    
    return notify_slack(message)


def notify_game_integrity_issues(slices_needing_refresh: int, total_slices: int) -> bool:
    """
    Send game integrity issues notification to Slack.
    
    Args:
        slices_needing_refresh: Number of slices needing refresh
        total_slices: Total number of slices checked
        
    Returns:
        True if notification was sent successfully, False otherwise
    """
    if slices_needing_refresh == 0:
        message = f"[SUCCESS] Game integrity check passed: {total_slices} slices healthy"
    else:
        message = (f"[WARNING] Game integrity issues detected: {slices_needing_refresh}/{total_slices} "
                  f"slices need refresh")
    
    return notify_slack(message)


def test_slack_connection() -> bool:
    """
    Test Slack webhook connection.
    
    Returns:
        True if connection test was successful, False otherwise
    """
    test_message = "🧪 Slack notification test from Youth Soccer Master Index"
    return notify_slack(test_message)


if __name__ == "__main__":
    """CLI interface for testing Slack notifications."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Slack Notification Utility")
    parser.add_argument("--test", action="store_true", help="Test Slack connection")
    parser.add_argument("--message", type=str, help="Send custom message")
    parser.add_argument("--registry-health", action="store_true", help="Send registry health notification")
    
    args = parser.parse_args()
    
    # Set up logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    if args.test:
        success = test_slack_connection()
        if success:
            print("[SUCCESS] Slack connection test successful")
        else:
            print("[ERROR] Slack connection test failed")
            exit(1)
    
    elif args.message:
        success = notify_slack(args.message)
        if success:
            print(f"[SUCCESS] Message sent: {args.message}")
        else:
            print("[ERROR] Failed to send message")
            exit(1)
    
    elif args.registry_health:
        try:
            from src.registry.registry import get_registry
            registry = get_registry()
            stats = registry.get_registry_stats()
            success = notify_registry_health(stats)
            if success:
                print("[SUCCESS] Registry health notification sent")
            else:
                print("[ERROR] Failed to send registry health notification")
                exit(1)
        except Exception as e:
            print(f"[ERROR] Error getting registry stats: {e}")
            exit(1)
    
    else:
        parser.print_help()
