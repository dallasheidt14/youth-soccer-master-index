#!/usr/bin/env python3
"""
JSON Safety Utilities - Handle Path serialization and other JSON edge cases.

This module provides utilities for safely serializing Python objects to JSON,
particularly handling pathlib.Path objects and other non-JSON-serializable types.
"""

import json
from pathlib import Path
from typing import Any


def serialize_paths(obj: Any) -> Any:
    """
    Recursively convert Path objects to strings for JSON serialization.
    Normalizes all path separators to forward slashes for cross-platform compatibility.
    
    Args:
        obj: Any Python object that may contain Path objects
        
    Returns:
        Object with all Path instances converted to normalized strings
    """
    if isinstance(obj, Path):
        return obj.as_posix()
    elif isinstance(obj, str) and ('\\' in obj or '/' in obj):
        # Normalize string paths to use forward slashes
        return obj.replace('\\', '/')
    elif isinstance(obj, dict):
        return {k: serialize_paths(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [serialize_paths(x) for x in obj]
    elif isinstance(obj, tuple):
        return tuple(serialize_paths(x) for x in obj)
    elif isinstance(obj, set):
        return {serialize_paths(x) for x in obj}
    return obj


def safe_json_dump(data: Any, file_path: str, **kwargs) -> None:
    """
    Safely dump data to JSON file with Path serialization.
    
    Args:
        data: Data to serialize to JSON
        file_path: Path to output JSON file
        **kwargs: Additional arguments passed to json.dump()
    """
    # Serialize Path objects before JSON dump
    cleaned_data = serialize_paths(data)
    
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(cleaned_data, f, **kwargs)
