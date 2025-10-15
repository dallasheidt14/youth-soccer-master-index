#!/usr/bin/env python3
"""
Safe Write Operations with Atomic Writes and Checksums

Provides atomic file writing operations that write to temporary files first,
compute checksums, and then atomically rename to the final destination.
Includes support for CSV and Parquet formats.
"""

import hashlib
import os
import tempfile
from pathlib import Path
from typing import Dict, Union, Optional
import pandas as pd
import logging

from src.scraper.utils.logger import get_logger


def compute_file_checksum(file_path: Path, algorithm: str = 'md5') -> str:
    """
    Compute checksum for a file.
    
    Args:
        file_path: Path to the file
        algorithm: Hash algorithm ('md5', 'sha1', 'sha256')
        
    Returns:
        Hexadecimal checksum string
    """
    hash_obj = hashlib.new(algorithm)
    
    with open(file_path, 'rb') as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_obj.update(chunk)
    
    return hash_obj.hexdigest()


def safe_write_csv(df: pd.DataFrame, path: Union[str, Path], 
                   logger: Optional[logging.Logger] = None) -> Dict[str, Union[str, int, Path]]:
    """
    Safely write DataFrame to CSV with atomic operation and checksum.
    
    Args:
        df: pandas DataFrame to write
        path: Destination file path
        logger: Optional logger instance
        
    Returns:
        Dictionary with path, checksum, and size information
        
    Raises:
        Exception: If write operation fails
    """
    if logger is None:
        logger = get_logger(__name__)
    
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    
    # Create temporary file in same directory
    temp_path = path.with_suffix('.tmp')
    
    try:
        logger.info(f"Writing CSV to temporary file: {temp_path}")
        
        # Write to temporary file
        df.to_csv(temp_path, index=False)
        
        # Compute checksum
        checksum = compute_file_checksum(temp_path)
        size_bytes = temp_path.stat().st_size
        
        # Atomically rename to final destination
        temp_path.rename(path)
        
        logger.info(f"Successfully wrote CSV: {path} ({size_bytes:,} bytes, MD5: {checksum})")
        
        return {
            "path": path,
            "checksum": checksum,
            "size_bytes": size_bytes,
            "format": "csv"
        }
        
    except Exception as e:
        # Clean up temporary file on error
        if temp_path.exists():
            temp_path.unlink()
        logger.error(f"Failed to write CSV to {path}: {e}")
        raise


def safe_write_parquet(df: pd.DataFrame, path: Union[str, Path],
                      logger: Optional[logging.Logger] = None) -> Dict[str, Union[str, int, Path]]:
    """
    Safely write DataFrame to Parquet with atomic operation and checksum.
    
    Args:
        df: pandas DataFrame to write
        path: Destination file path
        logger: Optional logger instance
        
    Returns:
        Dictionary with path, checksum, and size information
        
    Raises:
        Exception: If write operation fails
    """
    if logger is None:
        logger = get_logger(__name__)
    
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    
    # Create temporary file in same directory
    temp_path = path.with_suffix('.tmp')
    
    try:
        logger.info(f"Writing Parquet to temporary file: {temp_path}")
        
        # Write to temporary file
        df.to_parquet(temp_path, index=False)
        
        # Compute checksum
        checksum = compute_file_checksum(temp_path)
        size_bytes = temp_path.stat().st_size
        
        # Atomically rename to final destination
        temp_path.rename(path)
        
        logger.info(f"Successfully wrote Parquet: {path} ({size_bytes:,} bytes, MD5: {checksum})")
        
        return {
            "path": path,
            "checksum": checksum,
            "size_bytes": size_bytes,
            "format": "parquet"
        }
        
    except Exception as e:
        # Clean up temporary file on error
        if temp_path.exists():
            temp_path.unlink()
        logger.error(f"Failed to write Parquet to {path}: {e}")
        raise


def safe_write_json(data: Union[dict, list], path: Union[str, Path],
                   logger: Optional[logging.Logger] = None) -> Dict[str, Union[str, int, Path]]:
    """
    Safely write JSON data with atomic operation and checksum.
    
    Args:
        data: Dictionary or list to write as JSON
        path: Destination file path
        logger: Optional logger instance
        
    Returns:
        Dictionary with path, checksum, and size information
        
    Raises:
        Exception: If write operation fails
    """
    import json
    
    if logger is None:
        logger = get_logger(__name__)
    
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    
    # Create temporary file in same directory
    temp_path = path.with_suffix('.tmp')
    
    try:
        logger.info(f"Writing JSON to temporary file: {temp_path}")
        
        # Write to temporary file
        with open(temp_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        # Compute checksum
        checksum = compute_file_checksum(temp_path)
        size_bytes = temp_path.stat().st_size
        
        # Atomically rename to final destination
        temp_path.rename(path)
        
        logger.info(f"Successfully wrote JSON: {path} ({size_bytes:,} bytes, MD5: {checksum})")
        
        return {
            "path": path,
            "checksum": checksum,
            "size_bytes": size_bytes,
            "format": "json"
        }
        
    except Exception as e:
        # Clean up temporary file on error
        if temp_path.exists():
            temp_path.unlink()
        logger.error(f"Failed to write JSON to {path}: {e}")
        raise


def verify_file_integrity(file_path: Path, expected_checksum: str, 
                         algorithm: str = 'md5') -> bool:
    """
    Verify file integrity by comparing checksums.
    
    Args:
        file_path: Path to the file to verify
        expected_checksum: Expected checksum value
        algorithm: Hash algorithm used
        
    Returns:
        True if checksums match, False otherwise
    """
    if not file_path.exists():
        return False
    
    actual_checksum = compute_file_checksum(file_path, algorithm)
    return actual_checksum == expected_checksum


if __name__ == "__main__":
    # Test the safe write functions
    import pandas as pd
    import tempfile
    
    logger = get_logger(__name__)
    
    # Create test DataFrame
    test_data = {
        'team_name': ['FC Elite AZ', 'Premier Soccer Club', 'United FC'],
        'state': ['AZ', 'CA', 'NY'],
        'age_group': ['U10', 'U12', 'U14'],
        'gender': ['Male', 'Female', 'Male']
    }
    df = pd.DataFrame(test_data)
    
    # Test CSV writing
    with tempfile.TemporaryDirectory() as temp_dir:
        csv_path = Path(temp_dir) / "test.csv"
        result = safe_write_csv(df, csv_path, logger)
        print(f"CSV write result: {result}")
        
        # Verify integrity
        is_valid = verify_file_integrity(csv_path, result['checksum'])
        print(f"File integrity check: {is_valid}")
        
        # Test reading back
        df_read = pd.read_csv(csv_path)
        print(f"DataFrame shape: {df_read.shape}")
        print(f"DataFrame columns: {list(df_read.columns)}")
    
    print("Safe write test completed successfully!")

