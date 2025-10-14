"""
file_utils.py
-------------
Utility functions for safe file operations and timestamped file handling.

This module provides small, reusable helpers for common file operations
in the Youth Soccer Master Index project, including timestamped file naming,
directory creation, safe CSV writing, and file listing operations.

All functions include comprehensive error handling and logging support
to ensure robust file operations throughout the scraper framework.
"""

from pathlib import Path
import pandas as pd
import logging
from datetime import datetime
from typing import List, Optional


def get_timestamp(fmt: str = "%Y%m%d_%H%M") -> str:
    """
    Generate a formatted timestamp string for file naming.
    
    This function creates a standardized timestamp string that can be used
    for creating unique filenames with temporal ordering. The default format
    provides year, month, day, hour, and minute in a compact format.
    
    Args:
        fmt: DateTime format string (default: "%Y%m%d_%H%M")
             Common formats:
             - "%Y%m%d_%H%M" -> "20251013_1342" (default)
             - "%Y%m%d_%H%M%S" -> "20251013_134215" (with seconds)
             - "%Y-%m-%d_%H-%M" -> "2025-10-13_13-42" (readable)
    
    Returns:
        Formatted timestamp string
        
    Examples:
        >>> get_timestamp()
        '20251013_1342'
        >>> get_timestamp("%Y%m%d_%H%M%S")
        '20251013_134215'
    """
    return datetime.now().strftime(fmt)


def ensure_dir(path: Path) -> None:
    """
    Ensure that the directory for a file or folder path exists.
    
    This function creates all necessary parent directories for the given path
    if they don't already exist. It's safe to call multiple times and won't
    raise an error if the directory already exists.
    
    Args:
        path: Path object for file or directory to create parent directories for
        
    Raises:
        OSError: If directory creation fails due to permissions or other system issues
        
    Examples:
        >>> from pathlib import Path
        >>> ensure_dir(Path("data/master/sources/file.csv"))
        # Creates data/master/sources/ if it doesn't exist
        
        >>> ensure_dir(Path("logs/scraper.log"))
        # Creates logs/ directory if it doesn't exist
    """
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        raise OSError(f"Failed to create directory for {path}: {e}")


def safe_write_csv(
    df: pd.DataFrame, 
    path: Path, 
    logger: Optional[logging.Logger] = None
) -> Path:
    """
    Safely write a DataFrame to CSV with directory creation and logging.
    
    This function provides a robust way to save DataFrames to CSV files,
    ensuring the target directory exists and providing optional logging
    for successful operations.
    
    Args:
        df: Pandas DataFrame to save
        path: Target file path for the CSV
        logger: Optional logger instance for operation logging
        
    Returns:
        The path where the CSV was saved
        
    Raises:
        OSError: If directory creation fails
        PermissionError: If file cannot be written due to permissions
        Exception: For other unexpected errors during file writing
        
    Examples:
        >>> import pandas as pd
        >>> from pathlib import Path
        >>> df = pd.DataFrame({'col1': [1, 2], 'col2': ['a', 'b']})
        >>> safe_write_csv(df, Path("data/output.csv"), logger)
        # Creates data/ directory and saves CSV with logging
    """
    try:
        # Ensure directory exists
        ensure_dir(path)
        
        # Write CSV with UTF-8 encoding
        df.to_csv(path, index=False, encoding="utf-8")
        
        # Log success if logger provided
        if logger:
            logger.info(f"✅ Saved CSV → {path} ({len(df)} rows)")
        
        return path
        
    except OSError as e:
        error_msg = f"❌ Directory creation failed for {path}: {e}"
        if logger:
            logger.error(error_msg)
        raise OSError(error_msg)
        
    except PermissionError as e:
        error_msg = f"❌ Permission denied writing to {path}: {e}"
        if logger:
            logger.error(error_msg)
        raise PermissionError(error_msg)
        
    except Exception as e:
        error_msg = f"❌ Unexpected error saving CSV to {path}: {e}"
        if logger:
            logger.error(error_msg)
        raise Exception(error_msg)


def list_csvs(dir_path: Path, glob_pat: str = "*.csv") -> List[Path]:
    """
    List CSV files in a directory sorted by modification time (newest first).
    
    This function provides a convenient way to find and sort CSV files
    in a directory, useful for finding the most recent data files or
    processing files in chronological order.
    
    Args:
        dir_path: Directory path to search for CSV files
        glob_pat: Glob pattern for file matching (default: "*.csv")
                 Examples:
                 - "*.csv" -> All CSV files
                 - "gotsport_*.csv" -> Files starting with "gotsport_"
                 - "*_2025*.csv" -> Files containing "2025" in name
    
    Returns:
        List of Path objects sorted by modification time (newest first)
        
    Raises:
        FileNotFoundError: If the directory doesn't exist
        OSError: If directory cannot be read
        
    Examples:
        >>> from pathlib import Path
        >>> csv_files = list_csvs(Path("data/master"))
        # Returns all CSV files in data/master/ sorted by modification time
        
        >>> recent_files = list_csvs(Path("data/sources"), "gotsport_*.csv")
        # Returns only GotSport CSV files sorted by modification time
    """
    try:
        if not dir_path.exists():
            raise FileNotFoundError(f"Directory does not exist: {dir_path}")
        
        if not dir_path.is_dir():
            raise OSError(f"Path is not a directory: {dir_path}")
        
        # Find CSV files matching the pattern
        csv_files = list(dir_path.glob(glob_pat))
        
        # Sort by modification time (newest first)
        csv_files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
        
        return csv_files
        
    except FileNotFoundError as e:
        raise FileNotFoundError(f"Directory not found: {e}")
    except OSError as e:
        raise OSError(f"Cannot read directory {dir_path}: {e}")


def get_latest_csv(dir_path: Path, glob_pat: str = "*.csv") -> Optional[Path]:
    """
    Get the most recently modified CSV file in a directory.
    
    This is a convenience function that returns the newest CSV file
    from a directory, useful for finding the latest data file.
    
    Args:
        dir_path: Directory path to search for CSV files
        glob_pat: Glob pattern for file matching (default: "*.csv")
    
    Returns:
        Path to the most recent CSV file, or None if no files found
        
    Examples:
        >>> latest = get_latest_csv(Path("data/master"))
        # Returns the newest CSV file in data/master/
    """
    csv_files = list_csvs(dir_path, glob_pat)
    return csv_files[0] if csv_files else None


def create_timestamped_path(
    base_dir: Path, 
    filename_prefix: str, 
    extension: str = ".csv",
    timestamp_fmt: str = "%Y%m%d_%H%M"
) -> Path:
    """
    Create a timestamped file path with automatic directory creation.
    
    This function generates a unique filename with timestamp and ensures
    the target directory exists. Useful for creating unique output files
    with temporal ordering.
    
    Args:
        base_dir: Base directory for the file
        filename_prefix: Prefix for the filename (e.g., "gotsport_rankings")
        extension: File extension (default: ".csv")
        timestamp_fmt: Timestamp format string
    
    Returns:
        Complete Path object for the timestamped file
        
    Examples:
        >>> path = create_timestamped_path(
        ...     Path("data/master"), 
        ...     "gotsport_rankings", 
        ...     ".csv"
        ... )
        # Returns: data/master/gotsport_rankings_20251013_1342.csv
    """
    timestamp = get_timestamp(timestamp_fmt)
    filename = f"{filename_prefix}_{timestamp}{extension}"
    return base_dir / filename


if __name__ == "__main__":
    """
    Test the file utilities functions.
    """
    import tempfile
    import os
    
    print("Testing File Utilities")
    print("=" * 50)
    
    # Test timestamp generation
    print(f"Current timestamp: {get_timestamp()}")
    print(f"With seconds: {get_timestamp('%Y%m%d_%H%M%S')}")
    
    # Test with temporary directory
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        
        # Test directory creation
        test_file = temp_path / "subdir" / "test.csv"
        ensure_dir(test_file)
        print(f"Directory created: {test_file.parent}")
        
        # Test CSV writing
        df = pd.DataFrame({
            'team_name': ['Team A', 'Team B'],
            'age_group': ['U11', 'U12'],
            'points': [1000, 1200]
        })
        
        csv_path = safe_write_csv(df, test_file)
        print(f"CSV written: {csv_path}")
        
        # Test CSV listing
        csv_files = list_csvs(temp_path, "*.csv")
        print(f"Found CSV files: {len(csv_files)}")
        
        # Test timestamped path creation
        timestamped_path = create_timestamped_path(
            temp_path, 
            "test_data", 
            ".csv"
        )
        print(f"Timestamped path: {timestamped_path}")
        
        # Test latest CSV
        latest = get_latest_csv(temp_path)
        print(f"Latest CSV: {latest}")
    
    print("\nAll tests completed successfully!")
