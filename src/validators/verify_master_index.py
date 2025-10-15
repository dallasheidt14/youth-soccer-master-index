"""
verify_master_index.py
---------------------
Post-run validation and summary analytics for the master index DataFrame.

This utility provides comprehensive analysis of the master team index, including
data quality metrics, distribution statistics, and validation checks to ensure
the integrity of the scraped and merged data.

Features:
- Total row counts and data completeness
- State coverage analysis
- Age group and gender distributions
- Data quality validation (missing states, duplicates)
- Comprehensive logging with emoji-enhanced output
- CLI interface for quick verification
"""

import pandas as pd
import logging
from typing import Optional, Dict, Any
import sys
from pathlib import Path

# Add project root to Python path for imports
sys.path.append(str(Path(__file__).resolve().parents[2]))

from src.scraper.utils.file_utils import list_csvs


class ValidationError(Exception):
    """Custom exception for validation errors in the master index."""
    pass


def summarize_master(df: pd.DataFrame, logger: Optional[logging.Logger] = None) -> Dict[str, Any]:
    """
    Summarize and validate a master index DataFrame.

    This function provides comprehensive analysis of the master team index,
    including data quality metrics, distribution statistics, and validation
    checks to ensure the integrity of the scraped and merged data.

    Parameters
    ----------
    df : pd.DataFrame
        The master index DataFrame to analyze.
    logger : Optional[logging.Logger]
        Optional logger for structured output. If provided, will log detailed
        summary information with emoji-enhanced formatting.

    Returns
    -------
    Dict[str, Any]
        Summary dictionary containing:
        - total_rows: Total number of teams
        - unique_states_count: Number of unique states
        - unique_states: Sorted list of state codes
        - age_distribution: Count of teams per age group
        - gender_distribution: Count of teams per gender
        - missing_state_rows: Number of rows with missing state data
        - duplicate_rows_on_team_age_gender: Number of duplicate team entries
        - data_quality_score: Overall data quality score (0-100)
        - provider_distribution: Count of teams per data provider
        - top_states: Top 5 states by team count
        - age_gender_matrix: Cross-tabulation of age groups and genders

    Examples
    --------
    >>> import pandas as pd
    >>> df = pd.read_csv("data/master/master_team_index_20251013_1355.csv")
    >>> summary = summarize_master(df)
    >>> print(f"Total teams: {summary['total_rows']}")
    """
    try:
        # Basic metrics
        total_rows = len(df)
        
        # State analysis
        unique_states = sorted(df["state"].dropna().unique().tolist()) if "state" in df.columns else []
        missing_state_rows = int(df["state"].isna().sum()) if "state" in df.columns else 0
        
        # Distribution analysis
        age_distribution = df["age_group"].value_counts().to_dict() if "age_group" in df.columns else {}
        gender_distribution = df["gender"].value_counts().to_dict() if "gender" in df.columns else {}
        provider_distribution = df["provider"].value_counts().to_dict() if "provider" in df.columns else {}
        
        # Data quality checks
        duplicate_rows_on_team_age_gender = int(df.duplicated(subset=["team_name", "age_group", "gender"], keep=False).sum()) if all(col in df.columns for col in ["team_name", "age_group", "gender"]) else 0
        
        # Top states analysis
        if "state" in df.columns and not df["state"].isna().all():
            state_counts = df["state"].value_counts()
            top_states = state_counts.head(5).to_dict()
        else:
            top_states = {}
        
        # Age-gender cross-tabulation
        if "age_group" in df.columns and "gender" in df.columns:
            age_gender_matrix = pd.crosstab(df["age_group"], df["gender"]).to_dict()
        else:
            age_gender_matrix = {}
        
        # Data quality score calculation
        quality_factors = []
        
        # Factor 1: Completeness of state data
        if total_rows > 0:
            state_completeness = (total_rows - missing_state_rows) / total_rows
            quality_factors.append(state_completeness * 30)  # 30% weight
        
        # Factor 2: Absence of duplicates
        if total_rows > 0:
            duplicate_penalty = min(duplicate_rows_on_team_age_gender / total_rows, 0.2)  # Max 20% penalty
            quality_factors.append((1 - duplicate_penalty) * 25)  # 25% weight
        
        # Factor 3: Data diversity (multiple states, age groups, genders)
        diversity_score = 0
        if len(unique_states) > 0:
            diversity_score += min(len(unique_states) / 50, 1) * 20  # 20% weight for state diversity
        if len(age_distribution) > 0:
            diversity_score += min(len(age_distribution) / 9, 1) * 15  # 15% weight for age diversity
        if len(gender_distribution) > 0:
            diversity_score += min(len(gender_distribution) / 2, 1) * 10  # 10% weight for gender diversity
        
        quality_factors.append(diversity_score)
        
        # Calculate overall quality score
        data_quality_score = min(sum(quality_factors), 100)
        
        # Create comprehensive summary dictionary
        summary = {
            "total_rows": total_rows,
            "unique_states_count": len(unique_states),
            "unique_states": unique_states,
            "age_distribution": age_distribution,
            "gender_distribution": gender_distribution,
            "provider_distribution": provider_distribution,
            "missing_state_rows": missing_state_rows,
            "duplicate_rows_on_team_age_gender": duplicate_rows_on_team_age_gender,
            "data_quality_score": round(data_quality_score, 1),
            "top_states": top_states,
            "age_gender_matrix": age_gender_matrix,
            "state_completeness_percent": round((total_rows - missing_state_rows) / total_rows * 100, 1) if total_rows > 0 else 0,
            "duplicate_percent": round(duplicate_rows_on_team_age_gender / total_rows * 100, 1) if total_rows > 0 else 0
        }
        
        # Optional logging with emoji-enhanced output
        if logger:
            logger.info("📊 MASTER INDEX SUMMARY")
            logger.info("=" * 50)
            logger.info(f"📈 Total teams: {total_rows}")
            logger.info(f"🌎 States covered: {len(unique_states)} → {unique_states}")
            logger.info(f"📊 Age distribution: {age_distribution}")
            logger.info(f"⚽ Gender distribution: {gender_distribution}")
            logger.info(f"🏢 Provider distribution: {provider_distribution}")
            logger.info(f"⚠️ Missing state rows: {missing_state_rows}")
            logger.info(f"🔄 Duplicate team-age-gender rows: {duplicate_rows_on_team_age_gender}")
            logger.info(f"⭐ Data quality score: {data_quality_score}/100")
            logger.info(f"📈 State completeness: {summary['state_completeness_percent']}%")
            logger.info(f"🔄 Duplicate rate: {summary['duplicate_percent']}%")
            
            if top_states:
                logger.info(f"🏆 Top 5 states by team count: {top_states}")
            
            if age_gender_matrix:
                logger.info("📊 Age-Gender Matrix:")
                for age_group, genders in age_gender_matrix.items():
                    logger.info(f"   {age_group}: {genders}")
        
        return summary
        
    except Exception as e:
        error_msg = f"❌ Error analyzing master index: {e}"
        if logger:
            logger.exception(error_msg)
        else:
            print(error_msg)
        raise


def validate_data_quality(df: pd.DataFrame, logger: Optional[logging.Logger] = None) -> Dict[str, bool]:
    """
    Perform data quality validation checks on the master index.
    
    Args:
        df: Master index DataFrame to validate
        logger: Optional logger for validation results
        
    Returns:
        Dictionary of validation results with boolean flags
    """
    try:
        validation_results = {}
        
        # Check 1: Non-empty DataFrame
        validation_results["has_data"] = len(df) > 0
        
        # Check 2: Required columns present
        required_columns = ["team_name", "age_group", "gender", "state", "source"]
        validation_results["has_required_columns"] = all(col in df.columns for col in required_columns)
        
        # Check 3: No completely empty columns
        validation_results["no_empty_columns"] = not df.isnull().all().any()
        
        # Check 4: Reasonable state completeness (at least 80%)
        if "state" in df.columns and len(df) > 0:
            state_completeness = (len(df) - df["state"].isna().sum()) / len(df)
            validation_results["good_state_completeness"] = state_completeness >= 0.8
        else:
            validation_results["good_state_completeness"] = False
        
        # Check 5: Low duplicate rate (less than 5%)
        if all(col in df.columns for col in ["team_name", "age_group", "gender"]) and len(df) > 0:
            duplicate_rate = df.duplicated(subset=["team_name", "age_group", "gender"]).sum() / len(df)
            validation_results["low_duplicate_rate"] = duplicate_rate < 0.05
        else:
            validation_results["low_duplicate_rate"] = False
        
        # Check 6: Multiple states represented
        if "state" in df.columns:
            unique_states = df["state"].dropna().nunique()
            validation_results["multiple_states"] = unique_states >= 3
        else:
            validation_results["multiple_states"] = False
        
        # Overall validation score
        validation_results["overall_valid"] = all(validation_results.values())
        
        if logger:
            logger.info("🔍 DATA QUALITY VALIDATION")
            logger.info("=" * 50)
            for check, result in validation_results.items():
                status = "✅" if result else "❌"
                logger.info(f"{status} {check}: {result}")
        
        return validation_results
        
    except Exception as e:
        error_msg = f"❌ Error during data validation: {e}"
        if logger:
            logger.exception(error_msg)
        else:
            print(error_msg)
        raise


def analyze_trends(df: pd.DataFrame, logger: Optional[logging.Logger] = None) -> Dict[str, Any]:
    """
    Analyze trends and patterns in the master index data.
    
    Args:
        df: Master index DataFrame to analyze
        logger: Optional logger for trend analysis results
        
    Returns:
        Dictionary containing trend analysis results
    """
    try:
        trends = {}
        
        # Age group trends
        if "age_group" in df.columns:
            age_counts = df["age_group"].value_counts()
            trends["most_common_age_group"] = age_counts.index[0] if len(age_counts) > 0 else None
            trends["age_group_balance"] = age_counts.std() / age_counts.mean() if len(age_counts) > 0 else 0
        
        # Gender trends
        if "gender" in df.columns:
            gender_counts = df["gender"].value_counts()
            trends["gender_balance"] = gender_counts.std() / gender_counts.mean() if len(gender_counts) > 0 else 0
        
        # State trends
        if "state" in df.columns:
            state_counts = df["state"].value_counts()
            trends["state_concentration"] = state_counts.iloc[0] / len(df) if len(state_counts) > 0 else 0
            trends["geographic_diversity"] = len(state_counts) / 50  # Normalized by total US states
        
        # Provider trends
        if "provider" in df.columns:
            provider_counts = df["provider"].value_counts()
            trends["provider_diversity"] = len(provider_counts)
            trends["dominant_provider"] = provider_counts.index[0] if len(provider_counts) > 0 else None
        
        if logger:
            logger.info("📈 TREND ANALYSIS")
            logger.info("=" * 50)
            for trend, value in trends.items():
                logger.info(f"📊 {trend}: {value}")
        
        return trends
        
    except Exception as e:
        error_msg = f"❌ Error during trend analysis: {e}"
        if logger:
            logger.exception(error_msg)
        else:
            print(error_msg)
        raise


def validate_master_index_with_schema(df: pd.DataFrame, logger: Optional[logging.Logger] = None) -> Dict[str, Any]:
    """
    Enhanced master index validation using Pandera schema validation.
    
    Args:
        df: DataFrame containing master team index data
        logger: Optional logger instance for output
        
    Returns:
        Dictionary containing comprehensive validation results
        
    Raises:
        ValueError: If critical validation failures occur
    """
    if logger is None:
        from src.scraper.utils.logger import get_logger
        logger = get_logger(__name__)
    
    logger.info("🔍 Starting enhanced master index validation with schema...")
    
    try:
        # Import schema validation
        from src.schema.master_team_schema import validate_dataframe, MasterTeamSchema
        
        # Step 1: Validate required columns
        logger.info("📋 Step 1: Validating required columns...")
        required_columns = [
            'team_id', 'provider_team_id', 'team_name', 'age_group', 'age_u',
            'gender', 'state', 'provider', 'club_name', 'source_url', 'created_at'
        ]
        
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValidationError(f"Missing required columns: {missing_columns}")
        
        logger.info("✅ Required columns validation passed")
        
        # Step 2: Pandera schema validation
        logger.info("📊 Step 2: Running Pandera schema validation...")
        try:
            validate_dataframe(df)
            schema_status = "passed"
            schema_message = "All schema validations passed"
            logger.info("✅ Schema validation passed")
        except Exception as e:
            schema_status = "failed"
            schema_message = str(e)
            logger.error(f"❌ Schema validation failed: {e}")
            raise ValueError(f"Schema validation failed: {e}") from e
        
        # Step 3: Run existing comprehensive validation
        logger.info("📊 Step 3: Running comprehensive data quality checks...")
        validate_data_quality(df, logger)
        
        # Step 4: Enhanced duplicate checking
        logger.info("🔍 Step 4: Checking for duplicates...")
        duplicate_fields = ['team_name', 'state', 'age_u', 'gender']
        duplicates = df[df.duplicated(subset=duplicate_fields, keep=False)]
        duplicate_count = len(duplicates)
        
        # Check for team_id duplicates (should be unique)
        team_id_duplicates = df[df.duplicated(subset=['team_id'], keep=False)]
        team_id_duplicate_count = len(team_id_duplicates)
        
        if duplicate_count > 0:
            logger.warning(f"⚠️ Found {duplicate_count} duplicate teams")
        
        if team_id_duplicate_count > 0:
            logger.error(f"❌ Found {team_id_duplicate_count} duplicate team_ids")
            raise ValueError(f"Duplicate team_ids found: {team_id_duplicate_count}")
        
        # Step 5: State code validation
        logger.info("🗺️ Step 5: Validating state codes...")
        valid_states = {
            'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA',
            'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
            'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
            'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
            'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY',
            'DC'
        }
        
        invalid_states = df[~df['state'].isin(valid_states)]
        invalid_state_count = len(invalid_states)
        
        if invalid_state_count > 0:
            invalid_state_codes = invalid_states['state'].unique().tolist()
            logger.warning(f"⚠️ Found {invalid_state_count} rows with invalid state codes: {invalid_state_codes}")
            
            # If more than 1% invalid states, fail validation
            if invalid_state_count > len(df) * 0.01:
                raise ValueError(f"Too many invalid state codes: {invalid_state_count}")
        
        # Step 6: Gender validation
        logger.info("👥 Step 6: Validating gender normalization...")
        valid_genders = {'M', 'F'}
        invalid_genders = df[~df['gender'].isin(valid_genders)]
        invalid_gender_count = len(invalid_genders)
        
        if invalid_gender_count > 0:
            invalid_gender_values = invalid_genders['gender'].unique().tolist()
            logger.warning(f"⚠️ Found {invalid_gender_count} rows with invalid gender values: {invalid_gender_values}")
        
        # Step 7: Age validation
        logger.info("🎂 Step 7: Validating age ranges...")
        invalid_ages = df[(df['age_u'] < 10) | (df['age_u'] > 18)]
        invalid_age_count = len(invalid_ages)
        
        # Check age_group format consistency
        import re
        age_group_pattern = r'^U(1[0-8]|[0-9])$'
        invalid_age_groups = df[~df['age_group'].str.match(age_group_pattern, na=False)]
        invalid_age_group_count = len(invalid_age_groups)
        
        # Check consistency between age_group and age_u
        inconsistent_ages = df[df['age_group'] != df['age_u'].apply(lambda x: f"U{x}")]
        inconsistent_count = len(inconsistent_ages)
        
        if invalid_age_count > 0:
            logger.warning(f"⚠️ Found {invalid_age_count} rows with invalid ages")
        
        if invalid_age_group_count > 0:
            logger.warning(f"⚠️ Found {invalid_age_group_count} rows with invalid age group format")
        
        if inconsistent_count > 0:
            logger.warning(f"⚠️ Found {inconsistent_count} rows with inconsistent age data")
        
        # Calculate overall data quality score
        total_issues = (
            duplicate_count +
            invalid_state_count +
            invalid_gender_count +
            invalid_age_count +
            invalid_age_group_count +
            inconsistent_count
        )
        
        quality_score = max(0, 100 - (total_issues / len(df) * 100))
        
        # Create comprehensive validation results
        enhanced_results = {
            "schema_validation": {
                "status": schema_status,
                "message": schema_message
            },
            "total_rows": len(df),
            "total_columns": len(df.columns),
            "duplicates": {
                "total_duplicates": duplicate_count,
                "team_id_duplicates": team_id_duplicate_count
            },
            "states": {
                "invalid_state_count": invalid_state_count,
                "invalid_state_codes": invalid_states['state'].unique().tolist() if invalid_state_count > 0 else []
            },
            "genders": {
                "invalid_gender_count": invalid_gender_count,
                "invalid_gender_values": invalid_genders['gender'].unique().tolist() if invalid_gender_count > 0 else []
            },
            "ages": {
                "invalid_age_count": invalid_age_count,
                "invalid_age_group_count": invalid_age_group_count,
                "inconsistent_age_count": inconsistent_count
            },
            "data_quality_score": quality_score,
            "validation_timestamp": pd.Timestamp.now().isoformat(),
            "overall_status": "passed" if total_issues == 0 else "warnings" if total_issues < len(df) * 0.05 else "failed"
        }
        
        logger.info(f"📊 Enhanced validation completed")
        logger.info(f"   Total rows: {enhanced_results['total_rows']:,}")
        logger.info(f"   Total issues: {total_issues}")
        logger.info(f"   Quality score: {quality_score:.1f}%")
        logger.info(f"   Overall status: {enhanced_results['overall_status']}")
        
        return enhanced_results
        
    except Exception as e:
        logger.exception(f"❌ Enhanced master index validation failed: {e}")
        raise


if __name__ == "__main__":
    """
    CLI interface for quick verification of the latest master index.
    
    This allows running the verification utility directly from the command line
    to quickly analyze the most recent master index file.
    """
    try:
        print("Master Index Verification Utility")
        print("=" * 50)
        
        # Find the latest master index file
        master_dir = Path("data/master")
        if not master_dir.exists():
            print("ERROR: Master directory not found. Run the orchestrator first.")
            sys.exit(1)
        
        master_files = list_csvs(master_dir, "master_team_index_*.csv")
        
        if not master_files:
            print("ERROR: No master index files found. Run the orchestrator first.")
            sys.exit(1)
        
        latest_file = master_files[0]  # Already sorted by modification time (newest first)
        print(f"Analyzing latest master index: {latest_file}")
        
        # Load and analyze the DataFrame
        df = pd.read_csv(latest_file)
        
        # Run comprehensive analysis
        print("\nRunning comprehensive analysis...")
        summary = summarize_master(df)
        
        print("\nRunning data quality validation...")
        validation = validate_data_quality(df)
        
        print("\nRunning trend analysis...")
        trends = analyze_trends(df)
        
        # Print summary results
        print("\n" + "=" * 50)
        print("ANALYSIS COMPLETE")
        print("=" * 50)
        print(f"Total teams: {summary['total_rows']}")
        print(f"States covered: {summary['unique_states_count']}")
        print(f"Data quality score: {summary['data_quality_score']}/100")
        print(f"Overall validation: {'PASS' if validation['overall_valid'] else 'FAIL'}")
        
        if summary['total_rows'] > 0:
            print(f"Top states: {list(summary['top_states'].keys())[:3]}")
            print(f"Age groups: {list(summary['age_distribution'].keys())}")
            print(f"Providers: {list(summary['provider_distribution'].keys())}")
        
        print("\nVerification completed successfully!")
        
    except Exception as e:
        print(f"ERROR: Error during verification: {e}")
        sys.exit(1)
