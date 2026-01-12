"""
Basic data quality validation functions

Simple checks for schema validation, null completeness, and uniqueness
"""

from pyspark.sql import DataFrame
from typing import List


class DataQualityException(Exception):
    """Exception raised for data quality validation failures"""
    pass


def validate_schema(df: DataFrame, expected_columns: List[str], table_name: str):
    """
    Validate that DataFrame has all expected columns.
    
    Args:
        df: DataFrame to validate
        expected_columns: List of expected column names
        table_name: Name of table for error messages
        
    Raises:
        DataQualityException: If required columns are missing
    """
    actual_columns = set(df.columns)
    expected_set = set(expected_columns)
    missing = expected_set - actual_columns
    
    if missing:
        raise DataQualityException(
            f"Schema validation failed for {table_name}. Missing columns: {missing}"
        )


def check_null_completeness(df: DataFrame, required_columns: List[str], table_name: str):
    """
    Check that required columns have no null values.
    
    Args:
        df: DataFrame to validate
        required_columns: Columns that should not have nulls
        table_name: Name of table for error messages
        
    Raises:
        DataQualityException: If nulls found in required columns
    """
    for col in required_columns:
        null_count = df.filter(df[col].isNull()).count()
        if null_count > 0:
            raise DataQualityException(
                f"Completeness check failed for {table_name}.{col}: {null_count} null values found"
            )


def check_uniqueness(df: DataFrame, unique_columns: List[str], table_name: str):
    """
    Check that specified columns contain unique values.
    
    Args:
        df: DataFrame to validate
        unique_columns: Columns that should be unique
        table_name: Name of table for error messages
        
    Raises:
        DataQualityException: If duplicates found
    """
    total_rows = df.count()
    unique_rows = df.select(*unique_columns).distinct().count()
    
    if total_rows != unique_rows:
        duplicates = total_rows - unique_rows
        raise DataQualityException(
            f"Uniqueness check failed for {table_name}. Found {duplicates} duplicate(s)"
        )


def check_referential_integrity(child_df: DataFrame, parent_df: DataFrame, 
                                child_key: str, parent_key: str, relationship_name: str):
    """
    Check referential integrity between child and parent tables.
    
    Args:
        child_df: Child DataFrame
        parent_df: Parent DataFrame  
        child_key: Foreign key column in child
        parent_key: Primary key column in parent
        relationship_name: Name of relationship for error messages
        
    Raises:
        DataQualityException: If orphan records found
    """
    # Get non-null child keys
    child_keys = child_df.filter(child_df[child_key].isNotNull()).select(child_key).distinct()
    parent_keys = parent_df.select(parent_key).distinct()
    
    # Find orphans (child keys not in parent)
    orphans = child_keys.join(parent_keys, child_keys[child_key] == parent_keys[parent_key], "left_anti")
    orphan_count = orphans.count()
    
    if orphan_count > 0:
        raise DataQualityException(
            f"Referential integrity check failed for {relationship_name}: "
            f"{orphan_count} orphan record(s) found"
        )
