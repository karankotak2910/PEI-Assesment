"""
Basic data quality tests

Tests essential DQ validations: schema, completeness, uniqueness, and referential integrity
"""

import pytest
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from src.data_quality import (
    validate_schema, check_null_completeness, 
    check_uniqueness, check_referential_integrity,
    DataQualityException
)


class TestDataQuality:
    
    def test_schema_validation_passes(self, spark):
        """Test schema validation with all required columns present"""
        schema = StructType([
            StructField("customer_id", StringType(), False),
            StructField("customer_name", StringType(), False)
        ])
        
        df = spark.createDataFrame([("C001", "John")], schema)
        
        # Should not raise exception
        validate_schema(df, ["customer_id", "customer_name"], "customers")
    
    @pytest.mark.parametrize("columns,expected_missing", [
        (["customer_id", "customer_name", "email"], {"email"}),
        (["customer_id", "email", "country"], {"email", "country"}),
        (["order_id", "product_id"], {"order_id", "product_id"}),
    ])
    def test_schema_validation_fails_missing_columns(self, spark, columns, expected_missing):
        """Test schema validation fails when columns are missing"""
        schema = StructType([
            StructField("customer_id", StringType(), False)
        ])
        
        df = spark.createDataFrame([("C001",)], schema)
        
        with pytest.raises(DataQualityException) as exc_info:
            validate_schema(df, columns, "test_table")
        
        assert "Missing columns" in str(exc_info.value)
        for col in expected_missing:
            assert col in str(exc_info.value)
    
    def test_null_completeness_check(self, spark):
        """Test null completeness check catches null values"""
        schema = StructType([
            StructField("customer_id", StringType(), True),
            StructField("customer_name", StringType(), True)
        ])
        
        df = spark.createDataFrame([
            ("C001", "John"),
            (None, "Jane")  # Null customer_id
        ], schema)
        
        with pytest.raises(DataQualityException) as exc_info:
            check_null_completeness(df, ["customer_id"], "customers")
        
        assert "null values found" in str(exc_info.value)
    
    def test_uniqueness_check(self, spark):
        """Test uniqueness check catches duplicates"""
        schema = StructType([
            StructField("customer_id", StringType(), False)
        ])
        
        df = spark.createDataFrame([
            ("C001",),
            ("C001",),  # Duplicate
            ("C002",)
        ], schema)
        
        with pytest.raises(DataQualityException) as exc_info:
            check_uniqueness(df, ["customer_id"], "customers")
        
        assert "duplicate" in str(exc_info.value)
    
    def test_referential_integrity_check(self, spark):
        """Test referential integrity catches orphan records"""
        # Parent table
        parent_schema = StructType([StructField("customer_id", StringType(), False)])
        parent_df = spark.createDataFrame([("C001",), ("C002",)], parent_schema)
        
        # Child table with orphan
        child_schema = StructType([
            StructField("order_id", StringType(), False),
            StructField("customer_id", StringType(), True)
        ])
        child_df = spark.createDataFrame([
            ("ORD001", "C001"),
            ("ORD002", "C999")  # Orphan - customer doesn't exist
        ], child_schema)
        
        with pytest.raises(DataQualityException) as exc_info:
            check_referential_integrity(
                child_df, parent_df, 
                "customer_id", "customer_id", 
                "orders->customers"
            )
        
        assert "orphan" in str(exc_info.value)
