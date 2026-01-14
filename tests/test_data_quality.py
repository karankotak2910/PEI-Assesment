"""
Test cases for data quality validation functions
"""

import pytest
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from src.data_quality import (
    validate_schema, check_null_completeness, 
    check_uniqueness, check_referential_integrity,
    DataQualityException
)


class TestSchemaValidation:
    
    def test_schema_validation_success(self, spark):
        """Test schema validation passes when all columns present"""
        schema = StructType([
            StructField("customer_id", StringType(), False),
            StructField("customer_name", StringType(), False),
            StructField("country", StringType(), True)
        ])
        
        df = spark.createDataFrame([("C001", "John", "USA")], schema)
        
        # Should not raise exception
        validate_schema(df, ["customer_id", "customer_name"], "customers")
    
    def test_schema_validation_missing_column(self, spark):
        """Test schema validation fails when column missing"""
        schema = StructType([
            StructField("customer_id", StringType(), False)
        ])
        
        df = spark.createDataFrame([("C001",)], schema)
        
        with pytest.raises(DataQualityException) as exc:
            validate_schema(df, ["customer_id", "customer_name"], "customers")
        
        assert "Missing columns" in str(exc.value)
        assert "customer_name" in str(exc.value)
    
    def test_schema_validation_multiple_missing(self, spark):
        """Test schema validation detects multiple missing columns"""
        schema = StructType([
            StructField("customer_id", StringType(), False)
        ])
        
        df = spark.createDataFrame([("C001",)], schema)
        
        with pytest.raises(DataQualityException) as exc:
            validate_schema(df, ["customer_id", "email", "country"], "customers")
        
        error_msg = str(exc.value)
        assert "email" in error_msg
        assert "country" in error_msg


class TestNullCompleteness:
    
    def test_null_completeness_success(self, spark):
        """Test completeness check passes when no nulls"""
        schema = StructType([
            StructField("customer_id", StringType(), False),
            StructField("customer_name", StringType(), False)
        ])
        
        df = spark.createDataFrame([
            ("C001", "John"),
            ("C002", "Jane")
        ], schema)
        
        # Should not raise exception
        check_null_completeness(df, ["customer_id", "customer_name"], "customers")
    
    def test_null_completeness_detects_nulls(self, spark):
        """Test completeness check detects null values"""
        schema = StructType([
            StructField("customer_id", StringType(), True),
            StructField("customer_name", StringType(), True)
        ])
        
        df = spark.createDataFrame([
            ("C001", "John"),
            (None, "Jane")
        ], schema)
        
        with pytest.raises(DataQualityException) as exc:
            check_null_completeness(df, ["customer_id"], "customers")
        
        error_msg = str(exc.value)
        assert "null values found" in error_msg
        assert "customer_id" in error_msg


class TestUniqueness:
    
    def test_uniqueness_check_success(self, spark):
        """Test uniqueness check passes when all values unique"""
        schema = StructType([
            StructField("customer_id", StringType(), False),
            StructField("email", StringType(), True)
        ])
        
        df = spark.createDataFrame([
            ("C001", "john@example.com"),
            ("C002", "jane@example.com")
        ], schema)
        
        # Should not raise exception
        check_uniqueness(df, ["customer_id"], "customers")
    
    def test_uniqueness_detects_duplicates(self, spark):
        """Test uniqueness check detects duplicate values"""
        schema = StructType([
            StructField("customer_id", StringType(), False)
        ])
        
        df = spark.createDataFrame([
            ("C001",),
            ("C001",),
            ("C002",)
        ], schema)
        
        with pytest.raises(DataQualityException) as exc:
            check_uniqueness(df, ["customer_id"], "customers")
        
        error_msg = str(exc.value)
        assert "duplicate" in error_msg.lower()


class TestReferentialIntegrity:
    
    def test_referential_integrity_success(self, spark):
        """Test referential integrity check passes when all references valid"""
        parent_schema = StructType([
            StructField("customer_id", StringType(), False)
        ])
        parent_df = spark.createDataFrame([("C001",), ("C002",)], parent_schema)
        
        child_schema = StructType([
            StructField("order_id", StringType(), False),
            StructField("customer_id", StringType(), True)
        ])
        child_df = spark.createDataFrame([
            ("ORD001", "C001"),
            ("ORD002", "C002")
        ], child_schema)
        
        # Should not raise exception
        check_referential_integrity(
            child_df, parent_df,
            "customer_id", "customer_id",
            "orders->customers"
        )
    
    def test_referential_integrity_detects_orphans(self, spark):
        """Test referential integrity check detects orphan records"""
        parent_schema = StructType([
            StructField("customer_id", StringType(), False)
        ])
        parent_df = spark.createDataFrame([("C001",), ("C002",)], parent_schema)
        
        child_schema = StructType([
            StructField("order_id", StringType(), False),
            StructField("customer_id", StringType(), True)
        ])
        child_df = spark.createDataFrame([
            ("ORD001", "C001"),
            ("ORD002", "C999")  # Orphan record
        ], child_schema)
        
        with pytest.raises(DataQualityException) as exc:
            check_referential_integrity(
                child_df, parent_df,
                "customer_id", "customer_id",
                "orders->customers"
            )
        
        error_msg = str(exc.value)
        assert "orphan" in error_msg.lower()
        assert "orders->customers" in error_msg
    
    def test_referential_integrity_ignores_nulls(self, spark):
        """Test referential integrity check ignores null foreign keys"""
        parent_schema = StructType([
            StructField("customer_id", StringType(), False)
        ])
        parent_df = spark.createDataFrame([("C001",)], parent_schema)
        
        child_schema = StructType([
            StructField("order_id", StringType(), False),
            StructField("customer_id", StringType(), True)
        ])
        child_df = spark.createDataFrame([
            ("ORD001", "C001"),
            ("ORD002", None)  # Null should be ignored
        ], child_schema)
        
        # Should not raise exception for null values
        check_referential_integrity(
            child_df, parent_df,
            "customer_id", "customer_id",
            "orders->customers"
        )
