"""
Unit tests for Silver layer merge operations

Tests basic merge functionality for customers, products, and orders
"""

import pytest
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
from pyspark.sql.functions import col
from datetime import date
from src.data_loader import merge_silver_customers, merge_silver_products, merge_silver_orders


class TestMergeOperations:
    
    def test_merge_customers_creates_table(self, spark, enriched_customers):
        """Test merge creates table on first load"""
        spark.sql("DROP TABLE IF EXISTS workspace.silver.test_customers")
        
        merge_silver_customers(spark, enriched_customers, "test_customers")
        
        result = spark.table("workspace.silver.test_customers")
        assert result.count() == enriched_customers.count()
        
        spark.sql("DROP TABLE IF EXISTS workspace.silver.test_customers")
    
    def test_merge_customers_updates_existing(self, spark):
        """Test merge updates existing customer records"""
        spark.sql("DROP TABLE IF EXISTS workspace.silver.test_customers")
        
        schema = StructType([
            StructField("customer_id", StringType(), False),
            StructField("customer_name", StringType(), False),
            StructField("segment", StringType(), True)
        ])
        
        # Initial load
        initial = spark.createDataFrame([("C001", "John", "Consumer")], schema)
        merge_silver_customers(spark, initial, "test_customers")
        
        # Update segment
        updated = spark.createDataFrame([("C001", "John", "Corporate")], schema)
        merge_silver_customers(spark, updated, "test_customers")
        
        result = spark.table("workspace.silver.test_customers")
        assert result.first()["segment"] == "Corporate"
        
        spark.sql("DROP TABLE IF EXISTS workspace.silver.test_customers")
    
    def test_merge_orders_with_partitions(self, spark, bronze_orders, enriched_customers, enriched_products):
        """Test merge preserves partition structure for orders"""
        spark.sql("DROP TABLE IF EXISTS workspace.silver.test_orders")
        
        from src.transformations import enrich_order
        orders_enriched = enrich_order(bronze_orders, enriched_customers, enriched_products)
        
        merge_silver_orders(spark, orders_enriched, "test_orders")
        
        result = spark.table("workspace.silver.test_orders")
        assert "order_year" in result.columns
        assert "order_month" in result.columns
        
        spark.sql("DROP TABLE IF EXISTS workspace.silver.test_orders")
