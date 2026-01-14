"""
Test cases for Silver layer merge operations
"""

import pytest
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from src.data_loader import (
    merge_silver_customers, merge_silver_products, merge_silver_orders,
    load_silver_table
)
from src.transformations import enrich_order


class TestCustomerMerge:
    
    def test_merge_customers_first_load(self, spark, enriched_customers):
        """Test merge creates table on first load"""
        table_name = "test_customers_merge"
        
        merge_silver_customers(spark, enriched_customers, table_name)
        
        result_df = load_silver_table(spark, table_name)
        assert result_df.count() == enriched_customers.count()
        assert "updated_at" in result_df.columns
    
    def test_merge_customers_updates_existing(self, spark):
        """Test merge updates existing customer records"""
        table_name = "test_customers_update"
        
        schema = StructType([
            StructField("customer_id", StringType(), False),
            StructField("customer_name", StringType(), False),
            StructField("segment", StringType(), True),
            StructField("country", StringType(), True),
            StructField("email_domain", StringType(), True),
            StructField("is_valid_email", StringType(), True)
        ])
        
        # Initial load
        initial_data = spark.createDataFrame([
            ("C001", "John Doe", "Consumer", "USA", "example.com", "Yes")
        ], schema)
        merge_silver_customers(spark, initial_data, table_name)
        
        # Update segment for existing customer
        updated_data = spark.createDataFrame([
            ("C001", "John Doe", "Corporate", "USA", "example.com", "Yes")
        ], schema)
        merge_silver_customers(spark, updated_data, table_name)
        
        # Verify update
        result_df = load_silver_table(spark, table_name)
        row = result_df.filter(result_df.customer_id == "C001").first()
        
        assert row["segment"] == "Corporate"
    
    def test_merge_customers_inserts_new(self, spark):
        """Test merge inserts new customer records"""
        table_name = "test_customers_insert"
        
        schema = StructType([
            StructField("customer_id", StringType(), False),
            StructField("customer_name", StringType(), False),
            StructField("segment", StringType(), True),
            StructField("country", StringType(), True),
            StructField("email_domain", StringType(), True),
            StructField("is_valid_email", StringType(), True)
        ])
        
        # Initial load with 1 customer
        initial_data = spark.createDataFrame([
            ("C001", "John Doe", "Consumer", "USA", "example.com", "Yes")
        ], schema)
        merge_silver_customers(spark, initial_data, table_name)
        
        # Merge with 2 customers (1 existing, 1 new)
        new_data = spark.createDataFrame([
            ("C001", "John Doe", "Consumer", "USA", "example.com", "Yes"),
            ("C002", "Jane Smith", "Corporate", "UK", "test.com", "Yes")
        ], schema)
        merge_silver_customers(spark, new_data, table_name)
        
        # Verify both records exist
        result_df = load_silver_table(spark, table_name)
        assert result_df.count() == 2


class TestProductMerge:
    
    def test_merge_products_first_load(self, spark, enriched_products):
        """Test merge creates product table on first load"""
        table_name = "test_products_merge"
        
        merge_silver_products(spark, enriched_products, table_name)
        
        result_df = load_silver_table(spark, table_name)
        assert result_df.count() == enriched_products.count()
        assert "updated_at" in result_df.columns
    
    def test_merge_products_updates_existing(self, spark):
        """Test merge updates existing product records"""
        table_name = "test_products_update"
        
        schema = StructType([
            StructField("product_id", StringType(), False),
            StructField("product_name", StringType(), False),
            StructField("category", StringType(), True),
            StructField("sub_category", StringType(), True),
            StructField("price_per_unit", DoubleType(), True),
            StructField("product_state", StringType(), True)
        ])
        
        # Initial load
        initial_data = spark.createDataFrame([
            ("P001", "Office Chair", "Furniture", "Chairs", 100.0, "Active")
        ], schema)
        merge_silver_products(spark, initial_data, table_name)
        
        # Update price
        updated_data = spark.createDataFrame([
            ("P001", "Office Chair", "Furniture", "Chairs", 150.0, "Active")
        ], schema)
        merge_silver_products(spark, updated_data, table_name)
        
        # Verify update
        result_df = load_silver_table(spark, table_name)
        row = result_df.filter(result_df.product_id == "P001").first()
        
        assert abs(row["price_per_unit"] - 150.0) < 0.01


class TestOrderMerge:
    
    def test_merge_orders_first_load(self, spark, bronze_orders, enriched_customers, enriched_products):
        """Test merge creates partitioned orders table on first load"""
        table_name = "test_orders_merge"
        
        orders_enriched = enrich_order(bronze_orders, enriched_customers, enriched_products)
        merge_silver_orders(spark, orders_enriched, table_name)
        
        result_df = load_silver_table(spark, table_name)
        assert result_df.count() == orders_enriched.count()
        assert "order_year" in result_df.columns
        assert "order_month" in result_df.columns
    
    def test_merge_orders_updates_existing(self, spark):
        """Test merge updates existing order records"""
        table_name = "test_orders_update"
        
        schema = StructType([
            StructField("order_id", StringType(), False),
            StructField("customer_id", StringType(), False),
            StructField("product_id", StringType(), False),
            StructField("order_year", IntegerType(), False),
            StructField("order_month", IntegerType(), False),
            StructField("profit", DoubleType(), True),
            StructField("quantity", IntegerType(), True)
        ])
        
        # Initial load
        initial_data = spark.createDataFrame([
            ("ORD001", "C001", "P001", 2023, 1, 100.0, 2)
        ], schema)
        merge_silver_orders(spark, initial_data, table_name)
        
        # Update quantity
        updated_data = spark.createDataFrame([
            ("ORD001", "C001", "P001", 2023, 1, 100.0, 5)
        ], schema)
        merge_silver_orders(spark, updated_data, table_name)
        
        # Verify update
        result_df = load_silver_table(spark, table_name)
        row = result_df.filter(result_df.order_id == "ORD001").first()
        
        assert row["quantity"] == 5
    
    def test_merge_orders_composite_key(self, spark):
        """Test merge uses composite key: customer_id + order_id + product_id"""
        table_name = "test_orders_composite"
        
        schema = StructType([
            StructField("order_id", StringType(), False),
            StructField("customer_id", StringType(), False),
            StructField("product_id", StringType(), False),
            StructField("order_year", IntegerType(), False),
            StructField("order_month", IntegerType(), False),
            StructField("profit", DoubleType(), True)
        ])
        
        # Load order with composite key
        data = spark.createDataFrame([
            ("ORD001", "C001", "P001", 2023, 1, 100.0),
            ("ORD001", "C001", "P002", 2023, 1, 150.0)
        ], schema)
        merge_silver_orders(spark, data, table_name)
        
        # Verify both records exist (different products)
        result_df = load_silver_table(spark, table_name)
        assert result_df.count() == 2
