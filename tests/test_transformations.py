"""
Unit tests for data transformations

Covers basic functionality, edge cases, and requirement validation
"""

import pytest
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
from datetime import date
from src.transformations import (
    enrich_customer, enrich_product, enrich_order, 
    round_profit_to_2_decimals, create_profit_aggregate
)


class TestCustomerEnrichment:
    
    def test_enrich_customer_basic(self, spark, bronze_customers):
        """Test basic customer enrichment"""
        result = enrich_customer(bronze_customers)
        
        # Check columns exist
        assert "customer_id" in result.columns
        assert "customer_name" in result.columns
        assert "email_domain" in result.columns
        assert "is_valid_email" in result.columns
        
        # Check row count preserved
        assert result.count() == bronze_customers.count()
    
    @pytest.mark.parametrize("email,expected_domain,expected_valid", [
        ("john@example.com", "example.com", "Yes"),
        ("jane@test.org", "test.org", "Yes"),
        ("invalid-email", None, "No"),
        ("no-at-sign.com", None, "No"),
    ])
    def test_email_domain_extraction(self, spark, email, expected_domain, expected_valid):
        """Test email domain extraction with different email formats"""
        schema = StructType([
            StructField("Customer ID", StringType(), False),
            StructField("Customer Name", StringType(), False),
            StructField("Email", StringType(), True)
        ])
        
        data = spark.createDataFrame([("C001", "Test User", email)], schema)
        result = enrich_customer(data)
        
        row = result.first()
        assert row["email_domain"] == expected_domain
        assert row["is_valid_email"] == expected_valid


class TestProductEnrichment:
    
    def test_enrich_product_basic(self, spark, bronze_products):
        """Test basic product enrichment"""
        result = enrich_product(bronze_products)
        
        # Check column standardization
        assert "product_id" in result.columns
        assert "product_name" in result.columns
        assert "category" in result.columns
        assert "sub_category" in result.columns
        
        # Check row count preserved
        assert result.count() == bronze_products.count()


class TestOrderEnrichment:
    
    def test_enrich_order_basic(self, spark, bronze_orders, enriched_customers, enriched_products):
        """Test complete order enrichment"""
        result = enrich_order(bronze_orders, enriched_customers, enriched_products)
        
        # Check required columns exist
        assert "order_id" in result.columns
        assert "customer_name" in result.columns
        assert "country" in result.columns
        assert "category" in result.columns
        assert "sub_category" in result.columns
        assert "profit" in result.columns
        
        # Check profit is rounded to 2 decimals
        sample = result.first()
        profit_str = str(sample["profit"])
        if "." in profit_str:
            decimal_places = len(profit_str.split(".")[1])
            assert decimal_places <= 2
    
    @pytest.mark.parametrize("profit_value,expected_rounded", [
        (123.456, 123.46),
        (99.994, 99.99),
        (99.995, 100.00),
        (-50.123, -50.12),
        (0.005, 0.01),
        (100.001, 100.00),
    ])
    def test_profit_rounding_edge_cases(self, spark, profit_value, expected_rounded):
        """Test profit rounding to 2 decimal places"""
        schema = StructType([
            StructField("order_id", StringType(), False),
            StructField("source_profit", DoubleType(), True)
        ])
        
        data = spark.createDataFrame([("ORD001", profit_value)], schema)
        result = round_profit_to_2_decimals(data)
        
        actual_profit = result.first()["profit"]
        assert abs(actual_profit - expected_rounded) < 0.001
    
    def test_order_with_null_ship_date(self, spark, enriched_customers, enriched_products):
        """Test handling of null ship dates"""
        schema = StructType([
            StructField("Order ID", StringType(), False),
            StructField("Order Date", DateType(), True),
            StructField("Ship Date", DateType(), True),
            StructField("Customer ID", StringType(), False),
            StructField("Product ID", StringType(), False),
            StructField("Profit", DoubleType(), True)
        ])
        
        data = spark.createDataFrame([
            ("ORD001", date(2023, 1, 1), None, "CG-12520", "FUR-BO-10001798", 100.0)
        ], schema)
        
        result = enrich_order(data, enriched_customers, enriched_products)
        
        assert result.first()["days_to_ship"] is None


class TestAggregation:
    
    def test_create_profit_aggregate(self, spark, bronze_orders, enriched_customers, enriched_products):
        """Test profit aggregation - creates single table with all dimensions"""
        orders_enriched = enrich_order(bronze_orders, enriched_customers, enriched_products)
        
        result = create_profit_aggregate(orders_enriched)
        
        # Check required columns
        assert "year" in result.columns
        assert "category" in result.columns
        assert "sub_category" in result.columns
        assert "customer_id" in result.columns
        assert "customer_name" in result.columns
        assert "total_profit" in result.columns
        assert "total_sales" in result.columns
        assert "order_count" in result.columns
        
        # Verify aggregation happened
        assert result.count() > 0
        assert result.count() < orders_enriched.count()
