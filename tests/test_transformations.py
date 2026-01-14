"""
Test cases for data transformation functions
"""

import pytest
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType, IntegerType
from datetime import date
from src.transformations import (
    standardize_customer_columns, add_email_enrichment, enrich_customer,
    standardize_product_columns, enrich_product,
    standardize_order_columns, add_date_fields, calculate_days_to_ship,
    round_profit_to_2_decimals, join_with_customer, join_with_product,
    enrich_order, create_profit_aggregate
)


class TestCustomerTransformations:
    
    def test_standardize_customer_columns(self, spark):
        """Test column name standardization for customers"""
        schema = StructType([
            StructField("Customer ID", StringType(), False),
            StructField("Customer Name", StringType(), False),
            StructField("Country", StringType(), True)
        ])
        
        df = spark.createDataFrame([("C001", "John Doe", "USA")], schema)
        result = standardize_customer_columns(df)
        
        assert "customer_id" in result.columns
        assert "customer_name" in result.columns
        assert "country" in result.columns
        assert "Customer ID" not in result.columns
    
    def test_email_domain_extraction_valid(self, spark):
        """Test email domain extraction for valid emails"""
        schema = StructType([
            StructField("customer_id", StringType(), False),
            StructField("email", StringType(), True)
        ])
        
        df = spark.createDataFrame([
            ("C001", "john@example.com"),
            ("C002", "jane@test.org")
        ], schema)
        
        result = add_email_enrichment(df)
        
        row1 = result.filter(result.customer_id == "C001").first()
        assert row1["email_domain"] == "example.com"
        assert row1["is_valid_email"] == "Yes"
        
        row2 = result.filter(result.customer_id == "C002").first()
        assert row2["email_domain"] == "test.org"
        assert row2["is_valid_email"] == "Yes"
    
    def test_email_domain_extraction_invalid(self, spark):
        """Test email domain extraction for invalid emails"""
        schema = StructType([
            StructField("customer_id", StringType(), False),
            StructField("email", StringType(), True)
        ])
        
        df = spark.createDataFrame([
            ("C001", "invalid-email"),
            ("C002", None)
        ], schema)
        
        result = add_email_enrichment(df)
        
        row1 = result.filter(result.customer_id == "C001").first()
        assert row1["email_domain"] is None
        assert row1["is_valid_email"] == "No"
        
        row2 = result.filter(result.customer_id == "C002").first()
        assert row2["email_domain"] is None
    
    def test_enrich_customer_complete(self, spark, bronze_customers):
        """Test complete customer enrichment pipeline"""
        result = enrich_customer(bronze_customers)
        
        assert "customer_id" in result.columns
        assert "customer_name" in result.columns
        assert "email_domain" in result.columns
        assert "is_valid_email" in result.columns
        assert result.count() == bronze_customers.count()


class TestProductTransformations:
    
    def test_standardize_product_columns(self, spark):
        """Test column name standardization for products"""
        schema = StructType([
            StructField("Product ID", StringType(), False),
            StructField("Product Name", StringType(), False),
            StructField("Category", StringType(), True)
        ])
        
        df = spark.createDataFrame([("P001", "Chair", "Furniture")], schema)
        result = standardize_product_columns(df)
        
        assert "product_id" in result.columns
        assert "product_name" in result.columns
        assert "category" in result.columns
        assert "Product ID" not in result.columns
    
    def test_enrich_product_complete(self, spark, bronze_products):
        """Test complete product enrichment pipeline"""
        result = enrich_product(bronze_products)
        
        assert "product_id" in result.columns
        assert "product_name" in result.columns
        assert "category" in result.columns
        assert "sub_category" in result.columns
        assert result.count() == bronze_products.count()


class TestOrderTransformations:
    
    def test_standardize_order_columns(self, spark):
        """Test column name standardization for orders"""
        schema = StructType([
            StructField("Order ID", StringType(), False),
            StructField("Order Date", DateType(), True),
            StructField("Customer ID", StringType(), False),
            StructField("Profit", DoubleType(), True)
        ])
        
        df = spark.createDataFrame([
            ("ORD001", date(2023, 1, 1), "C001", 100.0)
        ], schema)
        
        result = standardize_order_columns(df)
        
        assert "order_id" in result.columns
        assert "order_date" in result.columns
        assert "customer_id" in result.columns
        assert "source_profit" in result.columns
        assert "Order ID" not in result.columns
    
    def test_add_date_fields(self, spark):
        """Test date field extraction from order date"""
        schema = StructType([
            StructField("order_id", StringType(), False),
            StructField("order_date", DateType(), True)
        ])
        
        df = spark.createDataFrame([
            ("ORD001", date(2023, 3, 15))
        ], schema)
        
        result = add_date_fields(df)
        row = result.first()
        
        assert row["order_year"] == 2023
        assert row["order_month"] == 3
        assert row["order_quarter"] == 1
    
    def test_calculate_days_to_ship_with_date(self, spark):
        """Test days to ship calculation when ship date exists"""
        schema = StructType([
            StructField("order_id", StringType(), False),
            StructField("order_date", DateType(), True),
            StructField("ship_date", DateType(), True)
        ])
        
        df = spark.createDataFrame([
            ("ORD001", date(2023, 1, 1), date(2023, 1, 5))
        ], schema)
        
        result = calculate_days_to_ship(df)
        row = result.first()
        
        assert row["days_to_ship"] == 4
    
    def test_calculate_days_to_ship_null_ship_date(self, spark):
        """Test days to ship calculation when ship date is null"""
        schema = StructType([
            StructField("order_id", StringType(), False),
            StructField("order_date", DateType(), True),
            StructField("ship_date", DateType(), True)
        ])
        
        df = spark.createDataFrame([
            ("ORD001", date(2023, 1, 1), None)
        ], schema)
        
        result = calculate_days_to_ship(df)
        row = result.first()
        
        assert row["days_to_ship"] is None
    
    def test_round_profit_to_2_decimals(self, spark):
        """Test profit rounding to 2 decimal places"""
        schema = StructType([
            StructField("order_id", StringType(), False),
            StructField("source_profit", DoubleType(), True)
        ])
        
        df = spark.createDataFrame([
            ("ORD001", 123.456),
            ("ORD002", 99.995),
            ("ORD003", -50.123),
            ("ORD004", 100.001)
        ], schema)
        
        result = round_profit_to_2_decimals(df)
        profits = {row["order_id"]: row["profit"] for row in result.collect()}
        
        assert abs(profits["ORD001"] - 123.46) < 0.01
        assert abs(profits["ORD002"] - 100.00) < 0.01
        assert abs(profits["ORD003"] - (-50.12)) < 0.01
        assert abs(profits["ORD004"] - 100.00) < 0.01
    
    def test_join_with_customer(self, spark):
        """Test joining orders with customer information"""
        orders_schema = StructType([
            StructField("order_id", StringType(), False),
            StructField("customer_id", StringType(), False)
        ])
        orders_df = spark.createDataFrame([
            ("ORD001", "C001")
        ], orders_schema)
        
        customers_schema = StructType([
            StructField("customer_id", StringType(), False),
            StructField("customer_name", StringType(), False),
            StructField("country", StringType(), True),
            StructField("segment", StringType(), True),
            StructField("region", StringType(), True)
        ])
        customers_df = spark.createDataFrame([
            ("C001", "John Doe", "USA", "Consumer", "East")
        ], customers_schema)
        
        result = join_with_customer(orders_df, customers_df)
        row = result.first()
        
        assert row["customer_name"] == "John Doe"
        assert row["country"] == "USA"
    
    def test_join_with_product(self, spark):
        """Test joining orders with product information"""
        orders_schema = StructType([
            StructField("order_id", StringType(), False),
            StructField("product_id", StringType(), False)
        ])
        orders_df = spark.createDataFrame([
            ("ORD001", "P001")
        ], orders_schema)
        
        products_schema = StructType([
            StructField("product_id", StringType(), False),
            StructField("product_name", StringType(), False),
            StructField("category", StringType(), True),
            StructField("sub_category", StringType(), True),
            StructField("price_per_unit", DoubleType(), True)
        ])
        products_df = spark.createDataFrame([
            ("P001", "Chair", "Furniture", "Chairs", 100.0)
        ], products_schema)
        
        result = join_with_product(orders_df, products_df)
        row = result.first()
        
        assert row["category"] == "Furniture"
        assert row["sub_category"] == "Chairs"
    
    def test_enrich_order_complete(self, spark, bronze_orders, enriched_customers, enriched_products):
        """Test complete order enrichment pipeline"""
        result = enrich_order(bronze_orders, enriched_customers, enriched_products)
        
        # Check customer info added
        assert "customer_name" in result.columns
        assert "country" in result.columns
        
        # Check product info added
        assert "category" in result.columns
        assert "sub_category" in result.columns
        
        # Check profit rounded
        assert "profit" in result.columns
        
        # Check date fields added
        assert "order_year" in result.columns
        assert "order_month" in result.columns


class TestAggregations:
    
    def test_create_profit_aggregate(self, spark, bronze_orders, enriched_customers, enriched_products):
        """Test profit aggregation with all dimensions"""
        orders_enriched = enrich_order(bronze_orders, enriched_customers, enriched_products)
        result = create_profit_aggregate(orders_enriched)
        
        # Check all required columns exist
        assert "year" in result.columns
        assert "category" in result.columns
        assert "sub_category" in result.columns
        assert "customer_id" in result.columns
        assert "customer_name" in result.columns
        assert "total_profit" in result.columns
        assert "total_sales" in result.columns
        assert "order_count" in result.columns
        
        # Check aggregation reduces row count
        assert result.count() > 0
        assert result.count() < orders_enriched.count()
    
    def test_profit_aggregate_calculations(self, spark):
        """Test aggregate calculations are correct"""
        schema = StructType([
            StructField("order_id", StringType(), False),
            StructField("order_year", IntegerType(), False),
            StructField("category", StringType(), False),
            StructField("sub_category", StringType(), False),
            StructField("customer_id", StringType(), False),
            StructField("customer_name", StringType(), False),
            StructField("country", StringType(), False),
            StructField("profit", DoubleType(), True),
            StructField("price", DoubleType(), True)
        ])
        
        df = spark.createDataFrame([
            ("ORD001", 2023, "Furniture", "Chairs", "C001", "John", "USA", 100.0, 200.0),
            ("ORD002", 2023, "Furniture", "Chairs", "C001", "John", "USA", 50.0, 100.0)
        ], schema)
        
        result = create_profit_aggregate(df)
        row = result.first()
        
        assert abs(row["total_profit"] - 150.0) < 0.01
        assert abs(row["total_sales"] - 300.0) < 0.01
        assert row["order_count"] == 2
