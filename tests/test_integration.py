"""
Integration test for complete data pipeline with DQ validation
Tests use schema definitions from schemas/ folder
"""

import pytest
from src.transformations import enrich_customer, enrich_product, enrich_order, create_profit_aggregate
from src.data_quality import (
    validate_schema, check_null_completeness, 
    check_uniqueness, check_referential_integrity
)
from schemas.bronze_schema import CUSTOMER_BRONZE, PRODUCT_BRONZE, ORDER_BRONZE
from schemas.silver_schema import CUSTOMER_ENRICHED, PRODUCT_ENRICHED, ORDER_ENRICHED
from schemas.gold_schema import PROFIT_AGGREGATE


class TestPipelineIntegration:
    
    def test_bronze_customer_data_quality_with_schema(self, spark, bronze_customers):
        """Test data quality checks on bronze customers using CUSTOMER_BRONZE schema"""
        
        # Schema validation using schema definition
        bronze_cols = [field.name for field in CUSTOMER_BRONZE.fields]
        validate_schema(bronze_customers, bronze_cols, "bronze.customers")
        
        # Null completeness for non-nullable fields
        required_fields = [field.name for field in CUSTOMER_BRONZE.fields if not field.nullable]
        check_null_completeness(bronze_customers, required_fields, "bronze.customers")
        
        # Uniqueness
        check_uniqueness(bronze_customers, ["Customer ID"], "bronze.customers")
    
    def test_bronze_product_data_quality_with_schema(self, spark, bronze_products):
        """Test data quality checks on bronze products using PRODUCT_BRONZE schema"""
        
        # Schema validation
        bronze_cols = [field.name for field in PRODUCT_BRONZE.fields]
        validate_schema(bronze_products, bronze_cols, "bronze.products")
        
        # Uniqueness
        check_uniqueness(bronze_products, ["Product ID"], "bronze.products")
    
    def test_bronze_order_data_quality_with_schema(self, spark, bronze_orders):
        """Test data quality checks on bronze orders using ORDER_BRONZE schema"""
        
        # Schema validation
        bronze_cols = [field.name for field in ORDER_BRONZE.fields]
        validate_schema(bronze_orders, bronze_cols, "bronze.orders")
        
        # Null completeness for non-nullable fields
        required_fields = [field.name for field in ORDER_BRONZE.fields if not field.nullable]
        check_null_completeness(bronze_orders, required_fields, "bronze.orders")
    
    def test_referential_integrity_orders_to_customers(self, spark, bronze_orders, bronze_customers):
        """Test orders reference valid customers"""
        
        check_referential_integrity(
            bronze_orders, bronze_customers,
            "Customer ID", "Customer ID",
            "orders->customers"
        )
    
    def test_referential_integrity_orders_to_products(self, spark, bronze_orders, bronze_products):
        """Test orders reference valid products"""
        
        check_referential_integrity(
            bronze_orders, bronze_products,
            "Product ID", "Product ID",
            "orders->products"
        )
    
    def test_silver_customer_schema_compliance(self, spark, bronze_customers):
        """Test enriched customers match CUSTOMER_ENRICHED schema"""
        
        customers_enriched = enrich_customer(bronze_customers)
        
        # Validate against schema definition
        silver_cols = [field.name for field in CUSTOMER_ENRICHED.fields]
        validate_schema(customers_enriched, silver_cols, "silver.customers_enriched")
        
        # Check enrichment columns added
        assert "email_domain" in customers_enriched.columns
        assert "is_valid_email" in customers_enriched.columns
    
    def test_silver_product_schema_compliance(self, spark, bronze_products):
        """Test enriched products match PRODUCT_ENRICHED schema"""
        
        products_enriched = enrich_product(bronze_products)
        
        # Validate against schema definition
        silver_cols = [field.name for field in PRODUCT_ENRICHED.fields]
        validate_schema(products_enriched, silver_cols, "silver.products_enriched")
    
    def test_silver_order_schema_compliance(self, spark, bronze_orders, enriched_customers, enriched_products):
        """Test enriched orders match ORDER_ENRICHED schema"""
        
        orders_enriched = enrich_order(bronze_orders, enriched_customers, enriched_products)
        
        # Validate against schema definition
        silver_cols = [field.name for field in ORDER_ENRICHED.fields]
        validate_schema(orders_enriched, silver_cols, "silver.orders_enriched")
        
        # Check enrichment columns added
        assert "customer_name" in orders_enriched.columns
        assert "country" in orders_enriched.columns
        assert "category" in orders_enriched.columns
        assert "sub_category" in orders_enriched.columns
        assert "profit" in orders_enriched.columns
    
    def test_gold_aggregate_schema_compliance(self, spark, bronze_orders, enriched_customers, enriched_products):
        """Test profit aggregate matches PROFIT_AGGREGATE schema"""
        
        orders_enriched = enrich_order(bronze_orders, enriched_customers, enriched_products)
        profit_aggregate = create_profit_aggregate(orders_enriched)
        
        # Validate against schema definition
        gold_cols = [field.name for field in PROFIT_AGGREGATE.fields]
        validate_schema(profit_aggregate, gold_cols, "gold.profit_aggregate")
        
        # Check all dimensions and metrics present
        assert "year" in profit_aggregate.columns
        assert "category" in profit_aggregate.columns
        assert "sub_category" in profit_aggregate.columns
        assert "customer_id" in profit_aggregate.columns
        assert "total_profit" in profit_aggregate.columns
        assert "total_sales" in profit_aggregate.columns
        assert "order_count" in profit_aggregate.columns
    
    def test_complete_pipeline_with_schema_validation(self, spark, bronze_customers, bronze_products, bronze_orders):
        """Test complete pipeline validates against schemas at each stage"""
        
        # Stage 1: Validate bronze schemas
        validate_schema(bronze_customers, [field.name for field in CUSTOMER_BRONZE.fields], "bronze.customers")
        validate_schema(bronze_products, [field.name for field in PRODUCT_BRONZE.fields], "bronze.products")
        validate_schema(bronze_orders, [field.name for field in ORDER_BRONZE.fields], "bronze.orders")
        
        # Stage 2: Check referential integrity
        check_referential_integrity(bronze_orders, bronze_customers, "Customer ID", "Customer ID", "orders->customers")
        check_referential_integrity(bronze_orders, bronze_products, "Product ID", "Product ID", "orders->products")
        
        # Stage 3: Enrich data
        customers_enriched = enrich_customer(bronze_customers)
        products_enriched = enrich_product(bronze_products)
        orders_enriched = enrich_order(bronze_orders, customers_enriched, products_enriched)
        
        # Stage 4: Validate silver schemas
        validate_schema(customers_enriched, [field.name for field in CUSTOMER_ENRICHED.fields], "silver.customers")
        validate_schema(products_enriched, [field.name for field in PRODUCT_ENRICHED.fields], "silver.products")
        validate_schema(orders_enriched, [field.name for field in ORDER_ENRICHED.fields], "silver.orders")
        
        # Stage 5: Create and validate gold aggregate
        profit_aggregate = create_profit_aggregate(orders_enriched)
        validate_schema(profit_aggregate, [field.name for field in PROFIT_AGGREGATE.fields], "gold.profit_aggregate")
        
        # Verify pipeline produced correct row counts
        assert customers_enriched.count() == bronze_customers.count()
        assert products_enriched.count() == bronze_products.count()
        assert orders_enriched.count() == bronze_orders.count()
        assert profit_aggregate.count() > 0
