"""
Data Quality Validation
Run data quality checks on Bronze, Silver, and Gold tables using schema definitions
"""

import sys
sys.path.insert(0, '.')

from src.data_loader import get_spark_session, load_bronze_table, load_silver_table, load_gold_table
from src.data_quality import (
    validate_schema, check_null_completeness, 
    check_uniqueness, check_referential_integrity,
    DataQualityException
)
from schemas.bronze_schema import CUSTOMER_BRONZE, PRODUCT_BRONZE, ORDER_BRONZE
from schemas.silver_schema import CUSTOMER_ENRICHED, PRODUCT_ENRICHED, ORDER_ENRICHED
from schemas.gold_schema import PROFIT_AGGREGATE


def main():
    spark = get_spark_session("DQ_Validation")
    
    try:
        print("="*80)
        print("DATA QUALITY VALIDATION")
        print("="*80)
        
        # Load tables
        print("\nLoading tables...")
        customers_bronze = load_bronze_table(spark, "customers")
        products_bronze = load_bronze_table(spark, "products")
        orders_bronze = load_bronze_table(spark, "orders")
        
        customers_silver = load_silver_table(spark, "customers_enriched")
        products_silver = load_silver_table(spark, "products_enriched")
        orders_silver = load_silver_table(spark, "orders_enriched")
        
        profit_aggregate = load_gold_table(spark, "profit_aggregate")
        
        print(f"Loaded customers bronze: {customers_bronze.count():,} records")
        print(f"Loaded products bronze: {products_bronze.count():,} records")
        print(f"Loaded orders bronze: {orders_bronze.count():,} records")
        
        # BRONZE LAYER VALIDATION
        print("\n" + "="*80)
        print("BRONZE LAYER VALIDATION")
        print("="*80)
        
        # 1. Validate Bronze Customers Schema
        print("\n" + "-"*80)
        print("1. Validating Bronze Customers Schema Against CUSTOMER_BRONZE")
        print("-"*80)
        try:
            bronze_customer_cols = [field.name for field in CUSTOMER_BRONZE.fields]
            validate_schema(customers_bronze, bronze_customer_cols, "bronze.customers")
            print(f"Schema validation PASSED - All {len(bronze_customer_cols)} columns present")
            print(f"  Expected columns: {bronze_customer_cols}")
        except DataQualityException as e:
            print(f"Schema validation FAILED: {e}")
        
        # 2. Validate Bronze Products Schema
        print("\n" + "-"*80)
        print("2. Validating Bronze Products Schema Against PRODUCT_BRONZE")
        print("-"*80)
        try:
            bronze_product_cols = [field.name for field in PRODUCT_BRONZE.fields]
            validate_schema(products_bronze, bronze_product_cols, "bronze.products")
            print(f"Schema validation PASSED - All {len(bronze_product_cols)} columns present")
            print(f"  Expected columns: {bronze_product_cols}")
        except DataQualityException as e:
            print(f"Schema validation FAILED: {e}")
        
        # 3. Validate Bronze Orders Schema
        print("\n" + "-"*80)
        print("3. Validating Bronze Orders Schema Against ORDER_BRONZE")
        print("-"*80)
        try:
            bronze_order_cols = [field.name for field in ORDER_BRONZE.fields]
            validate_schema(orders_bronze, bronze_order_cols, "bronze.orders")
            print(f"Schema validation PASSED - All {len(bronze_order_cols)} columns present")
            print(f"  Expected columns: {bronze_order_cols}")
        except DataQualityException as e:
            print(f"Schema validation FAILED: {e}")
        
        # 4. Check Null Completeness for Customers
        print("\n" + "-"*80)
        print("4. Checking Null Completeness for Required Customer Fields")
        print("-"*80)
        try:
            required_fields = ["Customer ID", "Customer Name", "Segment", "Country"]
            check_null_completeness(customers_bronze, required_fields, "bronze.customers")
            print(f"Completeness check PASSED - No nulls in {len(required_fields)} required fields")
        except DataQualityException as e:
            print(f"Completeness check FAILED: {e}")
        
        # 5. Check Null Completeness for Products
        print("\n" + "-"*80)
        print("5. Checking Null Completeness for Required Product Fields")
        print("-"*80)
        try:
            required_fields = ["Product ID", "Category", "Sub-Category", "Product Name", "Price per product"]
            check_null_completeness(products_bronze, required_fields, "bronze.products")
            print(f"Completeness check PASSED - No nulls in {len(required_fields)} required fields")
        except DataQualityException as e:
            print(f"Completeness check FAILED: {e}")
        
        # 6. Check Uniqueness of Customer ID
        print("\n" + "-"*80)
        print("6. Checking Uniqueness of Customer ID")
        print("-"*80)
        try:
            check_uniqueness(customers_bronze, ["Customer ID"], "bronze.customers")
            print("Uniqueness check PASSED - All Customer IDs are unique")
        except DataQualityException as e:
            print(f"Uniqueness check FAILED: {e}")
        
        # 7. Check Uniqueness of Product ID
        print("\n" + "-"*80)
        print("7. Checking Uniqueness of Product ID")
        print("-"*80)
        try:
            check_uniqueness(products_bronze, ["Product ID"], "bronze.products")
            print("Uniqueness check PASSED - All Product IDs are unique")
        except DataQualityException as e:
            print(f"Uniqueness check FAILED: {e}")
        
        # 8. Check Referential Integrity: Orders -> Customers
        print("\n" + "-"*80)
        print("8. Checking Referential Integrity: Orders -> Customers")
        print("-"*80)
        try:
            check_referential_integrity(
                orders_bronze, customers_bronze,
                "Customer ID", "Customer ID",
                "orders->customers"
            )
            print("Referential integrity PASSED - All orders reference valid customers")
        except DataQualityException as e:
            print(f"Referential integrity FAILED: {e}")
        
        # 9. Check Referential Integrity: Orders -> Products
        print("\n" + "-"*80)
        print("9. Checking Referential Integrity: Orders -> Products")
        print("-"*80)
        try:
            check_referential_integrity(
                orders_bronze, products_bronze,
                "Product ID", "Product ID",
                "orders->products"
            )
            print("Referential integrity PASSED - All orders reference valid products")
        except DataQualityException as e:
            print(f"Referential integrity FAILED: {e}")
        
        # SILVER LAYER VALIDATION
        print("\n" + "="*80)
        print("SILVER LAYER VALIDATION")
        print("="*80)
        
        # 10. Validate Silver Customers Schema
        print("\n" + "-"*80)
        print("10. Validating Silver Customers Schema Against CUSTOMER_ENRICHED")
        print("-"*80)
        try:
            silver_customer_cols = [field.name for field in CUSTOMER_ENRICHED.fields]
            validate_schema(customers_silver, silver_customer_cols, "silver.customers_enriched")
            print(f"Schema validation PASSED - All {len(silver_customer_cols)} columns present")
            print(f"  Enrichment added: email_domain, is_valid_email")
        except DataQualityException as e:
            print(f"Schema validation FAILED: {e}")
        
        # 11. Validate Silver Products Schema
        print("\n" + "-"*80)
        print("11. Validating Silver Products Schema Against PRODUCT_ENRICHED")
        print("-"*80)
        try:
            silver_product_cols = [field.name for field in PRODUCT_ENRICHED.fields]
            validate_schema(products_silver, silver_product_cols, "silver.products_enriched")
            print(f"Schema validation PASSED - All {len(silver_product_cols)} columns present")
        except DataQualityException as e:
            print(f"Schema validation FAILED: {e}")
        
        # 12. Validate Silver Orders Schema
        print("\n" + "-"*80)
        print("12. Validating Silver Orders Schema Against ORDER_ENRICHED")
        print("-"*80)
        try:
            silver_order_cols = [field.name for field in ORDER_ENRICHED.fields]
            validate_schema(orders_silver, silver_order_cols, "silver.orders_enriched")
            print(f"Schema validation PASSED - All {len(silver_order_cols)} columns present")
            print(f"  Enrichment added: customer_name, country, category, sub_category, profit (2 decimals)")
        except DataQualityException as e:
            print(f"Schema validation FAILED: {e}")
        
        # GOLD LAYER VALIDATION
        print("\n" + "="*80)
        print("GOLD LAYER VALIDATION")
        print("="*80)
        
        # 13. Validate Gold Aggregate Schema
        print("\n" + "-"*80)
        print("13. Validating Gold Aggregate Schema Against PROFIT_AGGREGATE")
        print("-"*80)
        try:
            gold_cols = [field.name for field in PROFIT_AGGREGATE.fields]
            validate_schema(profit_aggregate, gold_cols, "gold.profit_aggregate")
            print(f"Schema validation PASSED - All {len(gold_cols)} columns present")
            print(f"  Dimensions: year, category, sub_category, customer_id, customer_name, country")
            print(f"  Metrics: total_profit, total_sales, order_count")
        except DataQualityException as e:
            print(f"Schema validation FAILED: {e}")
        
        # 14. Check Null Completeness for Gold Aggregate
        print("\n" + "-"*80)
        print("14. Checking Null Completeness for Gold Aggregate")
        print("-"*80)
        try:
            required_fields = ["year", "category", "sub_category", "customer_id", "total_profit", "order_count"]
            check_null_completeness(profit_aggregate, required_fields, "gold.profit_aggregate")
            print(f"Completeness check PASSED - No nulls in {len(required_fields)} required fields")
        except DataQualityException as e:
            print(f"Completeness check FAILED: {e}")
        
        print("\n" + "="*80)
        print("DATA QUALITY VALIDATION COMPLETE")
        print("="*80)
        print("\nSummary:")
        print("  Bronze layer: 9 checks")
        print("  Silver layer: 3 checks")
        print("  Gold layer: 2 checks")
        print("  Total: 14 data quality checks")
        
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
