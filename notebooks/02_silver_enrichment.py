"""
Silver Layer - Data Enrichment with MERGE Operations
Load from Bronze schema, enrich, and MERGE into Silver schema in Unity Catalog
"""

import sys
sys.path.insert(0, '.')

from src.data_loader import (
    get_spark_session, load_bronze_table,
    merge_silver_customers, merge_silver_products, merge_silver_orders
)
from src.transformations import enrich_customer, enrich_product, enrich_order
from src.data_quality import validate_schema, check_referential_integrity
from schemas.silver_schema import CUSTOMER_ENRICHED, PRODUCT_ENRICHED, ORDER_ENRICHED


def main():
    spark = get_spark_session("Silver_Enrichment")
    
    try:
        print("Loading Bronze tables from Unity Catalog (workspace.bronze.*)...")
        customers_bronze = load_bronze_table(spark, "customers")
        products_bronze = load_bronze_table(spark, "products")
        orders_bronze = load_bronze_table(spark, "orders")
        
        print("\nEnriching customers...")
        customers_enriched = enrich_customer(customers_bronze)
        print(f"  Enriched {customers_enriched.count():,} customers")
        
        # Validate enriched customer schema
        customer_cols = [field.name for field in CUSTOMER_ENRICHED.fields]
        validate_schema(customers_enriched, customer_cols, "silver.customers_enriched")
        print("Schema validated against CUSTOMER_ENRICHED")
        
        print("\nEnriching products...")
        products_enriched = enrich_product(products_bronze)
        print(f"  Enriched {products_enriched.count():,} products")
        
        # Validate enriched product schema
        product_cols = [field.name for field in PRODUCT_ENRICHED.fields]
        validate_schema(products_enriched, product_cols, "silver.products_enriched")
        print("Schema validated against PRODUCT_ENRICHED")
        
        print("\nEnriching orders...")
        print("  Joining with customers and products")
        print("  Calculating profit (rounded to 2 decimal places)")
        print("  Adding customer name and country")
        print("  Adding product category and sub-category")
        orders_enriched = enrich_order(orders_bronze, customers_enriched, products_enriched)
        print(f"  Enriched {orders_enriched.count():,} orders")
        
        # Validate enriched order schema
        order_cols = [field.name for field in ORDER_ENRICHED.fields]
        validate_schema(orders_enriched, order_cols, "silver.orders_enriched")
        print("Schema validated against ORDER_ENRICHED")
        
        # Check referential integrity
        print("\n  Validating referential integrity...")
        check_referential_integrity(
            orders_enriched, customers_enriched,
            "customer_id", "customer_id",
            "orders->customers"
        )
        print("Orders reference valid customers")
        
        check_referential_integrity(
            orders_enriched, products_enriched,
            "product_id", "product_id",
            "orders->products"
        )
        print("Orders reference valid products")
        
        print("\nMERGING into Silver schema in Unity Catalog...")
        print("  Strategy: UPDATE when matched, INSERT when not matched")
        
        # Merge customers: Key = customer_id + customer_name
        merge_silver_customers(spark, customers_enriched, "customers_enriched")
        
        # Merge products: Key = product_id + product_name
        merge_silver_products(spark, products_enriched, "products_enriched")
        
        # Merge orders: Key = customer_id + order_id + product_id (partitioned by order_year, order_month)
        merge_silver_orders(spark, orders_enriched, "orders_enriched")
        
        print("\nSilver tables merged successfully")
        print("Unity Catalog: workspace.silver.*")
        print("Merge Keys:")
        print("  - customers: customer_id, customer_name")
        print("  - products: product_id, product_name")
        print("  - orders: customer_id, order_id, product_id")
        
        print("\nEnriched Orders Sample:")
        orders_enriched.select(
            "order_id", "order_date", "customer_name", "country",
            "category", "sub_category", "quantity", "price", "profit"
        ).show(10, truncate=False)
        
        print("\nProfit Statistics:")
        orders_enriched.selectExpr(
            "count(*) as total_orders",
            "round(sum(profit), 2) as total_profit",
            "round(min(profit), 2) as min_profit",
            "round(max(profit), 2) as max_profit"
        ).show(truncate=False)
        
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
